package kvtest

import (
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/kv"
	"github.com/perezryan26/wilson/internal/kvserver"
	"github.com/perezryan26/wilson/internal/storage"
	"github.com/perezryan26/wilson/node"
	"github.com/perezryan26/wilson/raft"
)

// Options describe a cluster.
type Options struct {
	// Size is the number of servers, numbered 1 to Size.
	Size int
	// ReadThroughLog makes servers serve a Get through the log instead of
	// through ReadIndex.
	ReadThroughLog bool
	// MaxSessions is how many sessions each store keeps. Zero means
	// kv.DefaultMaxSessions.
	MaxSessions int
	// Timeout is each server's kvserver.Options.Timeout. Zero means one
	// second.
	Timeout time.Duration
}

// Cluster is a cluster of key-value servers in one process.
type Cluster struct {
	t     *testing.T
	opts  Options
	addrs map[raft.NodeID]string

	mu       sync.Mutex
	nodes    map[raft.NodeID]*node.Node // the running nodes
	stores   map[raft.NodeID]*storage.Memory
	handlers map[raft.NodeID]http.Handler // each running server's API
	cut      map[raft.NodeID]bool         // servers cut off from the others
}

// Start starts a cluster. The test's cleanup stops it.
func Start(t *testing.T, opts Options) *Cluster {
	t.Helper()
	if opts.MaxSessions == 0 {
		opts.MaxSessions = kv.DefaultMaxSessions
	}
	if opts.Timeout == 0 {
		opts.Timeout = time.Second
	}
	c := &Cluster{
		t: t, opts: opts,
		addrs:    map[raft.NodeID]string{},
		nodes:    map[raft.NodeID]*node.Node{},
		stores:   map[raft.NodeID]*storage.Memory{},
		handlers: map[raft.NodeID]http.Handler{},
		cut:      map[raft.NodeID]bool{},
	}
	for id := range raft.NodeID(opts.Size) {
		srv := httptest.NewServer(c.api(id + 1))
		t.Cleanup(srv.Close)
		c.addrs[id+1] = strings.TrimPrefix(srv.URL, "http://")
		c.stores[id+1] = &storage.Memory{}
	}
	t.Cleanup(func() {
		for id := range raft.NodeID(opts.Size) {
			c.Stop(id + 1)
		}
	})
	for id := range raft.NodeID(opts.Size) {
		c.Restart(id + 1)
	}
	return c
}

// api returns server id's HTTP handler. While the server is stopped it
// drops every connection without an answer, as a crashed server would.
func (c *Cluster) api(id raft.NodeID) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		h := c.handlers[id]
		c.mu.Unlock()
		if h == nil {
			panic(http.ErrAbortHandler)
		}
		h.ServeHTTP(w, r)
	}
}

// Addrs returns every server's address, in order of ID.
func (c *Cluster) Addrs() []string {
	addrs := make([]string, len(c.addrs))
	for id, addr := range c.addrs {
		addrs[id-1] = addr
	}
	return addrs
}

// Addr returns server id's address.
func (c *Cluster) Addr(id raft.NodeID) string { return c.addrs[id] }

// Restart starts server id from its storage, with an empty store, if it
// is stopped.
func (c *Cluster) Restart(id raft.NodeID) {
	c.t.Helper()
	if c.Node(id) != nil {
		return
	}
	cfg, _ := raft.DefaultConfig()
	cfg.ID, cfg.Rand = id, rand.New(rand.NewPCG(uint64(id), rand.Uint64()))
	for p := range raft.NodeID(c.opts.Size) {
		cfg.Peers = append(cfg.Peers, p+1)
	}
	sm := kvserver.NewStateMachine(kv.NewStore(c.opts.MaxSessions))
	n, err := node.Start(node.Config{Raft: cfg, Tick: time.Millisecond}, c.stores[id], network{c}, sm)
	if err != nil {
		c.t.Fatal(err)
	}
	api := kvserver.New(n, sm, kvserver.Options{
		ID: id, Peers: c.addrs, ReadThroughLog: c.opts.ReadThroughLog, Timeout: c.opts.Timeout, MaxValue: cfg.MaxAppendSize,
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[id], c.handlers[id] = n, api.Handler()
}

// Stop stops server id, if it runs, as a crash would: it loses all but
// its storage, and it drops connections and messages until it restarts.
func (c *Cluster) Stop(id raft.NodeID) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	delete(c.handlers, id)
	c.mu.Unlock()
	if n == nil {
		return
	}
	if err := n.Stop(); err != nil {
		c.t.Errorf("server %d: %v", id, err)
	}
}

// Cut cuts server id off from the other servers, or reconnects it.
// Clients can still reach it.
func (c *Cluster) Cut(id raft.NodeID, cut bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cut[id] = cut
}

// Node returns server id's node, or nil if it is stopped.
func (c *Cluster) Node(id raft.NodeID) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Leader waits until a running server leads and every other running
// server not cut off follows it in its term, and returns it.
func (c *Cluster) Leader() raft.NodeID {
	c.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if lead := c.agreedLeader(); lead != raft.None {
			return lead
		}
	}
	c.t.Fatal("no agreed leader within ten seconds")
	return raft.None
}

func (c *Cluster) agreedLeader() raft.NodeID {
	var lead raft.NodeID
	var term raft.Term
	for id := range raft.NodeID(c.opts.Size) {
		c.mu.Lock()
		n, cut := c.nodes[id+1], c.cut[id+1]
		c.mu.Unlock()
		if n == nil || cut {
			continue
		}
		st := n.Status()
		if lead == raft.None {
			lead, term = st.Leader, st.Term
		}
		if st.Leader == raft.None || st.Leader != lead || st.Term != term {
			return raft.None
		}
	}
	if n := c.Node(lead); n == nil || n.Status().Role != raft.Leader {
		return raft.None
	}
	return lead
}

// network is the cluster's transport: it delivers each message at once,
// unless its sender or receiver is cut off or stopped.
type network struct{ c *Cluster }

func (nw network) Send(m raft.Message) {
	nw.c.mu.Lock()
	n, cut := nw.c.nodes[m.To], nw.c.cut[m.From] || nw.c.cut[m.To]
	nw.c.mu.Unlock()
	if n != nil && !cut {
		n.Step(m)
	}
}
