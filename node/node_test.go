package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/leaktest"
	"github.com/perezryan26/wilson/internal/storage"
	"github.com/perezryan26/wilson/node"
	"github.com/perezryan26/wilson/raft"
)

// commands is a state machine that keeps the list of entries applied,
// one string per log index: the command, or noOp for an election no-op.
// The node's goroutine applies to it while tests read it.
type commands struct {
	mu       sync.Mutex
	list     []string
	restores int // how many times a snapshot replaced the list
}

// noOp stands for an election no-op in a commands list.
const noOp = "(no-op)"

func (c *commands) Apply(e raft.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.Type == raft.EntryNoOp {
		c.list = append(c.list, noOp)
		return
	}
	c.list = append(c.list, string(e.Data))
}

func (c *commands) Snapshot() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return json.Marshal(c.list)
}

func (c *commands) Restore(data []byte) error {
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list = list
	c.restores++
	return nil
}

func (c *commands) get() ([]string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.list), c.restores
}

// cluster runs nodes in one process, with storage in memory and a
// network that hands each message straight to its receiver's Step.
type cluster struct {
	t            *testing.T
	size         int
	compactEvery int

	mu     sync.Mutex
	nodes  map[raft.NodeID]*node.Node // the running nodes
	stores map[raft.NodeID]*storage.Memory
	sms    map[raft.NodeID]*commands
}

// newCluster starts a cluster of servers 1 to size, ticking every
// millisecond, so elections take tens of milliseconds. The test's
// cleanup stops every node.
func newCluster(t *testing.T, size, compactEvery int) *cluster {
	c := &cluster{
		t: t, size: size, compactEvery: compactEvery,
		nodes:  map[raft.NodeID]*node.Node{},
		stores: map[raft.NodeID]*storage.Memory{},
		sms:    map[raft.NodeID]*commands{},
	}
	for id := range raft.NodeID(size) {
		c.stores[id+1] = &storage.Memory{}
		c.start(id + 1)
	}
	t.Cleanup(func() {
		for id := range raft.NodeID(size) {
			c.stop(id + 1)
		}
	})
	return c
}

// start boots server id from its storage, with an empty state machine.
func (c *cluster) start(id raft.NodeID) {
	c.t.Helper()
	cfg, _ := raft.DefaultConfig()
	cfg.ID, cfg.Rand = id, rand.New(rand.NewPCG(uint64(id), uint64(time.Now().UnixNano())))
	for p := range raft.NodeID(c.size) {
		cfg.Peers = append(cfg.Peers, p+1)
	}
	sm := &commands{}
	n, err := node.Start(node.Config{Raft: cfg, Tick: time.Millisecond, CompactEvery: c.compactEvery}, c.stores[id], network{c}, sm)
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[id], c.sms[id] = n, sm
}

// stop stops server id, if it runs, as a crash would: messages to it are
// lost until it starts again.
func (c *cluster) stop(id raft.NodeID) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n == nil {
		return
	}
	if err := n.Stop(); err != nil {
		c.t.Errorf("server %d: %v", id, err)
	}
}

func (c *cluster) node(id raft.NodeID) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// network is the cluster's transport: it delivers each message at once.
type network struct{ c *cluster }

func (nw network) Send(m raft.Message) {
	if n := nw.c.node(m.To); n != nil {
		n.Step(m)
	}
}

// leader waits until a running server leads and every other running
// server follows it in its term, and returns it.
func (c *cluster) leader() raft.NodeID {
	c.t.Helper()
	var lead raft.NodeID
	waitFor(c.t, "an agreed leader", func() bool {
		lead = raft.None
		var term raft.Term
		for id := range raft.NodeID(c.size) {
			n := c.node(id + 1)
			if n == nil {
				continue
			}
			st := n.Status()
			if lead == raft.None {
				lead, term = st.Leader, st.Term
			}
			if st.Leader == raft.None || st.Leader != lead || st.Term != term {
				return false
			}
		}
		return lead != raft.None
	})
	return lead
}

// propose proposes cmd at whichever server leads and returns the entry's
// index, failing the test if no server accepts it within ten seconds.
func (c *cluster) propose(cmd string) raft.Index {
	c.t.Helper()
	index, err := c.tryPropose(cmd)
	if err != nil {
		c.t.Fatal(err)
	}
	return index
}

// tryPropose proposes cmd at whichever server leads, trying again until
// one accepts it, and returns the entry's index. It is safe to call from
// any goroutine.
func (c *cluster) tryPropose(cmd string) (raft.Index, error) {
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		for id := range raft.NodeID(c.size) {
			n := c.node(id + 1)
			if n == nil || n.Status().Role != raft.Leader {
				continue
			}
			index, _, err := n.Propose(context.Background(), []byte(cmd))
			if err == nil || !errors.Is(err, raft.ErrNotLeader) && !errors.Is(err, node.ErrStopped) {
				return index, err
			}
		}
	}
	return 0, fmt.Errorf("no server accepted %s within ten seconds", cmd)
}

// applied waits until every running server has applied index, and
// returns the commands their state machines applied through it, which
// must agree, without the election no-ops.
func (c *cluster) applied(index raft.Index) []string {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("every server to apply index %d", index), func() bool {
		for id := range raft.NodeID(c.size) {
			if n := c.node(id + 1); n != nil && n.Status().LastApplied < index {
				return false
			}
		}
		return true
	})
	var want []string
	for id := range raft.NodeID(c.size) {
		if c.node(id+1) == nil {
			continue
		}
		got, _ := c.sms[id+1].get()
		got = got[:index]
		if want == nil {
			want = got
		}
		if !slices.Equal(got, want) {
			c.t.Fatalf("server %d applied %v, another server %v", id+1, got, want)
		}
	}
	return slices.DeleteFunc(want, func(cmd string) bool { return cmd == noOp })
}

// waitFor fails the test unless done reports true within ten seconds,
// asking every millisecond.
func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !done(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// Three nodes elect a leader and apply, in the same order everywhere,
// commands that four goroutines propose at once.
func TestNodesReplicateParallelProposals(t *testing.T) {
	leaktest.Check(t)
	c := newCluster(t, 3, 0)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var last raft.Index
	var errs []error
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 25 {
				index, err := c.tryPropose(fmt.Sprintf("g%d-%d", g, i))
				mu.Lock()
				last = max(last, index)
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	applied := c.applied(last)
	for g := range 4 {
		for i := range 25 {
			if cmd := fmt.Sprintf("g%d-%d", g, i); !slices.Contains(applied, cmd) {
				t.Errorf("%s was never applied", cmd)
			}
		}
	}
}

// When the leader stops, the other two elect a new one and go on
// applying commands, and the old leader catches up when it restarts.
func TestNodesReplaceStoppedLeader(t *testing.T) {
	leaktest.Check(t)
	c := newCluster(t, 3, 0)
	c.propose("before")
	old := c.leader()
	c.stop(old)
	c.propose("after")
	if lead := c.leader(); lead == old {
		t.Fatalf("server %d still leads after it stopped", lead)
	}
	c.start(old)
	if got := c.applied(c.propose("restarted")); !slices.Equal(got, []string{"before", "after", "restarted"}) {
		t.Errorf("applied %v, want before, after, restarted", got)
	}
}

// A server that misses the entries its leader compacted away catches up
// through InstallSnapshot, restoring its state machine from the leader's
// snapshot (Figure 13, step 8), and restarts from its own snapshot.
func TestNodeCatchesUpThroughSnapshot(t *testing.T) {
	leaktest.Check(t)
	c := newCluster(t, 3, 4)
	c.propose("first")
	lagging := raft.NodeID(1)
	if c.leader() == lagging {
		lagging = 2
	}
	c.stop(lagging)
	var index raft.Index
	for i := range 20 {
		index = c.propose(fmt.Sprintf("c%d", i))
	}
	c.start(lagging)
	want := c.applied(index)
	if _, restores := c.sms[lagging].get(); restores == 0 {
		t.Error("the lagging server never restored the leader's snapshot")
	}
	c.stop(lagging)
	c.start(lagging)
	if got, restores := c.sms[lagging].get(); restores != 1 || len(got) < int(index)-4 {
		t.Errorf("after a restart: %d commands from %d restores, want the snapshot's, at least %d", len(got), restores, index-4)
	}
	if got := c.applied(index); !slices.Equal(got, want) {
		t.Errorf("after a restart, applied %v, want %v", got, want)
	}
}

// Stopping nodes while proposals and messages are in flight ends every
// goroutine, and every Propose returns.
func TestStopDuringTraffic(t *testing.T) {
	leaktest.Check(t)
	c := newCluster(t, 3, 2)
	c.leader()
	var wg sync.WaitGroup
	for g := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := c.node(raft.NodeID(g + 1))
			for i := 0; ; i++ {
				_, _, err := n.Propose(context.Background(), fmt.Appendf(nil, "g%d-%d", g, i))
				if errors.Is(err, node.ErrStopped) {
					return
				}
			}
		}()
	}
	waitFor(t, "twenty entries to commit", func() bool { return c.node(c.leader()).Status().CommitIndex >= 20 })
	for id := range raft.NodeID(3) {
		c.stop(id + 1)
	}
	wg.Wait()
}

// events records, in order, what a node did through its storage,
// transport, and state machine.
type events struct {
	mu   sync.Mutex
	list []string
	fail bool          // makes every Save fail
	hold chan struct{} // if not nil, a Save with a command waits until it is closed
}

func (ev *events) add(e string) {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	ev.list = append(ev.list, e)
}

func (ev *events) get() []string {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	return slices.Clone(ev.list)
}

// recorder is the storage, transport, and state machine of one node,
// recording each call in events.
type recorder struct {
	*events
	mem storage.Memory
}

var errDiskFull = errors.New("disk full")

func (r *recorder) Save(rd raft.Ready) error {
	if r.fail {
		r.add("save failed")
		return errDiskFull
	}
	if rd.HardState != nil {
		r.add(fmt.Sprintf("save term=%d votedFor=%d", rd.HardState.CurrentTerm, rd.HardState.VotedFor))
	}
	for _, e := range rd.Entries {
		r.add(fmt.Sprintf("save entry %d", e.Index))
	}
	if r.hold != nil && slices.ContainsFunc(rd.Entries, func(e raft.Entry) bool { return e.Type == raft.EntryNormal }) {
		<-r.hold
	}
	return r.mem.Save(rd)
}

func (r *recorder) Load() (raft.RestoredState, error) { return r.mem.Load() }

func (r *recorder) Send(m raft.Message) {
	switch {
	case m.RequestVoteResponse != nil:
		r.add(fmt.Sprintf("send vote granted=%t", m.RequestVoteResponse.VoteGranted))
	case m.AppendEntriesResponse != nil:
		r.add(fmt.Sprintf("send append success=%t", m.AppendEntriesResponse.Success))
	}
}

func (r *recorder) Apply(e raft.Entry)        { r.add(fmt.Sprintf("apply %d", e.Index)) }
func (r *recorder) Snapshot() ([]byte, error) { return nil, nil }
func (r *recorder) Restore([]byte) error      { return nil }

// startRecorded starts server 1 of a cluster of peers on a recorder. With
// a tick of an hour it never ticks, and the test plays its peers.
func startRecorded(t *testing.T, ev *events, peers []raft.NodeID, tick time.Duration) *node.Node {
	t.Helper()
	cfg, _ := raft.DefaultConfig()
	cfg.ID, cfg.Peers, cfg.Rand = 1, peers, rand.New(rand.NewPCG(1, 1))
	r := &recorder{events: ev}
	n, err := node.Start(node.Config{Raft: cfg, Tick: tick}, r, r, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop() })
	return n
}

// ADR-0001's order: a node saves a vote, and entries, before the reply
// that depends on them leaves, and applies an entry only after both, so
// a crash can never make it vote twice in a term or forget an entry it
// acknowledged.
func TestNodeSavesBeforeItSendsAndApplies(t *testing.T) {
	leaktest.Check(t)
	ev := &events{}
	n := startRecorded(t, ev, []raft.NodeID{1, 2, 3}, time.Hour)
	n.Step(raft.Message{From: 2, To: 1, RequestVote: &raft.RequestVoteRequest{Term: 1, CandidateID: 2}})
	n.Step(raft.Message{From: 2, To: 1, AppendEntries: &raft.AppendEntriesRequest{
		Term: 1, LeaderID: 2, Entries: []raft.Entry{{Index: 1, Term: 1, Data: []byte("x")}}, LeaderCommit: 1,
	}})
	want := []string{"save term=1 votedFor=2", "send vote granted=true", "save entry 1", "send append success=true", "apply 1"}
	waitFor(t, "the node to apply entry 1", func() bool { return len(ev.get()) >= len(want) })
	if got := ev.get(); !slices.Equal(got, want) {
		t.Errorf("the node did\n\t%s\nwant\n\t%s", strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
	}
}

// A failed save stops the node for good, with nothing sent or applied:
// after a failed write or fsync, what the disk holds is unknown. Err and
// Stop report why, and Propose refuses.
func TestNodeStopsForGoodOnStorageError(t *testing.T) {
	leaktest.Check(t)
	ev := &events{fail: true}
	n := startRecorded(t, ev, []raft.NodeID{1, 2, 3}, time.Hour)
	n.Step(raft.Message{From: 2, To: 1, RequestVote: &raft.RequestVoteRequest{Term: 1, CandidateID: 2}})
	select {
	case <-n.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the node kept running after a failed save")
	}
	if err := n.Err(); !errors.Is(err, errDiskFull) {
		t.Errorf("Err() = %v, want the save's error", err)
	}
	if err := n.Stop(); !errors.Is(err, errDiskFull) {
		t.Errorf("Stop() = %v, want the save's error", err)
	}
	if _, _, err := n.Propose(context.Background(), []byte("x")); !errors.Is(err, node.ErrStopped) {
		t.Errorf("Propose after the failure: error = %v, want ErrStopped", err)
	}
	n.Step(raft.Message{From: 2, To: 1, RequestVote: &raft.RequestVoteRequest{Term: 2, CandidateID: 2}})
	if got := ev.get(); !slices.Equal(got, []string{"save failed"}) {
		t.Errorf("the node did %v, want only the failed save", got)
	}
}

// Propose answers only once the new entry is saved, so a caller never
// learns of an entry this server could lose in a crash. The save of the
// entry, which follows the server's election no-op, is held until the
// test lets it finish.
func TestProposeReturnsOnceTheEntryIsSaved(t *testing.T) {
	leaktest.Check(t)
	ev := &events{hold: make(chan struct{})}
	n := startRecorded(t, ev, []raft.NodeID{1}, time.Millisecond)
	release := sync.OnceFunc(func() { close(ev.hold) })
	t.Cleanup(release)
	waitFor(t, "the only server to lead", func() bool { return n.Status().Role == raft.Leader })
	returned := make(chan error, 1)
	go func() {
		_, _, err := n.Propose(context.Background(), []byte("x"))
		returned <- err
	}()
	waitFor(t, "the entry's save to begin", func() bool { return slices.Contains(ev.get(), "save entry 2") })
	select {
	case err := <-returned:
		t.Fatalf("Propose returned %v while its entry was still being saved", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
}
