package raft_test

import (
	"math/rand/v2"
	"testing"

	"github.com/perezryan26/wilson/internal/storage"
	"github.com/perezryan26/wilson/raft"
)

// cluster runs raft servers synchronously in one goroutine, with no clock
// and no faults beyond crashes. Each round ticks every running server once,
// then delivers messages in the order they were sent until none are left.
// Each server's Ready is handled as the runtime handles it: hard state is
// saved before the messages go out.
type cluster struct {
	t       *testing.T
	seed    uint64
	ids     []raft.NodeID
	nodes   map[raft.NodeID]*raft.Node // running servers only
	storage map[raft.NodeID]*storage.Memory
	queue   []raft.Message
	leaders map[raft.Term]raft.NodeID // every leader seen, by term
}

func newCluster(t *testing.T, size int, seed uint64) *cluster {
	c := &cluster{
		t:       t,
		seed:    seed,
		nodes:   map[raft.NodeID]*raft.Node{},
		storage: map[raft.NodeID]*storage.Memory{},
		leaders: map[raft.Term]raft.NodeID{},
	}
	for id := range raft.NodeID(size) {
		c.ids = append(c.ids, id+1)
		c.storage[id+1] = &storage.Memory{}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

// start boots server id from whatever its storage holds.
func (c *cluster) start(id raft.NodeID) {
	cfg, _ := raft.DefaultConfig()
	cfg.ID = id
	cfg.Peers = c.ids
	cfg.Rand = rand.New(rand.NewPCG(c.seed, uint64(id)))
	restored, err := c.storage[id].Load()
	if err != nil {
		c.t.Fatalf("server %d: load: %v", id, err)
	}
	c.nodes[id] = raft.NewNode(cfg, restored)
}

// crash stops server id. It loses all but its storage, and messages to it
// are lost until it starts again.
func (c *cluster) crash(id raft.NodeID) {
	delete(c.nodes, id)
}

func (c *cluster) round() {
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			n.Tick()
			c.handleReady(id)
		}
	}
	for len(c.queue) > 0 {
		m := c.queue[0]
		c.queue = c.queue[1:]
		if n, ok := c.nodes[m.To]; ok {
			n.Step(m)
			c.handleReady(m.To)
		}
	}
}

// handleReady saves server id's hard state, queues its messages, and checks
// Election Safety (Figure 3): no two servers ever lead the same term.
func (c *cluster) handleReady(id raft.NodeID) {
	n := c.nodes[id]
	rd := n.Ready()
	if err := c.storage[id].Save(rd); err != nil {
		c.t.Fatalf("server %d: save: %v", id, err)
	}
	c.queue = append(c.queue, rd.Messages...)
	st := n.Status()
	if st.Role != raft.Leader {
		return
	}
	if prev, ok := c.leaders[st.Term]; ok && prev != id {
		c.t.Fatalf("seed %d: servers %d and %d both lead term %d", c.seed, prev, id, st.Term)
	}
	c.leaders[st.Term] = id
}

// agreedLeader returns the running server that every running server,
// itself included, names as the leader of one shared term, or None while
// they disagree.
func (c *cluster) agreedLeader() (raft.NodeID, raft.Term) {
	var lead raft.NodeID
	var term raft.Term
	for _, id := range c.ids {
		n, ok := c.nodes[id]
		if !ok {
			continue
		}
		st := n.Status()
		if lead == raft.None {
			lead, term = st.Leader, st.Term
		}
		if st.Leader == raft.None || st.Leader != lead || st.Term != term {
			return raft.None, 0
		}
	}
	if _, ok := c.nodes[lead]; !ok {
		return raft.None, 0
	}
	return lead, term
}

// waitForLeader runs rounds until the running servers agree on a leader,
// and fails the test if that takes more than ten of the longest election
// timeouts.
func (c *cluster) waitForLeader() (raft.NodeID, raft.Term) {
	c.t.Helper()
	cfg, _ := raft.DefaultConfig()
	for range 10 * 2 * cfg.ElectionTick {
		c.round()
		if lead, term := c.agreedLeader(); lead != raft.None {
			return lead, term
		}
	}
	c.t.Fatalf("seed %d: no agreed leader after ten election timeouts", c.seed)
	return raft.None, 0
}

// §5.2 and Figure 3: a cluster elects exactly one leader, every server
// learns of it, and heartbeats keep it in office while nothing fails.
func TestClusterElectsOneLeader(t *testing.T) {
	cfg, _ := raft.DefaultConfig()
	for seed := range uint64(1000) {
		c := newCluster(t, 3, seed)
		lead, term := c.waitForLeader()
		for range 10 * 2 * cfg.ElectionTick {
			c.round()
		}
		if gotLead, gotTerm := c.agreedLeader(); gotLead != lead || gotTerm != term {
			t.Fatalf("seed %d: leader %d of term %d became %d of term %d with no failure",
				seed, lead, term, gotLead, gotTerm)
		}
	}
}

// §5.2: when the leader crashes, the other two elect a new leader in a
// later term. The old leader restarts from its storage in the term it
// saved, then learns of the new leader and follows it.
func TestClusterReplacesFailedLeader(t *testing.T) {
	for seed := range uint64(1000) {
		c := newCluster(t, 3, seed)
		old, oldTerm := c.waitForLeader()
		c.crash(old)
		lead, term := c.waitForLeader()
		if lead == old || term <= oldTerm {
			t.Fatalf("seed %d: after leader %d of term %d crashed, got leader %d of term %d",
				seed, old, oldTerm, lead, term)
		}
		c.start(old)
		if got := c.nodes[old].Status().Term; got != oldTerm {
			t.Fatalf("seed %d: restarted leader resumed in term %d, want its saved term %d", seed, got, oldTerm)
		}
		if got, gotTerm := c.waitForLeader(); got != lead || gotTerm != term {
			t.Fatalf("seed %d: restarted server joined leader %d of term %d, want %d of term %d",
				seed, got, gotTerm, lead, term)
		}
	}
}
