package sim

import (
	"container/heap"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/perezryan26/wilson/internal/storage"
	"github.com/perezryan26/wilson/raft"
)

// Options describe a simulated cluster.
type Options struct {
	// Size is the number of servers, numbered 1 to Size.
	Size int
	// Seed seeds every random choice the simulator makes, and through them
	// every server's random source, so a seed replays a run exactly.
	Seed uint64
	// TraceMessages adds every message's delivery or loss to the trace,
	// which otherwise records boots, crashes, faults, and every change of
	// role, term, or leader.
	TraceMessages bool
}

// Cluster is a simulated raft cluster. It runs every server in one
// goroutine on a virtual clock, carries out each Ready as the runtime
// does, and checks the safety invariants after every event. A Cluster is
// not safe for concurrent use.
type Cluster struct {
	rng   *rand.Rand
	now   time.Duration
	queue queue
	seq   uint64 // events scheduled so far

	commands int // commands proposed so far, for naming the next

	nodes   []*node // nodes[i] is server i+1
	net     Network
	blocked map[link]bool // links cut by a partition or a cut

	leaders map[raft.Term]raft.NodeID // every leader seen, by term
	err     error                     // the first violation, which ends the run

	trace         strings.Builder
	traceMessages bool
}

// node is one simulated server: a core while it runs, and storage that
// survives its crashes.
type node struct {
	id      raft.NodeID
	core    *raft.Node // nil while the server is down
	storage storage.Memory
	period  time.Duration // this server's tick interval
	boots   int           // how many times the server has started
	status  raft.Status   // as of the last event, for the trace

	maxTerm raft.Term                 // highest currentTerm seen, across restarts
	votes   map[raft.Term]raft.NodeID // every vote the server saved, by term
}

// New returns a cluster of opts.Size servers, all up and connected, at
// virtual time 0.
func New(opts Options) *Cluster {
	c := &Cluster{
		rng:     rand.New(rand.NewPCG(opts.Seed, 0)),
		net:     DefaultNetwork(),
		blocked: map[link]bool{},
		leaders: map[raft.Term]raft.NodeID{},

		traceMessages: opts.TraceMessages,
	}
	_, tick := raft.DefaultConfig()
	for i := range opts.Size {
		// Servers' clocks drift apart: each ticks up to 20% fast or slow.
		perMille := time.Duration(c.rng.IntN(401) - 200)
		c.nodes = append(c.nodes, &node{
			id:     raft.NodeID(i + 1),
			period: tick * (1000 + perMille) / 1000,
			votes:  map[raft.Term]raft.NodeID{},
		})
	}
	for _, n := range c.nodes {
		c.boot(n)
	}
	return c
}

// Crash stops server id. It loses everything but its storage, and messages
// that arrive while it is down are lost.
func (c *Cluster) Crash(id raft.NodeID) {
	if n := c.node(id); n.core != nil {
		n.core = nil
		c.tracef("%s crash", name(id))
	}
}

// Propose hands cmd to server id, as a client would, and carries out the
// server's Ready. It returns the new entry's index and term, or
// raft.ErrNotLeader, or an error if the server is down.
func (c *Cluster) Propose(id raft.NodeID, cmd []byte) (raft.Index, raft.Term, error) {
	n := c.node(id)
	if n.core == nil {
		return 0, 0, fmt.Errorf("server %d is down", id)
	}
	index, term, err := n.core.Propose(cmd)
	if err != nil {
		c.tracef("%s propose %s: %v", name(id), cmd, err)
		return 0, 0, err
	}
	c.tracef("%s propose %s: index=%d term=%d", name(id), cmd, index, term)
	c.handleReady(n)
	c.observe()
	return index, term, nil
}

// Restart boots server id from its storage, if it is down.
func (c *Cluster) Restart(id raft.NodeID) {
	if n := c.node(id); n.core == nil {
		c.boot(n)
	}
}

// Trace returns the run's trace so far, one event per line.
func (c *Cluster) Trace() string { return c.trace.String() }

// Now returns the virtual time since the cluster started.
func (c *Cluster) Now() time.Duration { return c.now }

// Status returns server id's status, and false if it is down or not in
// the cluster.
func (c *Cluster) Status(id raft.NodeID) (raft.Status, bool) {
	if id < 1 || int(id) > len(c.nodes) || c.node(id).core == nil {
		return raft.Status{}, false
	}
	return c.node(id).core.Status(), true
}

// Leaders returns every leader seen so far, by term.
func (c *Cluster) Leaders() map[raft.Term]raft.NodeID { return maps.Clone(c.leaders) }

// AgreedLeader returns the server among ids that every running server among
// ids follows in one shared term, and that term, provided it is up and
// leading that term. Otherwise it returns None. With no ids, it asks every
// server.
func (c *Cluster) AgreedLeader(ids ...raft.NodeID) (raft.NodeID, raft.Term) {
	if len(ids) == 0 {
		ids = c.ids()
	}
	lead, term := c.commonLeader(ids)
	st, up := c.Status(lead)
	if !up || st.Role != raft.Leader || st.Term != term || !slices.Contains(ids, lead) {
		return raft.None, 0
	}
	return lead, term
}

// commonLeader returns the leader and term that every running server among
// ids names, or None if they name different ones or none.
func (c *Cluster) commonLeader(ids []raft.NodeID) (raft.NodeID, raft.Term) {
	var lead raft.NodeID
	var term raft.Term
	for _, id := range ids {
		st, up := c.Status(id)
		if !up {
			continue
		}
		if lead == raft.None {
			lead, term = st.Leader, st.Term
		}
		if st.Leader == raft.None || st.Leader != lead || st.Term != term {
			return raft.None, 0
		}
	}
	return lead, term
}

// RunFor runs the cluster for d of virtual time and returns the first
// violation, if there is one.
func (c *Cluster) RunFor(d time.Duration) error {
	end := c.now + d
	for c.err == nil {
		if !c.next(end) {
			c.now = end
			break
		}
	}
	return c.err
}

// RunUntil runs the cluster until done, asked after every event, reports
// true. If that takes longer than d of virtual time, the run fails with an
// error naming goal.
func (c *Cluster) RunUntil(d time.Duration, goal string, done func() bool) error {
	end := c.now + d
	for c.err == nil && !done() {
		if !c.next(end) {
			c.now = end
			c.fail(fmt.Errorf("timed out after %v waiting for %s", d, goal))
		}
	}
	return c.err
}

// next runs the earliest event if it is due by end, then checks the
// invariants, and reports whether there was such an event.
func (c *Cluster) next(end time.Duration) bool {
	if len(c.queue) == 0 || c.queue[0].at > end {
		return false
	}
	e := heap.Pop(&c.queue).(*event)
	c.now = e.at
	e.run()
	c.observe()
	return true
}

// schedule queues run to happen at virtual time at.
func (c *Cluster) schedule(at time.Duration, run func()) {
	c.seq++
	heap.Push(&c.queue, &event{at: at, key: c.rng.Uint64(), seq: c.seq, run: run})
}

// boot starts n's core from its storage, with its first tick at a random
// phase so that servers do not tick in step.
func (c *Cluster) boot(n *node) {
	restored, err := n.storage.Load()
	if err != nil {
		c.fail(fmt.Errorf("server %d: load: %w", n.id, err))
		return
	}
	cfg, _ := raft.DefaultConfig()
	cfg.ID = n.id
	cfg.Peers = c.ids()
	cfg.Rand = rand.New(rand.NewPCG(c.rng.Uint64(), c.rng.Uint64()))
	n.core = raft.NewNode(cfg, restored)
	n.boots++
	n.status = n.core.Status()
	c.tracef("%s boot term=%d votedFor=%s tick=%s", name(n.id), restored.HardState.CurrentTerm,
		name(restored.HardState.VotedFor), ms(n.period))
	c.tick(n, n.boots, c.now+c.randDuration(0, n.period))
}

// tick schedules n's next tick at virtual time at. A tick left over from an
// earlier boot does nothing, so a crash stops the server's clock.
func (c *Cluster) tick(n *node, boot int, at time.Duration) {
	c.schedule(at, func() {
		if n.core == nil || n.boots != boot {
			return
		}
		n.core.Tick()
		c.handleReady(n)
		c.tick(n, boot, at+n.period)
	})
}

// handleReady carries out n's Ready as the runtime does: it saves the hard
// state, then sends the messages.
func (c *Cluster) handleReady(n *node) {
	rd := n.core.Ready()
	if rd.HardState != nil {
		c.checkVote(n, *rd.HardState)
	}
	if err := n.storage.Save(rd); err != nil {
		c.fail(fmt.Errorf("server %d: save: %w", n.id, err))
		return
	}
	for _, m := range rd.Messages {
		c.send(m)
	}
}

// observe traces every change of role, term, or leader, and checks the
// invariants that must hold at every instant. It runs after every event.
func (c *Cluster) observe() {
	for _, n := range c.nodes {
		if n.core == nil {
			continue
		}
		st := n.core.Status()
		if st != n.status {
			c.tracef("%s %s", name(n.id), change(n.status, st))
			n.status = st
		}
		c.checkTerm(n, st)
		c.checkElectionSafety(n, st)
	}
}

// checkElectionSafety fails the run if two servers ever lead the same term
// (Figure 3), even at different times.
func (c *Cluster) checkElectionSafety(n *node, st raft.Status) {
	if st.Role != raft.Leader {
		return
	}
	if prev, ok := c.leaders[st.Term]; ok && prev != n.id {
		c.fail(fmt.Errorf("two leaders in term %d, servers %d and %d (Election Safety)", st.Term, prev, n.id))
		return
	}
	c.leaders[st.Term] = n.id
}

// checkTerm fails the run if n's currentTerm ever decreases, even across a
// restart (Figure 2: it increases monotonically).
func (c *Cluster) checkTerm(n *node, st raft.Status) {
	if st.Term < n.maxTerm {
		c.fail(fmt.Errorf("server %d: currentTerm fell from %d to %d", n.id, n.maxTerm, st.Term))
		return
	}
	n.maxTerm = st.Term
}

// checkVote fails the run if the hard state n is about to save votes for a
// second candidate in a term, even after a restart.
func (c *Cluster) checkVote(n *node, hs raft.HardState) {
	if hs.VotedFor == raft.None {
		return
	}
	if prev, ok := n.votes[hs.CurrentTerm]; ok && prev != hs.VotedFor {
		c.fail(fmt.Errorf("server %d voted for both %d and %d in term %d", n.id, prev, hs.VotedFor, hs.CurrentTerm))
		return
	}
	n.votes[hs.CurrentTerm] = hs.VotedFor
}

// fail records the first violation, which ends the run.
func (c *Cluster) fail(err error) {
	if c.err == nil {
		c.err = err
		c.tracef("FAILED: %v", err)
	}
}

func (c *Cluster) node(id raft.NodeID) *node { return c.nodes[id-1] }

func (c *Cluster) ids() []raft.NodeID {
	ids := make([]raft.NodeID, len(c.nodes))
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}
	return ids
}

// randDuration draws a duration uniformly from [lo, hi] in whole
// microseconds. Integer arithmetic keeps a seed's run identical on every
// platform.
func (c *Cluster) randDuration(lo, hi time.Duration) time.Duration {
	us := c.rng.Int64N(int64((hi-lo)/time.Microsecond) + 1)
	return lo + time.Duration(us)*time.Microsecond
}
