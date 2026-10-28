package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// Config holds what NewNode needs to know about the server and its cluster.
type Config struct {
	// ID identifies this server. It must be one of Peers.
	ID NodeID
	// Peers lists every server in the cluster, this one included.
	Peers []NodeID
	// ElectionTick is T, the shortest election timeout in ticks. Each
	// election draws its timeout from [T, 2T) (§5.2).
	ElectionTick int
	// HeartbeatTick is the number of ticks between a leader's heartbeats.
	// It must be below ElectionTick, so followers hear from a healthy
	// leader before they time out (§5.6).
	HeartbeatTick int
	// Rand draws election timeouts. Seeding it makes a run reproducible.
	Rand *rand.Rand
}

// DefaultConfig returns the timing for a cluster on one local network: a
// tick every 10 ms, a heartbeat every 5 ticks (50 ms), and election
// timeouts drawn from [300 ms, 600 ms). The runtime calls Tick once per
// returned interval, and the caller fills in ID, Peers, and Rand.
func DefaultConfig() (Config, time.Duration) {
	return Config{ElectionTick: 30, HeartbeatTick: 5}, 10 * time.Millisecond
}

// validate reports the first way c would break the algorithm. A server that
// misjudges the membership can count a majority that is not one, and a
// heartbeat interval at or above the election timeout lets followers time
// out under a healthy leader.
func (c Config) validate() error {
	peers := slices.Sorted(slices.Values(c.Peers))
	switch {
	case !slices.Contains(peers, c.ID):
		return fmt.Errorf("ID %d is not one of the peers %v", c.ID, c.Peers)
	case slices.Contains(peers, None):
		return fmt.Errorf("peers %v include None (0), which names no server", c.Peers)
	case len(slices.Compact(peers)) < len(c.Peers):
		return fmt.Errorf("peers %v include a duplicate", c.Peers)
	case c.HeartbeatTick < 1 || c.HeartbeatTick >= c.ElectionTick:
		return fmt.Errorf("need 1 <= HeartbeatTick < ElectionTick, have %d and %d",
			c.HeartbeatTick, c.ElectionTick)
	case c.Rand == nil:
		return errors.New("nil Rand")
	}
	return nil
}

// RestoredState is what a server's storage held when the server last
// stopped. It is the zero value for a server that has never run.
type RestoredState struct {
	HardState HardState
	Entries   []Entry // the log, in index order
}

// Role is a server's state in §5.1: follower, candidate, or leader.
type Role uint8

const (
	// Follower answers candidates and leaders and issues no requests of
	// its own until its election timeout elapses.
	Follower Role = iota
	// Candidate is running an election for its current term (§5.2).
	Candidate
	// Leader won its current term's election.
	Leader
)

// String returns the role's name in lower case, as traces print it.
func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("Role(%d)", r)
}

// Status is a read-only snapshot of a node's state, for tests, traces, and
// log lines.
type Status struct {
	Role   Role
	Term   Term   // currentTerm
	Leader NodeID // the leader of Term as far as this node knows, or None
}

// Node is one Raft server as a deterministic state machine: it reads no
// clock and does no I/O. A runtime drives it from a single goroutine and
// carries out the effects that Ready returns. It is not safe for
// concurrent use.
type Node struct {
	id            NodeID
	peers         []NodeID // every server, this one included, sorted
	electionTick  int
	heartbeatTick int
	rand          *rand.Rand

	// Persistent state on all servers (Figure 2).
	currentTerm Term
	votedFor    NodeID
	log         []Entry

	role   Role
	leader NodeID          // see Status.Leader
	votes  map[NodeID]bool // a candidate's granted votes this term, itself included

	electionElapsed  int // ticks since the election timer was last reset
	electionTimeout  int // drawn from [electionTick, 2*electionTick)
	heartbeatElapsed int // a leader's ticks since its last heartbeat

	msgs          []Message // messages for the next Ready
	prevHardState HardState // the hard state as of the last Ready
}

// NewNode returns a server that starts as a follower (§5.2) with the state
// its storage restored. It panics if cfg is invalid, because a server that
// misjudges its cluster can help elect two leaders in one term.
func NewNode(cfg Config, restored RestoredState) *Node {
	if err := cfg.validate(); err != nil {
		panic("raft: invalid Config: " + err.Error())
	}
	hs := restored.HardState
	n := &Node{
		id:            cfg.ID,
		peers:         slices.Sorted(slices.Values(cfg.Peers)),
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		rand:          cfg.Rand,
		currentTerm:   hs.CurrentTerm,
		votedFor:      hs.VotedFor,
		log:           slices.Clone(restored.Entries),
		prevHardState: hs,
	}
	n.becomeFollower(hs.CurrentTerm, None)
	n.resetElectionTimer()
	return n
}

// Tick advances the node's logical clock by one tick. A follower or
// candidate whose election timeout elapses starts an election, and a leader
// sends a heartbeat every heartbeatTick ticks (Figure 2).
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.broadcastHeartbeat()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
	}
}

// Step handles one inbound message: an RPC request or a response to one.
func (n *Node) Step(m Message) {
	// Figure 2, all servers: a request or response from a later term moves
	// this server to that term as a follower (§5.1).
	if term := m.term(); term > n.currentTerm {
		n.becomeFollower(term, None)
	}
	switch {
	case m.RequestVote != nil:
		n.handleRequestVote(m.RequestVote)
	case m.RequestVoteResponse != nil:
		n.handleRequestVoteResponse(m.From, m.RequestVoteResponse)
	case m.AppendEntries != nil:
		n.handleAppendEntries(m.AppendEntries)
	}
	// A heartbeat's response matters only for its term, handled above.
}

// Ready returns the effects produced since the previous call and clears
// them. The runtime handles each Ready completely, persisting before it
// sends, before it calls the node again.
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs}
	n.msgs = nil
	if hs := (HardState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}); hs != n.prevHardState {
		rd.HardState = &hs
		n.prevHardState = hs
	}
	return rd
}

// Status returns the node's role, term, and leader.
func (n *Node) Status() Status {
	return Status{Role: n.role, Term: n.currentTerm, Leader: n.leader}
}

// send queues m for the next Ready.
func (n *Node) send(m Message) {
	m.From = n.id
	n.msgs = append(n.msgs, m)
}

// becomeFollower makes the node a follower in term, following leader, which
// is None if unknown. A later term clears votedFor, because the node has not
// voted in that term yet.
func (n *Node) becomeFollower(term Term, leader NodeID) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = None
	}
	n.role = Follower
	n.leader = leader
}

// becomeCandidate makes the node a candidate for the next term: it
// increments currentTerm, votes for itself, and resets the election timer
// (Figure 2, candidates).
func (n *Node) becomeCandidate() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leader = None
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionTimer()
}

// becomeLeader makes the node the leader of its current term and sends the
// initial heartbeat that establishes its authority (Figure 2, leaders).
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.broadcastHeartbeat()
}
