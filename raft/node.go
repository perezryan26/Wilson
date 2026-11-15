package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// ErrNotLeader is the error Propose returns on a server that is not the
// leader. The caller should retry at the leader, which Status names when
// this server knows it.
var ErrNotLeader = errors.New("raft: not the leader")

// ErrProposalTooLarge is the error Propose returns for a command whose
// entry alone would exceed Config.MaxAppendSize. No AppendEntries within
// the cap could carry it, so no follower could ever store it.
var ErrProposalTooLarge = errors.New("raft: proposal too large")

// Config holds what NewNode needs to know about the server and its cluster.
type Config struct {
	// ID identifies this server. It must be one of Peers.
	ID NodeID
	// Peers lists every server in the cluster, this one included.
	Peers []NodeID
	// ElectionTick is T, the shortest election timeout in ticks. Each
	// election draws its timeout from [T, 2T) (§5.2).
	ElectionTick int
	// HeartbeatTick is the number of ticks between a leader's heartbeats,
	// and between a candidate's requests to servers that have not answered.
	// It must be below ElectionTick, so followers hear from a healthy
	// leader before they time out (§5.6).
	HeartbeatTick int
	// Rand draws election timeouts. Seeding it makes a run reproducible.
	Rand *rand.Rand
	// SnapshotChunkSize is the most snapshot bytes one InstallSnapshot
	// carries (Figure 13's chunks). Zero sends the whole snapshot in one
	// message.
	SnapshotChunkSize int
	// MaxAppendSize caps the entries one AppendEntries carries, so that a
	// transport that limits the size of a message can carry every one. An
	// entry counts as the length of its command plus a fixed allowance
	// for its index and term. A message holds as many entries as fit,
	// but always at least one, so Propose refuses a command whose entry
	// alone would exceed the cap. Zero means no cap.
	MaxAppendSize int
}

// DefaultConfig returns the settings for a cluster on one local network:
// a tick every 10 ms, a heartbeat every 5 ticks (50 ms), election
// timeouts drawn from [300 ms, 600 ms), and snapshots and entries sent
// in messages of at most 1 MiB. The runtime calls Tick once per returned
// interval, and the caller fills in ID, Peers, and Rand.
func DefaultConfig() (Config, time.Duration) {
	return Config{ElectionTick: 30, HeartbeatTick: 5, SnapshotChunkSize: 1 << 20, MaxAppendSize: 1 << 20}, 10 * time.Millisecond
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
	case c.SnapshotChunkSize < 0:
		return fmt.Errorf("negative SnapshotChunkSize %d", c.SnapshotChunkSize)
	case c.MaxAppendSize < 0:
		return fmt.Errorf("negative MaxAppendSize %d", c.MaxAppendSize)
	}
	return nil
}

// RestoredState is what a server's storage held when the server last
// stopped. It is the zero value for a server that has never run.
type RestoredState struct {
	HardState HardState
	Snapshot  *Snapshot // the latest snapshot, or nil
	Entries   []Entry   // the log after the snapshot, in index order
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
	Role        Role
	Term        Term   // currentTerm
	Leader      NodeID // the leader of Term as far as this node knows, or None
	CommitIndex Index  // highest log index known to be committed
	LastApplied Index  // highest log index released through Ready to be applied
}

// Node is one Raft server as a deterministic state machine: it reads no
// clock and does no I/O. A runtime drives it from a single goroutine and
// carries out the effects that Ready returns. It is not safe for
// concurrent use.
type Node struct {
	id                NodeID
	peers             []NodeID // every server, this one included, sorted
	electionTick      int
	heartbeatTick     int
	rand              *rand.Rand
	snapshotChunkSize int
	maxAppendSize     int

	// Persistent state on all servers (Figure 2), and the snapshot that
	// replaced the log's earlier entries (§7), which a leader sends to
	// followers that need them.
	currentTerm Term
	votedFor    NodeID
	log         raftLog
	snapshot    *Snapshot

	// Volatile, as in Figure 2: a restarted server relearns commitIndex
	// from the leader and applies its log again from its snapshot on.
	commitIndex Index
	lastApplied Index

	// Volatile state on leaders, reinitialized after each election.
	nextIndex  map[NodeID]Index     // the next log index to send each server
	matchIndex map[NodeID]Index     // the highest index known replicated on each server
	transfers  map[NodeID]*transfer // the snapshot each server is being sent, if any

	// The snapshot a follower is receiving in chunks (Figure 13, steps
	// 2–4): the bytes so far, and the term of the leader sending them.
	partial     *Snapshot
	partialTerm Term

	role   Role
	leader NodeID          // see Status.Leader
	votes  map[NodeID]bool // a candidate's answers this term by voter, true if granted; its own included

	electionElapsed  int // ticks since the election timer was last reset
	electionTimeout  int // drawn from [electionTick, 2*electionTick)
	heartbeatElapsed int // ticks since a leader's last heartbeat or a candidate's last requests

	msgs             []Message // messages for the next Ready
	prevHardState    HardState // the hard state as of the last Ready
	unstable         Index     // the first log index not yet handed to storage
	snapshotUnstable bool      // the snapshot is not yet handed to storage
}

// NewNode returns a server that starts as a follower (§5.2) with the state
// its storage restored. The runtime has already restored the state
// machine from the snapshot, if there is one, and the snapshot covers
// only committed entries, so both commitIndex and lastApplied start at
// its last included index. NewNode panics if cfg is invalid, because a
// server that misjudges its cluster can help elect two leaders in one
// term.
func NewNode(cfg Config, restored RestoredState) *Node {
	if err := cfg.validate(); err != nil {
		panic("raft: invalid Config: " + err.Error())
	}
	hs := restored.HardState
	n := &Node{
		id:                cfg.ID,
		peers:             slices.Sorted(slices.Values(cfg.Peers)),
		electionTick:      cfg.ElectionTick,
		heartbeatTick:     cfg.HeartbeatTick,
		rand:              cfg.Rand,
		snapshotChunkSize: cfg.SnapshotChunkSize,
		maxAppendSize:     cfg.MaxAppendSize,
		currentTerm:       hs.CurrentTerm,
		votedFor:          hs.VotedFor,
		log:               newLog(restored.Snapshot, restored.Entries),
		snapshot:          restored.Snapshot,
		prevHardState:     hs,
	}
	n.commitIndex = n.log.snapshotIndex()
	n.lastApplied = n.commitIndex
	n.unstable = n.log.lastIndex() + 1
	n.becomeFollower(hs.CurrentTerm, None)
	n.resetElectionTimer()
	return n
}

// Tick advances the node's logical clock by one tick. A follower or
// candidate whose election timeout elapses starts an election (Figure 2).
// Every heartbeatTick ticks, a leader sends a heartbeat, and a candidate
// asks again each server that has not answered it.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
		return
	}
	if n.role == Candidate {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.requestVotes()
		}
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
	case m.AppendEntriesResponse != nil:
		n.handleAppendEntriesResponse(m.From, m.AppendEntriesResponse)
	case m.InstallSnapshot != nil:
		n.handleInstallSnapshot(m.InstallSnapshot)
	case m.InstallSnapshotResponse != nil:
		n.handleInstallSnapshotResponse(m.From, m.InstallSnapshotResponse)
	}
}

// Propose appends cmd to the leader's log as a new entry of the current
// term and returns the entry's index and term (Figure 2, leaders). The
// entry counts as committed only once a majority stores it; until then a
// later leader may overwrite it. A command too large for MaxAppendSize is
// refused on every server.
func (n *Node) Propose(cmd []byte) (Index, Term, error) {
	if n.maxAppendSize > 0 && entrySize(cmd) > n.maxAppendSize {
		return 0, 0, ErrProposalTooLarge
	}
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.currentTerm, Data: cmd}
	n.log.append(e)
	n.broadcastAppend()
	n.maybeCommit()
	return e.Index, e.Term, nil
}

// Compact replaces the log's entries up to and including index with a
// snapshot of the state machine as of index, whose state data holds
// (§7). Each server compacts on its own, once its state machine has
// applied index, so index must lie past the current snapshot and at or
// before lastApplied. The next Ready hands the snapshot, and every entry
// after it, to storage. Compact keeps a copy of data.
func (n *Node) Compact(index Index, data []byte) error {
	if index <= n.log.snapshotIndex() || index > n.lastApplied {
		return fmt.Errorf("raft: cannot compact through index %d: entries %d to %d are applied and not yet compacted",
			index, n.log.snapshotIndex()+1, n.lastApplied)
	}
	term, _ := n.log.term(index)
	n.snapshot = &Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: bytes.Clone(data)}
	n.snapshotUnstable = true
	// The log holds entry index, so the entries after it stay (Figure 13,
	// step 6), and storage rewrites them after the snapshot.
	n.log.restore(n.snapshot)
	n.unstable = index + 1
	return nil
}

// Ready returns the effects produced since the previous call and clears
// them. The runtime handles each Ready completely, persisting before it
// sends, before it calls the node again.
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs}
	n.msgs = nil
	if n.snapshotUnstable {
		s := *n.snapshot
		rd.Snapshot = &s
		n.snapshotUnstable = false
	}
	if n.unstable <= n.log.lastIndex() {
		rd.Entries = n.log.from(n.unstable)
		n.unstable = n.log.lastIndex() + 1
	}
	if n.lastApplied < n.commitIndex {
		rd.CommittedEntries = n.log.between(n.lastApplied+1, n.commitIndex)
		n.lastApplied = n.commitIndex
	}
	if hs := (HardState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}); hs != n.prevHardState {
		rd.HardState = &hs
		n.prevHardState = hs
	}
	return rd
}

// Status returns the node's role, term, and leader.
func (n *Node) Status() Status {
	return Status{
		Role:        n.role,
		Term:        n.currentTerm,
		Leader:      n.leader,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
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

// becomeLeader makes the node the leader of its current term, resets its
// view of every follower's log, and sends the initial heartbeat that
// establishes its authority (Figure 2, leaders).
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.nextIndex = map[NodeID]Index{}
	n.matchIndex = map[NodeID]Index{}
	n.transfers = map[NodeID]*transfer{}
	for _, id := range n.peers {
		n.nextIndex[id] = n.log.lastIndex() + 1
	}
	n.broadcastAppend()
}
