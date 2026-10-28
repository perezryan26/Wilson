package raft

import (
	"math/rand/v2"
	"testing"
)

// testConfig returns the default timing for server id of a cluster of
// servers 1 to size, with a random source seeded by seed and the ID.
func testConfig(id NodeID, size int, seed uint64) Config {
	cfg, _ := DefaultConfig()
	cfg.ID = id
	for p := range NodeID(size) {
		cfg.Peers = append(cfg.Peers, p+1)
	}
	cfg.Rand = rand.New(rand.NewPCG(seed, uint64(id)))
	return cfg
}

func newTestNode(id NodeID, size int, seed uint64) *Node {
	return NewNode(testConfig(id, size, seed), RestoredState{})
}

// tickUntilElection ticks n until it starts an election and returns how
// many ticks that took. It fails the test if no election starts within the
// longest possible timeout.
func tickUntilElection(t *testing.T, n *Node) int {
	t.Helper()
	term := n.Status().Term
	for ticks := 1; ticks <= 2*n.electionTick; ticks++ {
		n.Tick()
		if n.Status().Term > term {
			return ticks
		}
	}
	t.Fatalf("no election within %d ticks", 2*n.electionTick)
	return 0
}

// newCandidate returns server 1 of a cluster of servers 1 to size, as a
// candidate in term 1 with its Ready drained.
func newCandidate(t *testing.T, size int) *Node {
	t.Helper()
	n := newTestNode(1, size, 1)
	tickUntilElection(t, n)
	n.Ready()
	return n
}

// newLeader returns server 1 of a cluster of servers 1 to size, as the
// leader of term 1 with its Ready drained.
func newLeader(t *testing.T, size int) *Node {
	t.Helper()
	n := newCandidate(t, size)
	for id := NodeID(2); id <= NodeID(size) && n.Status().Role == Candidate; id++ {
		n.Step(voteResponse(id, 1, true))
	}
	if n.Status().Role != Leader {
		t.Fatal("a candidate with every vote did not become leader")
	}
	n.Ready()
	return n
}

// voteRequest returns a RequestVote to server 1 from candidate in term,
// whose log ends at lastIndex and lastTerm.
func voteRequest(candidate NodeID, term Term, lastIndex Index, lastTerm Term) Message {
	return Message{From: candidate, To: 1, RequestVote: &RequestVoteRequest{
		Term:         term,
		CandidateID:  candidate,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}}
}

// voteResponse returns voter's answer to server 1's RequestVote.
func voteResponse(voter NodeID, term Term, granted bool) Message {
	return Message{From: voter, To: 1, RequestVoteResponse: &RequestVoteResponse{
		Term:        term,
		VoteGranted: granted,
	}}
}

// voteReply returns the one message in rd, which must answer a RequestVote
// from candidate.
func voteReply(t *testing.T, rd Ready, candidate NodeID) RequestVoteResponse {
	t.Helper()
	if len(rd.Messages) != 1 {
		t.Fatalf("Ready has %d messages, want one RequestVote reply", len(rd.Messages))
	}
	m := rd.Messages[0]
	if m.From != 1 || m.To != candidate || m.RequestVoteResponse == nil {
		t.Fatalf("Ready message from %d to %d is not a RequestVote reply from 1 to %d", m.From, m.To, candidate)
	}
	return *m.RequestVoteResponse
}

// heartbeat returns an AppendEntries with no entries to server 1 from leader
// in term.
func heartbeat(leader NodeID, term Term) Message {
	return Message{From: leader, To: 1, AppendEntries: &AppendEntriesRequest{
		Term:     term,
		LeaderID: leader,
	}}
}

// appendReply returns the one message in rd, which must answer an
// AppendEntries from leader.
func appendReply(t *testing.T, rd Ready, leader NodeID) AppendEntriesResponse {
	t.Helper()
	if len(rd.Messages) != 1 {
		t.Fatalf("Ready has %d messages, want one AppendEntries reply", len(rd.Messages))
	}
	m := rd.Messages[0]
	if m.From != 1 || m.To != leader || m.AppendEntriesResponse == nil {
		t.Fatalf("Ready message from %d to %d is not an AppendEntries reply from 1 to %d", m.From, m.To, leader)
	}
	return *m.AppendEntriesResponse
}

func wantHardState(t *testing.T, rd Ready, want HardState) {
	t.Helper()
	if rd.HardState == nil || *rd.HardState != want {
		t.Errorf("Ready hard state = %v, want %+v", rd.HardState, want)
	}
}

func TestNewNodeRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"ID not a peer", func(c *Config) { c.ID = 4 }},
		{"None as a peer", func(c *Config) { c.Peers = []NodeID{0, 1, 2} }},
		{"duplicate peer", func(c *Config) { c.Peers = []NodeID{1, 2, 2} }},
		{"no heartbeat interval", func(c *Config) { c.HeartbeatTick = 0 }},
		{"heartbeat as long as the election timeout", func(c *Config) { c.HeartbeatTick = c.ElectionTick }},
		{"nil Rand", func(c *Config) { c.Rand = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(1, 3, 1)
			tt.edit(&cfg)
			defer func() {
				if recover() == nil {
					t.Error("NewNode accepted the config")
				}
			}()
			NewNode(cfg, RestoredState{})
		})
	}
}

// A restarted server resumes as a follower in the term and with the vote it
// persisted, does not ask the runtime to persist them again, and does not
// vote a second time in that term.
func TestNewNodeRestoresHardState(t *testing.T) {
	hs := HardState{CurrentTerm: 5, VotedFor: 2}
	n := NewNode(testConfig(1, 3, 1), RestoredState{HardState: hs})
	if st := n.Status(); st != (Status{Role: Follower, Term: 5}) {
		t.Errorf("status = %+v, want a follower in term 5", st)
	}
	if rd := n.Ready(); rd.HardState != nil {
		t.Errorf("Ready hard state = %+v, want nil: the restored state is already durable", *rd.HardState)
	}
	n.Step(voteRequest(3, 5, 0, 0))
	if got := voteReply(t, n.Ready(), 3); got.VoteGranted {
		t.Error("granted a second vote in term 5 after a restart")
	}
}
