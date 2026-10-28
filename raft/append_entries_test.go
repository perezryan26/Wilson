package raft

import "testing"

// wantHeartbeats checks that rd holds exactly one empty AppendEntries from
// server 1, leader of term, for each other server of a cluster of servers 1
// to size.
func wantHeartbeats(t *testing.T, rd Ready, size int, term Term) {
	t.Helper()
	if len(rd.Messages) != size-1 {
		t.Fatalf("Ready has %d messages, want %d heartbeats", len(rd.Messages), size-1)
	}
	want := AppendEntriesRequest{Term: term, LeaderID: 1}
	for i, m := range rd.Messages {
		to := NodeID(i + 2)
		if m.From != 1 || m.To != to || m.AppendEntries == nil || *m.AppendEntries != want {
			t.Errorf("message %d from %d to %d is not heartbeat %+v to %d", i, m.From, m.To, want, to)
		}
	}
}

// Figure 2, leaders: upon election a leader sends an empty AppendEntries to
// each server, and repeats it every heartbeatTick ticks.
func TestLeaderSendsHeartbeats(t *testing.T) {
	n := newCandidate(t, 3)
	n.Step(voteResponse(2, 1, true))
	wantHeartbeats(t, n.Ready(), 3, 1)
	for range 3 {
		for range n.heartbeatTick - 1 {
			n.Tick()
		}
		if rd := n.Ready(); len(rd.Messages) != 0 {
			t.Fatalf("leader sent %d messages before its heartbeat interval elapsed", len(rd.Messages))
		}
		n.Tick()
		wantHeartbeats(t, n.Ready(), 3, 1)
	}
}

// §5.2: a follower that hears from its leader every heartbeat interval
// never starts an election, and it acknowledges each heartbeat.
func TestHeartbeatsSuppressElection(t *testing.T) {
	n := newTestNode(1, 3, 1)
	for tick := range 20 * n.electionTick {
		if tick%n.heartbeatTick == 0 {
			n.Step(heartbeat(2, 1))
			if got := appendReply(t, n.Ready(), 2); got != (AppendEntriesResponse{Term: 1, Success: true}) {
				t.Fatalf("tick %d: reply = %+v, want success in term 1", tick, got)
			}
		}
		n.Tick()
	}
	if st := n.Status(); st != (Status{Role: Follower, Term: 1, Leader: 2}) {
		t.Errorf("status = %+v, want a follower of server 2 in term 1", st)
	}
}

// Figure 2, candidates, and §5.2: a candidate that hears from a leader of
// its own term or a later one returns to follower. In its own term it keeps
// the vote it cast for itself, so its hard state does not change.
func TestCandidateStepsDownOnAppendEntriesFromLeader(t *testing.T) {
	tests := []struct {
		name string
		term Term
		hs   *HardState
	}{
		{"same term", 1, nil},
		{"later term", 2, &HardState{CurrentTerm: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newCandidate(t, 3)
			n.Step(heartbeat(2, tt.term))
			if st := n.Status(); st != (Status{Role: Follower, Term: tt.term, Leader: 2}) {
				t.Errorf("status = %+v, want a follower of server 2 in term %d", st, tt.term)
			}
			rd := n.Ready()
			if (rd.HardState == nil) != (tt.hs == nil) || (tt.hs != nil && *rd.HardState != *tt.hs) {
				t.Errorf("Ready hard state = %v, want %v", rd.HardState, tt.hs)
			}
			if got := appendReply(t, rd, 2); got != (AppendEntriesResponse{Term: tt.term, Success: true}) {
				t.Errorf("reply = %+v, want success in term %d", got, tt.term)
			}
		})
	}
}

// Figure 2, AppendEntries receiver step 1, and §5.2: a request from an
// earlier term is refused, with currentTerm in the reply so the stale
// leader can update itself, and a candidate stays a candidate.
func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	tests := []struct {
		name   string
		node   func(*testing.T) *Node
		status Status
	}{
		{"follower", func(*testing.T) *Node {
			return NewNode(testConfig(1, 3, 1), RestoredState{HardState: HardState{CurrentTerm: 2}})
		}, Status{Role: Follower, Term: 2}},
		{"candidate", func(t *testing.T) *Node {
			n := newCandidate(t, 3)
			tickUntilElection(t, n) // a second election, in term 2
			n.Ready()
			return n
		}, Status{Role: Candidate, Term: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.node(t)
			n.Step(heartbeat(2, 1))
			if st := n.Status(); st != tt.status {
				t.Errorf("status = %+v, want %+v", st, tt.status)
			}
			if got := appendReply(t, n.Ready(), 2); got != (AppendEntriesResponse{Term: 2}) {
				t.Errorf("reply = %+v, want a refusal in term 2", got)
			}
		})
	}
}

// A leader that hears from another leader of its own term proves Election
// Safety broken, and stops rather than act on it.
func TestTwoLeadersInOneTermPanics(t *testing.T) {
	n := newLeader(t, 3)
	defer func() {
		if recover() == nil {
			t.Error("a leader accepted a heartbeat from a rival leader of its own term")
		}
	}()
	n.Step(heartbeat(2, 1))
}
