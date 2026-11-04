package raft

import (
	"slices"
	"testing"
)

// Figure 2, leaders: an entry of the current term commits once a majority
// stores it, the leader included: not on two servers of five, but on three.
// The leader's next AppendEntries tells the followers.
func TestLeaderCommitsOnMajority(t *testing.T) {
	n := newLeader(t, 5)
	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	n.Ready()
	for _, step := range []struct {
		from NodeID
		want Index
	}{{2, 0}, {3, 1}} {
		n.Step(appendResponse(step.from, 1, true, 1))
		if got := n.Status().CommitIndex; got != step.want {
			t.Fatalf("after server %d stored entry 1: commitIndex = %d, want %d", step.from, got, step.want)
		}
	}
	n.Ready()
	for range n.heartbeatTick {
		n.Tick()
	}
	for _, m := range n.Ready().Messages {
		if m.AppendEntries == nil || m.AppendEntries.LeaderCommit != 1 {
			t.Errorf("heartbeat to %d does not carry leaderCommit 1", m.To)
		}
	}
}

// A one-server cluster is its own majority, so each proposal commits at
// once.
func TestSingleServerCommitsItsProposals(t *testing.T) {
	n := newTestNode(1, 1, 1)
	tickUntilElection(t, n)
	index, _, err := n.Propose([]byte("x"))
	if err != nil || n.Status().CommitIndex != index {
		t.Errorf("after Propose: commitIndex %d, error %v; want %d and nil", n.Status().CommitIndex, err, index)
	}
}

// figure8Servers returns S1 to S5 of Figure 8 as they stand before (c):
// S1 led term 2 and copied its entry at index 2 to S2 only; S5 led term 3
// and wrote a different entry at index 2.
func figure8Servers() []*Node {
	restart := func(id NodeID, term Term, log ...Term) *Node {
		return NewNode(testConfig(id, 5, 1), RestoredState{HardState: HardState{CurrentTerm: term}, Entries: logOf(log...)})
	}
	return []*Node{restart(1, 3, 1, 2), restart(2, 3, 1, 2), restart(3, 3, 1), restart(4, 3, 1), restart(5, 4, 1, 3)}
}

// figure8c brings s to Figure 8(c): S1 wins term 4 with votes from S2 and
// S3 and copies its entry from term 2 to S3, so a majority stores it, while
// S4 and S5 hear nothing. S1 must not have committed anything.
func figure8c(t *testing.T, s []*Node) {
	t.Helper()
	tickUntilElection(t, s[0])
	exchange(t, s[0], s[1], s[2])
	if st := s[0].Status(); st.Role != Leader || st.Term != 4 {
		t.Fatalf("S1 status = %+v, want the leader of term 4", st)
	}
	if got := logTerms(s[2]); !slices.Equal(got, []Term{1, 2}) {
		t.Fatalf("S3 log terms = %v, want [1 2]", got)
	}
	if got := s[0].Status().CommitIndex; got != 0 {
		t.Fatalf("S1 committed through index %d by counting replicas of an entry from term 2", got)
	}
}

// Figure 8 and §5.4.2: a leader never commits an entry from an earlier
// term by counting replicas. In (c) S1 holds back, and rightly: in (d) S5
// can still win term 5 and overwrite that entry everywhere. Once an entry
// from term 4 is on a majority, as in (e), S1 commits it and everything
// before it, and S5 can no longer win.
func TestFigure8OldTermEntryNotCommittedByCounting(t *testing.T) {
	t.Run("d", func(t *testing.T) {
		s := figure8Servers()
		figure8c(t, s)
		// S1 crashes; S5 times out and campaigns with votes from S2, S3, S4.
		tickUntilElection(t, s[4])
		exchange(t, s[1], s[2], s[3], s[4])
		if st := s[4].Status(); st.Role != Leader || st.Term != 5 {
			t.Fatalf("S5 status = %+v, want the leader of term 5", st)
		}
		for i, n := range s[1:] {
			if got := logTerms(n); !slices.Equal(got, []Term{1, 3}) {
				t.Errorf("S%d log terms = %v, want [1 3]: S5's entry overwrites index 2", i+2, got)
			}
		}
	})
	t.Run("e", func(t *testing.T) {
		s := figure8Servers()
		figure8c(t, s)
		if _, _, err := s[0].Propose([]byte("x")); err != nil {
			t.Fatal(err)
		}
		exchange(t, s[0], s[1], s[2])
		if got := s[0].Status().CommitIndex; got != 3 {
			t.Fatalf("S1 commitIndex = %d after a majority stored its entry from term 4, want 3", got)
		}
		tickUntilElection(t, s[4])
		exchange(t, s[1], s[2], s[3], s[4])
		if st := s[4].Status(); st.Role == Leader {
			t.Errorf("S5 won term %d without the committed entries", st.Term)
		}
	})
}
