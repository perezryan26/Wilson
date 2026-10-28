package raft

import "testing"

// Figure 2, followers: an election timeout with no word from a leader and
// no vote granted turns a follower into a candidate, which increments
// currentTerm and votes for itself.
func TestFollowerTimeoutStartsElection(t *testing.T) {
	n := newTestNode(1, 3, 1)
	tickUntilElection(t, n)
	if st := n.Status(); st != (Status{Role: Candidate, Term: 1}) {
		t.Errorf("status = %+v, want a candidate in term 1", st)
	}
	wantHardState(t, n.Ready(), HardState{CurrentTerm: 1, VotedFor: 1})
}

// §5.2: every election timeout is drawn from [T, 2T), the first one in
// NewNode and a fresh one each time an election starts, and the draws
// cover the whole range.
func TestElectionTimeoutInRange(t *testing.T) {
	seen := map[int]bool{}
	var tick int
	for seed := range uint64(100) {
		n := newTestNode(1, 3, seed)
		tick = n.electionTick
		for range 2 {
			ticks := tickUntilElection(t, n)
			if ticks < tick || ticks >= 2*tick {
				t.Fatalf("seed %d: election after %d ticks, want [%d, %d)", seed, ticks, tick, 2*tick)
			}
			seen[ticks] = true
		}
	}
	for ticks := tick; ticks < 2*tick; ticks++ {
		if !seen[ticks] {
			t.Errorf("no election timeout of %d ticks in 200 draws from [%d, %d)", ticks, tick, 2*tick)
		}
	}
}

// Figure 2, candidates: a candidate whose election timeout elapses without
// a winner, as after a split vote, starts a new election in the next term.
func TestSplitVoteStartsNewElection(t *testing.T) {
	n := newTestNode(1, 3, 1)
	tickUntilElection(t, n)
	n.Ready()
	tickUntilElection(t, n)
	if st := n.Status(); st != (Status{Role: Candidate, Term: 2}) {
		t.Errorf("status = %+v, want a candidate in term 2", st)
	}
	wantHardState(t, n.Ready(), HardState{CurrentTerm: 2, VotedFor: 1})
}

// A one-server cluster is its own majority, so its first election makes it
// leader.
func TestSingleServerElectsItself(t *testing.T) {
	n := newTestNode(1, 1, 1)
	tickUntilElection(t, n)
	if st := n.Status(); st != (Status{Role: Leader, Term: 1, Leader: 1}) {
		t.Errorf("status = %+v, want the leader of term 1", st)
	}
}
