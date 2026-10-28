package raft

import "testing"

// Figure 2, followers and candidates: an election timeout with no word from
// a leader and no vote granted turns a follower into a candidate, which
// increments currentTerm, votes for itself, and sends RequestVote, with its
// last log index and term (§5.4), to every other server.
func TestFollowerTimeoutStartsElection(t *testing.T) {
	n := NewNode(testConfig(1, 3, 1), RestoredState{
		HardState: HardState{CurrentTerm: 1},
		Entries:   []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
	})
	tickUntilElection(t, n)
	if st := n.Status(); st != (Status{Role: Candidate, Term: 2}) {
		t.Errorf("status = %+v, want a candidate in term 2", st)
	}
	rd := n.Ready()
	wantHardState(t, rd, HardState{CurrentTerm: 2, VotedFor: 1})
	wantVoteRequests(t, rd, 3, RequestVoteRequest{Term: 2, CandidateID: 1, LastLogIndex: 2, LastLogTerm: 1})
}

// wantVoteRequests checks that rd holds exactly one RequestVote, equal to
// want, for each server of a cluster of servers 1 to size but server 1.
func wantVoteRequests(t *testing.T, rd Ready, size int, want RequestVoteRequest) {
	t.Helper()
	if len(rd.Messages) != size-1 {
		t.Fatalf("Ready has %d messages, want %d RequestVotes", len(rd.Messages), size-1)
	}
	for i, m := range rd.Messages {
		to := NodeID(i + 2)
		if m.From != 1 || m.To != to || m.RequestVote == nil || *m.RequestVote != want {
			t.Errorf("message %d from %d to %d is not RequestVote %+v to %d", i, m.From, m.To, want, to)
		}
	}
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
	n := newCandidate(t, 3)
	// Server 2 voted for server 3, and server 3 for itself.
	n.Step(voteResponse(2, 1, false))
	if st := n.Status(); st != (Status{Role: Candidate, Term: 1}) {
		t.Fatalf("status = %+v, want a candidate in term 1", st)
	}
	tickUntilElection(t, n)
	if st := n.Status(); st != (Status{Role: Candidate, Term: 2}) {
		t.Errorf("status = %+v, want a candidate in term 2", st)
	}
	rd := n.Ready()
	wantHardState(t, rd, HardState{CurrentTerm: 2, VotedFor: 1})
	wantVoteRequests(t, rd, 3, RequestVoteRequest{Term: 2, CandidateID: 1})
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

// Figure 2, candidates: votes from a majority of the cluster, the
// candidate's own included, make it leader. Rejections and votes from an
// earlier election count for nothing.
func TestCandidateWinsWithMajority(t *testing.T) {
	n := newCandidate(t, 5)
	tickUntilElection(t, n) // a second election, in term 2
	n.Ready()
	steps := []struct {
		name string
		msg  Message
		role Role
	}{
		{"vote from the term 1 election", voteResponse(5, 1, true), Candidate},
		{"second vote of five", voteResponse(2, 2, true), Candidate},
		{"rejection", voteResponse(3, 2, false), Candidate},
		{"third vote of five", voteResponse(4, 2, true), Leader},
	}
	for _, step := range steps {
		n.Step(step.msg)
		if got := n.Status().Role; got != step.role {
			t.Fatalf("after the %s: role = %v, want %v", step.name, got, step.role)
		}
	}
	if st := n.Status(); st != (Status{Role: Leader, Term: 2, Leader: 1}) {
		t.Errorf("status = %+v, want the leader of term 2", st)
	}
}

// A duplicated vote must not count twice, or a minority could elect a
// leader.
func TestDuplicateVoteResponseCountsOnce(t *testing.T) {
	n := newCandidate(t, 5)
	n.Step(voteResponse(2, 1, true))
	n.Step(voteResponse(2, 1, true))
	if got := n.Status().Role; got != Candidate {
		t.Fatalf("role = %v after one vote delivered twice, want candidate", got)
	}
	n.Step(voteResponse(3, 1, true))
	if got := n.Status().Role; got != Leader {
		t.Errorf("role = %v after three distinct votes of five, want leader", got)
	}
}

// Figure 2, RequestVote receiver step 1: a request from an earlier term is
// refused, and the reply carries currentTerm so the candidate can update
// itself.
func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	n := NewNode(testConfig(1, 3, 1), RestoredState{HardState: HardState{CurrentTerm: 5}})
	n.Step(voteRequest(2, 4, 0, 0))
	rd := n.Ready()
	if got := voteReply(t, rd, 2); got != (RequestVoteResponse{Term: 5}) {
		t.Errorf("reply = %+v, want a refusal in term 5", got)
	}
	if rd.HardState != nil {
		t.Errorf("Ready hard state = %+v, want nil: a refusal changes nothing", *rd.HardState)
	}
}

// Figure 2, RequestVote receiver step 2: a server votes for at most one
// candidate per term, first come, first served, and answers a repeated
// request from that candidate with the same vote. A candidate has already
// voted for itself.
func TestOneVotePerTerm(t *testing.T) {
	n := newTestNode(1, 3, 1)
	for _, step := range []struct {
		candidate NodeID
		granted   bool
	}{{2, true}, {3, false}, {2, true}} {
		n.Step(voteRequest(step.candidate, 1, 0, 0))
		want := RequestVoteResponse{Term: 1, VoteGranted: step.granted}
		if got := voteReply(t, n.Ready(), step.candidate); got != want {
			t.Errorf("reply to %d = %+v, want %+v", step.candidate, got, want)
		}
	}

	c := newCandidate(t, 3)
	c.Step(voteRequest(2, 1, 0, 0))
	if got := voteReply(t, c.Ready(), 2); got.VoteGranted {
		t.Error("a candidate voted for a rival in its own term")
	}
}

// Figure 2, RequestVote receiver step 2 and §5.4.1: a vote goes only to a
// candidate whose log is at least as up-to-date as the voter's. The voter's
// log ends at index 2 in term 2.
func TestVoteRequiresUpToDateLog(t *testing.T) {
	tests := []struct {
		name      string
		lastIndex Index
		lastTerm  Term
		granted   bool
	}{
		{"later last term, shorter log", 1, 3, true},
		{"same last term, longer log", 3, 2, true},
		{"same last term, same length", 2, 2, true},
		{"same last term, shorter log", 1, 2, false},
		{"earlier last term, longer log", 5, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNode(testConfig(1, 3, 1), RestoredState{
				HardState: HardState{CurrentTerm: 2},
				Entries:   []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			})
			n.Step(voteRequest(2, 4, tt.lastIndex, tt.lastTerm))
			want := RequestVoteResponse{Term: 4, VoteGranted: tt.granted}
			if got := voteReply(t, n.Ready(), 2); got != want {
				t.Errorf("reply = %+v, want %+v", got, want)
			}
		})
	}
}

// The reply that grants a vote leaves in the same Ready as the hard state
// recording the vote, so the runtime makes the vote durable before it
// sends the reply (Figure 2: persistent state is updated on stable storage
// before responding to RPCs).
func TestVoteIsDurableBeforeReply(t *testing.T) {
	n := newTestNode(1, 3, 1)
	n.Step(voteRequest(2, 1, 0, 0))
	rd := n.Ready()
	wantHardState(t, rd, HardState{CurrentTerm: 1, VotedFor: 2})
	if got := voteReply(t, rd, 2); !got.VoteGranted {
		t.Errorf("reply = %+v, want a granted vote", got)
	}
}

// Figure 2, all servers: a request from a later term makes any server a
// follower in that term, with no vote cast in it yet.
func TestHigherTermInRequestStepsDown(t *testing.T) {
	tests := []struct {
		name string
		node func(*testing.T) *Node
	}{
		{"follower", func(*testing.T) *Node { return newTestNode(1, 3, 1) }},
		{"candidate", func(t *testing.T) *Node { return newCandidate(t, 3) }},
		{"leader", func(t *testing.T) *Node { return newLeader(t, 3) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.node(t)
			n.Step(voteRequest(3, 5, 0, 0))
			if st := n.Status(); st != (Status{Role: Follower, Term: 5}) {
				t.Errorf("status = %+v, want a follower in term 5", st)
			}
			// The new term cleared votedFor, so server 3 gets the vote.
			wantHardState(t, n.Ready(), HardState{CurrentTerm: 5, VotedFor: 3})
		})
	}
}

// Figure 2, all servers: a response from a later term makes a candidate or
// leader a follower in that term.
func TestHigherTermInResponseStepsDown(t *testing.T) {
	tests := []struct {
		name string
		node func(*testing.T) *Node
		msg  Message
	}{
		{"candidate", func(t *testing.T) *Node { return newCandidate(t, 3) }, voteResponse(2, 3, false)},
		{"leader", func(t *testing.T) *Node { return newLeader(t, 3) }, Message{
			From: 2, To: 1, AppendEntriesResponse: &AppendEntriesResponse{Term: 3},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.node(t)
			n.Step(tt.msg)
			if st := n.Status(); st != (Status{Role: Follower, Term: 3}) {
				t.Errorf("status = %+v, want a follower in term 3", st)
			}
			wantHardState(t, n.Ready(), HardState{CurrentTerm: 3})
		})
	}
}

// Figure 2, followers: granting a vote restarts the election timer, but a
// refused request does not, whatever the reason for refusing. The voter is
// in term 2, has voted for server 3, and its log ends at index 2 in term 2.
func TestElectionTimerReset(t *testing.T) {
	tests := []struct {
		name  string
		msg   Message
		reset bool
	}{
		{"granted vote", voteRequest(2, 3, 2, 2), true},
		{"refused: stale term", voteRequest(2, 1, 2, 2), false},
		{"refused: already voted", voteRequest(2, 2, 2, 2), false},
		{"refused: stale log in a later term", voteRequest(2, 3, 1, 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNode(testConfig(1, 3, 1), RestoredState{
				HardState: HardState{CurrentTerm: 2, VotedFor: 3},
				Entries:   []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			})
			for range n.electionTimeout - 1 {
				n.Tick()
			}
			n.Step(tt.msg)
			term := n.Status().Term
			n.Tick() // the last tick of the old timeout
			if started := n.Status().Term > term; started == tt.reset {
				t.Errorf("election started on the next tick: %v, want %v", started, !tt.reset)
			}
		})
	}
}
