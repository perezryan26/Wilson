package raft

import (
	"slices"
	"testing"
)

// logOf returns a log of one entry per term, from index 1.
func logOf(terms ...Term) []Entry { return entriesAt(1, terms...) }

// entriesAt returns one entry per term, from index start.
func entriesAt(start Index, terms ...Term) []Entry {
	entries := make([]Entry, len(terms))
	for i, term := range terms {
		entries[i] = Entry{Index: start + Index(i), Term: term}
	}
	return entries
}

// logTerms returns the term of each entry in n's log, in index order.
func logTerms(n *Node) []Term {
	terms := []Term{}
	for _, e := range n.log.entries[1:] {
		terms = append(terms, e.Term)
	}
	return terms
}

// newFollower returns server 1 of a cluster of three, restarted in term
// with a log of the given terms.
func newFollower(term Term, log ...Term) *Node {
	return NewNode(testConfig(1, 3, 1), RestoredState{HardState: HardState{CurrentTerm: term}, Entries: logOf(log...)})
}

// newLeaderWithLog returns server 1 of a cluster of three, elected leader
// of term with a log of the given terms, with its Ready drained.
func newLeaderWithLog(t *testing.T, term Term, log ...Term) *Node {
	t.Helper()
	n := NewNode(testConfig(1, 3, 1), RestoredState{HardState: HardState{CurrentTerm: term - 1}, Entries: logOf(log...)})
	tickUntilElection(t, n)
	n.Step(voteResponse(2, term, true))
	if st := n.Status(); st.Role != Leader || st.Term != term {
		t.Fatalf("status = %+v, want the leader of term %d", st, term)
	}
	n.Ready()
	return n
}

// appendFrom returns an AppendEntries to server 1 from server 2, leader of
// term.
func appendFrom(term Term, prevIndex Index, prevTerm Term, entries []Entry, leaderCommit Index) Message {
	return Message{From: 2, To: 1, AppendEntries: &AppendEntriesRequest{
		Term:         term,
		LeaderID:     2,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}}
}

// appendResponse returns follower's reply to server 1's AppendEntries.
func appendResponse(follower NodeID, term Term, success bool, matchIndex Index) Message {
	return Message{From: follower, To: 1, AppendEntriesResponse: &AppendEntriesResponse{
		Term:       term,
		Success:    success,
		MatchIndex: matchIndex,
	}}
}

// oneAppend returns the one message in rd, which must be an AppendEntries
// from server 1 to to.
func oneAppend(t *testing.T, rd Ready, to NodeID) *AppendEntriesRequest {
	t.Helper()
	if len(rd.Messages) != 1 || rd.Messages[0].To != to || rd.Messages[0].AppendEntries == nil {
		t.Fatalf("Ready messages = %+v, want one AppendEntries to %d", rd.Messages, to)
	}
	return rd.Messages[0].AppendEntries
}

// Figure 2, AppendEntries receiver step 2: a follower refuses entries
// unless its log holds an entry at PrevLogIndex of term PrevLogTerm, and
// a refusal changes nothing. Index 0 always matches, with term 0.
func TestAppendEntriesChecksPrevEntry(t *testing.T) {
	tests := []struct {
		name      string
		prevIndex Index
		prevTerm  Term
		success   bool
	}{
		{"no entry at prevLogIndex", 4, 2, false},
		{"entry of another term", 3, 1, false},
		{"matching entry", 3, 2, true},
		{"the start of the log", 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newFollower(2, 1, 1, 2)
			n.Step(appendFrom(3, tt.prevIndex, tt.prevTerm, nil, 0))
			rd := n.Ready()
			if got := appendReply(t, rd, 2); got.Success != tt.success {
				t.Errorf("reply = %+v, want success %t", got, tt.success)
			}
			if len(rd.Entries) != 0 || !slices.Equal(logTerms(n), []Term{1, 1, 2}) {
				t.Errorf("log changed to %v", logTerms(n))
			}
		})
	}
}

// Figure 2, AppendEntries receiver steps 3 and 4: an entry that conflicts
// with a new one is deleted with all that follow it, and the new entries
// are appended. They leave in the same Ready as the reply, so they are
// durable before the leader can count them.
func TestAppendEntriesReplacesConflictingEntries(t *testing.T) {
	n := newFollower(2, 1, 1, 2, 2)
	n.Step(appendFrom(3, 2, 1, entriesAt(3, 3, 3), 0))
	rd := n.Ready()
	wantEntries(t, "Ready entries", rd.Entries, entriesAt(3, 3, 3))
	if got := appendReply(t, rd, 2); got != (AppendEntriesResponse{Term: 3, Success: true, MatchIndex: 4}) {
		t.Errorf("reply = %+v, want success up to index 4", got)
	}
	if got := logTerms(n); !slices.Equal(got, []Term{1, 1, 3, 3}) {
		t.Errorf("log terms = %v, want [1 1 3 3]", got)
	}
}

// Figure 2, AppendEntries receiver step 4: entries already in the log stay
// as they are; only new ones are appended and persisted.
func TestAppendEntriesAppendsOnlyNewEntries(t *testing.T) {
	n := newFollower(1, 1, 1)
	n.Step(appendFrom(1, 1, 1, entriesAt(2, 1, 1), 0))
	rd := n.Ready()
	wantEntries(t, "Ready entries", rd.Entries, entriesAt(3, 1))
	if got := appendReply(t, rd, 2); got != (AppendEntriesResponse{Term: 1, Success: true, MatchIndex: 3}) {
		t.Errorf("reply = %+v, want success up to index 3", got)
	}
}

// §5.3: a stale AppendEntries, whose entries the follower already holds,
// truncates nothing, even though the follower's log runs past them. Its
// reply acknowledges only what the request carried.
func TestStaleAppendEntriesDoesNotTruncate(t *testing.T) {
	n := newFollower(1, 1, 1, 1, 1)
	n.Step(appendFrom(1, 1, 1, entriesAt(2, 1), 0))
	rd := n.Ready()
	if len(rd.Entries) != 0 || !slices.Equal(logTerms(n), []Term{1, 1, 1, 1}) {
		t.Errorf("log changed to %v", logTerms(n))
	}
	if got := appendReply(t, rd, 2); got != (AppendEntriesResponse{Term: 1, Success: true, MatchIndex: 2}) {
		t.Errorf("reply = %+v, want success up to index 2 only", got)
	}
}

// §5.5: Raft RPCs are idempotent. An AppendEntries delivered twice changes
// the log only the first time, and both deliveries get the same reply.
func TestDuplicateAppendEntriesIsIdempotent(t *testing.T) {
	n := newFollower(1, 1)
	msg := appendFrom(1, 1, 1, entriesAt(2, 1, 1), 0)
	n.Step(msg)
	first := n.Ready()
	n.Step(msg)
	second := n.Ready()
	wantEntries(t, "first Ready entries", first.Entries, entriesAt(2, 1, 1))
	if len(second.Entries) != 0 {
		t.Errorf("second Ready entries = %v, want none", second.Entries)
	}
	if a, b := appendReply(t, first, 2), appendReply(t, second, 2); a != b {
		t.Errorf("replies differ: %+v, then %+v", a, b)
	}
}

// Figure 2, AppendEntries receiver step 5: commitIndex becomes
// min(leaderCommit, index of the last new entry), where the last new entry
// is the request's, not the follower's last; commitIndex never moves back.
func TestFollowerAdvancesCommitIndex(t *testing.T) {
	n := newFollower(1, 1, 1, 1, 1, 1)
	steps := []struct {
		msg  Message
		want Index
	}{
		{appendFrom(1, 1, 1, entriesAt(2, 1), 5), 2},
		{appendFrom(1, 3, 1, nil, 3), 3},
		{appendFrom(1, 1, 1, nil, 1), 3},
	}
	for i, step := range steps {
		n.Step(step.msg)
		n.Ready()
		if got := n.Status().CommitIndex; got != step.want {
			t.Errorf("step %d: commitIndex = %d, want %d", i, got, step.want)
		}
	}
}

// Figure 2, leaders: a new leader sets every follower's nextIndex just past
// its own last entry, and sends each new entry at once to every follower,
// with the entry before it for the consistency check.
func TestLeaderSendsEntriesFromNextIndex(t *testing.T) {
	n := newLeaderWithLog(t, 3, 1, 2)
	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	if len(rd.Messages) != 2 {
		t.Fatalf("Ready has %d messages, want an AppendEntries to each of 2 and 3", len(rd.Messages))
	}
	for i, m := range rd.Messages {
		req := m.AppendEntries
		if m.To != NodeID(i+2) || req == nil || req.Term != 3 || req.PrevLogIndex != 2 || req.PrevLogTerm != 2 {
			t.Fatalf("message %d = %+v, want AppendEntries after entry 2 of term 2", i, m)
		}
		wantEntries(t, "entries sent", req.Entries, []Entry{{Index: 3, Term: 3, Data: []byte("x")}})
	}
}

// §5.3 and ADR-0004: when a follower refuses AppendEntries for a log
// inconsistency, the leader backs nextIndex up past the whole conflicting
// term the reply names, and retries at once. The leader of term 4 holds
// terms [1 1 3 3] and first sends entry 5's predecessor, index 4.
func TestLeaderSkipsConflictingTerm(t *testing.T) {
	tests := []struct {
		name          string
		conflictTerm  Term
		conflictIndex Index
		prev          Index
	}{
		{"follower's log ends at index 2", 0, 3, 2},
		{"follower's term 2 starts at index 2, a term the leader lacks", 2, 2, 1},
		{"follower has term 1 at index 4; the leader's term 1 ends at 2", 1, 1, 2},
		{"a reply pointing forward still backs up one entry", 0, 9, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newLeaderWithLog(t, 4, 1, 1, 3, 3)
			n.Step(Message{From: 2, To: 1, AppendEntriesResponse: &AppendEntriesResponse{
				Term: 4, ConflictTerm: tt.conflictTerm, ConflictIndex: tt.conflictIndex,
			}})
			req := oneAppend(t, n.Ready(), 2)
			if req.PrevLogIndex != tt.prev || len(req.Entries) != int(4-tt.prev) {
				t.Errorf("retry after entry %d with %d entries, want after entry %d", req.PrevLogIndex, len(req.Entries), tt.prev)
			}
		})
	}
}

// ADR-0004: a refusal says where the logs part. With no entry at
// PrevLogIndex, it gives the follower's last index + 1; with an entry of
// another term, that term and the first index the follower holds it at.
func TestRejectionNamesConflict(t *testing.T) {
	tests := []struct {
		name      string
		prevIndex Index
		prevTerm  Term
		want      AppendEntriesResponse
	}{
		{"no entry at prevLogIndex", 7, 3, AppendEntriesResponse{Term: 3, ConflictIndex: 6}},
		{"term 2 where the leader has 3", 4, 3, AppendEntriesResponse{Term: 3, ConflictTerm: 2, ConflictIndex: 3}},
		{"term 1 where the leader has 2", 2, 2, AppendEntriesResponse{Term: 3, ConflictTerm: 1, ConflictIndex: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newFollower(2, 1, 1, 2, 2, 2)
			n.Step(appendFrom(3, tt.prevIndex, tt.prevTerm, nil, 0))
			if got := appendReply(t, n.Ready(), 2); got != tt.want {
				t.Errorf("reply = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// A follower whose log diverges over many entries but few terms converges
// in round trips per term, not per entry: one rejection here, where backing
// up one entry at a time would take fifty.
func TestDivergentLogConvergesPerTerm(t *testing.T) {
	repeat := func(term Term, n int) []Term { return slices.Repeat([]Term{term}, n) }
	leaderLog := slices.Concat(repeat(1, 100), repeat(3, 50))
	leader := newLeaderWithLog(t, 4, leaderLog...)
	follower := NewNode(testConfig(2, 3, 1), RestoredState{
		HardState: HardState{CurrentTerm: 2},
		Entries:   logOf(slices.Concat(repeat(1, 100), repeat(2, 100))...),
	})
	if _, _, err := leader.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if got := exchange(t, leader, follower); got != 1 {
		t.Errorf("converged after %d rejections, want 1: one per conflicting term", got)
	}
	if got, want := logTerms(follower), logTerms(leader); !slices.Equal(got, want) {
		t.Errorf("follower log does not match the leader's: %d entries, want %d", len(got), len(want))
	}
}

// A late or repeated success never moves matchIndex back, and replies from
// an earlier term are ignored.
func TestMatchIndexOnlyMovesForward(t *testing.T) {
	n := newLeaderWithLog(t, 3, 1, 1, 2)
	n.Step(appendResponse(2, 3, true, 3))
	n.Step(appendResponse(2, 3, true, 1))
	n.Step(appendResponse(2, 2, true, 0))
	if n.matchIndex[2] != 3 || n.nextIndex[2] != 4 {
		t.Errorf("server 2: matchIndex %d, nextIndex %d; want 3 and 4", n.matchIndex[2], n.nextIndex[2])
	}
}

// When a success leaves a follower behind the leader's log, the leader
// sends it the rest at once instead of waiting for the next heartbeat.
func TestLeaderSendsRestToLaggingFollower(t *testing.T) {
	n := newLeaderWithLog(t, 3, 1, 1)
	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	n.Ready()
	// The reply to the heartbeat sent on election arrives after the proposal.
	n.Step(appendResponse(2, 3, true, 2))
	req := oneAppend(t, n.Ready(), 2)
	if req.PrevLogIndex != 2 || len(req.Entries) != 1 || req.Entries[0].Index != 3 {
		t.Errorf("sent %d entries after entry %d, want entry 3 after entry 2", len(req.Entries), req.PrevLogIndex)
	}
}

// exchange delivers messages among nodes in send order until none are left,
// dropping those addressed to anyone else, and returns how many rejected
// AppendEntries replies it delivered.
func exchange(t *testing.T, nodes ...*Node) int {
	t.Helper()
	byID := map[NodeID]*Node{}
	var queue []Message
	for _, n := range nodes {
		byID[n.id] = n
		queue = append(queue, n.Ready().Messages...)
	}
	rejections := 0
	for delivered := 0; len(queue) > 0; delivered++ {
		if delivered > 10000 {
			t.Fatal("messages still flowing after 10,000 deliveries")
		}
		m := queue[0]
		queue = queue[1:]
		to, ok := byID[m.To]
		if !ok {
			continue
		}
		if r := m.AppendEntriesResponse; r != nil && !r.Success {
			rejections++
		}
		to.Step(m)
		queue = append(queue, to.Ready().Messages...)
	}
	return rejections
}

// Figure 7: whatever the follower's log looks like when a leader of term 8
// comes to power, missing entries (a, b), extra entries (c, d), or both
// (e, f), the follower's log ends up identical to the leader's once the
// leader appends an entry of its own term, after one rejection per
// conflicting term.
func TestFigure7FollowersConverge(t *testing.T) {
	leaderLog := []Term{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	tests := []struct {
		name       string
		log        []Term
		rejections int // with fast backtracking: one per conflicting term
	}{
		{"a", []Term{1, 1, 1, 4, 4, 5, 5, 6, 6}, 1},
		{"b", []Term{1, 1, 1, 4}, 1},
		{"c", []Term{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}, 0},
		{"d", []Term{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}, 0},
		{"e", []Term{1, 1, 1, 4, 4, 4, 4}, 2},
		{"f", []Term{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leader := newLeaderWithLog(t, 8, leaderLog...)
			follower := NewNode(testConfig(2, 3, 1), RestoredState{
				HardState: HardState{CurrentTerm: tt.log[len(tt.log)-1]},
				Entries:   logOf(tt.log...),
			})
			if _, _, err := leader.Propose([]byte("x")); err != nil {
				t.Fatal(err)
			}
			if got := exchange(t, leader, follower); got != tt.rejections {
				t.Errorf("converged after %d rejections, want %d", got, tt.rejections)
			}
			if got, want := logTerms(follower), logTerms(leader); !slices.Equal(got, want) {
				t.Errorf("follower log terms = %v, want the leader's %v", got, want)
			}
		})
	}
}
