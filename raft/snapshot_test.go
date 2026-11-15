package raft

import (
	"fmt"
	"testing"
)

// snapshotOf returns a snapshot that replaces the entries up to index,
// whose entry at index has term, with data that names them.
func snapshotOf(index Index, term Term) *Snapshot {
	return &Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: fmt.Appendf(nil, "state@%d", index)}
}

// restoredFollower returns server 1 of a cluster of three, restarted in
// term from snapshot s and, after it, entries of the given terms.
func restoredFollower(term Term, s *Snapshot, log ...Term) *Node {
	return NewNode(testConfig(1, 3, 1), RestoredState{
		HardState: HardState{CurrentTerm: term},
		Snapshot:  s,
		Entries:   entriesAt(s.LastIncludedIndex+1, log...),
	})
}

// installFrom returns an InstallSnapshot of all of s, in one chunk, to
// server 1 from server 2, leader of term.
func installFrom(term Term, s *Snapshot) Message {
	return Message{From: 2, To: 1, InstallSnapshot: &InstallSnapshotRequest{
		Term:              term,
		LeaderID:          2,
		LastIncludedIndex: s.LastIncludedIndex,
		LastIncludedTerm:  s.LastIncludedTerm,
		Data:              s.Data,
		Done:              true,
	}}
}

// snapshotReply returns the one message in rd, which must answer an
// InstallSnapshot from server 2.
func snapshotReply(t *testing.T, rd Ready) InstallSnapshotResponse {
	t.Helper()
	if len(rd.Messages) != 1 || rd.Messages[0].To != 2 || rd.Messages[0].InstallSnapshotResponse == nil {
		t.Fatalf("Ready messages = %+v, want one InstallSnapshot reply to 2", rd.Messages)
	}
	return *rd.Messages[0].InstallSnapshotResponse
}

// wantSnapshot checks that rd hands storage exactly s.
func wantSnapshot(t *testing.T, rd Ready, s *Snapshot) {
	t.Helper()
	got := rd.Snapshot
	if got == nil || got.LastIncludedIndex != s.LastIncludedIndex || got.LastIncludedTerm != s.LastIncludedTerm ||
		string(got.Data) != string(s.Data) {
		t.Errorf("Ready snapshot = %+v, want %+v", got, s)
	}
}

// A server restarts from its snapshot and the log after it. The snapshot
// holds only committed entries, which the runtime has restored into the
// state machine, so commitIndex and lastApplied start at its last
// included index, and nothing goes back to storage. Entries the leader
// sends after the snapshot are applied from the next index on.
func TestNewNodeRestoresSnapshot(t *testing.T) {
	n := restoredFollower(3, snapshotOf(5, 2), 2, 3)
	if st := n.Status(); st != (Status{Role: Follower, Term: 3, CommitIndex: 5, LastApplied: 5}) {
		t.Errorf("status = %+v, want a follower in term 3 with entries through 5 applied", st)
	}
	if rd := n.Ready(); rd.Snapshot != nil || len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 {
		t.Errorf("Ready = %+v, want nothing to persist or apply", rd)
	}
	n.Step(appendFrom(3, 7, 3, entriesAt(8, 3), 8))
	rd := n.Ready()
	if got := appendReply(t, rd, 2); !got.Success || got.MatchIndex != 8 {
		t.Errorf("reply = %+v, want success through 8", got)
	}
	wantEntries(t, "Ready committed entries", rd.CommittedEntries, entriesAt(6, 2, 3, 3))
}

// §5.4.1 with a snapshot: when nothing follows the snapshot, its last
// included entry is the log's last entry for the election restriction.
func TestVoteComparesSnapshotLastEntry(t *testing.T) {
	tests := []struct {
		lastIndex Index
		lastTerm  Term
		granted   bool
	}{{5, 2, true}, {4, 2, false}, {9, 1, false}, {1, 3, true}}
	for _, tt := range tests {
		n := restoredFollower(3, snapshotOf(5, 2))
		n.Step(voteRequest(2, 4, tt.lastIndex, tt.lastTerm))
		if got := voteReply(t, n.Ready(), 2); got.VoteGranted != tt.granted {
			t.Errorf("candidate whose log ends at %d of term %d: granted %t, want %t", tt.lastIndex, tt.lastTerm, got.VoteGranted, tt.granted)
		}
	}
}

// Figure 13, receiver step 1: reply immediately if term < currentTerm,
// changing nothing.
func TestInstallSnapshotRejectsStaleTerm(t *testing.T) {
	n := newFollower(3, 1, 1)
	n.Step(installFrom(2, snapshotOf(5, 2)))
	rd := n.Ready()
	if got := snapshotReply(t, rd); got != (InstallSnapshotResponse{Term: 3}) {
		t.Errorf("reply = %+v, want a bare reply in term 3", got)
	}
	if rd.Snapshot != nil || n.Status().LastApplied != 0 || n.log.lastIndex() != 2 {
		t.Errorf("a stale InstallSnapshot changed the node: Ready snapshot %+v, status %+v, last index %d",
			rd.Snapshot, n.Status(), n.log.lastIndex())
	}
}

// Figure 13, receiver step 5: the snapshot goes to storage, which keeps it
// in place of any earlier, smaller one, and it is the snapshot the server
// sends if it leads later. The reply acknowledges it.
func TestInstallSnapshotSavesSnapshot(t *testing.T) {
	n := restoredFollower(2, snapshotOf(2, 1))
	s := snapshotOf(5, 2)
	n.Step(installFrom(2, s))
	rd := n.Ready()
	wantSnapshot(t, rd, s)
	if got := snapshotReply(t, rd); got != (InstallSnapshotResponse{Term: 2, LastIncludedIndex: 5, Done: true}) {
		t.Errorf("reply = %+v, want an acknowledgment of the snapshot through 5", got)
	}
	if n.snapshot.LastIncludedIndex != 5 || n.log.snapshotIndex() != 5 {
		t.Errorf("the node keeps snapshot %+v and a log after %d, want the new snapshot through 5", n.snapshot, n.log.snapshotIndex())
	}
}

// Figure 13, receiver step 6: if the log holds an entry with the
// snapshot's last included index and term, the entries after it stay,
// and storage gets them along with the snapshot that replaces the rest.
func TestInstallSnapshotKeepsEntriesAfterMatchingEntry(t *testing.T) {
	n := newFollower(2, 1, 1, 2, 2, 2)
	n.Step(installFrom(2, snapshotOf(3, 2)))
	rd := n.Ready()
	if got := logTerms(n); len(got) != 2 || n.log.lastIndex() != 5 {
		t.Errorf("log after the snapshot has terms %v and ends at %d, want entries 4 and 5", got, n.log.lastIndex())
	}
	wantEntries(t, "Ready entries", rd.Entries, entriesAt(4, 2, 2))
}

// Figure 13, receiver step 7: a log without the snapshot's last included
// entry is discarded entirely, whether that entry has another term or the
// log is too short to hold it.
func TestInstallSnapshotDiscardsLogWithoutLastIncludedEntry(t *testing.T) {
	for name, n := range map[string]*Node{
		"other term": newFollower(2, 1, 1, 1, 1),
		"short log":  newFollower(2, 1),
	} {
		n.Step(installFrom(2, snapshotOf(3, 2)))
		rd := n.Ready()
		if got := logTerms(n); len(got) != 0 || n.log.lastIndex() != 3 || n.log.lastTerm() != 2 {
			t.Errorf("%s: log after the snapshot has terms %v and ends at %d of term %d, want nothing after 3 of term 2",
				name, got, n.log.lastIndex(), n.log.lastTerm())
		}
		if len(rd.Entries) != 0 {
			t.Errorf("%s: Ready entries = %v, want none after the snapshot", name, rd.Entries)
		}
	}
}

// Figure 13, receiver step 8: the state machine is reset to the snapshot,
// even when step 6 kept the log after it, because the entries the
// snapshot covers are gone. commitIndex and lastApplied move to its last
// included index, and the entries after it are applied from there.
func TestInstallSnapshotResetsStateMachine(t *testing.T) {
	n := newFollower(2, 1, 1, 2, 2)
	n.Step(installFrom(2, snapshotOf(3, 2)))
	rd := n.Ready()
	if st := n.Status(); st.CommitIndex != 3 || st.LastApplied != 3 {
		t.Errorf("status = %+v, want commitIndex and lastApplied 3", st)
	}
	if rd.Snapshot == nil || len(rd.CommittedEntries) != 0 {
		t.Errorf("Ready snapshot %+v and committed entries %v, want the snapshot to restore and nothing to apply", rd.Snapshot, rd.CommittedEntries)
	}
	n.Step(appendFrom(2, 4, 2, entriesAt(5, 2), 5))
	wantEntries(t, "Ready committed entries", n.Ready().CommittedEntries, entriesAt(4, 2, 2))
}

// A snapshot that covers nothing past commitIndex is ignored, so applied
// state never moves backwards. The reply still acknowledges it, because
// this log holds every entry it covers.
func TestInstallSnapshotIgnoresCommittedPrefix(t *testing.T) {
	for _, index := range []Index{3, 4} {
		n := newFollower(2, 1, 1, 2, 2)
		n.Step(appendFrom(2, 4, 2, nil, 4))
		n.Ready()
		n.Step(installFrom(2, snapshotOf(index, 2)))
		rd := n.Ready()
		if got := snapshotReply(t, rd); got != (InstallSnapshotResponse{Term: 2, LastIncludedIndex: index, Done: true}) {
			t.Errorf("snapshot through %d: reply = %+v, want an acknowledgment", index, got)
		}
		if rd.Snapshot != nil || n.Status().LastApplied != 4 || n.log.snapshotIndex() != 0 || len(logTerms(n)) != 4 {
			t.Errorf("snapshot through %d changed the node: Ready snapshot %+v, status %+v, log terms %v",
				index, rd.Snapshot, n.Status(), logTerms(n))
		}
	}
}

// The leader sends InstallSnapshot to a follower in place of AppendEntries,
// so it restarts the follower's election timer, and a candidate of the
// same term concedes to its sender.
func TestInstallSnapshotRestartsElectionTimer(t *testing.T) {
	n := newFollower(2)
	s := snapshotOf(5, 2)
	for range 3 {
		for range n.electionTimeout - 1 {
			n.Tick()
		}
		n.Step(installFrom(2, s))
	}
	if st := n.Status(); st.Role != Follower || st.Term != 2 || st.Leader != 2 {
		t.Errorf("status = %+v, want a follower of server 2 in term 2", st)
	}
	c := newCandidate(t, 3)
	c.Step(installFrom(1, s))
	if st := c.Status(); st.Role != Follower || st.Leader != 2 {
		t.Errorf("candidate's status = %+v, want a follower of server 2", st)
	}
}

// leaderWithSnapshot returns server 1 of a cluster of three, elected
// leader of term 3 after restarting from a snapshot through index 5 of
// term 1 and entries 6 and 7 of term 2, with its Ready drained.
func leaderWithSnapshot(t *testing.T) *Node {
	t.Helper()
	n := NewNode(testConfig(1, 3, 1), RestoredState{
		HardState: HardState{CurrentTerm: 2},
		Snapshot:  snapshotOf(5, 1),
		Entries:   entriesAt(6, 2, 2),
	})
	tickUntilElection(t, n)
	n.Step(voteResponse(2, 3, true))
	n.Ready()
	return n
}

// sentTo returns the messages in rd addressed to server to.
func sentTo(rd Ready, to NodeID) []Message {
	var sent []Message
	for _, m := range rd.Messages {
		if m.To == to {
			sent = append(sent, m)
		}
	}
	return sent
}

// rejectedBy returns the one message leader n sends server 2 after server
// 2 rejects an AppendEntries because its log ends at logEnd.
func rejectedBy(t *testing.T, n *Node, logEnd Index) Message {
	t.Helper()
	n.Step(Message{From: 2, To: 1, AppendEntriesResponse: &AppendEntriesResponse{Term: 3, PrevLogIndex: 7, ConflictIndex: logEnd + 1}})
	sent := sentTo(n.Ready(), 2)
	if len(sent) != 1 {
		t.Fatalf("log ending at %d: sent %+v, want one message", logEnd, sent)
	}
	return sent[0]
}

// §7: a leader sends its snapshot to a follower that needs an entry the
// snapshot replaced, the last included one among them, and AppendEntries
// to one that needs only later entries.
func TestLeaderSendsSnapshotWhenEntriesAreGone(t *testing.T) {
	want := InstallSnapshotRequest{Term: 3, LeaderID: 1, LastIncludedIndex: 5, LastIncludedTerm: 1, Data: snapshotOf(5, 1).Data, Done: true}
	for _, logEnd := range []Index{2, 4} {
		got := rejectedBy(t, leaderWithSnapshot(t), logEnd).InstallSnapshot
		if got == nil || fmt.Sprintf("%+v", *got) != fmt.Sprintf("%+v", want) {
			t.Errorf("log ending at %d: sent InstallSnapshot %+v, want %+v", logEnd, got, want)
		}
	}
	req := rejectedBy(t, leaderWithSnapshot(t), 5).AppendEntries
	if req == nil || req.PrevLogIndex != 5 || req.PrevLogTerm != 1 || len(req.Entries) != 2 {
		t.Errorf("log ending at 5: sent AppendEntries %+v, want entries 6 and 7 after entry 5 of term 1", req)
	}
}

// The leader resends the snapshot with every heartbeat until the follower
// acknowledges it, then records the follower's log as matching through
// the snapshot and sends the entries after it.
func TestLeaderResendsSnapshotUntilAcknowledged(t *testing.T) {
	n := leaderWithSnapshot(t)
	n.Step(Message{From: 2, To: 1, AppendEntriesResponse: &AppendEntriesResponse{Term: 3, PrevLogIndex: 7, ConflictIndex: 1}})
	for heartbeat := range 3 {
		if sent := sentTo(n.Ready(), 2); len(sent) != 1 || sent[0].InstallSnapshot == nil {
			t.Fatalf("heartbeat %d: sent server 2 %+v, want one InstallSnapshot", heartbeat, sent)
		}
		for range n.heartbeatTick {
			n.Tick()
		}
	}
	n.Ready()
	n.Step(Message{From: 2, To: 1, InstallSnapshotResponse: &InstallSnapshotResponse{Term: 3, LastIncludedIndex: 5, Done: true}})
	req := oneAppend(t, n.Ready(), 2)
	if req.PrevLogIndex != 5 || req.PrevLogTerm != 1 || len(req.Entries) != 2 {
		t.Errorf("after the snapshot, sent %d entries after entry %d of term %d, want 2 after entry 5 of term 1",
			len(req.Entries), req.PrevLogIndex, req.PrevLogTerm)
	}
	if n.matchIndex[2] != 5 {
		t.Errorf("matchIndex[2] = %d, want 5", n.matchIndex[2])
	}
}

// AppendEntries receiver step 2 with a snapshot: entries before the
// snapshot's last included one are committed, so they match the leader's
// although their terms are gone. A request that starts inside the
// snapshot succeeds, and only its entries after the snapshot reach the
// log.
func TestAppendEntriesMatchesInsideSnapshot(t *testing.T) {
	n := restoredFollower(2, snapshotOf(5, 2), 2)
	n.Step(appendFrom(2, 3, 1, entriesAt(4, 2, 2, 2, 3), 7))
	rd := n.Ready()
	if got := appendReply(t, rd, 2); !got.Success || got.MatchIndex != 7 {
		t.Errorf("reply = %+v, want success through 7", got)
	}
	wantEntries(t, "Ready entries", rd.Entries, entriesAt(7, 3))
	if st := n.Status(); st.CommitIndex != 7 {
		t.Errorf("commitIndex = %d, want 7", st.CommitIndex)
	}
	n.Step(appendFrom(2, 1, 1, entriesAt(2, 1, 1), 3))
	rd = n.Ready()
	if got := appendReply(t, rd, 2); !got.Success || got.MatchIndex != 3 || len(rd.Entries) != 0 {
		t.Errorf("a request inside the snapshot: reply %+v and entries %v, want success through 3 and nothing stored", got, rd.Entries)
	}
}

// leaderWithApplied returns server 1 of a cluster of three as the leader
// of term 1, with entries 1 to 5 committed and applied.
func leaderWithApplied(t *testing.T) *Node {
	t.Helper()
	n := newLeader(t, 3)
	for i := range 5 {
		if _, _, err := n.Propose(fmt.Appendf(nil, "c%d", i+1)); err != nil {
			t.Fatal(err)
		}
	}
	n.Step(appendResponse(2, 1, true, 5))
	if rd := n.Ready(); len(rd.CommittedEntries) != 5 {
		t.Fatalf("applied %d entries, want 5", len(rd.CommittedEntries))
	}
	return n
}

// §7: a server replaces the entries its state machine has applied with
// the snapshot the service takes. Storage gets the snapshot and the
// entries after it, the log keeps those entries, and a follower that
// needs a replaced entry gets the snapshot.
func TestCompactReplacesAppliedEntries(t *testing.T) {
	n := leaderWithApplied(t)
	data := []byte("state@3")
	if err := n.Compact(3, data); err != nil {
		t.Fatal(err)
	}
	data[0] = 'X' // the node keeps a copy
	rd := n.Ready()
	wantSnapshot(t, rd, &Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("state@3")})
	wantEntries(t, "Ready entries", rd.Entries, []Entry{{Index: 4, Term: 1, Data: []byte("c4")}, {Index: 5, Term: 1, Data: []byte("c5")}})
	if _, ok := n.log.term(2); ok || n.log.snapshotIndex() != 3 || n.log.lastIndex() != 5 {
		t.Errorf("log after the snapshot through 3 ends at %d, still holding entry 2: %t", n.log.lastIndex(), ok)
	}
	if st := n.Status(); st.CommitIndex != 5 || st.LastApplied != 5 {
		t.Errorf("status = %+v, want commitIndex and lastApplied unchanged at 5", st)
	}
	n.Step(Message{From: 3, To: 1, AppendEntriesResponse: &AppendEntriesResponse{Term: 1, ConflictIndex: 1}})
	want := InstallSnapshotRequest{Term: 1, LeaderID: 1, LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("state@3"), Done: true}
	sent := sentTo(n.Ready(), 3)
	if len(sent) != 1 || sent[0].InstallSnapshot == nil || fmt.Sprintf("%+v", *sent[0].InstallSnapshot) != fmt.Sprintf("%+v", want) {
		t.Errorf("sent server 3 %+v, want InstallSnapshot %+v", sent, want)
	}
}

// Compact refuses an index the state machine has not applied, or one the
// snapshot already covers, and changes nothing.
func TestCompactRefusesUnappliedOrCompactedIndex(t *testing.T) {
	n := leaderWithApplied(t)
	if _, _, err := n.Propose([]byte("c6")); err != nil {
		t.Fatal(err)
	}
	n.Ready()
	for _, index := range []Index{0, 6} {
		if err := n.Compact(index, nil); err == nil {
			t.Errorf("Compact(%d) with entries 1 to 5 applied succeeded", index)
		}
	}
	if err := n.Compact(3, nil); err != nil {
		t.Fatal(err)
	}
	n.Ready()
	for _, index := range []Index{2, 3} {
		if err := n.Compact(index, nil); err == nil {
			t.Errorf("Compact(%d) after a snapshot through 3 succeeded", index)
		}
	}
	if rd := n.Ready(); rd.Snapshot != nil || n.log.snapshotIndex() != 3 {
		t.Errorf("refused compactions left Ready snapshot %+v and a snapshot through %d", rd.Snapshot, n.log.snapshotIndex())
	}
}

// chunkFrom returns a chunk of snapshotOf(index, 2), whose data is
// "state@<index>", to server 1 from server 2, leader of term: the data at
// offset, the last chunk if done.
func chunkFrom(term Term, index Index, offset int, data string, done bool) Message {
	return Message{From: 2, To: 1, InstallSnapshot: &InstallSnapshotRequest{
		Term:              term,
		LeaderID:          2,
		LastIncludedIndex: index,
		LastIncludedTerm:  2,
		Offset:            offset,
		Data:              []byte(data),
		Done:              done,
	}}
}

// Figure 13, receiver steps 2 to 4: the first chunk starts a new
// snapshot, each chunk is written at its offset, and until the last one
// the follower replies with how much it holds and waits. The last chunk
// completes the snapshot, which is installed.
func TestInstallSnapshotAssemblesChunks(t *testing.T) {
	n := newFollower(2, 1)
	for _, c := range []struct {
		offset int
		data   string
	}{{0, "sta"}, {3, "te@"}} {
		n.Step(chunkFrom(2, 5, c.offset, c.data, false))
		rd := n.Ready()
		want := InstallSnapshotResponse{Term: 2, LastIncludedIndex: 5, Offset: c.offset + len(c.data)}
		if got := snapshotReply(t, rd); got != want || rd.Snapshot != nil {
			t.Fatalf("chunk at %d: reply %+v and Ready snapshot %+v, want %+v and none", c.offset, got, rd.Snapshot, want)
		}
	}
	n.Step(chunkFrom(2, 5, 6, "5", true))
	rd := n.Ready()
	wantSnapshot(t, rd, snapshotOf(5, 2))
	if got := snapshotReply(t, rd); got != (InstallSnapshotResponse{Term: 2, LastIncludedIndex: 5, Done: true}) {
		t.Errorf("last chunk: reply = %+v, want done", got)
	}
}

// Figure 13, receiver step 3: the leader sends each chunk once the one
// before is acknowledged, so a chunk that does not continue the snapshot
// arriving is a repeat or comes from an earlier transfer. It is dropped,
// and the reply says how much of its snapshot the follower holds. A first
// chunk from the leader of a later term starts over (step 2).
func TestInstallSnapshotDropsChunksThatDoNotContinue(t *testing.T) {
	n := newFollower(2, 1)
	for _, step := range []struct {
		name   string
		msg    Message
		holds  int
		latest Term
	}{
		{"a chunk past what arrived", chunkFrom(2, 5, 3, "te@", false), 0, 2},
		{"the first chunk", chunkFrom(2, 5, 0, "sta", false), 3, 2},
		{"the second chunk", chunkFrom(2, 5, 3, "te@", false), 6, 2},
		{"the first chunk again", chunkFrom(2, 5, 0, "sta", false), 6, 2},
		{"the second chunk again", chunkFrom(2, 5, 3, "te@", false), 6, 2},
		{"a chunk of another snapshot", chunkFrom(2, 7, 6, "7", true), 0, 2},
		{"a first chunk from a later leader", chunkFrom(3, 5, 0, "sta", false), 3, 3},
	} {
		n.Step(step.msg)
		rd := n.Ready()
		if got := snapshotReply(t, rd); got.Offset != step.holds || got.Done || got.Term != step.latest || rd.Snapshot != nil {
			t.Fatalf("%s: reply %+v and Ready snapshot %+v, want %d bytes held in term %d", step.name, got, rd.Snapshot, step.holds, step.latest)
		}
	}
	n.Step(chunkFrom(3, 5, 3, "te@", false))
	n.Step(chunkFrom(3, 5, 6, "5", true))
	wantSnapshot(t, n.Ready(), snapshotOf(5, 2))
}

// catchUp delivers messages between leader n and follower f, in send
// order, until none are left, dropping those to anyone else, and returns
// the snapshot chunks f received.
func catchUp(t *testing.T, n, f *Node) []InstallSnapshotRequest {
	t.Helper()
	var chunks []InstallSnapshotRequest
	queue := append(n.Ready().Messages, f.Ready().Messages...)
	for len(queue) > 0 {
		if len(chunks) > 100 {
			t.Fatal("more than 100 chunks sent")
		}
		m := queue[0]
		queue = queue[1:]
		to := map[NodeID]*Node{n.id: n, f.id: f}[m.To]
		if to == nil {
			continue
		}
		if m.InstallSnapshot != nil {
			chunks = append(chunks, *m.InstallSnapshot)
		}
		to.Step(m)
		queue = append(queue, to.Ready().Messages...)
	}
	return chunks
}

// Figure 13's chunking: SnapshotChunkSize bounds each chunk, so the
// 7-byte snapshot "state@5" takes one message when the size is 0 or at
// least 7, and more otherwise, and arrives whole either way. The follower
// then takes the entries after it.
func TestSnapshotTravelsInChunks(t *testing.T) {
	for size, want := range map[int]int{0: 1, 3: 3, 6: 2, 7: 1, 100: 1} {
		n := leaderWithSnapshot(t)
		n.snapshotChunkSize = size
		f := NewNode(testConfig(2, 3, 1), RestoredState{})
		for range n.heartbeatTick {
			n.Tick()
		}
		chunks := catchUp(t, n, f)
		if len(chunks) != want || !chunks[len(chunks)-1].Done {
			t.Errorf("chunk size %d: sent %d chunks, the last done: %t; want %d", size, len(chunks), len(chunks) > 0 && chunks[len(chunks)-1].Done, want)
		}
		if f.snapshot == nil || string(f.snapshot.Data) != "state@5" || len(logTerms(f)) != 2 {
			t.Errorf("chunk size %d: follower has snapshot %+v and log terms %v, want state@5 and two entries", size, f.snapshot, logTerms(f))
		}
	}
}

// progressFrom returns server 2's reply to a chunk of the snapshot
// through index, saying it holds offset bytes of it.
func progressFrom(index Index, offset int) Message {
	return Message{From: 2, To: 1, InstallSnapshotResponse: &InstallSnapshotResponse{Term: 3, LastIncludedIndex: index, Offset: offset}}
}

// wantChunk checks that rd sends server 2 just the chunk of the snapshot
// through index that begins at offset, or nothing if offset is negative.
func wantChunk(t *testing.T, what string, rd Ready, index Index, offset int) {
	t.Helper()
	sent := sentTo(rd, 2)
	switch {
	case offset < 0 && len(sent) != 0:
		t.Errorf("%s: sent %+v, want nothing", what, sent)
	case offset >= 0 && (len(sent) != 1 || sent[0].InstallSnapshot == nil ||
		sent[0].InstallSnapshot.LastIncludedIndex != index || sent[0].InstallSnapshot.Offset != offset):
		t.Errorf("%s: sent %+v, want the chunk at %d of the snapshot through %d", what, sent, offset, index)
	}
}

// The leader sends the next chunk as soon as a reply shows the follower
// took the last one. A reply that does not, a repeat or one from a
// follower that lost what it held, waits for the next heartbeat to resend
// from where the follower is, so duplicated replies never start a second
// stream of chunks. Compacting again leaves the transfer under way alone:
// the follower gets the new snapshot once it has the old one.
func TestLeaderSendsNextChunkOnProgress(t *testing.T) {
	n := leaderWithSnapshot(t)
	n.snapshotChunkSize = 3
	n.Step(Message{From: 2, To: 1, AppendEntriesResponse: &AppendEntriesResponse{Term: 3, PrevLogIndex: 7, ConflictIndex: 1}})
	wantChunk(t, "after the rejection", n.Ready(), 5, 0)
	n.Step(progressFrom(5, 3))
	wantChunk(t, "after progress", n.Ready(), 5, 3)
	for _, reply := range []struct {
		name string
		msg  Message
	}{
		{"a repeated reply", progressFrom(5, 3)},
		{"a reply about another snapshot", progressFrom(4, 6)},
		{"a follower that lost what it received", progressFrom(5, 0)},
	} {
		n.Step(reply.msg)
		wantChunk(t, "after "+reply.name, n.Ready(), 5, -1)
	}
	for range n.heartbeatTick {
		n.Tick()
	}
	wantChunk(t, "at the heartbeat", n.Ready(), 5, 0)
	// Server 3 acknowledges entries 6 and 7 and a new one, which commits
	// them all, and the leader compacts them.
	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	n.Step(progressFrom(5, 3))
	n.Step(appendResponse(3, 3, true, 8))
	n.Ready()
	if err := n.Compact(8, []byte("state@8")); err != nil {
		t.Fatal(err)
	}
	n.Ready()
	for range n.heartbeatTick {
		n.Tick()
	}
	wantChunk(t, "after compacting again", n.Ready(), 5, 3)
	done5 := Message{From: 2, To: 1, InstallSnapshotResponse: &InstallSnapshotResponse{Term: 3, LastIncludedIndex: 5, Done: true}}
	n.Step(done5)
	wantChunk(t, "once the old snapshot is in", n.Ready(), 8, 0)
	n.Step(progressFrom(8, 3))
	wantChunk(t, "after progress with the new snapshot", n.Ready(), 8, 3)
	// A late repeat of the old snapshot's last reply ends nothing.
	n.Step(done5)
	n.Ready()
	for range n.heartbeatTick {
		n.Tick()
	}
	wantChunk(t, "after a repeated reply about the old snapshot", n.Ready(), 8, 3)
}

// compactAnother has leader n, with server 3 up to date, commit one more
// entry and compact its log through it.
func compactAnother(t *testing.T, n *Node) {
	t.Helper()
	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	n.Step(appendResponse(3, 3, true, n.log.lastIndex()))
	n.Ready()
	if err := n.Compact(n.lastApplied, fmt.Appendf(nil, "state@%d", n.lastApplied)); err != nil {
		t.Fatal(err)
	}
}

// A transfer ends however often the leader compacts: it sends the
// snapshot it began with to the end. Here the leader compacts between
// each chunk and the reply to it, and the follower still holds the
// 7-byte snapshot after its four 2-byte chunks. Starting over with each
// new snapshot, the leader kept a lagging follower from ever catching up
// in the randomized simulator.
func TestSnapshotTransferOutlastsCompaction(t *testing.T) {
	n := leaderWithSnapshot(t)
	n.snapshotChunkSize = 2
	n.Step(appendResponse(3, 3, true, 7))
	f := NewNode(testConfig(2, 3, 1), RestoredState{})
	n.Step(Message{From: 2, To: 1, AppendEntriesResponse: &AppendEntriesResponse{Term: 3, PrevLogIndex: 7, ConflictIndex: 1}})
	for chunks := 0; f.snapshot == nil; chunks++ {
		if chunks == 4 {
			t.Fatal("the follower has no snapshot after four chunks")
		}
		for _, m := range sentTo(n.Ready(), 2) {
			f.Step(m)
		}
		replies := f.Ready().Messages
		compactAnother(t, n)
		n.Ready()
		for _, m := range replies {
			n.Step(m)
		}
	}
	if f.snapshot.LastIncludedIndex != 5 || string(f.snapshot.Data) != "state@5" {
		t.Errorf("the follower holds %+v, want the snapshot the transfer began with", f.snapshot)
	}
}
