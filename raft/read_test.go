package raft

import (
	"errors"
	"slices"
	"testing"
)

// readAck returns follower's reply, in term, to an AppendEntries of round
// that matched through matchIndex.
func readAck(follower NodeID, term Term, matchIndex Index, round uint64) Message {
	m := appendResponse(follower, term, true, matchIndex)
	m.AppendEntriesResponse.ReadRound = round
	return m
}

// newReadyLeader returns server 1 of a cluster of servers 1 to size, the
// leader of term 1 with its no-op at index 1 committed, and its Ready
// drained.
func newReadyLeader(t *testing.T, size int) *Node {
	t.Helper()
	n := NewNode(noOpConfig(1, size), RestoredState{})
	tickUntilElection(t, n)
	for id := NodeID(2); id <= NodeID(size); id++ {
		n.Step(voteResponse(id, 1, true))
		n.Step(appendResponse(id, 1, true, 1))
	}
	if st := n.Status(); st.Role != Leader || st.CommitIndex != 1 {
		t.Fatalf("status = %+v, want the leader of term 1 with its no-op committed", st)
	}
	n.Ready()
	return n
}

// wantReads checks that rd confirms exactly want, and that applying rd's
// committed entries brings the state machine to each read's index.
func wantReads(t *testing.T, n *Node, rd Ready, want ...ReadState) {
	t.Helper()
	if !slices.Equal(rd.ReadStates, want) {
		t.Fatalf("Ready read states = %+v, want %+v", rd.ReadStates, want)
	}
	for _, rs := range rd.ReadStates {
		if applied := n.Status().LastApplied; applied < rs.Index {
			t.Errorf("read %d at index %d, but this Ready applies only through %d", rs.ID, rs.Index, applied)
		}
	}
}

// §8: a leader confirms a read only once a majority, itself included, has
// answered a round of heartbeats sent after the read arrived; until then
// it might have been deposed. The read's index is the leader's
// commitIndex.
func TestReadIndexWaitsForMajority(t *testing.T) {
	n := newReadyLeader(t, 5)
	if err := n.ReadIndex(7); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	wantReads(t, n, rd)
	if len(rd.Messages) != 4 {
		t.Fatalf("Ready messages = %+v, want an AppendEntries to each of 4 followers", rd.Messages)
	}
	for _, m := range rd.Messages {
		if m.AppendEntries == nil || m.AppendEntries.ReadRound != 1 {
			t.Fatalf("message to %d = %+v, want an AppendEntries of round 1", m.To, m)
		}
	}
	n.Step(readAck(2, 1, 1, 1))
	wantReads(t, n, n.Ready())
	n.Step(readAck(3, 1, 1, 1))
	wantReads(t, n, n.Ready(), ReadState{ID: 7, Index: 1})
}

// An answer to a request sent before a read arrived shows only that the
// leader led back then, so it does not confirm the read. Reads that
// arrive later wait for later rounds; repeated answers count once.
func TestReadIndexIgnoresAnswersToEarlierRounds(t *testing.T) {
	n := newReadyLeader(t, 3)
	for id := range uint64(2) {
		if err := n.ReadIndex(id + 1); err != nil {
			t.Fatal(err)
		}
		n.Ready()
	}
	n.Step(readAck(2, 1, 1, 0))
	wantReads(t, n, n.Ready())
	n.Step(readAck(2, 1, 1, 1))
	wantReads(t, n, n.Ready(), ReadState{ID: 1, Index: 1})
	n.Step(readAck(2, 1, 1, 1))
	n.Step(readAck(3, 1, 1, 1))
	wantReads(t, n, n.Ready())
	n.Step(readAck(3, 1, 1, 2))
	wantReads(t, n, n.Ready(), ReadState{ID: 2, Index: 1})
}

// §8: a leader that has committed no entry of its term may not know which
// entries are committed, so it confirms no read, however many servers
// answer, until one commits. Without the election no-op, that is the
// first command of its term, and the read's index is then past it.
func TestReadIndexWaitsForCommitInTerm(t *testing.T) {
	n := newLeaderWithLog(t, 2, 1)
	if err := n.ReadIndex(5); err != nil {
		t.Fatal(err)
	}
	n.Ready()
	n.Step(readAck(2, 2, 1, 1))
	wantReads(t, n, n.Ready())
	index, _, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	n.Ready()
	n.Step(readAck(2, 2, index, 1))
	wantReads(t, n, n.Ready(), ReadState{ID: 5, Index: index})
}

// A leader that learns of a later term before it confirms a read drops
// the read: as a follower it confirms none, answers to its old rounds
// count for nothing, and the read does not come back if the server leads
// again.
func TestLeaderDropsReadsWhenItStepsDown(t *testing.T) {
	n := newReadyLeader(t, 3)
	if err := n.ReadIndex(1); err != nil {
		t.Fatal(err)
	}
	n.Ready()
	n.Step(voteRequest(2, 2, 1, 1))
	n.Step(readAck(2, 1, 1, 1))
	n.Step(readAck(3, 1, 1, 1))
	wantReads(t, n, n.Ready())
	if err := n.ReadIndex(2); !errors.Is(err, ErrNotLeader) {
		t.Errorf("ReadIndex on a follower: error %v, want ErrNotLeader", err)
	}
	tickUntilElection(t, n)
	n.Step(voteResponse(2, 3, true))
	if err := n.ReadIndex(9); err != nil {
		t.Fatal(err)
	}
	n.Step(readAck(2, 3, 2, 1))
	if st := n.Status(); st.Role != Leader || st.CommitIndex != 2 {
		t.Fatalf("status = %+v, want the leader of term 3 with its no-op committed", st)
	}
	wantReads(t, n, n.Ready(), ReadState{ID: 9, Index: 2})
}

// A one-server cluster is its own majority, so it confirms a read at once.
func TestSingleServerConfirmsReadAtOnce(t *testing.T) {
	n := newReadyLeader(t, 1)
	if err := n.ReadIndex(3); err != nil {
		t.Fatal(err)
	}
	wantReads(t, n, n.Ready(), ReadState{ID: 3, Index: 1})
}

// A follower echoes the round of each AppendEntries it answers in the
// leader's term, whether it accepts or rejects it, so that the leader can
// tell which of its requests the reply answers.
func TestFollowerEchoesReadRound(t *testing.T) {
	n := newFollower(1, 1)
	for _, prevIndex := range []Index{1, 5} {
		req := appendFrom(1, prevIndex, 1, nil, 0)
		req.AppendEntries.ReadRound = uint64(prevIndex) + 10
		n.Step(req)
		m := oneAppendResponse(t, n.Ready())
		if m.ReadRound != req.AppendEntries.ReadRound {
			t.Errorf("reply to an AppendEntries after entry %d = %+v, want round %d", prevIndex, m, req.AppendEntries.ReadRound)
		}
	}
}

// oneAppendResponse returns the one message in rd, which must be an
// AppendEntriesResponse.
func oneAppendResponse(t *testing.T, rd Ready) *AppendEntriesResponse {
	t.Helper()
	if len(rd.Messages) != 1 || rd.Messages[0].AppendEntriesResponse == nil {
		t.Fatalf("Ready messages = %+v, want one AppendEntriesResponse", rd.Messages)
	}
	return rd.Messages[0].AppendEntriesResponse
}
