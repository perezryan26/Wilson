package storage

import (
	"slices"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

// Load returns the hard state of the last Save that carried one; a Ready
// whose hard state is unchanged leaves it alone.
func TestMemoryKeepsLatestHardState(t *testing.T) {
	var m Memory
	steps := []struct {
		save *raft.HardState
		want raft.HardState
	}{
		{nil, raft.HardState{}},
		{&raft.HardState{CurrentTerm: 1, VotedFor: 2}, raft.HardState{CurrentTerm: 1, VotedFor: 2}},
		{nil, raft.HardState{CurrentTerm: 1, VotedFor: 2}},
		{&raft.HardState{CurrentTerm: 3}, raft.HardState{CurrentTerm: 3}},
	}
	for i, step := range steps {
		if err := m.Save(raft.Ready{HardState: step.save}); err != nil {
			t.Fatalf("step %d: Save: %v", i, err)
		}
		got, err := m.Load()
		if err != nil {
			t.Fatalf("step %d: Load: %v", i, err)
		}
		if got.HardState != step.want {
			t.Errorf("step %d: loaded %+v, want %+v", i, got.HardState, step.want)
		}
	}
}

// Entries are appended in index order, and entries that start at or below
// the last stored index first truncate the log there, as a follower
// overwriting a conflict needs. A gap is refused and changes nothing.
func TestMemoryAppendsAndTruncatesEntries(t *testing.T) {
	var m Memory
	e := func(index raft.Index, term raft.Term) raft.Entry { return raft.Entry{Index: index, Term: term} }
	steps := []struct {
		save []raft.Entry
		ok   bool
		want []raft.Entry
	}{
		{[]raft.Entry{e(1, 1), e(2, 1), e(3, 1)}, true, []raft.Entry{e(1, 1), e(2, 1), e(3, 1)}},
		{[]raft.Entry{e(2, 2)}, true, []raft.Entry{e(1, 1), e(2, 2)}},
		{[]raft.Entry{e(3, 2), e(4, 2)}, true, []raft.Entry{e(1, 1), e(2, 2), e(3, 2), e(4, 2)}},
		{[]raft.Entry{e(6, 2)}, false, []raft.Entry{e(1, 1), e(2, 2), e(3, 2), e(4, 2)}},
	}
	for i, step := range steps {
		if err := m.Save(raft.Ready{Entries: step.save}); (err == nil) != step.ok {
			t.Fatalf("step %d: Save error = %v, want success %t", i, err, step.ok)
		}
		got, err := m.Load()
		if err != nil {
			t.Fatalf("step %d: Load: %v", i, err)
		}
		if !slices.EqualFunc(got.Entries, step.want, func(a, b raft.Entry) bool { return a.Index == b.Index && a.Term == b.Term }) {
			t.Errorf("step %d: loaded %v, want %v", i, got.Entries, step.want)
		}
	}
	loaded, _ := m.Load()
	loaded.Entries[0].Term = 9
	if again, _ := m.Load(); again.Entries[0].Term != 1 {
		t.Error("changing loaded entries changed the stored log")
	}
}
