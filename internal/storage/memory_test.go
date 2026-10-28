package storage

import (
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
