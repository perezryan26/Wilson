package storage

import (
	"fmt"
	"slices"

	"github.com/perezryan26/wilson/raft"
)

// Memory keeps a node's persistent state in memory, for tests and the
// simulator. A write is durable as soon as Save returns, and nothing
// outlives the process.
type Memory struct {
	hardState raft.HardState
	snapshot  *raft.Snapshot // the latest snapshot, or nil
	entries   []raft.Entry   // the log after the snapshot
}

// Save persists what rd holds for stable storage. The runtime calls it, and
// it must return, before any of rd's messages are sent. A snapshot, with
// the entries that follow it, replaces the whole stored log; otherwise
// entries replace any stored entries from rd.Entries[0].Index on. Save
// fails, changing nothing, only for what no correct node asks: entries
// that would leave a gap in the log, or a snapshot no newer than the
// stored one.
func (m *Memory) Save(rd raft.Ready) error {
	snap, entries := m.snapshot, m.entries
	if rd.Snapshot != nil {
		if rd.Snapshot.LastIncludedIndex <= snapshotIndex(snap) {
			return fmt.Errorf("storage: a snapshot through index %d is no newer than the stored one through %d",
				rd.Snapshot.LastIncludedIndex, snapshotIndex(snap))
		}
		s := *rd.Snapshot
		snap, entries = &s, nil
	}
	if len(rd.Entries) > 0 {
		base, first := snapshotIndex(snap), rd.Entries[0].Index
		if first <= base || first > base+raft.Index(len(entries))+1 {
			return fmt.Errorf("storage: entries start at index %d, but the log runs from %d to %d", first, base+1, base+raft.Index(len(entries)))
		}
		entries = append(entries[:first-base-1], rd.Entries...)
	}
	m.snapshot, m.entries = snap, entries
	if rd.HardState != nil {
		m.hardState = *rd.HardState
	}
	return nil
}

// Load returns the state to restart a node from: everything saved so far.
// The entries are a copy, so the node cannot change what is stored.
func (m *Memory) Load() (raft.RestoredState, error) {
	return raft.RestoredState{HardState: m.hardState, Snapshot: m.snapshot, Entries: slices.Clone(m.entries)}, nil
}

// snapshotIndex returns the last index s includes, or 0 if s is nil.
func snapshotIndex(s *raft.Snapshot) raft.Index {
	if s == nil {
		return 0
	}
	return s.LastIncludedIndex
}
