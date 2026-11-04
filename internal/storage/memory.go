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
	entries   []raft.Entry // the log, from index 1
}

// Save persists what rd holds for stable storage. The runtime calls it, and
// it must return, before any of rd's messages are sent. Entries replace any
// stored entries from rd.Entries[0].Index on. Save fails, changing nothing,
// only if the entries would leave a gap in the log, which no correct node
// asks for.
func (m *Memory) Save(rd raft.Ready) error {
	if len(rd.Entries) > 0 {
		first := rd.Entries[0].Index
		if first < 1 || first > raft.Index(len(m.entries))+1 {
			return fmt.Errorf("storage: entries start at index %d, but the log ends at index %d", first, len(m.entries))
		}
		m.entries = append(m.entries[:first-1], rd.Entries...)
	}
	if rd.HardState != nil {
		m.hardState = *rd.HardState
	}
	return nil
}

// Load returns the state to restart a node from: everything saved so far.
// The entries are a copy, so the node cannot change what is stored.
func (m *Memory) Load() (raft.RestoredState, error) {
	return raft.RestoredState{HardState: m.hardState, Entries: slices.Clone(m.entries)}, nil
}
