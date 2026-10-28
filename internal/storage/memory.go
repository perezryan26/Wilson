package storage

import "github.com/perezryan26/wilson/raft"

// Memory keeps a node's persistent state in memory, for tests and the
// simulator. A write is durable as soon as Save returns, and nothing
// outlives the process.
type Memory struct {
	hardState raft.HardState
}

// Save persists what rd holds for stable storage. The runtime calls it, and
// it must return, before any of rd's messages are sent. Memory never fails,
// so the error is always nil.
func (m *Memory) Save(rd raft.Ready) error {
	if rd.HardState != nil {
		m.hardState = *rd.HardState
	}
	return nil
}

// Load returns the state to restart a node from: everything saved so far.
func (m *Memory) Load() (raft.RestoredState, error) {
	return raft.RestoredState{HardState: m.hardState}, nil
}
