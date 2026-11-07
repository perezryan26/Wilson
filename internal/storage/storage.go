package storage

import "github.com/perezryan26/wilson/raft"

// Storage keeps what Figure 2 requires a server to hold on stable
// storage: currentTerm, votedFor, and the log. The runtime saves each
// Ready's hard state and entries through it before sending the Ready's
// messages, and a server restarts from what Load returns.
type Storage interface {
	// Save persists rd.HardState, if it is not nil, and rd.Entries, and
	// returns only once they are durable. Entries replace every stored
	// entry from rd.Entries[0].Index on.
	Save(rd raft.Ready) error
	// Load returns everything saved so far.
	Load() (raft.RestoredState, error)
}

var (
	_ Storage = (*Memory)(nil)
	_ Storage = (*FileStorage)(nil)
)
