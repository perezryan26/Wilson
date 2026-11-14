package storage

import "github.com/perezryan26/wilson/raft"

// Storage keeps what Figure 2 requires a server to hold on stable
// storage, currentTerm, votedFor, and the log, and the snapshot that
// replaced the log's earlier entries (§7). The runtime saves each
// Ready's hard state, snapshot, and entries through it before sending
// the Ready's messages, and a server restarts from what Load returns.
type Storage interface {
	// Save persists rd.HardState, if it is not nil, rd.Snapshot, if it is
	// not nil, and rd.Entries, and returns only once they are durable. A
	// snapshot and the entries saved with it replace the whole stored
	// log; otherwise entries replace every stored entry from
	// rd.Entries[0].Index on.
	Save(rd raft.Ready) error
	// Load returns everything saved so far.
	Load() (raft.RestoredState, error)
}

var (
	_ Storage = (*Memory)(nil)
	_ Storage = (*FileStorage)(nil)
)
