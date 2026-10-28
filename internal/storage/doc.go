// Package storage keeps a raft node's hard state, log, and snapshots.
//
// It defines the interface the runtime persists through, an in-memory
// implementation for tests and the simulator, and a file implementation
// built on a segmented write-ahead log and snapshot files. A write is
// complete only once it is fsynced; the runtime depends on that to keep a
// reply from leaving before the state it reflects is durable.
package storage
