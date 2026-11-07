// Package storage keeps a raft node's hard state and log, and with log
// compaction its snapshots.
//
// Storage is the interface the runtime persists through. Memory
// implements it in memory, for tests and the simulator, and FileStorage
// implements it with a segmented write-ahead log on disk
// (docs/adr/0003-wal-format.md). A write is complete only once it is
// fsynced; the runtime depends on that to keep a reply from leaving
// before the state it reflects is durable.
//
// FileStorage reaches the disk only through FS. OSFS is the operating
// system's file system, and FaultFS is one in memory that crashes on
// demand and loses what was not synced, for crash tests.
package storage
