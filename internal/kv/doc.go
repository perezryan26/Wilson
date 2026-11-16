// Package kv is the replicated key-value state machine.
//
// It decodes committed log entries into Register, Get, Put, and Delete
// commands and applies them in log order. Each client registers a
// session first, whose ID is the log index of its Register, and numbers
// its writes. A session's record answers a retried write instead of
// applying it again (§8), and once the store holds its limit of sessions,
// a new one evicts the least recently active, the same one on every
// server. Snapshots hold every key and every session (§7). Commands and
// snapshots have fixed, versioned byte layouts
// (docs/adr/0007-client-sessions.md). The package uses only the standard
// library and knows nothing of Raft beyond a log index.
package kv
