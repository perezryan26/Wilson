// Package kv is the replicated key-value state machine.
//
// It decodes committed log entries into Get, Put, and Delete commands and
// applies them in log order. A per-client session table answers a retried
// write from its recorded response instead of applying it again (§8), and
// the table is part of every snapshot (§7).
package kv
