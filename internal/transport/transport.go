package transport

import "github.com/perezryan26/wilson/raft"

// Transport carries raft messages to other servers. The runtime sends
// every message of a Ready through it, from its one goroutine, so Send
// must never block. Raft tolerates lost messages and retries what it
// needs, so a transport may drop a message rather than wait.
type Transport interface {
	// Send hands m over for delivery to server m.To and returns at once.
	Send(m raft.Message)
}

var _ Transport = (*HTTP)(nil)
