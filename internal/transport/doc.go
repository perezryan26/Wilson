// Package transport carries raft messages between nodes.
//
// It defines the interface the runtime sends through and an implementation
// that posts JSON-encoded messages over HTTP/1.1 with net/http (see
// docs/adr/0002-http-json-transport.md). Raft tolerates lost messages, so
// the transport drops a message rather than block when a peer is slow or
// unreachable.
package transport
