// Package client is the Go client library for the key-value store.
//
// It finds the leader, following the hints of servers that do not lead,
// and retries after a timeout or a failure, with a backoff. Before its
// first write it registers a session, whose ID the cluster assigns, and
// it numbers every write in that session, retrying a write with the same
// number so that the servers apply it only once (§8). A write that no
// server answered in time returns ErrUnknownOutcome. The package uses
// only the standard library.
package client
