// Package client is the Go client library for the key-value store.
//
// It finds the leader, retries after a timeout or redirect, and tags every
// write with a client ID and sequence number so that the servers apply a
// retried write only once (§8).
package client
