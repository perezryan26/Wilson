// Package kvtest runs a cluster of key-value servers inside a test, for
// the tests of kvserver and of the client library. Each server is a raft
// node on memory storage that ticks every millisecond, over a network
// that hands each message straight to its receiver, and serves the
// key-value API on its own HTTP server on 127.0.0.1. A test can stop and
// restart servers, and cut them off from the others.
package kvtest
