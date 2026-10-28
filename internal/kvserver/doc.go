// Package kvserver is a node's client-facing HTTP API.
//
// It turns client requests into proposals to the local raft node, waits
// until each command is applied, and replies with the result. A follower
// points the client at the leader it knows of, and a request whose outcome
// cannot be known in time says so explicitly, so the client can retry it
// safely.
package kvserver
