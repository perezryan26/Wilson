// Package kvserver is a node's client-facing HTTP API.
//
// It turns client requests into commands proposed to the local raft node,
// waits until each command applies, and answers with its result. A Get
// goes through ReadIndex, which confirms that the node still leads, or,
// if so configured, through the log as a command (§8). A server that
// does not lead names the leader it knows of, and a request that cannot
// finish in time says that its outcome is unknown, so the client can
// send it again, with the same sequence number, safely.
package kvserver
