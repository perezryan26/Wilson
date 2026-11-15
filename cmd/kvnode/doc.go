// Kvnode runs one server of the replicated key-value store: a raft node,
// its file storage, and the HTTP transport to its peers.
//
// Usage:
//
//	kvnode -id 1 -peers 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 -data /var/lib/wilson/n1
//
// It creates the data directory if it is missing, recovers what the
// directory holds, and serves its peers' raft messages on its own address
// from -peers, or on -listen. Every log line names the node and its term
// and role. SIGINT or SIGTERM shuts it down gracefully; a storage error
// stops it with exit status 1, because after a failed write or fsync what
// the disk holds is unknown.
//
// Until the key-value service exists, the state machine is a list of the
// commands applied. POST /debug/propose proposes the request body as a
// command, and GET /debug/state shows the node's role, term, leader,
// commitIndex, lastApplied, and every command it applied.
package main
