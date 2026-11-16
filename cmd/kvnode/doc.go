// Kvnode runs one server of the replicated key-value store: a raft node,
// its file storage, the HTTP transport to its peers, and the key-value
// API for clients.
//
// Usage:
//
//	kvnode -id 1 -peers 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 -data /var/lib/wilson/n1
//
// It creates the data directory if it is missing, recovers what the
// directory holds, and serves its peers' raft messages and its clients'
// requests on its own address from -peers, or on -listen. -reads picks
// how it serves a Get: index, the default, confirms with a round of
// heartbeats that it still leads (ReadIndex), and log runs the Get
// through the log. Every log line names the node and its term and role.
// SIGINT or SIGTERM shuts it down gracefully; a storage error stops it
// with exit status 1, because after a failed write or fsync what the
// disk holds is unknown.
//
// The client API, which kvctl and the client package use:
//
//	POST   /v1/sessions                   register a session
//	GET    /v1/kv/{key}                   read a key
//	PUT    /v1/kv/{key}?session=S&seq=N   write a key; the body is the value
//	DELETE /v1/kv/{key}?session=S&seq=N   delete a key
//	GET    /v1/status                     the server's role, term, and leader
package main
