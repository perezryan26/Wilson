// Kvctl is the command-line client for the key-value store.
//
// Usage:
//
//	kvctl [-servers host:port,...] [-timeout 10s] get KEY
//	kvctl [-servers host:port,...] [-timeout 10s] put KEY VALUE
//	kvctl [-servers host:port,...] [-timeout 10s] delete KEY
//	kvctl [-servers host:port,...] [-timeout 10s] status
//
// The servers default to 127.0.0.1:8001 to 8003, where
// scripts/cluster.sh starts a cluster. get prints the value; get and
// delete of a key that does not exist exit with status 1. status prints
// each server's role, term, leader, commitIndex, and lastApplied. A write
// that no server answered in time says that its outcome is unknown.
package main
