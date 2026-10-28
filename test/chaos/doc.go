// Package chaos is the multi-process chaos test.
//
// It starts real kvnode processes on localhost behind test-controlled TCP
// proxies, kills and restarts them, partitions links, and runs concurrent
// clients, then checks the recorded history for linearizability and for
// lost acknowledged writes.
package chaos
