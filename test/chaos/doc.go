// Package chaos is the multi-process chaos test.
//
// It starts five real kvnode processes on localhost with a test-controlled
// TCP proxy on every directed link between them, so the servers carry no
// test hooks. It kills servers with SIGKILL and restarts them from their
// disks, and cuts and heals links through the proxies, while concurrent
// clients use the client library. Porcupine then checks the clients'
// history for linearizability; the whole cluster is killed and restarted,
// and every key is read back to check that no acknowledged write was lost.
//
// The test runs only when given a duration, as make chaos DURATION=30m
// does, and nightly in CI.
package chaos
