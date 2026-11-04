// Package sim is a deterministic simulator for clusters of raft cores.
//
// It runs every server in one goroutine on a virtual clock. A single
// seeded random source decides every message loss, delay, reordering, and
// duplication, every partition, crash, and restart, and each server's
// clock skew. After every event it checks safety invariants, among them
// Election Safety from Figure 3. A failing run prints its seed and a
// readable trace, and rerunning that seed reproduces the run exactly.
package sim
