// Package sim is a deterministic simulator for clusters of raft cores.
//
// It runs every server in one goroutine on a virtual clock. A single
// seeded random source decides every message loss, delay, reordering, and
// duplication, every partition, crash, and restart, and each server's
// clock skew. Each server keeps its hard state and log in the real file
// storage, on a simulated disk that a crash can strike in the middle of a
// write, losing what was not yet synced. After every event the simulator
// checks safety invariants, among them the five properties of Figure 3. A
// failing run prints its seed and a readable trace, and rerunning that
// seed reproduces the run exactly.
package sim
