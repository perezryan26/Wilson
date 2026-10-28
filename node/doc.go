// Package node runs a raft core inside a real process.
//
// It owns the core's goroutine, turns wall-clock time into ticks, and
// carries out each batch of effects the core returns in the order that
// docs/adr/0001-deterministic-core.md requires: save and fsync through
// storage, send through the transport, then apply committed entries. It is
// the only layer that touches time, disk, and the network; the simulator
// and the MIT 6.5840 test adapter supply their own versions of all three.
//
// Like raft, node stays outside internal/ because the MIT adapter compiles
// inside the course's own module, and Go forbids importing another module's
// internal packages.
package node
