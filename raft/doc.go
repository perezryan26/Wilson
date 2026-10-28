// Package raft implements the Raft consensus algorithm as a deterministic
// state machine. It follows the extended Raft paper by Ongaro and
// Ousterhout, with Figure 2 as its specification, and names things as the
// paper does so that a reader can search the paper for any of them.
//
// The package has no goroutines, clock, disk, or network, and it imports
// only the standard library. A runtime drives it from a single goroutine
// with clock ticks, inbound messages, and proposals, and carries out the
// effects it returns in a fixed order: persist, then send, then apply.
// Because the same seed and inputs always produce the same effects, the
// simulator in internal/sim can replay any run exactly. See
// docs/adr/0001-deterministic-core.md.
package raft
