# ADR-0001: Deterministic core and the Ready contract

- Status: Accepted
- Date: 2026-10-27

## Context

Raft's hardest bugs live in rare interleavings: a vote granted just before a crash, a stale AppendEntries that arrives after a newer one, a leader that loses its majority mid-replication. When the algorithm shares goroutines, timers, disks, and sockets with the code around it, such a bug shows up once in thousands of runs and cannot be replayed.

The same algorithm code has to run in three settings:

- a deterministic simulator that controls time, message delivery, and crashes, and replays any failure from a seed;
- the MIT 6.5840 test suites, through an adapter that supplies the course's RPC library and persister;
- real processes, with real timers, disks, and sockets.

All three need the algorithm without its I/O.

Figure 2 also fixes an ordering rule: persistent state is "updated on stable storage before responding to RPCs." Where that rule is enforced decides whether a crash can make a server vote twice in one term or lose an entry it acknowledged.

## Decision

The `raft` package is a deterministic state machine with no goroutines, clock, disk, network, or global random source. Its contract is:

```go
func NewNode(cfg Config, restored RestoredState) *Node
func (n *Node) Tick()                       // advance one logical clock tick
func (n *Node) Step(m Message)              // handle an inbound RPC or response
func (n *Node) Propose(cmd []byte) (Index, Term, error)
func (n *Node) Ready() Ready                // effects produced since the last call
```

Log compaction, read-index reads, and membership changes add `Compact`, `ReadIndex`, and `ProposeConfChange` to this method set without changing the rules below.

- **One goroutine.** The runtime calls the core from a single goroutine; the core is not safe for concurrent use.
- **Logical time.** Time is counted in ticks. Each election draws its timeout from [T, 2T) ticks (§5.2) using an injected, seeded `*rand.Rand`.
- **No hidden nondeterminism.** No output depends on map iteration order; peers live in a sorted slice. The same seed and inputs produce byte-identical outputs.
- **Every effect leaves through `Ready`.** A `Ready` holds hard state to persist (`currentTerm`, `votedFor`), log entries to append or truncate, messages to send, committed entries to apply, and a snapshot to install. Calling `Ready` returns everything produced since the previous call and clears it.

The runtime handles each `Ready` completely, in this order, before it gives the core another input:

1. Persist hard state and entries, then fsync. A storage error stops the process (fail-stop).
2. Send messages.
3. Apply committed entries to the state machine in index order.

Step 1 before step 2 enforces Figure 2's rule in one place for every message type: a granted vote or a successful AppendEntries reply never leaves before the state it reflects is durable. Step 1 before step 3 means a server never applies an entry, or answers a client for it, before the entry is on its own disk.

**Alternatives considered**

- **Goroutines, locks, and timers inside the algorithm**, the usual shape of 6.5840 solutions. It is the shortest path to a first election, but failures cannot be replayed, and races can hide in the algorithm itself.
- **etcd/raft's `Ready` plus `Advance`.** The second call lets a runtime overlap storage writes with the next batch of work, at the cost of a core that tracks which effects are still in flight. A single-goroutine runtime gains nothing from it.
- **A core that calls injected storage and transport interfaces.** It keeps the ordering inside the core, but it puts blocking I/O and its errors into the algorithm and makes crash points harder for the simulator to place.

## Consequences

- The simulator can run a whole cluster in one goroutine on a virtual clock and replay any seed exactly. The MIT adapter and the production runtime each supply their own time, storage, and transport.
- Unit tests drive one core with `Tick` and `Step` and inspect the `Ready` it returns, with no sleeps.
- Races can only exist in the runtime, transport, kvserver, and client, which is where race-detector tests concentrate.
- Correct ordering is now the runtime's job. A runtime that sends before it syncs would break durability without failing any core test, so the runtime gets ordering tests of its own, and the simulator and chaos test check durability end to end.
- Disk writes and network sends never overlap, so a leader cannot write its log in parallel with replicating it (Ongaro's dissertation, §10.2.1). That costs latency, and clarity wins over that cost here.
- A failed fsync stops the process rather than being retried, because the state on disk is unknown after one.
