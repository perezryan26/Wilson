# Architecture

## The Ready contract

The Raft core in [`raft/`](../raft) is a deterministic state machine. It reads no clock, starts no goroutines, and does no I/O, and every effect it has leaves through a `Ready` ([ADR-0001](adr/0001-deterministic-core.md)). A runtime drives it from a single goroutine; the core is not safe for concurrent use.

```go
func NewNode(cfg Config, restored RestoredState) *Node
func (n *Node) Tick()          // advance one logical clock tick
func (n *Node) Step(m Message) // handle an inbound RPC request or response
func (n *Node) Ready() Ready   // effects produced since the last call
func (n *Node) Status() Status // read-only role, term, and leader
```

After every `Tick` or `Step`, the runtime calls `Ready` and handles the result completely, in this order, before it gives the core another input:

1. **Persist.** Save `HardState`, when it is not nil, to stable storage and fsync. A storage error stops the process, because after a failed fsync the state on disk is unknown.
2. **Send.** Deliver `Messages`.

| Field | Holds | Step |
| --- | --- | --- |
| `HardState *HardState` | `currentTerm` and `votedFor`, when either changed since the last `Ready`; nil otherwise | 1 |
| `Messages []Message` | RPC requests and responses to send | 2 |

Persisting before sending is how Figure 2's rule, that persistent state is updated on stable storage before responding to RPCs, holds for every message. The core puts a vote and the reply granting it in the same `Ready`, so the reply cannot leave before the vote is durable. Were it the other way round, a server that crashed between sending and persisting could restart, forget its vote, and vote again in the same term, and two candidates could win it.

`Ready` drains: it returns everything produced since the previous call and clears it. There is no acknowledgment call, because the runtime finishes one `Ready` before it calls the core again.

This is the contract as the code implements it today. [ADR-0001](adr/0001-deterministic-core.md) describes it in full, including the log entries to append, the committed entries to apply after sending, and the snapshots that `Ready` gains with log replication and compaction.

## Driving the core

Every driver runs the same loop:

```text
for each input, a tick or an inbound message {
	node.Tick() or node.Step(m)
	rd := node.Ready()
	storage.Save(rd) // persist and fsync; a failure stops the process
	for _, m := range rd.Messages {
		transport.Send(m)
	}
}
```

Today the only driver is the three-node test harness in [`raft/cluster_test.go`](../raft/cluster_test.go). It runs this loop for every server in one goroutine, with [`storage.Memory`](../internal/storage/memory.go) for storage and an in-order message queue for the network. The runtime in `node/` is not written yet.

`NewNode` takes what storage held when the server last stopped (`RestoredState`, the zero value for a new server), and the server starts as a follower in that term with that vote.

## Time

The core counts time in ticks and never sees a duration. `DefaultConfig` returns the timing and the tick interval separately: a 10 ms tick, a heartbeat every 5 ticks (50 ms), and election timeouts drawn from [30, 60) ticks ([300 ms, 600 ms)).

- **Election timer.** Each timeout is drawn uniformly from [`ElectionTick`, 2 × `ElectionTick`) ticks with the injected `*rand.Rand`, again at every reset (§5.2). A follower's timer restarts only when it hears an AppendEntries from the current leader or grants a vote; a candidate's restarts when it starts an election (Figure 2). A refused vote never restarts it, so a candidate that cannot win cannot hold off an election by asking.
- **Heartbeats.** A leader sends an empty AppendEntries to every other server as soon as it is elected, then every `HeartbeatTick` ticks. `HeartbeatTick` must be below `ElectionTick`, so a follower hears from a healthy leader before it times out.

## Messages

A `Message` names its sender and receiver and carries exactly one RPC, as a pointer to a struct named after the paper with Figure 2's fields: `RequestVoteRequest`, `RequestVoteResponse`, `AppendEntriesRequest`, and `AppendEntriesResponse`. A response is a message of its own, not a return value, so it can wait in a `Ready` until the state it reflects is durable.

`Step` applies Figure 2's rule for all servers before any handler runs: a request or response from a later term moves the server to that term as a follower. Each handler can then assume the message's term is at most `currentTerm`.

## Roles

A server is a follower, a candidate, or a leader (§5.1). Roles change only in `becomeFollower`, `becomeCandidate`, and `becomeLeader`, and each is the one place its role's state is set.

| From | To | When |
| --- | --- | --- |
| follower | candidate | its election timeout elapses |
| candidate | candidate | its election timeout elapses with no winner; it starts a new election in the next term |
| candidate | leader | a majority of the cluster, itself included, has granted it a vote this term |
| candidate | follower | it hears AppendEntries from a leader of its term or later, or any message from a later term |
| leader | follower | it hears any message from a later term |

A leader that hears an AppendEntries from another leader of its own term panics: two leaders in one term means Election Safety is already broken.

## Determinism

The same seed and the same inputs produce the same sequence of `Ready` values.

- `raft/` imports only the standard library and uses no clock, goroutines, channels, or global random source. Election timeouts come from the `*rand.Rand` in `Config`.
- `NewNode` sorts the peers. Messages to all peers go out in that order, and votes are counted by walking the sorted peers, never a map.

## Files in raft/

| File | Contents |
| --- | --- |
| [`types.go`](../raft/types.go) | `Term`, `Index`, `NodeID`, `Entry`, `HardState`, `Message` and the RPC structs, `Ready` |
| [`node.go`](../raft/node.go) | `Config`, `DefaultConfig`, `Node`, `NewNode`, `Tick`, `Step`, `Ready`, `Status`, and the three `become*` functions |
| [`election.go`](../raft/election.go) | The election timer, `campaign`, the RequestVote receiver, vote counting, and the §5.4.1 up-to-date check |
| [`append_entries.go`](../raft/append_entries.go) | Heartbeats and the AppendEntries receiver |
