# Architecture

## The Ready contract

The Raft core in [`raft/`](../raft) is a deterministic state machine. It reads no clock, starts no goroutines, and does no I/O, and every effect it has leaves through a `Ready` ([ADR-0001](adr/0001-deterministic-core.md)). A runtime drives it from a single goroutine; the core is not safe for concurrent use.

```go
func NewNode(cfg Config, restored RestoredState) *Node
func (n *Node) Tick()                                   // advance one logical clock tick
func (n *Node) Step(m Message)                          // handle an inbound RPC request or response
func (n *Node) Propose(cmd []byte) (Index, Term, error) // append a command on the leader
func (n *Node) Ready() Ready                            // effects produced since the last call
func (n *Node) Status() Status                          // read-only role, term, leader, commitIndex, lastApplied
```

After every `Tick`, `Step`, or `Propose`, the runtime calls `Ready` and handles the result completely, in this order, before it gives the core another input:

1. **Persist.** Save `HardState`, when it is not nil, and `Entries` to stable storage, then fsync. When `Entries[0].Index` is at or below the last stored index, first discard every stored entry from that index on: the server is overwriting a conflicting suffix. A storage error stops the process, because after a failed fsync the state on disk is unknown.
2. **Send.** Deliver `Messages`.
3. **Apply.** Apply `CommittedEntries` to the state machine, in index order.

| Field | Holds | Step |
| --- | --- | --- |
| `HardState *HardState` | `currentTerm` and `votedFor`, when either changed since the last `Ready`; nil otherwise | 1 |
| `Entries []Entry` | Log entries not yet handed to storage, in index order | 1 |
| `Messages []Message` | RPC requests and responses to send | 2 |
| `CommittedEntries []Entry` | Entries committed since the last `Ready`, in index order, each released once per boot | 3 |

Persisting before sending is how Figure 2's rule, that persistent state is updated on stable storage before responding to RPCs, holds for every message. The core puts a vote and the reply granting it in the same `Ready`, and new entries and the reply acknowledging them likewise, so no reply leaves before the state it reflects is durable. Were it the other way round, a server that crashed between sending and persisting could restart, forget its vote, and vote again in the same term, and two candidates could win it; or forget entries a leader already counted toward a majority.

Persisting before applying means no entry reaches the state machine before it is on this server's disk. It is also why a leader may count its own entries toward a majority before they are fsynced: the commit becomes visible, as a `LeaderCommit` in a message or an applied entry, only after the `Ready` holding those entries was persisted.

`Ready` drains: it returns everything produced since the previous call and clears it. There is no acknowledgment call, because the runtime finishes one `Ready` before it calls the core again. `Entries` and `CommittedEntries`, like the entries inside messages, are copies, so nothing the core does later can change them.

This is the contract as the code implements it today. [ADR-0001](adr/0001-deterministic-core.md) describes it in full, including the snapshot that `Ready` gains with log compaction.

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
	for _, e := range rd.CommittedEntries {
		stateMachine.Apply(e)
	}
}
```

Two drivers exist today, both for testing. The three-node harness in [`raft/cluster_test.go`](../raft/cluster_test.go) runs this loop for every server in one goroutine, with [`storage.Memory`](../internal/storage/memory.go) for storage and an in-order message queue for the network. The deterministic simulator in [`internal/sim`](../internal/sim) runs the same loop on a virtual clock, with a network that delays, loses, duplicates, and partitions messages, and saves through the real file storage on a simulated disk that a crash can strike mid-write ([TESTING.md](TESTING.md#deterministic-simulator)). The runtime in `node/` is not written yet.

`NewNode` takes what storage held when the server last stopped (`RestoredState`, the zero value for a new server), and the server starts as a follower in that term, with that vote and that log. `commitIndex` and `lastApplied` are volatile, as in Figure 2: a restarted server starts both at 0, relearns what is committed from the leader, and applies its log again from the start.

## Storage

The runtime persists through the `Storage` interface in [`internal/storage`](../internal/storage): `Save(Ready)` returns once the `Ready`'s hard state and entries are durable, and `Load` returns what a server restarts from. `Memory` keeps both in memory, for tests. `FileStorage` keeps them in a write-ahead log (WAL) on disk, with a copy in memory for `Load`; [ADR-0003](adr/0003-wal-format.md) records why the format is what it is.

### Records

The WAL is a sequence of records. Every number in one is little-endian.

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | `length`: the size of `type` and `payload` together, at least 1 |
| 4 | 4 | `crc32c`: the CRC-32C (Castagnoli) checksum of `type` and `payload` |
| 8 | 1 | `type` |
| 9 | `length` − 1 | `payload` |

| `type` | Record | Payload |
| --- | --- | --- |
| 1 | Hard state | `currentTerm u64`, `votedFor u64` (0 for none): 16 bytes |
| 2 | Entry | `index u64`, `term u64`, an entry type byte (0, a normal entry), then the command, to the end of the record: 17 bytes plus the command |
| 3 | Truncation | `index u64`, the first of the stored entries to discard, all the rest with it: 8 bytes |

Type 0 and every type above 3 are invalid. So is an entry type byte other than 0, until the election no-op and configuration entries arrive. Here are three records byte by byte, in hex: the hard state of term 3 with a vote for server 2, entry 7 of term 3 with the command `x=1`, and a truncation from index 5. `TestRecordBytes` checks that the encoder produces exactly these.

```text
length       crc32c       type  payload
11 00 00 00  0b e5 89 04  01    03 00 00 00 00 00 00 00  02 00 00 00 00 00 00 00
                                currentTerm 3            votedFor 2

15 00 00 00  88 fb 3f 2f  02    07 00 00 00 00 00 00 00  03 00 00 00 00 00 00 00  00      78 3d 31
                                index 7                  term 3                   normal  "x=1"

09 00 00 00  c0 9b 6b ca  03    05 00 00 00 00 00 00 00
                                index 5
```

### Segments and saves

- The WAL lives in segment files in one directory, each named after the sequence number of its first record in 16 lowercase hex digits, such as `000000000000002a.wal` for a segment that begins with record 42. Records are numbered from 0 over the whole log, so the names sort in log order.
- `Save` writes a `Ready`'s records with one write and one fsync, in a fixed order: the hard state, if it changed; a truncation, if the entries start at or below the last stored index; then one record per entry. A crash may keep any prefix of them, and every prefix is a safe state to restart from: the term is never behind an entry's, and a truncated suffix held only entries that conflict with the leader's, which are not committed.
- Before a write, a segment that has reached the size limit (64 MiB by default) is closed and the next one is created. The directory is fsynced before any record goes into the new file, or a crash could make the file vanish with its records.
- After a write or fsync fails, every later `Save` fails too: what the disk holds is unknown, and the runtime must stop.

### Recovery

`OpenFileStorage` replays every segment in order, and each segment must begin with the record after the previous segment's last.

- **Torn final record.** In the last segment, a record that runs past the end of the file, its header or its body cut short, is a write torn by a crash, which no `Save` returned for. Recovery truncates the file where the record begins, fsyncs, and logs the segment, offset, and bytes dropped.
- **Corruption.** Anything else refuses to start with an error wrapping `ErrCorrupt` that names the segment and offset: a checksum mismatch, an empty or malformed record, an entry that is not the next index, a truncation outside the log, a record cut short in an earlier segment, or a segment missing from the sequence. Starting anyway could forget a vote or an acknowledged entry.

### File systems

`FileStorage` reaches the disk only through `FS`: `OpenFile`, `ReadDir`, and `SyncDir`, with a `File` that reads, writes, syncs, truncates, and closes. `OSFS` is the operating system's file system. `FaultFS` lives in memory and crashes on demand, at once or during the nth write, truncation, file creation, or fsync. A crash keeps everything synced and a random prefix of the rest, as a power failure would: of each file's writes and truncations since its last fsync, the last write possibly cut mid-record, and of the files created since their directory's last fsync. Crash tests and the simulator run file storage on it.

## Log replication

The log (`raftLog`) holds a sentinel entry at index 0 of term 0 ahead of the real entries, so the entry before the first always exists. It is the only code that turns a log index into a slice position.

- **Proposals.** `Propose` on the leader appends an entry of the current term and sends AppendEntries to every follower at once; on any other server it returns `ErrNotLeader`.
- **Sending.** For each follower the leader keeps `nextIndex` and `matchIndex`, reset after every election. Every AppendEntries carries all the entries from the follower's `nextIndex` on, so a heartbeat also retries whatever a lost message failed to deliver.
- **Receiving.** The follower checks the entry at `PrevLogIndex`, keeps the entries it already holds, truncates only at a real conflict, appends the rest, and raises `commitIndex` to `min(LeaderCommit, the request's last entry)`. A stale or repeated request truncates nothing.
- **Replies.** A success names what it acknowledges, `MatchIndex = PrevLogIndex + len(Entries)`, because replies arrive late, twice, or out of order. `matchIndex` only moves forward. A rejection names where the logs part, and the leader skips a whole conflicting term per round trip ([ADR-0004](adr/0004-fast-log-backtracking.md)).
- **Commitment.** The leader commits the highest index a majority stores, counting its own log, but only if that entry is from its current term (§5.4.2, Figure 8); earlier entries then commit with it.

## Time

The core counts time in ticks and never sees a duration. `DefaultConfig` returns the timing and the tick interval separately: a 10 ms tick, a heartbeat every 5 ticks (50 ms), and election timeouts drawn from [30, 60) ticks ([300 ms, 600 ms)).

- **Election timer.** Each timeout is drawn uniformly from [`ElectionTick`, 2 × `ElectionTick`) ticks with the injected `*rand.Rand`, again at every reset (§5.2). A follower's timer restarts only when it hears an AppendEntries from the current leader or grants a vote; a candidate's restarts when it starts an election (Figure 2). A refused vote never restarts it, so a candidate that cannot win cannot hold off an election by asking.
- **Heartbeats.** A leader sends an empty AppendEntries to every other server as soon as it is elected, then every `HeartbeatTick` ticks. `HeartbeatTick` must be below `ElectionTick`, so a follower hears from a healthy leader before it times out.
- **Vote retries.** Servers retry RPCs that get no timely response (§5.1). Every `HeartbeatTick` ticks, a candidate sends its RequestVote again to each server that has not answered this election, so a lost request or reply costs one heartbeat interval instead of a new election and term. A server that already granted the vote grants it again, and votes are counted by voter, so a retry never counts twice.

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
| [`node.go`](../raft/node.go) | `Config`, `DefaultConfig`, `Node`, `NewNode`, `Tick`, `Step`, `Propose`, `Ready`, `Status`, and the three `become*` functions |
| [`log.go`](../raft/log.go) | The log, `raftLog`: lookups, appends, and the merge that carries out AppendEntries steps 3 and 4 |
| [`election.go`](../raft/election.go) | The election timer, `campaign` and its RequestVote retries, the RequestVote receiver, vote counting, and the §5.4.1 up-to-date check |
| [`append_entries.go`](../raft/append_entries.go) | Sending AppendEntries, the receiver's steps 1–5, the leader's handling of replies, fast backtracking, and the commit rule |

## Files in internal/storage/

| File | Contents |
| --- | --- |
| [`storage.go`](../internal/storage/storage.go) | `Storage`, the interface the runtime persists through |
| [`memory.go`](../internal/storage/memory.go) | `Memory`, storage in memory for tests |
| [`file.go`](../internal/storage/file.go) | `FileStorage`, `OpenFileStorage`, and `FileOptions`: a `Ready` into WAL records, and records back into hard state and a log |
| [`wal.go`](../internal/storage/wal.go) | Segments, appends, rotation, and recovery; `ErrCorrupt` |
| [`record.go`](../internal/storage/record.go) | The record layout, its encoder, and its decoder |
| [`fs.go`](../internal/storage/fs.go) | `FS`, `File`, and `OSFS` |
| [`faultfs.go`](../internal/storage/faultfs.go) | `FaultFS`, the in-memory file system that crashes on demand; `ErrCrashed` |
