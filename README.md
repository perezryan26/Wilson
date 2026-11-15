# Wilson

A linearizable key-value store built on the Raft consensus algorithm, written in Go from scratch and meant to be read beside the [extended Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf).

> **Status: runtime and binaries.** The Raft core elects leaders and replicates a log, so committed commands are applied in the same order on every server. Each server compacts its log into snapshots, and a leader catches lagging followers up by sending its snapshot in chunks. Each server keeps its hard state, log, and latest snapshot on disk, in a form that survives a crash at any instant. Real `kvnode` processes now run the core over HTTP, and `scripts/cluster.sh` starts a local cluster. Unit tests, scripted scenarios from the paper's Figures 7 and 8, crash tests at every write boundary of the storage, a deterministic simulator, and an integration test that kills a leader with `kill -9` show it. The simulator injects partitions, message faults, crashes in the middle of disk writes, and frequent compaction. The key-value service is not written yet: until it is, each server replicates a list of commands. Each section below says what exists today.

## Overview and guarantees

Wilson is a replicated key-value store that keeps serving Get, Put, and Delete while a majority of its servers are up. It implements §5–§8 of the paper by Ongaro and Ousterhout: leader election, log replication, safety, membership changes, log compaction, and client interaction. The code is written to be read and maintained by people, with the paper open beside it.

The table lists the guarantees the system is built to provide and the evidence each one will rest on. So far the five Figure 3 properties and durability have evidence. The deterministic simulator checks each property after every event under thousands of randomized fault schedules. Its servers crash mid-write, restart from their snapshot and write-ahead log, and compact as often as every entry. Unit tests, scenarios from the paper, and crash tests of the storage at every write boundary add to that evidence (see [Verification](#verification)). The chaos test and the other guarantees wait for the code they depend on. The first five rows are the paper's Figure 3 properties.

| Guarantee | Meaning | Verified by |
| --- | --- | --- |
| Election Safety | At most one leader per term | Simulator invariant; MIT Lab 3A |
| Leader Append-Only | A leader never overwrites or deletes its own entries | Simulator invariant |
| Log Matching | Same index and term means identical logs up to that index | Simulator invariant; Figure 7 fixtures |
| Leader Completeness | A committed entry is in every later leader's log | Simulator invariant; Figure 8 scenario; MIT Lab 3C |
| State Machine Safety | No two servers apply different commands at one index | Simulator invariant; MIT Lab 3B–3D |
| Durability | An acknowledged write survives crashes of any minority and restarts of the whole cluster | Crash-point storage tests; simulator crash runs; chaos test |
| Linearizability | Each Get, Put, and Delete appears to take effect at one instant between call and return | Porcupine on simulator and chaos histories; MIT Lab 4 |
| At-most-once writes | A retried write is applied once | Session dedup tests; Porcupine |

## Quick start

You need Go. `go.mod` pins the toolchain (Go 1.27.3), and any Go from 1.21 on downloads that version automatically. On macOS, `brew install go` is enough.

```sh
git clone https://github.com/perezryan26/Wilson.git wilson
cd wilson
make check
```

### Running a cluster

```sh
scripts/cluster.sh      # three servers on 127.0.0.1:8001 to 8003; Ctrl-C stops them all
scripts/cluster.sh 5    # five servers
```

The script builds `kvnode` and keeps each server's data in `DATA/nN`, by default under the system's temporary directory. The data outlives the run, so running the script again restarts the same cluster from its disks; remove the directory to start afresh. Until the key-value service exists, each server's state machine is the list of commands it applied, and two debug endpoints drive it:

```sh
curl -s 127.0.0.1:8001/debug/state                # role, term, leader, commitIndex, lastApplied, and the commands applied
curl -s -d hello 127.0.0.1:8001/debug/propose     # at the leader; a follower answers 421 and names the leader
```

`go run ./cmd/kvnode -h` lists the flags: `-id`, `-peers` (every server as `id=host:port`, this one included), `-data`, `-listen` (by default the server's own address in `-peers`), `-preset` (timing; `default` is 10 ms ticks, 50 ms heartbeats, and 300–600 ms election timeouts), and `-compact-every` (entries between snapshots, 1,000 by default). Every log line names the node, its term, and its role. SIGINT or SIGTERM shuts a server down gracefully. `kvctl` is still a placeholder.

### Operator notes

- **Storage must keep its promises.** Raft assumes stable storage (Figure 2). A server's votes and log entries count toward elections and commitment only because they survive a crash, so `kvnode` fsyncs every write before it answers. Never disable fsync, and never run on a disk or file system that acknowledges an fsync without making the data durable, such as a drive with a volatile write cache that ignores flushes. A server that loses entries it acknowledged can make the cluster lose a committed entry, and one that loses its vote can help elect two leaders in a term. It also stalls its leader for good: the leader never moves that server's `nextIndex` back to an entry the server acknowledged, so it resends the same entries forever.
- **A lost disk makes a new server.** Never restart a server under its old ID with a data directory that was wiped, replaced, or restored from an old backup: it would have forgotten its votes and its log, with the consequences above. Such a server has to rejoin as a new server through a membership change. Membership changes are not built yet, so until they are, the cluster carries on without that server while a majority is up.
- **The data directory.** `kvnode` creates `-data` if it is missing (its parent must exist) and fsyncs the parent, so that the directory survives a crash. Give each server a directory of its own.
- **Storage errors stop the server.** After a failed write or fsync, what the disk holds is unknown, so `kvnode` stops with exit status 1 rather than carry on. Check the disk before restarting it. Recovery truncates a record that a crash tore at the end of the log, and refuses to start on any other damage rather than guess.
- **Restarting the whole cluster.** A leader commits entries from earlier terms only together with an entry of its own term (§5.4.2, Figure 8). After a full restart, the servers apply their old commands again once a new command commits. The election no-op that makes this immediate arrives with the key-value service.
- **No security.** There is no TLS and no authentication. Anyone who can reach a server's port can send it Raft messages and proposals, so run clusters on a trusted network.

## Architecture

The Raft algorithm lives in a deterministic core with no goroutines, clock, disk, or network. Everything else is a thin layer that feeds the core events and carries out the effects it returns ([ADR-0001](docs/adr/0001-deterministic-core.md)).

```text
        kvctl, client/
              │ HTTP/JSON
              ▼
  ┌───────────────────────┐  result  ┌──────────────────────┐
  │ internal/kvserver     │◀─────────│ internal/kv          │
  └───────────┬───────────┘          └──────────▲───────────┘
              │ propose                         │ 3. apply
              ▼                                 │
  ┌───────────────────────┐                     │
  │ node/                 │─────────────────────┘
  │ one goroutine: ticks, │  1. save, fsync  ┌──────────────────────┐
  │ messages, proposals   │─────────────────▶│ internal/storage     │
  │                       │  2. send         ├──────────────────────┤
  │                       │◀────────────────▶│ internal/transport   │◀──▶ peers
  └───────┬───────▲───────┘                  └──────────────────────┘
          │       │
 Tick,    │       │ Ready: hard state, entries, messages,
 Step,    │       │ committed entries
 Propose  ▼       │
  ┌───────────────┴───────┐
  │ raft/                 │  deterministic: no goroutines,
  └───────────────────────┘  clock, disk, or network
```

Only `node/` touches time, disk, and the network. The simulator and the MIT 6.5840 test adapter take its place, each with its own clock, storage, and network.

Every package below exists with its package documentation. `raft/`, `node/`, `internal/storage/`, `internal/transport/`, `internal/sim/`, and `cmd/kvnode/` have code so far.

- **`raft/`** is the core: roles, RPC handlers, the log, the commit rule, snapshots, and membership. It imports only the standard library, and the same seed and inputs always produce the same outputs. So far it implements leader election, log replication, and log compaction: the three roles, the randomized election timer, RequestVote, proposals, AppendEntries with the consistency check and fast backtracking, the commit rule, `Compact`, and InstallSnapshot in chunks. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) describes its contract with the runtime.
- **`node/`** is the runtime. One goroutine feeds the core ticks from a wall-clock ticker, inbound messages, and proposals, then handles each `Ready` in order: persist and fsync, send, restore and apply. It snapshots the state machine every `CompactEvery` entries. A storage error stops the node for good, and `kvnode` then exits. `Propose` returns once the new entry is durable on this server.
- **`internal/storage/`** persists hard state, log entries, and snapshots. `FileStorage` keeps a segmented write-ahead log and the latest snapshot on disk, and fsyncs every save before returning ([ADR-0003](docs/adr/0003-wal-format.md), [ADR-0005](docs/adr/0005-snapshot-format.md)). A snapshot is written whole and starts the log over, after which the segments it replaces are removed. Recovery survives a crash at any instant: it truncates a record torn by the crash and refuses to start on any other damage. `Memory` keeps the same state in memory, for tests. File storage reaches the disk through a small file-system interface, so tests and the simulator run it on `FaultFS`, which crashes on demand and loses what was not synced.
- **`internal/transport/`** carries Raft messages between nodes as JSON over HTTP/1.1 ([ADR-0002](docs/adr/0002-http-json-transport.md)). Each message is a POST to `/raft/v1/message`. One sender goroutine per peer drains a bounded queue that drops messages on overflow, every request has a timeout, and a server refuses a body over 4 MiB. The core caps the entries and the snapshot chunk in one message at 1 MiB each, so every message fits.
- **`internal/kv/`** is the replicated state machine: the command encoding, Get, Put, and Delete, and the per-client session table that makes retried writes apply once.
- **`internal/kvserver/`** is the client-facing HTTP API. It proposes commands, waits for them to apply, and points clients at the leader.
- **`client/`** is the Go client library: leader discovery, retries, client IDs, and sequence numbers.
- **`internal/sim/`** is the deterministic simulator: a virtual clock, a simulated network with injected faults, crashes and restarts, and an invariant checker that runs after every event. Each server keeps its log and snapshots in the real file storage on a simulated disk that a crash can strike mid-write, and compacts as the seed chooses. So far it checks the five Figure 3 properties, that terms never fall and votes are never cast twice in a term, even across restarts, the order entries are applied in, and that every snapshot holds exactly the commands applied through it, plus liveness once faults heal.
- **`internal/linearizability/`** holds the Porcupine model and the history recorder.
- **`cmd/kvnode/`** and **`cmd/kvctl/`** are the server and CLI binaries. `kvnode` runs one server from its flags, as [Running a cluster](#running-a-cluster) shows; `kvctl` is still a placeholder.
- **`test/chaos/`** is the multi-process chaos test.

`raft/` and `node/` stay outside `internal/` on purpose. The MIT 6.5840 adapter compiles inside the course's own Go module, and Go forbids importing another module's `internal/` packages.

## Paper-to-code map

Each row names a concept or rule from the paper, the code that implements it, and the tests that cover it. The rows cover §5.1–§5.6, every rule of Figure 2, the Figure 3 properties, and §7 with Figures 12 and 13. Every test named here is in `raft/`, except those marked (sim), which are in `internal/sim/`; (storage), in `internal/storage/`; (node), in `node/`; and (kvnode), in `cmd/kvnode/`.

| Paper | Rule | Code | Tests |
| --- | --- | --- | --- |
| Figure 2, State | `currentTerm`, `votedFor`, and `log[]` reach stable storage before the server responds to an RPC | [`Node.Ready`](raft/node.go): a vote or new entries leave in the same `Ready` as the reply that depends on them, and the runtime, [`node.handleReady`](node/node.go), saves before it sends and stops for good if a save fails ([ARCHITECTURE.md](docs/ARCHITECTURE.md#the-ready-contract)); [`FileStorage.Save`](internal/storage/file.go) returns only after an fsync | `TestVoteIsDurableBeforeReply`, `TestAppendEntriesReplacesConflictingEntries`, `TestCrashAtEveryWriteBoundary` (storage), `TestNodeSavesBeforeItSendsAndApplies`, `TestNodeStopsForGoodOnStorageError` (node) |
| Figure 2, State | Persistent state survives a crash at any instant: nothing fsynced is lost, a crash mid-write loses at most that write, and damage refuses to start rather than pass for a crash | [`FileStorage`](internal/storage/file.go) and [the WAL's recovery](internal/storage/wal.go) ([ADR-0003](docs/adr/0003-wal-format.md)) | `TestCrashAtEveryWriteBoundary`, `TestTornFinalRecordIsTruncated`, `TestDamagedRecordRefusesToStart`, `TestMissingSegmentRefusesToStart` (storage); `TestCrashesMidWriteKeepSafety`, `TestRandomized` (sim) |
| Figure 2, State | `currentTerm` starts at 0 and only increases; `votedFor` is null until the server votes in the current term | [`becomeFollower`, `becomeCandidate`](raft/node.go) | `TestRequestVoteRejectsStaleTerm`, `TestHigherTermInRequestStepsDown`, `TestRandomized` (sim) |
| Figure 2, State | `log[]`: entries from index 1, each with a command and the term the leader received it in | [`raftLog`](raft/log.go), behind a sentinel at index 0 | `TestLogSentinel`, `TestNewLogRejectsGaps` |
| Figure 2, State | `commitIndex` and `lastApplied` start at 0, or at a restored snapshot's last included index, only increase, and are volatile | [`commitTo`](raft/append_entries.go), [`NewNode`, `Node.Ready`](raft/node.go) | `TestFollowerAdvancesCommitIndex`, `TestFollowerReleasesCommittedEntries`, `TestNewNodeRestoresSnapshot`, `TestRandomized` (sim) |
| Figure 2, State | After each election, `nextIndex[]` starts just past the leader's last entry and `matchIndex[]` at 0; `matchIndex` only increases | [`becomeLeader`](raft/node.go), [`handleAppendEntriesResponse`](raft/append_entries.go) | `TestLeaderSendsEntriesFromNextIndex`, `TestMatchIndexOnlyMovesForward` |
| §5.2 | Servers start up as followers, from the state they persisted | [`NewNode`](raft/node.go) | `TestNewNodeRestoresHardState`, `TestClusterReplacesFailedLeader` |
| Figure 2, AppendEntries arguments | `term`, `leaderId`, `prevLogIndex`, `prevLogTerm`, `entries[]`, `leaderCommit` | [`AppendEntriesRequest`](raft/types.go), sent by [`sendAppend`](raft/append_entries.go) | `TestLeaderSendsEntriesFromNextIndex` |
| Figure 2, AppendEntries results | `term` and `success`, plus `MatchIndex`, `PrevLogIndex`, `ConflictTerm`, and `ConflictIndex` ([ADR-0004](docs/adr/0004-fast-log-backtracking.md)) | [`AppendEntriesResponse`](raft/types.go) | `TestAppendEntriesReplacesConflictingEntries`, `TestRejectionNamesConflict` |
| Figure 2, AppendEntries receiver, step 1 | Reply false if `term < currentTerm` (§5.1) | [`handleAppendEntries`](raft/append_entries.go) | `TestAppendEntriesRejectsStaleTerm` |
| Figure 2, AppendEntries receiver, step 2 | Reply false if the log has no entry at `prevLogIndex` whose term matches `prevLogTerm` (§5.3); an entry a snapshot replaced is committed, so it matches | [`handleAppendEntries`, `hasEntry`](raft/append_entries.go) | `TestAppendEntriesChecksPrevEntry`, `TestAppendEntriesMatchesInsideSnapshot` |
| Figure 2, AppendEntries receiver, step 3 | If an existing entry conflicts with a new one, delete it and all that follow it (§5.3) | [`raftLog.merge`](raft/log.go) | `TestAppendEntriesReplacesConflictingEntries`, `TestStaleAppendEntriesDoesNotTruncate` |
| Figure 2, AppendEntries receiver, step 4 | Append any new entries not already in the log | [`raftLog.merge`](raft/log.go) | `TestAppendEntriesAppendsOnlyNewEntries`, `TestDuplicateAppendEntriesIsIdempotent` |
| Figure 2, AppendEntries receiver, step 5 | If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)` | [`handleAppendEntries`](raft/append_entries.go) | `TestFollowerAdvancesCommitIndex` |
| Figure 2, RequestVote arguments | `term`, `candidateId`, `lastLogIndex`, `lastLogTerm` | [`RequestVoteRequest`](raft/types.go), sent by [`requestVotes`](raft/election.go) | `TestFollowerTimeoutStartsElection` |
| Figure 2, RequestVote results | `term`, `voteGranted` | [`RequestVoteResponse`](raft/types.go), sent by [`replyVote`](raft/election.go) | `TestOneVotePerTerm` |
| Figure 2, RequestVote receiver, step 1 | Reply false if `term < currentTerm` (§5.1) | [`handleRequestVote`](raft/election.go) | `TestRequestVoteRejectsStaleTerm` |
| Figure 2, RequestVote receiver, step 2 | Grant the vote only if `votedFor` is null or `candidateId`: one vote per term, first come, first served (§5.2) | [`handleRequestVote`](raft/election.go) | `TestOneVotePerTerm`, `TestNewNodeRestoresHardState` |
| Figure 2, RequestVote receiver, step 2; §5.4.1 | ... and only if the candidate's log is at least as up-to-date as the receiver's | [`isUpToDate`](raft/election.go) | `TestVoteRequiresUpToDateLog`, `TestStaleLogCandidateLoses` |
| Figure 2, all servers | If `commitIndex > lastApplied`, apply the next entry to the state machine (§5.3) | [`Node.Ready`](raft/node.go) releases `CommittedEntries` in index order, and the runtime, [`node.apply`](node/node.go), applies them after the same `Ready`'s entries are persisted and its messages sent | `TestCommittedEntriesComeOutOnceInOrder`, `TestFollowerReleasesCommittedEntries`, `TestNodeSavesBeforeItSendsAndApplies` (node) |
| Figure 2, all servers | A request or response with term T > `currentTerm`: set `currentTerm` = T and convert to follower (§5.1) | [`Node.Step`](raft/node.go), before any handler | `TestHigherTermInRequestStepsDown`, `TestHigherTermInResponseStepsDown` |
| Figure 2, followers | Respond to RPCs from candidates and leaders | [`handleRequestVote`](raft/election.go), [`handleAppendEntries`](raft/append_entries.go) | `TestOneVotePerTerm`, `TestHeartbeatsSuppressElection` |
| Figure 2, followers | If the election timeout elapses without an AppendEntries from the current leader or a vote granted, convert to candidate | [`Node.Tick`](raft/node.go); the timer restarts only in [`followLeader`](raft/append_entries.go), on AppendEntries or InstallSnapshot, and on a grant in [`handleRequestVote`](raft/election.go) | `TestFollowerTimeoutStartsElection`, `TestHeartbeatsSuppressElection`, `TestElectionTimerReset`, `TestInstallSnapshotRestartsElectionTimer` |
| Figure 2, candidates | On conversion, start an election: increment `currentTerm`, vote for self, reset the election timer, send RequestVote to all other servers | [`becomeCandidate`](raft/node.go), [`campaign`, `requestVotes`](raft/election.go) | `TestFollowerTimeoutStartsElection` |
| §5.1 | Servers retry RPCs that get no timely response: every heartbeat interval, a candidate sends RequestVote again to each server that has not answered | [`Node.Tick`](raft/node.go), [`requestVotes`](raft/election.go) | `TestCandidateResendsUnansweredRequestVote`, `TestOneVotePerTerm` |
| Figure 2, candidates | Votes from a majority of servers: become leader | [`handleRequestVoteResponse`, `hasMajority`](raft/election.go) | `TestCandidateWinsWithMajority`, `TestDuplicateVoteResponseCountsOnce`, `TestSingleServerElectsItself` |
| Figure 2, candidates; §5.2 | AppendEntries from the new leader: convert to follower | [`followLeader`](raft/append_entries.go) | `TestCandidateStepsDownOnAppendEntriesFromLeader`, `TestAppendEntriesRejectsStaleTerm`, `TestInstallSnapshotRestartsElectionTimer` |
| Figure 2, candidates | If the election timeout elapses, start a new election | [`Node.Tick`](raft/node.go), [`campaign`](raft/election.go) | `TestSplitVoteStartsNewElection` |
| Figure 2, leaders | Upon election, send an empty AppendEntries (heartbeat) to each server; repeat during idle periods to prevent election timeouts | [`becomeLeader`](raft/node.go), [`broadcastAppend`](raft/append_entries.go) | `TestLeaderSendsHeartbeats`, `TestHeartbeatsSuppressElection` |
| Figure 2, leaders | If a command is received from a client, append an entry to the local log; respond after the entry is applied (§5.3) | [`Node.Propose`](raft/node.go) appends and returns the entry's index and term, and the runtime's [`Node.Propose`](node/node.go) returns once the entry is durable; answering the client once that entry comes back through `CommittedEntries` belongs to the key-value service, not written yet | `TestProposeAppendsOnLeader`, `TestProposeRefusedOffLeader`, `TestNodesReplicateParallelProposals` (node) |
| Figure 2, leaders | If last log index ≥ `nextIndex` for a follower, send AppendEntries with the entries starting at `nextIndex` | [`sendAppend`](raft/append_entries.go): every AppendEntries carries as many of them as `MaxAppendSize` allows, at least one, heartbeats included, so each heartbeat also retries; each success sends the next batch | `TestLeaderSendsEntriesFromNextIndex`, `TestLeaderSendsRestToLaggingFollower`, `TestAppendEntriesCarryAtMostMaxAppendSize` |
| Figure 2, leaders | If successful, update `nextIndex` and `matchIndex` for the follower (§5.3) | [`acknowledge`](raft/append_entries.go), from the reply's `MatchIndex` | `TestMatchIndexOnlyMovesForward`, `TestLeaderSendsRestToLaggingFollower` |
| Figure 2, leaders; §5.3 | If AppendEntries fails because of log inconsistency, back up `nextIndex` and retry, skipping a whole conflicting term per round trip, and acting only on a rejection of the latest probe | [`handleAppendEntriesResponse`, `backtrack`](raft/append_entries.go) ([ADR-0004](docs/adr/0004-fast-log-backtracking.md)) | `TestLeaderSkipsConflictingTerm`, `TestDivergentLogConvergesPerTerm`, `TestFigure7FollowersConverge`, `TestLeaderIgnoresStaleRejection` |
| Figure 2, leaders | If some N > `commitIndex` has a majority of `matchIndex[i] ≥ N` and `log[N].term == currentTerm`, set `commitIndex = N` (§5.3, §5.4) | [`maybeCommit`](raft/append_entries.go), counting the leader's own log | `TestLeaderCommitsOnMajority`, `TestSingleServerCommitsItsProposals`, `TestFigure8OldTermEntryNotCommittedByCounting` |
| §5.2 | Election timeouts are chosen at random from a fixed interval, [T, 2T), again for every election | [`resetElectionTimer`](raft/election.go) | `TestElectionTimeoutInRange` |
| §5.2; Figure 3, Election Safety | At most one leader per term, because each server votes once per term and a winner needs a majority | [`hasMajority`](raft/election.go); a leader that meets a rival of its term panics in [`followLeader`](raft/append_entries.go) | `TestClusterElectsOneLeader`, `TestTwoLeadersInOneTermPanics`, `TestRandomized` (sim) |
| §5.2 | A minority partition never elects a leader, however high its terms climb | [`hasMajority`](raft/election.go) | `TestMinorityPartitionNeverElects` (sim) |
| §5.2 | When the leader fails, the remaining majority elects a new one in a later term | The rules above, together | `TestClusterReplacesFailedLeader`, `TestCrashedLeaderIsReplaced` (sim) |
| §5.1, §5.2 | A leader cut off from the majority is replaced in a later term, and steps down when it hears that term | The rules above, together | `TestIsolatedLeaderIsReplaced` (sim) |
| §5.3; Figure 3, Leader Append-Only | A leader never overwrites or deletes entries in its log | [`Node.Propose`](raft/node.go) only appends, and a leader never runs `merge` | `TestRandomized` (sim) |
| §5.3; Figure 3, Log Matching | Two logs with an entry of the same index and term hold the same command there and are identical up to it | The consistency check in [`handleAppendEntries`](raft/append_entries.go) | `TestFigure7FollowersConverge`, `TestRandomized` (sim) |
| §5.4.1; Figure 3, Leader Completeness | An entry committed in a term is in the log of every leader of a later term | [`isUpToDate`](raft/election.go) | `TestStaleLogCandidateLoses`, `TestRandomized` (sim) |
| §5.4.2 | A leader never commits an entry from an earlier term by counting replicas; it commits with the first entry of its own term | [`maybeCommit`](raft/append_entries.go) | `TestFigure8OldTermEntryNotCommittedByCounting` |
| §5.4.3; Figure 3, State Machine Safety | No two servers apply different entries at one index | The rules above, together | `TestRandomized` (sim) |
| §5.5 | RPCs are idempotent: a repeated AppendEntries changes nothing | [`raftLog.merge`](raft/log.go) | `TestDuplicateAppendEntriesIsIdempotent`, `TestStaleAppendEntriesDoesNotTruncate` |
| §5.5 | A follower or candidate that crashes restarts from its persisted state; one that crashed after completing an RPC but before responding receives the same RPC again | [`NewNode`](raft/node.go) restores the saved state; leaders resend entries with every heartbeat and candidates resend RequestVote | `TestCrashedLeaderIsReplaced`, `TestCrashesMidWriteKeepSafety`, `TestRandomized` (sim) |
| §5.6 | Broadcast time ≪ election timeout ≪ mean time between failures: heartbeats every 50 ms and election timeouts of 300–600 ms, on a network where a message takes about a millisecond | [`DefaultConfig`](raft/node.go); [`node`](node/node.go) turns wall-clock time into ticks; the [transport](internal/transport/http.go) drops messages rather than let a slow peer hold up the rest | `TestNodesReplaceStoppedLeader` (node), `TestIntegrationKillLeader` (kvnode) |
| §7 | Each server snapshots its state machine on its own, covering only entries it applied, and discards the log through the snapshot's last included entry, keeping that entry's index and term | [`Node.Compact`](raft/node.go); [`raftLog`](raft/log.go), whose sentinel is the last included entry | `TestCompactReplacesAppliedEntries`, `TestCompactRefusesUnappliedOrCompactedIndex`, `TestLogAfterSnapshot` |
| §7; Figure 12 | A snapshot holds the last included index and term and the state machine's state, and is written whole | [`raft.Snapshot`](raft/types.go); [the snapshot file](internal/storage/snapshot.go) ([ADR-0005](docs/adr/0005-snapshot-format.md)) | `TestSnapshotRoundTrip`, `TestSnapshotBytes`, `TestDamagedSnapshotIsRefused`, `FuzzSnapshotDecode` (storage) |
| §7 | Once a snapshot is durable, the log entries and the older snapshot it replaces are deleted | [`FileStorage.saveSnapshot`](internal/storage/file.go), [`startOver`](internal/storage/wal.go) | `TestSnapshotRemovesWhatItReplaces`, `TestRecoveryRemovesLeftovers`, `TestCrashAtEveryWriteBoundary` (storage) |
| §7 | A server restarts from its snapshot and the log after it | [`OpenFileStorage`](internal/storage/file.go), [`NewNode`](raft/node.go), and [`node.Start`](node/node.go), which restores the state machine first | `TestNewNodeRestoresSnapshot`, `TestVoteComparesSnapshotLastEntry`; `TestRestoreEqualsFullReplay`, `TestBrokenSnapshotRefusesToStart` (storage); `TestRandomized` (sim); `TestNodeCatchesUpThroughSnapshot` (node) |
| §7 | The leader sends InstallSnapshot to a follower that needs an entry the leader has discarded | [`sendAppend`](raft/append_entries.go), [`sendSnapshot`](raft/snapshot.go) | `TestLeaderSendsSnapshotWhenEntriesAreGone`, `TestLeaderResendsSnapshotUntilAcknowledged`, `TestLaggingServerCatchesUpThroughSnapshot` (sim) |
| Figure 13, arguments | `term`, `leaderId`, `lastIncludedIndex`, `lastIncludedTerm`, `offset`, `data[]`, `done`; chunks of at most `SnapshotChunkSize` bytes | [`InstallSnapshotRequest`](raft/types.go), [`sendSnapshot`](raft/snapshot.go) | `TestSnapshotTravelsInChunks` |
| Figure 13, results | `term`, plus `LastIncludedIndex`, `Offset`, and `Done`, which say what the reply acknowledges ([ADR-0005](docs/adr/0005-snapshot-format.md)) | [`InstallSnapshotResponse`](raft/types.go), [`handleInstallSnapshotResponse`, `advanceTransfer`](raft/snapshot.go) | `TestLeaderSendsNextChunkOnProgress`, `TestSnapshotTransferOutlastsCompaction` |
| Figure 13, receiver, step 1 | Reply immediately if `term < currentTerm` | [`handleInstallSnapshot`](raft/snapshot.go) | `TestInstallSnapshotRejectsStaleTerm` |
| Figure 13, receiver, steps 2–4 | Create a new snapshot on the first chunk, write each chunk at its offset, and reply and wait until done | [`receiveChunk`](raft/snapshot.go), in memory | `TestInstallSnapshotAssemblesChunks`, `TestInstallSnapshotDropsChunksThatDoNotContinue` |
| Figure 13, receiver, step 5 | Save the snapshot, discarding any existing snapshot with a smaller index | [`installSnapshot`](raft/snapshot.go) hands it to storage through `Ready` | `TestInstallSnapshotSavesSnapshot`, `TestSnapshotRemovesWhatItReplaces` (storage) |
| Figure 13, receiver, step 6 | If the log holds the snapshot's last included entry, keep the entries after it | [`raftLog.restore`](raft/log.go) | `TestInstallSnapshotKeepsEntriesAfterMatchingEntry` |
| Figure 13, receiver, step 7 | Otherwise discard the entire log | [`raftLog.restore`](raft/log.go) | `TestInstallSnapshotDiscardsLogWithoutLastIncludedEntry` |
| Figure 13, receiver, step 8 | Reset the state machine from the snapshot, after step 6 too (see [Deviations](#deviations-from-the-paper)) | [`installSnapshot`](raft/snapshot.go); the runtime, [`node.apply`](node/node.go), restores from `Ready.Snapshot` | `TestInstallSnapshotResetsStateMachine`, `TestNodeCatchesUpThroughSnapshot` (node), `TestIntegrationKillLeader` (kvnode) |
| §7 | A snapshot at or below `commitIndex` is ignored, so applied state never moves backwards | [`handleInstallSnapshot`](raft/snapshot.go) | `TestInstallSnapshotIgnoresCommittedPrefix`, `TestRandomized` (sim) |

## Verification

Six independent harnesses produce the evidence for correctness, and no single one is trusted alone. [docs/TESTING.md](docs/TESTING.md) covers each in depth.

| Harness | What it catches | How to run | State |
| --- | --- | --- | --- |
| Race detector | Data races in the runtime, transport, kvserver, and client | `make test` | Runs every test: the runtime's parallel proposals and stops during traffic, the transport's, and three `kvnode` processes built with it |
| Deterministic simulator | Safety violations under drops, delays, reordering, duplication, partitions, crashes, and clock skew | `make sim SEEDS=N` | Checks the five Figure 3 properties, terms, votes, applied order, and every snapshot after every event, with crashes in the middle of disk writes and compaction as often as every entry, and liveness after faults heal |
| MIT 6.5840 Labs 3 and 4 | Bugs found by an independent test suite; timing and RPC-count limits | Private adapter | Not built |
| Porcupine | Client histories that are not linearizable | Inside the simulator and chaos tests | Not built |
| Fuzzing and disk faults | Decoder panics, torn writes, lost fsyncs, corrupted files | `make fuzz FUZZTIME=60s`; `go test ./internal/storage` | Fuzzes the WAL record, snapshot, and Raft message decoders; crash tests at every write boundary of the storage, snapshot saves included |
| Chaos test | Bugs only real processes, sockets, `kill -9`, and disks expose | Not built | Not built |

The deterministic simulator runs whole clusters on a virtual clock under randomized partitions, crashes, restarts, and lossy, duplicating, slow networks, and checks the invariants after every event. Servers keep their log and snapshots in the real file storage on a simulated disk, a crash can strike in the middle of a write, and servers compact their logs as often as every entry, so lagging servers catch up through InstallSnapshot. A failing seed prints its trace, and `go test ./internal/sim -run TestRandomized -seed=N -v` replays it exactly, every message included. The runtime's tests run three nodes in one process over an in-memory network, with `-race`, and an integration test runs three real `kvnode` processes: it kills the leader with `kill -9`, has the others elect a new one, and restarts the old leader, which catches up through a snapshot sent over HTTP. Every committed command must be applied at its index on every server. The storage's crash tests run it on an in-memory file system that crashes during each write, truncation, file creation, rename, removal, and fsync, and check that recovery never loses a synced record and that a snapshot save is all or nothing. Unit tests drive one core with `Tick` and `Step`, and a synchronous three-node harness in `raft/` runs whole elections. [docs/TESTING.md](docs/TESTING.md) describes each, and how to read a trace.

## Deviations from the paper

Every deviation gets an ADR in [docs/adr/](docs/adr/) and a row here.

| Paper | This implementation | Reason | ADR |
| --- | --- | --- | --- |
| Figure 13, InstallSnapshot receiver, step 6: keep the entries after a matching last included entry "and reply" | After step 6 the receiver also carries out step 8, resetting its state machine from the snapshot. It ignores outright a snapshot at or below `commitIndex` | The snapshot replaces the entries it covers, so a follower that had not applied them all would skip them. A snapshot at or below `commitIndex` adds nothing and would move applied state backwards | [ADR-0005](docs/adr/0005-snapshot-format.md) |

## Limitations and non-goals

- **Byzantine faults.** Servers are assumed to fail by stopping, never by lying.
- **Sharding.** One Raft group holds the whole keyspace.
- **Multi-key transactions.** Each operation touches one key.
- **TLS and authentication.** Anyone who can reach a node's port can talk to it.
- **Deployment tooling.** There are no packages, images, or service files.
- **Matching etcd's performance.** Clarity beats performance; code is optimized only when a test requires it.

## Project layout

| Path | Responsibility |
| --- | --- |
| `raft/` | Deterministic core: roles, RPC handlers, log, commit rule, snapshots, membership |
| `node/` | Runtime: event loop, timers, `Ready` handling |
| `internal/storage/` | `Storage` interface; in-memory and file implementations (WAL, snapshots) |
| `internal/transport/` | `Transport` interface; HTTP/JSON implementation |
| `internal/kv/` | KV state machine, session table, snapshot encoding |
| `internal/kvserver/` | Client-facing HTTP API, leader redirect, waits for apply |
| `client/` | Go client library: leader discovery, retries, client ID and sequence numbers |
| `internal/sim/` | Deterministic simulator, simulated network, fault schedules, invariant checker |
| `internal/linearizability/` | Porcupine models and history recording |
| `cmd/kvnode/`, `cmd/kvctl/` | Server and CLI binaries |
| `test/chaos/` | Multi-process chaos test |
| `docs/` | [ARCHITECTURE.md](docs/ARCHITECTURE.md), [TESTING.md](docs/TESTING.md), and the decision records in [adr/](docs/adr/) |
| `internal/leaktest/` | A check that a test leaves no goroutine running |
| `scripts/` | `cluster.sh`, which runs a local cluster, and the fuzzing script the Makefile calls |

## Development

`make check` runs everything CI runs on every push. The other targets run one step each.

| Target | What it does |
| --- | --- |
| `make check` | All of the steps below |
| `make fmt` | Fails if `gofmt` would change any file |
| `make vet` | `go vet ./...` |
| `make lint` | golangci-lint with the limits in [.golangci.yml](.golangci.yml) |
| `make test` | `go test -race ./...` |
| `make sim SEEDS=N` | Simulator sweep over N seeds (default 1,000) |
| `make fuzz FUZZTIME=D` | Every fuzz target for duration D each (default 60s) |

**Linting.** golangci-lint v2.15.0 is pinned in the Makefile and runs through `go run`, so there is nothing to install. The first run builds it, which takes about half a minute. It runs `staticcheck`, `revive` (including doc comments on every exported identifier), `errcheck`, `gocyclo`, `funlen`, `nestif`, and `misspell`. The limits are cyclomatic complexity 10, 60 lines per function, and nesting depth 3; test functions are exempt from the length limit. Exceeding one needs a `//nolint` comment with a stated reason.

**Commits.** Subjects take the form `<area>: <imperative summary>`, and every commit carries a `Phase: N` trailer. [CONTRIBUTING.md](CONTRIBUTING.md) has the full conventions.

**CI.** GitHub Actions runs `make check` on every push and pull request ([.github/workflows/ci.yml](.github/workflows/ci.yml)), and every night 50,000 simulator seeds and ten minutes of fuzzing per target ([.github/workflows/nightly.yml](.github/workflows/nightly.yml)).

## Phase history

| Tag | Date | What landed |
| --- | --- | --- |
| `phase-0` | 2026-10-27 | Module and package layout, Makefile, lint configuration, CI, contributing and testing guides, ADR-0001 (deterministic core) and ADR-0002 (HTTP/JSON transport) |
| `phase-1` | 2026-10-27 | Core types and leader election: the three roles, the randomized election timer, RequestVote, heartbeats, in-memory storage, a three-node election harness, and `docs/ARCHITECTURE.md` |
| `phase-2` | 2026-11-03 | The deterministic simulator: a virtual clock, a seeded event queue, message loss, duplication, and delay, partitions, crashes, restarts, invariant checks after every event, liveness after healing, traces, seed replay, and a nightly 50,000-seed sweep; candidates re-send unanswered RequestVotes |
| `phase-3` | 2026-11-03 | Log replication and commitment: the log, proposals, AppendEntries with the consistency check and fast backtracking (ADR-0004), the commit rule, committed entries through `Ready`, Figures 7 and 8 as tests, and simulator checks for all five Figure 3 properties |
| `phase-4` | 2026-11-06 | Durable storage and crash recovery: a segmented write-ahead log of CRC-32C records, recovery that truncates a torn final record and refuses any other damage (ADR-0003), a file system that crashes on demand with tests at every write boundary, fuzzing of the record decoder, and simulator servers that crash mid-write, half of all crashes aimed at the leader |
| `phase-5` | 2026-11-14 | Log compaction with snapshots: `Compact`, InstallSnapshot sent in chunks per Figure 13, snapshot files written whole beside a WAL that starts over from each snapshot (ADR-0005), restart from a snapshot and the log after it, fuzzing of the snapshot decoder, and simulator servers that compact as often as every entry |
| `phase-6` | 2026-11-14 | Runtime, transport, and binaries: the `node` runtime that saves, then sends, then applies, and stops for good on a storage error; the HTTP/JSON transport with fuzzing of its decoder; `kvnode` and `scripts/cluster.sh`; a three-process integration test with `kill -9`; AppendEntries capped at 1 MiB of entries; rejections that name their probe, so a stale one no longer starts a needless snapshot transfer; simulator servers that compact on schedules of their own |
