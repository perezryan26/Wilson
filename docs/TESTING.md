# Testing

Six independent harnesses produce the evidence for Wilson's correctness, and no single one is trusted alone. This file says what each one catches, how to run it, and how to read its output. Harnesses that are not built yet are marked as such.

## Everyday checks

`make check` is what CI runs on every push: gofmt, go vet, golangci-lint, `go test -race ./...`, a simulator sweep of 1,000 seeds, and 60 seconds of fuzzing per target. Override the sizes with `make sim SEEDS=N` and `make fuzz FUZZTIME=D`.

| Harness | What it catches | When it runs | State |
| --- | --- | --- | --- |
| Race detector (`-race`) | Data races in the runtime, transport, kvserver, and client | Every test run | Runs every test, among them the runtime's and the transport's, and three `kvnode` processes built with it |
| Deterministic simulator | Safety violations under drops, delays, reordering, duplication, partitions, crashes, and tick skew | 1,000 seeds per push; 20,000 before each phase tag; 50,000 nightly | Checks the five Figure 3 properties, terms, votes, applied order, and every snapshot after every event, with crashes in the middle of disk writes and compaction as often as every entry, and liveness after faults heal |
| MIT 6.5840 Labs 3 and 4 | Bugs found by an independent test suite; timing and RPC-count limits | 20 consecutive passing runs | Not built |
| Porcupine | Client histories that are not linearizable | Every KV simulator seed; chaos runs; MIT Lab 4 | Not built |
| Fuzzing and disk faults | Decoder panics, torn writes, lost fsyncs, corrupted files | 60 s per target per push; 10 min per target nightly and before each phase tag; crash tests on every test run | Fuzzes the WAL record, snapshot, and Raft message decoders; crash tests at every write boundary of the storage, snapshot saves included |
| Chaos test | Bugs only real processes, sockets, `kill -9`, and disks expose | Nightly | Not built |

A nightly workflow ([nightly.yml](../.github/workflows/nightly.yml)) runs `make sim SEEDS=50000` and `make fuzz FUZZTIME=10m`. GitHub runs it on the default branch, and it can be started by hand from the Actions tab.

## Unit tests and the election harness

These test the core directly. Both live in `raft/` and run with every `make test`; the simulator below then tests the same rules under faults.

- **Unit tests** drive one core with `Tick`, `Step`, and `Propose` and check the `Ready` and `Status` it returns, with no sleeps. Each is named after the rule it covers, and the README's [paper-to-code map](../README.md#paper-to-code-map) lists which test covers which rule of Figure 2.
- **Scenarios from the paper** pass messages between a few cores by hand. `TestFigure7FollowersConverge` brings each of Figure 7's six follower logs into line with the leader's and counts the rejections it takes. `TestFigure8OldTermEntryNotCommittedByCounting` plays Figure 8's (c), (d), and (e) on five cores.
- **The three-node harness** ([`raft/cluster_test.go`](../raft/cluster_test.go)) runs whole elections in one goroutine. Each round ticks every running server once, then delivers messages in the order they were sent until none are left. Every server's `Ready` is handled as a runtime handles it: hard state saved to in-memory storage, then messages sent. A crashed server loses all but its storage and the messages sent to it while it is down; a restarted one boots from that storage. After every `Ready` the harness checks Election Safety: no two servers ever lead the same term.

`TestClusterElectsOneLeader` and `TestClusterReplacesFailedLeader` each run 1,000 seeds. A seed fixes every server's random source, so a failure names its seed and repeats on every run. Run them alone with:

```sh
go test ./raft -run TestCluster
```

## The runtime, the transport, and real processes

These run with every `make test`, under the race detector.

- **The runtime** ([`node/`](../node)): three nodes run in one process, each with storage in memory, over a network that hands each message straight to its receiver, ticking every millisecond. `TestNodesReplicateParallelProposals` has four goroutines propose a hundred commands at once and checks that every server applies them in the same order. `TestNodesReplaceStoppedLeader` stops the leader, has the other two elect another, and restarts the old one. `TestNodeCatchesUpThroughSnapshot` keeps a server down while the others commit and compact, then requires it to install the leader's snapshot, and later to restart from its own. `TestStopDuringTraffic` stops every node while proposals and messages are in flight. Two tests play a node's peers by hand and record everything it does. `TestNodeSavesBeforeItSendsAndApplies` requires a vote and new entries to be saved before the replies that depend on them leave, and entries to be applied only after both. `TestNodeStopsForGoodOnStorageError` requires a failed save to stop the node with nothing sent or applied. `TestProposeReturnsOnceTheEntryIsSaved` holds a proposal's save back and requires `Propose` to wait for it.
- **The transport** ([`internal/transport/`](../internal/transport)) is tested over real HTTP on localhost. The tests deliver every RPC and check its encoding round trip. They hold a peer's answers back to show that `Send` never blocks and that a request times out, and take a peer down and bring it back to check that one log line reports each change. They also send the handler malformed, oversized, and misaddressed messages. `TestEveryMessageFitsMaxMessageSize` drives a core set up by `DefaultConfig` to its largest messages, with indexes and terms near the top of `uint64`: the most entries one AppendEntries may carry, the largest command `Propose` accepts, and a full snapshot chunk. Each must fit in the 4 MiB a server accepts; the largest takes 1.4 MB.
- **No goroutine outlives a test.** [`internal/leaktest`](../internal/leaktest) counts goroutines when a test starts and again after its cleanup, and prints every stack if any are left over.
- **The integration test**, `TestIntegrationKillLeader` in `cmd/kvnode`, builds `kvnode`, with the race detector when the test runs with it, and starts three processes on localhost, each compacting its log every four entries. It commits six commands, kills the leader with `kill -9`, waits for the other two to elect a new leader in a later term, and commits six more. Then it restarts the old leader from its disk, and the old leader catches up through the new leader's snapshot, sent over HTTP. Every committed command must be applied at its index on every server, all servers must agree on what they applied, and each must exit cleanly on SIGTERM. Under `-race`, a clean exit also means the server saw no data race. The test's cleanup kills every server still running before their directories are removed, pass or fail. Every wait gives up well before `go test`'s timeout, whose panic would skip the cleanup and leave servers listening. Before a phase is tagged, it must pass 50 times in a row, which takes about two minutes:

```sh
go test -race -count=50 -run TestIntegration ./...
```

## Race detector

`make test` runs every package with `-race`, locally and in CI. The Raft core is single-threaded by design, so races can only live in the runtime, transport, kvserver, and client. The runtime's and the transport's tests exercise concurrency on purpose: parallel proposals, a stop while messages and proposals are in flight, and a leader stopped mid-run. The integration test builds the server itself with the race detector. The kvserver and client tests will do the same when they are written.

## Deterministic simulator

[`internal/sim`](../internal/sim) runs a whole cluster in one goroutine on a virtual clock. One random source, seeded by the run's seed, makes every choice: each server's clock skew (up to 20% fast or slow), each message's delay, loss, and duplication, every fault, and each server core's own random source for election timeouts. The simulator uses only integer arithmetic, so a seed replays byte for byte on any machine; traces match on arm64 and amd64.

Each server handles every `Ready` as a runtime does: it saves the hard state, snapshot, and entries, sends the messages, restores its state machine from a snapshot the leader sent, then applies the committed entries. Its state machine is the list of commands it applied, and a snapshot holds that list as JSON. It compacts its log once it has applied a set number of entries past its last snapshot, as often as every entry in some seeds. It saves through the real `FileStorage`, on its own `FaultFS`: a disk in memory that keeps what was synced through every crash, and a random prefix of the rest ([Fuzzing and disk fault injection](#fuzzing-and-disk-fault-injection)). A crash between events loses nothing, since every `Save` synced before it returned. A crash during a disk operation fails the `Save` under way, so none of that `Ready`'s messages leave. It may leave a torn record for recovery to truncate when the server restarts, or a snapshot save undone. After every event, the simulator checks the five properties of Figure 3 and more:

- **Election Safety:** no two servers ever lead the same term, over the whole run.
- **Leader Append-Only:** a leader never writes at or below its last stored index.
- **Log Matching:** every stored copy of an entry agrees on its command and on the term of the entry before it, which by induction makes two logs sharing an entry identical up to it.
- **Leader Completeness:** every new leader, and every leader when an entry newly commits, holds each entry committed in an earlier term.
- **State Machine Safety:** no two servers ever apply different entries at one index.
- **Terms never fall:** a server's `currentTerm` never decreases, and a restarted server comes back in at least the highest term it saved. A term lost with a crashed `Save` never left in a message, so it does not count.
- **One vote per term:** no server votes for a second candidate in a term, even after a restart. A vote counts once its `Save` returns, and every restart checks the vote it restored against the votes saved.
- **Applied order:** each server keeps `lastApplied ≤ commitIndex ≤` its last index, neither ever falls within a boot, and it applies entries one by one after its snapshot, jumping forward only to install a snapshot from the leader.
- **Snapshots:** every snapshot a server compacts, installs, or restarts from holds exactly the commands applied through its last included index, and that entry's term (State Machine Safety). The simulator's copy of each log keeps the committed entries a snapshot replaced, so the checks above go on reading whole logs, and a leader's compaction may not change the entries after its snapshot (Leader Append-Only).

The first violation ends the run.

### The randomized schedule

`TestRandomized` runs seeds 0 to N−1 in parallel. Seed s runs a cluster of 1 + (s mod 5) servers for ten seconds of virtual time and injects a fault every 50 ms to 1 s: a partition into two or three groups, a heal, a one-way cut, a crash, a restart, or new network conditions (up to 50% loss, 20% duplication, and 400 ms delay). Half of all crashes hit the current leader, if one is up; half strike at once, between events, and the rest during one of the server's next six disk operations. Meanwhile a client proposes a new command every 10 to 200 ms to a server that believes it leads, stale leaders included. In half the seeds it first waits up to two seconds whenever a leader of a later term appears, which leaves a new leader without an entry of its own term to commit. The seed also sets how often servers compact: never, or after every 1, 2, 4, or 8 entries applied, the same for every server or, in half the seeds, drawn for each server on its own. It sets how leaders send snapshots, whole or in chunks of 16 or 128 bytes, and in half the seeds it caps each AppendEntries at three entries. Then every fault heals: all links return, the network goes back to 1–10 ms with no loss, crashes still to come are cancelled, and crashed servers restart. The liveness check follows: within ten seconds the servers must agree on a leader, and within ten more apply a command proposed to it, which commits every entry before it in the leader's log.

Over 1,000 seeds:

- 821 crashes are set to strike mid-write and 756 do, 256 of them leaving a torn record that recovery truncates. 844 more crashes strike between events.
- Servers compact 34,175 times, restart from a snapshot 1,006 times, and install 2,432 snapshots from a leader.

Some bugs need rare interleavings. A commit rule that counted replicas of earlier-term entries, the hazard of Figure 8, failed only 3 of 50,000 seeds while the client never paused, even with half of all crashes aimed at the leader. With the pause after each new leader it fails 23 of 50,000, nearly all of them seeds that pause. A core that treated the entries inside a follower's snapshot as mismatched passed every seed while all servers compacted alike, because a leader then rarely sends a follower entries that its own snapshot covers. With each server on its own schedule, it fails 6 of 2,000. A core whose InstallSnapshot did not restart the election timer still passes every seed, and only its unit test catches it. The nightly sweep is there for bugs like these, and Figure 8 also has its own scripted test.

| Run | Seeds |
| --- | --- |
| `go test ./...` | 20 |
| `make sim`, part of `make check` on every push | 1,000 |
| `make sim SEEDS=20000`, before each phase tag | 20,000 |
| The nightly workflow | 50,000 |

A seed costs a few milliseconds of CPU, so 50,000 seeds take under a minute.

### Reading a trace

Every run keeps a trace, and a failing seed prints it after the error. These lines are picked from seed 31's, replayed with `-seed=31`, which adds the messages:

```text
t=0.000ms n1 boot term=0 votedFor=none snapshot=0 lastIndex=0 tick=11.920ms
t=470.672ms n2 candidate→leader term=1 leader=none→n2
t=595.639ms n2 compact lastIncludedIndex=1
t=595.639ms n2 leader term=1 leader=n2 commitIndex=0→1 lastApplied=0→1
t=1729.891ms n2 will crash during disk operation 2 from now
t=1822.665ms n2 crash during a disk write
t=3584.306ms n2 boot term=1 votedFor=n2 snapshot=13 lastIndex=14 tick=10.690ms
t=3937.444ms n2 candidate→leader term=5 leader=none→n2
t=10423.191ms heal
t=10423.191ms n2 msg="storage: truncated a record torn by a crash at the end of the WAL" segment=0000000000000043.wal offset=302 bytes=19
t=10423.191ms n2 boot term=5 votedFor=n2 snapshot=33 lastIndex=42 tick=10.690ms
t=10998.818ms n2 candidate→leader term=18 leader=none→n2
t=11006.704ms n2→n1 AppendEntries term=18 leaderId=2 prevLogIndex=42 prevLogTerm=5 entries=[] leaderCommit=33
t=11007.846ms n2→n1 AppendEntries term=18 leaderId=2 prevLogIndex=42 prevLogTerm=5 entries=[43@18] leaderCommit=33
t=11013.848ms n1→n2 AppendEntriesResponse term=18 success=false prevLogIndex=42 conflictTerm=0 conflictIndex=34
t=11013.915ms n1→n2 AppendEntriesResponse term=18 success=false prevLogIndex=42 conflictTerm=0 conflictIndex=34
t=11022.171ms n2→n1 AppendEntries term=18 leaderId=2 prevLogIndex=33 prevLogTerm=5 entries=[34@5 35@5 36@5 37@5 38@5 39@5 40@5 41@5 42@5 43@18] leaderCommit=33
t=11027.234ms n1→n2 AppendEntriesResponse term=18 success=true matchIndex=43
t=11027.234ms n2 leader term=18 leader=n2 commitIndex=33→43 lastApplied=33→43
```

Seed 31 compacts after every entry and sends snapshots in 16-byte chunks. In its last lines, n1's log ends at index 33, exactly where n2's snapshot ends. n1 refuses both a heartbeat and a proposal that probe at entry 42. n2 acts on the first rejection, ignores the second because it answers the same probe, and sends the entries after 33, so no snapshot is needed.

- `t=` is virtual time since the run began, in milliseconds to the microsecond.
- `boot` starts a server from its snapshot and write-ahead log, showing the term and vote it restored, its snapshot's last included index (0 for none), its last log index, and its skewed tick interval. `crash` stops a server between events. `will crash during disk operation N from now` sets a crash to strike during the server's Nth disk operation from then, and `crash during a disk write` is that crash striking.
- `compactEvery=N` at the start of a run names the server's own compaction threshold, in seeds that give each server one.
- `compact` is a server's state machine taking a snapshot through the index shown. `install snapshot` is a server resetting its state machine from a snapshot the leader sent.
- A `msg=` line is the server's storage logging; here recovery truncates a record torn by the crash, naming the segment, the offset, and the bytes dropped.
- A server's own line shows its role, term, and leader after an event, with `old→new` for each one that changed, and `commitIndex` and `lastApplied` when they moved.
- `propose` shows a client's command reaching a server: the index and term of the new entry, or why it was refused.
- `partition` (groups separated by `|`), `cut`, `heal`, and `network` are faults.
- A final `FAILED:` line names the violation, or the liveness check that timed out.

### Replaying a seed

```sh
go test ./internal/sim -run TestRandomized -seed=N -v
```

This reruns seed N alone and logs its trace with every message added. A delivery shows as `n2→n3 RequestVote term=4 candidateId=2 lastLogIndex=0 lastLogTerm=0`, or `n3→n1 AppendEntries term=1 leaderId=3 prevLogIndex=0 prevLogTerm=0 entries=[1@1] leaderCommit=0` with entries as index@term, or `n2→n1 InstallSnapshot term=18 leaderId=2 lastIncludedIndex=33 lastIncludedTerm=5 offset=16 bytes=16 done=false` with the chunk's size in bytes, using the paper's field names. A rejection names the `prevLogIndex` of the request it answers. A message the network loses or copies as it is sent is marked `lost` or `duplicated`; one stopped on arrival by a partition or a crashed receiver is marked `cut` or `down`.

### Scripted scenarios

`sim.New` builds a cluster for a scripted test, with `Options` for its size, seed, compaction threshold, and snapshot chunk size. `RunFor`, `RunUntil`, `Propose`, `Partition`, `Cut`, `Heal`, `Crash`, `CrashDuringDiskOp`, `Restart`, and `SetNetwork` drive it, and `Status`, `AgreedLeader`, and `Leaders` inspect it. `TestIsolatedLeaderIsReplaced`, `TestMinorityPartitionNeverElects`, `TestCrashedLeaderIsReplaced`, and `TestElectsLeaderDespiteLossAndDuplication` are examples. `TestCrashesMidWriteKeepSafety` crashes a server mid-write every 200 ms in a cluster of five taking proposals and compacting every three entries, about 900 crashes over its ten seeds, and then requires the cluster to commit a new command everywhere. `TestLaggingServerCatchesUpThroughSnapshot` cuts a server off while the others commit and compact thirty commands, then requires it to catch up from the leader's snapshot, sent in 16-byte chunks.

## MIT 6.5840 test suites

Not built yet. MIT's 6.5840 course publishes test suites for Raft (Lab 3, parts 3A–3D) and for a key-value service on top of it (Lab 4, parts 4A–4C). Running them against this code gives evidence from tests written by someone else.

The course asks that solutions not be published. This repository is public, so the adapter that connects the course's API to this code lives in a separate private repository, and no course source is ever committed here. The course checkout goes in `.mitlab/`, which git ignores.

## Porcupine linearizability checks

Not built yet. Client histories recorded by the simulator (with virtual timestamps), the chaos test, and MIT Lab 4 are checked with [Porcupine](https://github.com/anishathalye/porcupine). An operation whose outcome is unknown, such as one that timed out, is recorded as pending, and the model accepts any result for it. An illegal history fails the test and writes Porcupine's HTML visualization next to the test output.

## Fuzzing and disk fault injection

`make fuzz` runs [scripts/fuzz.sh](../scripts/fuzz.sh), which finds every `Fuzz*` target in the module with `go test -list` and runs each for `FUZZTIME`. A new target is picked up without any change to the Makefile or CI. Corpora live in each package's `testdata/fuzz/`.

- `FuzzWALRecordDecode` (in `internal/storage`) feeds arbitrary bytes to the WAL record decoder. It must never panic, and every record it accepts must encode back to exactly the bytes it came from, so no two byte strings decode to the same record.
- `FuzzSnapshotDecode` (in `internal/storage`) does the same for the snapshot file decoder.
- `FuzzMessageDecode` (in `internal/transport`) feeds arbitrary bytes to the decoder for Raft messages arriving over HTTP. It must never panic, and a message it accepts must encode to JSON that decodes to the same message again.
- KV command decoding gets a target when it is written.

File storage reaches the disk only through a small file-system interface ([ARCHITECTURE.md](ARCHITECTURE.md#file-systems)), and `FaultFS` implements it in memory with crashes on demand: at once, or during the nth write, truncation, file creation, rename, removal, or fsync. A crash keeps everything synced and a random prefix of the rest, as a power failure would, the last write possibly cut mid-record and the last renames and removals undone. A seed fixes every choice, so a failure repeats.

- `TestCrashAtEveryWriteBoundary` saves a run of `Ready`s, overwrites, segment changes, and two snapshots among them. It crashes during each of their 55 disk operations, 30 seeds each, then again during each of the first four operations of the recovery that follows. Recovery must return everything each finished `Save` wrote plus a prefix of the `Save` that crashed, never less, and must keep taking writes. A `Save` with a snapshot is all or nothing: the old snapshot and log, or the new ones.
- `TestRestoreEqualsFullReplay` runs a one-server cluster that compacts at random and restarts from a crashed disk about two hundred times over twenty seeds. Each time, the state machine it rebuilds from its snapshot and the log after it must hold exactly the commands replaying the whole log would.
- `TestSnapshotRemovesWhatItReplaces`, `TestRecoveryRemovesLeftovers`, and `TestBrokenSnapshotRefusesToStart` check that a durable snapshot removes the segments and snapshot it replaces, that recovery removes what a crash left behind, and that a missing, damaged, or mismatched snapshot file refuses to start.
- On the real file system, `TestTornFinalRecordIsTruncated` cuts the last record at every length, `TestDamagedRecordRefusesToStart` damages every byte of every record, and `TestMissingSegmentRefusesToStart` deletes and shortens segments. [ADR-0003](adr/0003-wal-format.md) says what recovery truncates and what it refuses.
- `TestFileStorageMatchesMemory` checks file storage step by step against the in-memory reference, reopening it after every step.

## Chaos test

Not built yet. It will start five real `kvnode` processes behind test-controlled TCP proxies, kill and restart them with `kill -9`, partition links, and run concurrent clients. It then checks the history with Porcupine, restarts the whole cluster, and verifies every acknowledged write.

## Static analysis

`make lint` runs golangci-lint (pinned in the Makefile) with `staticcheck`, `revive`, `errcheck`, `gocyclo`, `funlen`, `nestif`, and `misspell`, configured in [.golangci.yml](../.golangci.yml). `make fmt` and `make vet` run gofmt and go vet on their own.
