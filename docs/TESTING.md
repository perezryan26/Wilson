# Testing

Six independent harnesses produce the evidence for Wilson's correctness, and no single one is trusted alone. This file says what each one catches, how to run it, and how to read its output.

## Everyday checks

`make check` is what CI runs on every push: gofmt, go vet, golangci-lint, `go test -race ./...`, a simulator sweep of 1,000 seeds, and 60 seconds of fuzzing per target. Override the sizes with `make sim SEEDS=N` and `make fuzz FUZZTIME=D`.

| Harness | What it catches | When it runs | State |
| --- | --- | --- | --- |
| Race detector (`-race`) | Data races in the runtime, transport, kvserver, and client | Every test run | Runs every test, among them the runtime's, the transport's, the key-value server's, and the client's, and three `kvnode` processes built with it |
| Deterministic simulator | Safety violations under drops, delays, reordering, duplication, partitions, crashes, and tick skew | 1,000 seeds per push; 20,000 before each phase tag; 50,000 nightly | Checks the five Figure 3 properties, terms, votes, applied order, and every snapshot after every event, with crashes in the middle of disk writes and compaction as often as every entry, and liveness after faults heal; five clients of the key-value service make requests throughout |
| MIT 6.5840 Labs 3 and 4 | Bugs found by an independent test suite; timing and RPC-count limits | By hand, through the private adapter; a lab counts as passed after 20 consecutive passing runs with `-race` | Lab 3 (3A–3D) passes 20 runs in a row, no test over 58 s and no run over 412 s; so does Lab 4 (4A–4C), no test over 31 s and no run over 279 s |
| Porcupine | Client histories that are not linearizable | Every KV simulator seed; chaos runs; MIT Lab 4 | Checks the clients' history of every simulator seed, with reads through the log in half the seeds and through ReadIndex in the other half, and of every chaos run, before and after a full restart; Lab 4's tests check theirs with the course's own copy |
| Fuzzing and disk faults | Decoder panics, torn writes, lost fsyncs, corrupted files | 60 s per target per push; 10 min per target nightly and before each phase tag; crash tests on every test run | Fuzzes the WAL record, snapshot file, Raft message, key-value command, and key-value snapshot decoders; crash tests at every write boundary of the storage, snapshot saves included |
| Chaos test | Bugs only real processes, sockets, `kill -9`, and disks expose | Nightly, for 30 minutes; `make chaos DURATION=D` by hand | Passes 30 minutes of SIGKILLs, restarts, and partitions under five clients: 1,638 faults and 170,380 operations, with no violation and no lost acknowledged write |

A nightly workflow ([nightly.yml](../.github/workflows/nightly.yml)) runs `make sim SEEDS=50000` and `make fuzz FUZZTIME=10m`, and beside them, in a job of its own, `make chaos DURATION=30m`. GitHub runs it on the default branch, and it can be started by hand from the Actions tab.

## Unit tests and the election harness

These test the core directly. Both live in `raft/` and run with every `make test`; the simulator below then tests the same rules under faults.

- **Unit tests** drive one core with `Tick`, `Step`, `Propose`, and `ReadIndex` and check the `Ready` and `Status` it returns, with no sleeps. Each is named after the rule it covers, and the README's [paper-to-code map](../README.md#paper-to-code-map) lists which test covers which rule of Figure 2 and of §8. Tests of Figure 2's rules turn the election no-op off, so that a leader's log holds exactly what the test gave it; the no-op has tests of its own.
- **Scenarios from the paper** pass messages between a few cores by hand. `TestFigure7FollowersConverge` brings each of Figure 7's six follower logs into line with the leader's and counts the rejections it takes. `TestFigure8OldTermEntryNotCommittedByCounting` plays Figure 8's (c), (d), and (e) on five cores, and `TestNoOpCommitsEarlierTermEntries` plays (c) again with the no-op, which commits the entry of term 2 at once.
- **The three-node harness** ([`raft/cluster_test.go`](../raft/cluster_test.go)) runs whole elections in one goroutine. Each round ticks every running server once, then delivers messages in the order they were sent until none are left. Every server's `Ready` is handled as a runtime handles it: hard state saved to in-memory storage, then messages sent. A crashed server loses all but its storage and the messages sent to it while it is down; a restarted one boots from that storage. After every `Ready` the harness checks Election Safety: no two servers ever lead the same term.

`TestClusterElectsOneLeader` and `TestClusterReplacesFailedLeader` each run 1,000 seeds. A seed fixes every server's random source, so a failure names its seed and repeats on every run. Run them alone with:

```sh
go test ./raft -run TestCluster
```

## The key-value service

- **The store** ([`internal/kv`](../internal/kv)) is tested by applying commands at successive log indexes, as the log would. The tests show a retried write answered from its session's record, a retried Delete answering what it found the first time, a write older than its session's latest refused, eviction of the least recently active session and the refusal of every later write from it, sessions carried through a snapshot and a restore, equal states encoding equally, and malformed commands changing nothing. `TestCommandBytes` and `TestStateBytes` pin the two byte layouts that [ARCHITECTURE.md](ARCHITECTURE.md#the-key-value-service) shows.
- **The model** ([`internal/linearizability`](../internal/linearizability)) is tested on scripted histories: a sequential one, a stale read, a read of a value never written, a Delete that says a missing key existed, concurrent operations either way, a write that never returned taking effect at any time or never, and keys checked apart. An illegal history writes Porcupine's visualization.
- **The server and the client** ([`internal/kvserver`](../internal/kvserver), [`client`](../client), [`cmd/kvctl`](../cmd/kvctl)) are tested against clusters of three servers in one process, from [`internal/kvtest`](../internal/kvtest): raft nodes on memory storage that tick every millisecond, over a network that hands each message straight to its receiver, each serving the API on its own HTTP server. A test can stop and restart servers, and cut them off from the others. The tests cover both read modes; a follower naming the leader; a leader cut off from the others answering 504, and saying that a write's outcome is unknown; a client finding the leader past a stopped server and from one follower's address alone; a write that survives its leader being cut off, sent again with the same number to the next leader; a session evicted and replaced; a client that gives up when no majority can be reached; and one client used from eight goroutines at once.

## The runtime, the transport, and real processes

These run with every `make test`, under the race detector.

- **The runtime** ([`node/`](../node)): three nodes run in one process, each with storage in memory, over a network that hands each message straight to its receiver, ticking every millisecond. `TestNodesReplicateParallelProposals` has four goroutines propose a hundred commands at once and checks that every server applies them in the same order. `TestNodesReplaceStoppedLeader` stops the leader, has the other two elect another, and restarts the old one. `TestNodeCatchesUpThroughSnapshot` keeps a server down while the others commit and compact, then requires it to install the leader's snapshot, and later to restart from its own. `TestStopDuringTraffic` stops every node while proposals and messages are in flight. `TestReadIndexAtLeader` has four goroutines read through `ReadIndex` while one proposes, and `TestCutOffLeaderServesNoRead` cuts the leader off: it never confirms a read while cut off, though it still believes it leads, and fails the read once it steps down. Two tests play a node's peers by hand and record everything it does. `TestNodeSavesBeforeItSendsAndApplies` requires a vote and new entries to be saved before the replies that depend on them leave, and entries to be applied only after both. `TestNodeStopsForGoodOnStorageError` requires a failed save to stop the node with nothing sent or applied. `TestProposeReturnsOnceTheEntryIsSaved` holds a proposal's save back and requires `Propose` to wait for it.
- **The transport** ([`internal/transport/`](../internal/transport)) is tested over real HTTP on localhost. The tests deliver every RPC and check its encoding round trip. They hold a peer's answers back to show that `Send` never blocks and that a request times out, and take a peer down and bring it back to check that one log line reports each change. They also send the handler malformed, oversized, and misaddressed messages. `TestEveryMessageFitsMaxMessageSize` drives a core set up by `DefaultConfig` to its largest messages, with indexes and terms near the top of `uint64`: the most entries one AppendEntries may carry, the largest command `Propose` accepts, and a full snapshot chunk. Each must fit in the 4 MiB a server accepts; the largest takes 1.4 MB.
- **No goroutine outlives a test.** [`internal/leaktest`](../internal/leaktest) counts goroutines when a test starts and again after its cleanup, and prints every stack if any are left over.
- **The integration test**, `TestIntegrationKillLeader` in `cmd/kvnode`, builds `kvnode`, with the race detector when the test runs with it, and starts three processes on localhost, each compacting its log every four entries. Through the client library it writes six keys, kills the leader with `kill -9`, waits for the other two to elect a new leader in a later term, and writes six more. Then it restarts the old leader from its disk, and the old leader catches up through the new leader's snapshot, sent over HTTP. Every acknowledged write must read back. Then every server shuts down on SIGTERM, which must be clean, and the whole cluster restarts from its disks; every write must read back again, which takes each new leader's election no-op, since a read waits for an entry of the leader's term to commit. Under `-race`, a clean exit also means the server saw no data race. The test's cleanup kills every server still running before their directories are removed, pass or fail. Every wait gives up well before `go test`'s timeout, whose panic would skip the cleanup and leave servers listening. Before a phase is tagged, it must pass 50 times in a row, which takes about four minutes:

```sh
go test -race -count=50 -run TestIntegration ./...
```

## Race detector

`make test` runs every package with `-race`, locally and in CI. The Raft core is single-threaded by design, so races can only live in the runtime, transport, kvserver, and client. Their tests exercise concurrency on purpose: parallel proposals and reads, a stop while messages and proposals are in flight, a leader stopped mid-run, and one client used from many goroutines. The integration and chaos tests build the server itself with the race detector and fail if a server reports a race.

## Deterministic simulator

[`internal/sim`](../internal/sim) runs a whole cluster in one goroutine on a virtual clock. One random source, seeded by the run's seed, makes every choice: each server's clock skew (up to 20% fast or slow), each message's delay, loss, and duplication, every fault, every client's choices, and each server core's own random source for election timeouts. The simulator uses only integer arithmetic, so a seed replays byte for byte on any machine; traces match on arm64 and amd64.

Each server handles every `Ready` as a runtime does: it saves the hard state, snapshot, and entries, sends the messages, restores its key-value store from a snapshot the leader sent, applies the committed entries to it, then answers the reads the `Ready` confirmed. It compacts its log once it has applied a set number of entries past its last snapshot, as often as every entry in some seeds, with its store's snapshot as the data. It saves through the real `FileStorage`, on its own `FaultFS`: a disk in memory that keeps what was synced through every crash, and a random prefix of the rest ([Fuzzing and disk fault injection](#fuzzing-and-disk-fault-injection)). A crash between events loses nothing, since every `Save` synced before it returned. A crash during a disk operation fails the `Save` under way, so none of that `Ready`'s messages leave. It may leave a torn record for recovery to truncate when the server restarts, or a snapshot save undone. After every event, the simulator checks the five properties of Figure 3 and more:

- **Election Safety:** no two servers ever lead the same term, over the whole run.
- **Leader Append-Only:** a leader never writes at or below its last stored index.
- **Log Matching:** every stored copy of an entry agrees on its type, its command, and the term of the entry before it, which by induction makes two logs sharing an entry identical up to it.
- **Leader Completeness:** every new leader, and every leader when an entry newly commits, holds each entry committed in an earlier term.
- **State Machine Safety:** no two servers ever apply different entries at one index.
- **Terms never fall:** a server's `currentTerm` never decreases, and a restarted server comes back in at least the highest term it saved. A term lost with a crashed `Save` never left in a message, so it does not count.
- **One vote per term:** no server votes for a second candidate in a term, even after a restart. A vote counts once its `Save` returns, and every restart checks the vote it restored against the votes saved.
- **Applied order:** each server keeps `lastApplied ≤ commitIndex ≤` its last index, neither ever falls within a boot, and it applies entries one by one after its snapshot, jumping forward only to install a snapshot from the leader. A read is confirmed only once the server has applied its read index.
- **Snapshots:** every snapshot a server compacts, installs, or restarts from holds exactly the state of a store that applied the entries through its last included index, and that entry's term (State Machine Safety). A reference store applies each committed entry once, for the comparison. The simulator's copy of each log keeps the committed entries a snapshot replaced, so the checks above go on reading whole logs, and a leader's compaction may not change the entries after its snapshot (Leader Append-Only).

The first violation ends the run.

### Clients

Five clients of the key-value service run throughout. Each registers a session, then makes one request at a time: a Get, Put, or Delete of one of two keys, few so that operations overlap, each Put with a value no other uses. Half its requests go to the server that last answered it and half to a random server, as a client that lost track of the leader would, so that old leaders cut off in a minority get requests too. Requests and answers cross the simulated network, lost, duplicated, and delayed like Raft messages, but no partition cuts a client off. A client sends a request again, unchanged, to another server when it hears nothing within 300 ms, follows a server's hint when that server does not lead, and registers a new session when its own has expired.

Servers answer as the key-value service does. A server that does not lead says so and names the leader it knows of. A leader proposes a write, a Register, or a Get through the log under a request ID of its own, and answers once the command applies on it; it confirms a Get through ReadIndex, then answers from its store. A crash loses the requests a server was working on.

Every Get, Put, and Delete goes into the run's history, with virtual times. A write whose session expired stays open to the end of the history, as does every request still unanswered when the run ends, since whether it ran is unknown. At the end, Porcupine checks the history ([below](#porcupine-linearizability-checks)).

### The randomized schedule

`TestRandomized` runs seeds 0 to N−1 in parallel. Seed s runs a cluster of 1 + (s mod 5) servers for ten seconds of virtual time and injects a fault every 50 ms to 1 s: a partition into two or three groups, a heal, a one-way cut, a crash, a restart, or new network conditions (up to 50% loss, 20% duplication, and 400 ms delay). Half of all crashes hit the current leader, if one is up; half strike at once, between events, and the rest during one of the server's next six disk operations. Meanwhile the clients make requests. In half the seeds they first wait up to two seconds whenever a leader of a later term appears, which leaves a new leader without a command of its own term to commit. The seed also chooses:

- how often servers compact: never, or after every 1, 2, 4, or 8 entries applied, the same for every server or, in half the seeds, drawn for each server on its own;
- how leaders send snapshots, whole or in chunks of 16 or 128 bytes, and, in half the seeds, at most three entries per AppendEntries;
- whether each new leader commits a no-op (seeds whose second bit is 0) and whether reads go through ReadIndex (odd seeds) or the log, so each of the four pairings gets a quarter of the seeds;
- whether each store keeps five sessions, as many as there are clients, so that a repeated Register evicts a session still in use, or 1,000.

Then every fault heals: all links return, the network goes back to 1–10 ms with no loss, crashes still to come are cancelled, and crashed servers restart. The liveness check follows: within ten seconds the servers must agree on a leader, within ten more apply a command proposed to it, which commits every entry before it in the leader's log, and within ten more every client must finish a request. Last, Porcupine checks the history; a check that has not finished within ten seconds skips the seed with a warning, which `-v` shows, and means the workload needs resizing.

Over 1,000 seeds:

- 844 crashes are set to strike mid-write and 776 do, 286 of them leaving a torn record that recovery truncates. 776 more crashes strike between events.
- Servers compact 95,568 times, restart from a snapshot 1,082 times, and install 4,262 snapshots from a leader.
- Clients begin 138,247 requests and send 366,270 attempts: 135,742 are redirected, 92,556 time out, and 578 find their session expired. Leaders confirm 28,888 reads through ReadIndex.

Some bugs need rare interleavings, so probes, bugs put in on purpose and then removed, measure what the sweep catches:

- A commit rule that counted replicas of earlier-term entries, the hazard of Figure 8, fails 11 of 50,000 seeds, all as Leader Completeness violations. Nine are seeds without the no-op, where a new leader has no entry of its own term to commit until a client's command arrives, and six of the eleven pause the clients after each new leader. Before the clients and the no-op, a single client proposing every 10 to 200 ms made it fail 30 of 50,000.
- A core that treated the entries inside a follower's snapshot as mismatched fails 5 of 2,000, all liveness timeouts in seeds that give each server its own compaction schedule. One whose InstallSnapshot did not restart the election timer fails 1 of 2,000, so its unit test remains the check that counts.
- A leader that confirmed reads without a round of heartbeats fails 305 of 20,000 seeds, and one that did not first commit an entry of its term fails 245 of 20,000, each as a history Porcupine finds not linearizable, and each only in seeds that read through ReadIndex. The second fails in clusters of one and two servers too: a server that restarts and leads a new term does not yet know which of its entries are committed, which is exactly §8's point.
- A store that ran a retried write again fails 700 of 2,000.

The nightly sweep is there for the rare ones, and Figure 8, ReadIndex's two conditions, and the timer restart also have scripted or unit tests of their own.

| Run | Seeds |
| --- | --- |
| `go test ./...` | 20 |
| `make sim`, part of `make check` on every push | 1,000 |
| `make sim SEEDS=20000`, before each phase tag | 20,000 |
| The nightly workflow | 50,000 |

A seed costs about 6 ms of CPU, its clients and Porcupine check included, so 50,000 seeds take under five minutes of CPU: 35 seconds on a 10-core Mac, and 20,000 take 15.

### Reading a trace

Every run keeps a trace, and a failing seed prints it after the error. These lines are picked from seed 306's, replayed with `-seed=306`, which adds the messages:

```text
t=0.000ms n1 boot term=0 votedFor=none snapshot=0 lastIndex=0 tick=11.280ms
t=331.259ms n2 candidate→leader term=1 leader=none→n2
t=4301.003ms n2 crash
t=10071.605ms c1 → n2 delete("y") session=4 seq=8
t=10091.170ms heal
t=10091.170ms n2 boot term=4 votedFor=n2 snapshot=104 lastIndex=105 tick=9.690ms
t=10112.943ms n1→n2 RequestVote term=14 candidateId=1 lastLogIndex=105 lastLogTerm=4
t=10116.232ms n2→n1 RequestVoteResponse term=14 voteGranted=true
t=10116.232ms n1 candidate→leader term=14 leader=none→n1
t=10124.042ms c5 timed out
t=10124.042ms c5 → n1 put("y", "c5.12") session=2 seq=12
t=10124.422ms n1 propose get("x"): index=106 term=14
t=10126.193ms n1→n2 AppendEntries term=14 leaderId=1 prevLogIndex=105 prevLogTerm=4 entries=[106@14] leaderCommit=104
t=10128.615ms n1 propose put("y", "c5.12") session=2 seq=12: index=107 term=14
t=10131.021ms n2→n1 AppendEntriesResponse term=14 success=true matchIndex=106
t=10131.021ms n1 compact lastIncludedIndex=106
t=10131.021ms n1 leader term=14 leader=n1 commitIndex=104→106 lastApplied=104→106
t=10140.234ms n1 leader term=14 leader=n1 commitIndex=106→107 lastApplied=106→107
t=10148.874ms n1 → c5 ok
t=10161.007ms n1 → c3 "c1.7"
t=10257.088ms n2 delete("y") session=4 seq=8: raft: not the leader
t=10260.315ms n2 → c1 not the leader; leader=n1
t=10277.309ms c1 → n1 delete("y") session=4 seq=8
```

Seed 306 runs two servers that compact after every entry, with leaders sending at most three entries per AppendEntries, no election no-op, reads through the log, and room for five sessions. Midway through the faults n2 crashes, and it restarts when they heal, from its snapshot through entry 104 and the one entry after it, 105 of term 4. n1 wins term 14 with n2's vote. With no no-op, n1 commits entry 105 only together with an entry of its own term (§5.4.2): the liveness check's Get, entry 106, which commits once n2 acknowledges it, and commitIndex jumps from 104 to 106. Client c5, whose Put went unanswered, sends it again, unchanged, to n1, which answers once entry 107 applies; c3's Get is answered with the value c1 wrote. A Delete that c1 sent to n2 just before the heal reaches n2 after it restarts, slowed by the faulty network; n2 answers that it does not lead and names n1, and c1 sends the Delete there with the same session and number.

- `t=` is virtual time since the run began, in milliseconds to the microsecond.
- `boot` starts a server from its snapshot and write-ahead log, showing the term and vote it restored, its snapshot's last included index (0 for none), its last log index, and its skewed tick interval. `crash` stops a server between events. `will crash during disk operation N from now` sets a crash to strike during the server's Nth disk operation from then, and `crash during a disk write` is that crash striking.
- `compactEvery=N` at the start of a run names the server's own compaction threshold, in seeds that give each server one.
- `compact` is a server's store taking a snapshot through the index shown. `install snapshot` is a server resetting its store from a snapshot the leader sent.
- A `msg=` line is the server's storage logging, such as recovery truncating a record torn by a crash, with the segment, the offset, and the bytes dropped.
- A server's own line shows its role, term, and leader after an event, with `old→new` for each one that changed, and `commitIndex` and `lastApplied` when they moved.
- `propose` shows a command reaching a server's log, `get`, `put`, or `delete` with the session and number of a write, or `register`: the index and term of the new entry, or why it was refused. `read "x": id=7` is a leader asked to confirm a read through ReadIndex, and `confirm read id=7 index=40` its confirmation.
- `c5 → n1 …` is client c5 sending a request to n1, and `n1 → c5 …` n1's answer: `ok` for a Put, the value or `found=` for a Get or a Delete, `session=` for a Register, an error, or `not the leader` with the leader's name. `c5 timed out` means it heard nothing in time and will send the request again elsewhere.
- `partition` (groups separated by `|`), `cut`, `heal`, and `network` are faults.
- A final `FAILED:` line names the violation, the liveness check that timed out, or a history that is not linearizable, with the file holding Porcupine's visualization of it.

### Replaying a seed

```sh
go test ./internal/sim -run TestRandomized -seed=N -v
```

This reruns seed N alone and logs its trace with every message added. A delivery shows as `n2→n3 RequestVote term=4 candidateId=2 lastLogIndex=0 lastLogTerm=0`, or `n3→n1 AppendEntries term=1 leaderId=3 prevLogIndex=0 prevLogTerm=0 entries=[1@1:no-op] leaderCommit=0` with entries as index@term, and a no-op marked as such, or `n2→n1 InstallSnapshot term=18 leaderId=2 lastIncludedIndex=33 lastIncludedTerm=5 offset=16 bytes=16 done=false` with the chunk's size in bytes, using the paper's field names. An AppendEntries and its reply show `readRound=N` once the leader has begun a round of leadership checks. A rejection names the `prevLogIndex` of the request it answers. A message the network loses or copies as it is sent is marked `lost` or `duplicated`; one stopped on arrival by a partition or a crashed receiver is marked `cut` or `down`.

### Scripted scenarios

`sim.New` builds a cluster for a scripted test, with `Options` for its size, seed, compaction threshold, snapshot chunk size, election no-op, read mode, and session limit. `RunFor`, `RunUntil`, `Propose`, `Request`, `Partition`, `Cut`, `Heal`, `Crash`, `CrashDuringDiskOp`, `Restart`, and `SetNetwork` drive it, and `Status`, `AgreedLeader`, and `Leaders` inspect it. `Request` hands a key-value command to a server as a client would, and the returned call says when, and with what, the server answered. `TestIsolatedLeaderIsReplaced`, `TestMinorityPartitionNeverElects`, `TestCrashedLeaderIsReplaced`, and `TestElectsLeaderDespiteLossAndDuplication` are examples. `TestCrashesMidWriteKeepSafety` crashes a server mid-write every 200 ms in a cluster of five taking proposals and compacting every three entries, about 900 crashes over its ten seeds, and then requires the cluster to commit a new command everywhere. `TestLaggingServerCatchesUpThroughSnapshot` cuts a server off while the others commit thirty writes and compact, then requires it to catch up from the leader's snapshot, sent in 16-byte chunks. `TestProposalBurstSendsFewAppendEntries` proposes twelve commands at one instant and counts the AppendEntries each follower receives until every server applies them: two or three, where a leader that resent unacknowledged entries on every proposal sent 14 to 18. It allows six. Three scenarios cover §8:

- `TestPartitionedLeaderServesNoStaleRead` cuts a leader off in a minority of five while the majority elects another and takes a write; in both read modes the old leader never answers a read while cut off.
- `TestNewLeaderServesNoReadBeforeItsTermCommits` crashes a leader right after it committed a write and answered for it, before any follower learned that the write committed. Without the no-op, the next leader confirms no read until a write of its own term commits, and the read then sees the first write.
- `TestRetriedWriteRunsOnceAcrossLeaderChange` loses the answer to a committed write with its leader. The client's retry, with the same number, is answered from the session's record at the next leader, after another session's write to the same key, and again after every server restarts from snapshots taken after every entry.

## MIT 6.5840 test suites

MIT's 6.5840 course publishes test suites for Raft (Lab 3, parts 3A–3D) and for a key-value service on top of it (Lab 4, parts 4A–4C). Running them against this code gives evidence from tests written by someone else, with limits of their own: at most ten heartbeats a second, a new leader within five seconds of a failure, few RPCs per agreement and few bytes per command, a small persisted state once snapshots begin, at most 120 seconds for any test, and at most 600 seconds for a whole lab. Both run through the adapter below, and both run again after every change to the core.

**Keep the adapter private.** The course asks that solutions not be published, and this repository is public. The adapter that connects the course's API to this code therefore lives in a separate private repository, and no course source is ever committed here or pushed anywhere. The course checkout goes in `.mitlab/`, which git ignores. Never copy course files, or the adapter, into a tracked directory.

**Setup.** You need git, Go, and a checkout of the private adapter. [scripts/mitlab.sh](../scripts/mitlab.sh) clones the course repository into `.mitlab/` once, over `git://`, so the network must let git's port 9418 through. `git://` is not authenticated, so the pinned commit is what vouches for the code: git checks every object it fetches against its hash. On every invocation the script resets the clone to that commit, copies the adapter's files over the course's skeletons (`raft1/raft.go` for Lab 3; `kvraft1/server.go`, `kvraft1/client.go`, and `kvraft1/rsm/rsm.go` for Lab 4), and adds a `replace` directive that points the course module at this checkout. `go get` then raises the course module's `go` line to this module's, as Go requires of a module that depends on a newer one, and its Porcupine to this module's v1.3.1. A second `replace` pins Porcupine back to the course's own v1.0.3, so Lab 4's linearizability checks run as the course ships them; the adapter imports only `raft`, which needs none. The raised `go` line turns on a vet check that the course's own test files fail, so the tests run with `-vet=off`. Lab 4's parts live in two packages: 4A in `kvraft1/rsm`, 4B in `kvraft1`, and 4C in both, since `TestSnapshot4C` tests the rsm; the script runs the packages one after the other. Each part runs with `-race`:

```sh
MITLAB_ADAPTER=../wilson-mitlab make mitlab PART=3B             # one part: 3A to 3D, or 4A to 4C
MITLAB_ADAPTER=../wilson-mitlab make mitlab RUNS=20             # all of Lab 3, twenty times
MITLAB_ADAPTER=../wilson-mitlab make mitlab PART=4 RUNS=20      # all of Lab 4, twenty times
MITLAB_ADAPTER=../wilson-mitlab scripts/mitlab.sh -n 20 -p 4 4  # all of Lab 4, four runs at a time
```

**The adapter.** In the course's tests each Raft peer runs in a process of its own, and the test process relays every RPC through a simulated network that delays, drops, reorders, and partitions. The adapter is the runtime for one core in such a process, playing the part `node/` plays in `kvnode`. It configures the core with `raft.MITLabConfig()`, which leaves the election no-op off because the tests check which command sits at each index, ticks it every 10 ms, and maps `Start`, `GetState`, and `Snapshot` onto `Propose`, `Status`, and `Compact`. After every call into the core it handles the `Ready` in the contract's order: it saves the hard state, the snapshot's index and term, and the log after it, together with the snapshot, in one call to the course's persister; then it sends the messages; then it queues the committed entries, and any snapshot from the leader, for a goroutine that delivers them to the tests in order. Each Raft RPC is a single call of the course's RPC library, with the core's response as its reply, because the tests count calls and a response sent as a message of its own would double them. Every server process is built with the race detector, and the script fails a run whose output reports a data race, even if every test passed.

For Lab 4 the adapter adds the three pieces the course leaves to its students. The rsm proposes each operation through `Start`, tagged with a random 64-bit ID, and waits until its one reader goroutine applies it, or until its server leaves the term it proposed in, when it answers that the server is not the leader; the ID tells an operation from another one a new leader put at the same index, which a counter could not after a restart. The reader applies operations in log order, restores the leader's snapshots, and snapshots once the persisted state reaches the tests' limit. Its core runs with the election no-op on, as `kvnode`'s does, since Lab 4's tests never look at log indexes; the course's `applyCh` carries no entry types, so no-ops never reach it. On top of the rsm, the course's key-value server keeps a value and a version for each key and applies a Put only when its version matches, which makes a retried Put safe without client sessions, and its clerk finds the leader and retries, reporting a Put whose retry finds its version taken as maybe applied. Those versioned semantics stay inside the adapter; this project's own key-value API does not change.

**The repeat runner.** With `-n N`, the script runs the part N times, `-p` at a time, logs each run under `.mitlab/logs/`, and prints how many runs each test passed, each test's longest time, and the slowest run. It exits non-zero if any run failed.

**Results.** Against the course's tests at the pinned commit (February 2026), all of Lab 3 passes twenty runs in a row with `-race`, four at a time on a 10-core Mac: 560 test runs, with no failure and no data race. The slowest test took 57.2 s (`TestSnapshotInstallUnreliable3D`), against the course's limit of 120 s, and a whole run took 383 to 411 s, a mean of 395 s, against its 600 s. These runs include the adapter's changes for Lab 4, which leave Lab 3's configuration as it was: `MITLabConfig` leaves the no-op off, and the tests make no reads, so their AppendEntries carry read round 0. Before the election no-op and ReadIndex existed, the same twenty runs took 381 to 405 s.

| Part | Tests | Mean time | Slowest |
| --- | --- | --- | --- |
| 3A, elections | 3 | 15.4 s | 16.4 s |
| 3B, agreement | 10 | 49.3 s | 52.2 s |
| 3C, persistence | 8 | 141.0 s | 146.9 s |
| 3D, snapshots | 7 | 187.6 s | 197.8 s |

The first run, before the election no-op and ReadIndex existed, found one failure. `TestCount3B` counted 46 RPCs to agree on twelve commands, where it allows 42: the leader sent each follower its unacknowledged entries again on every proposal, and again on every reply that left the follower behind. `TestProposalBurstSendsFewAppendEntries` reproduces it in the simulator, and the leader now sends a new entry at once only to followers owed nothing earlier ([ARCHITECTURE.md](ARCHITECTURE.md#log-replication)).

All of Lab 4 passes twenty runs in a row with `-race`, four at a time on the same Mac: 580 test runs, with no failure, no data race, and no labgob warning, and it passed every test from its first run. The slowest test took 30.6 s (`TestPersistPartitionUnreliableLinearizable4B`), and a whole run took 261 to 279 s, a mean of 269 s, against the course's 600 s.

| Part | Tests | Mean time | Slowest |
| --- | --- | --- | --- |
| 4A, the rsm | 7 | 56.2 s | 57.6 s |
| 4B, the key-value service | 13 | 126.3 s | 140.0 s |
| 4C, snapshots | 9 | 83.6 s | 93.3 s |

A failure found here is handled like any other heavyweight-harness failure: a scripted simulator test reproduces it first, then the core is fixed. The adapter changes only when the adapter itself is wrong.

## Porcupine linearizability checks

[Porcupine](https://github.com/anishathalye/porcupine) v1.3.1, the project's one test dependency, checks client histories for linearizability, through [`internal/linearizability`](../internal/linearizability):

- **The recorder** logs each operation's call and return times from one clock under one lock, so that times are ordered across goroutines as they happened; Porcupine warns that times read without such ordering can show violations that never happened. The simulator's clock is its virtual time.
- **The model** is the key-value store, one key per partition, since operations on different keys never constrain each other; that keeps checks fast. A Get must return the latest value, and a Delete must say whether the key existed. It describes each operation and state, so the visualization reads as `put("x", "c1.3") → ok` and `get("x") → "c1.3"`.
- **Unknown outcomes.** An operation whose outcome its client never learned, because its answer never came or its session expired, stays open to the end of the history, and the model accepts any answer for it: it may have taken effect at any time after its call, or never.
- **Failures.** An illegal history fails the run, and Porcupine's HTML visualization of it is written to a file in the system's temporary directory, which the error names. A check that does not finish within its timeout is reported as a warning, not a pass.

Every seed of the randomized schedule checks its history, and 20,000 seeds pass: 10,000 reading through ReadIndex and 10,000 through the log, each half with the no-op on and half with it off. The probes [above](#the-randomized-schedule) show what the check catches: stale reads from a leader that skipped its heartbeat round or its term's first commit, and retried writes run twice. The [chaos test](#chaos-test) checks its clients' history twice, before and after a full restart, and MIT's Lab 4 tests check theirs with the course's own copy of Porcupine.

## Fuzzing and disk fault injection

`make fuzz` runs [scripts/fuzz.sh](../scripts/fuzz.sh), which finds every `Fuzz*` target in the module with `go test -list` and runs each for `FUZZTIME`. A new target is picked up without any change to the Makefile or CI. Corpora live in each package's `testdata/fuzz/`.

- `FuzzWALRecordDecode` (in `internal/storage`) feeds arbitrary bytes to the WAL record decoder. It must never panic, and every record it accepts must encode back to exactly the bytes it came from, so no two byte strings decode to the same record.
- `FuzzSnapshotDecode` (in `internal/storage`) does the same for the snapshot file decoder.
- `FuzzMessageDecode` (in `internal/transport`) feeds arbitrary bytes to the decoder for Raft messages arriving over HTTP. It must never panic, and a message it accepts must encode to JSON that decodes to the same message again.
- `FuzzCommandDecode` and `FuzzStateDecode` (in `internal/kv`) do the same for the key-value command decoder and the store's snapshot decoder: each must refuse every byte string its encoder would not write.

File storage reaches the disk only through a small file-system interface ([ARCHITECTURE.md](ARCHITECTURE.md#file-systems)), and `FaultFS` implements it in memory with crashes on demand: at once, or during the nth write, truncation, file creation, rename, removal, or fsync. A crash keeps everything synced and a random prefix of the rest, as a power failure would, the last write possibly cut mid-record and the last renames and removals undone. A seed fixes every choice, so a failure repeats.

- `TestCrashAtEveryWriteBoundary` saves a run of `Ready`s, overwrites, segment changes, and two snapshots among them. It crashes during each of their 55 disk operations, 30 seeds each, then again during each of the first four operations of the recovery that follows. Recovery must return everything each finished `Save` wrote plus a prefix of the `Save` that crashed, never less, and must keep taking writes. A `Save` with a snapshot is all or nothing: the old snapshot and log, or the new ones.
- `TestRestoreEqualsFullReplay` runs a one-server cluster that compacts at random and restarts from a crashed disk about two hundred times over twenty seeds, each restart beginning a term with its no-op. Each time, the state machine it rebuilds from its snapshot must hold exactly the commands its snapshot includes, and after the log that follows, exactly the commands replaying the whole log would.
- `TestSnapshotRemovesWhatItReplaces`, `TestRecoveryRemovesLeftovers`, and `TestBrokenSnapshotRefusesToStart` check that a durable snapshot removes the segments and snapshot it replaces, that recovery removes what a crash left behind, and that a missing, damaged, or mismatched snapshot file refuses to start.
- On the real file system, `TestTornFinalRecordIsTruncated` cuts the last record at every length, `TestDamagedRecordRefusesToStart` damages every byte of every record, and `TestMissingSegmentRefusesToStart` deletes and shortens segments. [ADR-0003](adr/0003-wal-format.md) says what recovery truncates and what it refuses.
- `TestFileStorageMatchesMemory` checks file storage step by step against the in-memory reference, reopening it after every step.

## Chaos test

`TestChaos` in [`test/chaos`](../test/chaos) runs the system as it runs in production, five `kvnode` processes on localhost with their logs on disk, and does to it what the simulator cannot: it kills processes with SIGKILL and cuts real TCP connections.

- **Proxies.** A TCP proxy sits on each of the 20 directed links between servers. Server i's `-peers` names proxy i→j as server j's address, so every message from i to j crosses that proxy, and the servers carry no test hooks. A cut proxy closes the connections it carries and holds new ones without forwarding a byte, as a partitioned network would, so senders time out rather than see a refused connection; healing it releases them. Clients reach each server at its own address. A leader hint names the address the hinting server sends to, which is a proxy, so a client that follows one may cross a link the test has cut; it then retries elsewhere or gives up. `TestProxyCutsAndHeals` checks the proxy on every `make test`.
- **Servers.** They are built with the race detector. Odd-numbered servers serve reads through ReadIndex and even-numbered ones through the log, and server i compacts its log every 10·2^(i−1) entries, from 10 to 160, so both read paths and InstallSnapshot run under every fault.
- **Faults.** Every 0.2 to 2 s the test picks one: kill a server with SIGKILL, the leader half the time, while at most two are down; restart one from its disk; partition one or two servers from the rest, both ways; cut one directed link; or heal every link. A server that exits on its own, by a panic or a refusal to recover, fails the test. The schedule goes to `faults.log`.
- **Clients.** Five clients, each with its own session, run Gets (half of their operations), Puts, and Deletes back to back through the client library, each giving up after a random 0.2 to 5 s. A write given up on stays open in the history, since it may yet take effect; a Get given up on returns with any value, since it changed nothing. Keys change every five seconds, three at a time, so each key's history, and so each of Porcupine's partitions, stays short.
- **Checks.** When the faults stop, every link heals, every server restarts, and Porcupine checks the history. Then every server is killed at once and restarted from its disk, and a sixth client reads every key back. The history with those reads must still be linearizable, so an acknowledged write that the restart lost, or one that came back after being overwritten, fails the test. Last, every server must shut down cleanly on SIGTERM, and no server's output, in any of its lives, may report a data race.

```sh
make chaos DURATION=30m
```

Without `-duration` the test is skipped, so `make test` leaves it out. `make chaos` turns off `go test`'s timeout, whose panic would skip the cleanup that kills the servers; every wait has a bound of its own instead. A failed run keeps the servers' logs, the fault schedule, and any visualization in a temporary directory, which the failure names.

**Results.** A 30-minute run on a 10-core Mac injected 1,638 faults, 263 kills, 261 restarts, 391 partitions, 355 single-link cuts, and 368 heals, through 2,146 terms. Its five clients ran 170,380 operations on 1,083 keys: 84,402 Gets answered and 514 given up, and 84,895 writes acknowledged and 569 of unknown outcome. Both histories were linearizable, each checked in under 0.3 s; all 1,083 keys read back after the full restart; and every server shut down cleanly, with no race reported.

**What it catches.** Three deliberate bugs, each given three minutes, fail it at its first check: reads confirmed without a heartbeat round, retried writes applied a second time, and Puts acknowledged as soon as they are proposed rather than once they apply.

The test cannot see a missing fsync: SIGKILL leaves the operating system's page cache intact, so everything a server wrote before it died survives. The storage's [crash tests](#fuzzing-and-disk-fault-injection) cover that, with a file system that loses unsynced writes.

## Static analysis

`make lint` runs golangci-lint (pinned in the Makefile) with `staticcheck`, `revive`, `errcheck`, `gocyclo`, `funlen`, `nestif`, and `misspell`, configured in [.golangci.yml](../.golangci.yml). `make fmt` and `make vet` run gofmt and go vet on their own.
