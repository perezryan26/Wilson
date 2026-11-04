# Testing

Six independent harnesses produce the evidence for Wilson's correctness, and no single one is trusted alone. This file says what each one catches, how to run it, and how to read its output. Harnesses that are not built yet are marked as such.

## Everyday checks

`make check` is what CI runs on every push: gofmt, go vet, golangci-lint, `go test -race ./...`, a simulator sweep of 1,000 seeds, and 60 seconds of fuzzing per target. Override the sizes with `make sim SEEDS=N` and `make fuzz FUZZTIME=D`.

| Harness | What it catches | When it runs | State |
| --- | --- | --- | --- |
| Race detector (`-race`) | Data races in the runtime, transport, kvserver, and client | Every test run | Runs every test; the code so far is single-threaded |
| Deterministic simulator | Safety violations under drops, delays, reordering, duplication, partitions, crashes, and tick skew | 1,000 seeds per push; 20,000 before each phase tag; 50,000 nightly | Checks Election Safety, terms, and votes after every event, and liveness after faults heal |
| MIT 6.5840 Labs 3 and 4 | Bugs found by an independent test suite; timing and RPC-count limits | 20 consecutive passing runs | Not built |
| Porcupine | Client histories that are not linearizable | Every KV simulator seed; chaos runs; MIT Lab 4 | Not built |
| Fuzzing and disk faults | Decoder panics, torn writes, lost fsyncs, corrupted files | 60 s per target per push; 10 min per target nightly and before each phase tag | Wired; no targets yet |
| Chaos test | Bugs only real processes, sockets, `kill -9`, and disks expose | Nightly | Not built |

A nightly workflow ([nightly.yml](../.github/workflows/nightly.yml)) runs `make sim SEEDS=50000` and `make fuzz FUZZTIME=10m`. GitHub runs it on the default branch, and it can be started by hand from the Actions tab.

## Unit tests and the election harness

These test the core directly. Both live in `raft/` and run with every `make test`; the simulator below then tests the same rules under faults.

- **Unit tests** drive one core with `Tick` and `Step` and check the `Ready` and `Status` it returns, with no sleeps. Each is named after the rule it covers, and the README's [paper-to-code map](../README.md#paper-to-code-map) lists which test covers which rule of Figure 2.
- **The three-node harness** ([`raft/cluster_test.go`](../raft/cluster_test.go)) runs whole elections in one goroutine. Each round ticks every running server once, then delivers messages in the order they were sent until none are left. Every server's `Ready` is handled as a runtime handles it: hard state saved to in-memory storage, then messages sent. A crashed server loses all but its storage and the messages sent to it while it is down; a restarted one boots from that storage. After every `Ready` the harness checks Election Safety: no two servers ever lead the same term.

`TestClusterElectsOneLeader` and `TestClusterReplacesFailedLeader` each run 1,000 seeds. A seed fixes every server's random source, so a failure names its seed and repeats on every run. Run them alone with:

```sh
go test ./raft -run TestCluster
```

## Race detector

`make test` runs every package with `-race`, locally and in CI. The Raft core is single-threaded by design, so races can only live in the runtime, transport, kvserver, and client. Their tests exercise concurrency on purpose: parallel proposals, shutdown during sends, and a leader change mid-request.

## Deterministic simulator

[`internal/sim`](../internal/sim) runs a whole cluster in one goroutine on a virtual clock. One random source, seeded by the run's seed, makes every choice: each server's clock skew (up to 20% fast or slow), each message's delay, loss, and duplication, every fault, and each server core's own random source for election timeouts. The simulator uses only integer arithmetic, so a seed replays byte for byte on any machine; traces match on arm64 and amd64.

Each server handles every `Ready` as a runtime does: it saves the hard state, then sends the messages. Crashes happen between events, so a crash never interrupts a `Ready`, and storage keeps everything saved. After every event, the simulator checks:

- **Election Safety** (Figure 3): no two servers ever lead the same term, over the whole run.
- **Terms never fall:** a server's `currentTerm` never decreases, even across a restart.
- **One vote per term:** no server saves a vote for a second candidate in a term, even after a restart.

The first violation ends the run.

### The randomized schedule

`TestRandomized` runs seeds 0 to N−1 in parallel. Seed s runs a cluster of 1 + (s mod 5) servers for ten seconds of virtual time and injects a fault every 50 ms to 1 s: a partition into two or three groups, a heal, a one-way cut, a crash, a restart, or new network conditions (up to 50% loss, 20% duplication, and 400 ms delay). Then every fault heals: all links return, the network goes back to 1–10 ms with no loss, and crashed servers restart. Within ten more seconds, the servers must agree on a leader, which is the liveness check.

| Run | Seeds |
| --- | --- |
| `go test ./...` | 20 |
| `make sim`, part of `make check` on every push | 1,000 |
| `make sim SEEDS=20000`, before each phase tag | 20,000 |
| The nightly workflow | 50,000 |

A seed costs about a millisecond of CPU, so 50,000 seeds take seconds.

### Reading a trace

Every run keeps a trace, and a failing seed prints it after the error:

```text
t=0.000ms n1 boot term=0 votedFor=none tick=11.670ms
t=301.982ms n2 follower→candidate term=0→1 leader=none
t=309.515ms n2 candidate→leader term=1 leader=none→n2
t=312.656ms n3 follower term=1 leader=none→n2
t=475.330ms partition n5 | n1 n2 n3 n4
t=928.868ms n5 follower→candidate term=1→2 leader=n2→none
t=1328.216ms network loss=50% duplication=0% delay=1.000ms..400.000ms
t=1636.130ms n2 leader→follower term=1→2 leader=n2→none
```

- `t=` is virtual time since the run began, in milliseconds to the microsecond.
- `boot` starts a server from its storage, showing the term and vote it restored and its skewed tick interval. `crash` stops a server.
- A server's own line shows its role, term, and leader after an event, with `old→new` for each one that changed.
- `partition` (groups separated by `|`), `cut`, `heal`, and `network` are faults.
- A final `FAILED:` line names the violation, or the liveness check that timed out.

### Replaying a seed

```sh
go test ./internal/sim -run TestRandomized -seed=N -v
```

This reruns seed N alone and logs its trace with every message added. A delivery shows as `n2→n3 RequestVote term=4 candidateId=2 lastLogIndex=0 lastLogTerm=0`, with the paper's field names. A message the network loses or copies as it is sent is marked `lost` or `duplicated`; one stopped on arrival by a partition or a crashed receiver is marked `cut` or `down`.

### Scripted scenarios

`sim.New` builds a cluster for a scripted test. `RunFor`, `RunUntil`, `Partition`, `Cut`, `Heal`, `Crash`, `Restart`, and `SetNetwork` drive it, and `Status`, `AgreedLeader`, and `Leaders` inspect it. `TestIsolatedLeaderIsReplaced`, `TestMinorityPartitionNeverElects`, `TestCrashedLeaderIsReplaced`, and `TestElectsLeaderDespiteLossAndDuplication` are examples.

## MIT 6.5840 test suites

Not built yet. MIT's 6.5840 course publishes test suites for Raft (Lab 3, parts 3A–3D) and for a key-value service on top of it (Lab 4, parts 4A–4C). Running them against this code gives evidence from tests written by someone else.

The course asks that solutions not be published. This repository is public, so the adapter that connects the course's API to this code lives in a separate private repository, and no course source is ever committed here. The course checkout goes in `.mitlab/`, which git ignores.

## Porcupine linearizability checks

Not built yet. Client histories recorded by the simulator (with virtual timestamps), the chaos test, and MIT Lab 4 are checked with [Porcupine](https://github.com/anishathalye/porcupine). An operation whose outcome is unknown, such as one that timed out, is recorded as pending, and the model accepts any result for it. An illegal history fails the test and writes Porcupine's HTML visualization next to the test output.

## Fuzzing and disk fault injection

`make fuzz` runs [scripts/fuzz.sh](../scripts/fuzz.sh), which finds every `Fuzz*` target in the module with `go test -list` and runs each for `FUZZTIME`. A new target is picked up without any change to the Makefile or CI. Corpora live in each package's `testdata/fuzz/`. There are no targets yet; the WAL record decoder, snapshot decoder, RPC JSON decoding, and KV command decoding each get one.

File storage will go through a small filesystem interface with a fault-injecting implementation that crashes at every write boundary. Recovery must return a prefix of what was written and never lose an fsynced record.

## Chaos test

Not built yet. It will start five real `kvnode` processes behind test-controlled TCP proxies, kill and restart them with `kill -9`, partition links, and run concurrent clients. It then checks the history with Porcupine, restarts the whole cluster, and verifies every acknowledged write.

## Static analysis

`make lint` runs golangci-lint (pinned in the Makefile) with `staticcheck`, `revive`, `errcheck`, `gocyclo`, `funlen`, `nestif`, and `misspell`, configured in [.golangci.yml](../.golangci.yml). `make fmt` and `make vet` run gofmt and go vet on their own.
