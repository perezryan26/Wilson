# Testing

Six independent harnesses produce the evidence for Wilson's correctness, and no single one is trusted alone. This file says what each one catches, how to run it, and how to read its output. Harnesses that are not built yet are marked as such.

## Everyday checks

`make check` is what CI runs on every push: gofmt, go vet, golangci-lint, `go test -race ./...`, a simulator sweep of 1,000 seeds, and 60 seconds of fuzzing per target. Override the sizes with `make sim SEEDS=N` and `make fuzz FUZZTIME=D`.

| Harness | What it catches | When it runs | State |
| --- | --- | --- | --- |
| Race detector (`-race`) | Data races in the runtime, transport, kvserver, and client | Every test run | Runs every test; the code so far is single-threaded |
| Deterministic simulator | Safety violations under drops, delays, reordering, duplication, partitions, crashes, and tick skew | 1,000 seeds per push; 20,000 before each phase tag | Not built |
| MIT 6.5840 Labs 3 and 4 | Bugs found by an independent test suite; timing and RPC-count limits | 20 consecutive passing runs | Not built |
| Porcupine | Client histories that are not linearizable | Every KV simulator seed; chaos runs; MIT Lab 4 | Not built |
| Fuzzing and disk faults | Decoder panics, torn writes, lost fsyncs, corrupted files | 60 s per target per push; 10 min per target before each phase tag | Wired; no targets yet |
| Chaos test | Bugs only real processes, sockets, `kill -9`, and disks expose | Nightly | Not built |

## Unit tests and the election harness

Until the simulator exists, these are the evidence for leader election. Both live in `raft/` and run with every `make test`.

- **Unit tests** drive one core with `Tick` and `Step` and check the `Ready` and `Status` it returns, with no sleeps. Each is named after the rule it covers, and the README's [paper-to-code map](../README.md#paper-to-code-map) lists which test covers which rule of Figure 2.
- **The three-node harness** ([`raft/cluster_test.go`](../raft/cluster_test.go)) runs whole elections in one goroutine. Each round ticks every running server once, then delivers messages in the order they were sent until none are left. Every server's `Ready` is handled as a runtime handles it: hard state saved to in-memory storage, then messages sent. A crashed server loses all but its storage and the messages sent to it while it is down; a restarted one boots from that storage. After every `Ready` the harness checks Election Safety: no two servers ever lead the same term.

`TestClusterElectsOneLeader` and `TestClusterReplacesFailedLeader` each run 1,000 seeds. A seed fixes every server's random source, so a failure names its seed and repeats on every run. Run them alone with:

```sh
go test ./raft -run TestCluster
```

## Race detector

`make test` runs every package with `-race`, locally and in CI. The Raft core is single-threaded by design, so races can only live in the runtime, transport, kvserver, and client. Their tests exercise concurrency on purpose: parallel proposals, shutdown during sends, and a leader change mid-request.

## Deterministic simulator

Not built yet. It will run a whole cluster in one goroutine on a virtual clock, with one seeded random source deciding every message drop, delay, reordering, and duplication, every partition, crash, and restart, and each node's tick skew. After every event it checks the Figure 3 safety properties and the implementation invariants. After faults heal, it checks liveness: a leader emerges and pending proposals commit within a bounded virtual time.

A failing run prints its seed and a trace, and `go test ./internal/sim -run TestRandomized -seed=N` replays it exactly. This section will explain how to read the trace once the simulator exists.

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
