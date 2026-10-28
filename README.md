# Wilson

A linearizable key-value store built on the Raft consensus algorithm, written in Go from scratch and meant to be read beside the [extended Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf).

> **Status: leader election.** The Raft core elects a leader and replaces one that fails, shown by unit tests and a three-node test harness. Log replication, durable storage, the runtime, and the key-value service are not written yet. Each section below says what exists today.

## Overview and guarantees

Wilson is a replicated key-value store that keeps serving Get, Put, and Delete while a majority of its servers are up. It implements §5–§8 of the paper by Ongaro and Ousterhout: leader election, log replication, safety, membership changes, log compaction, and client interaction. The code is written to be read and maintained by people, with the paper open beside it.

The table lists the guarantees the system is built to provide and the evidence each one will rest on. So far only Election Safety has evidence: unit tests, and a three-node harness that checks it after every step (see [Verification](#verification)). The others wait for the code they depend on. The first five rows are the paper's Figure 3 properties.

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

There is no cluster to run yet: `kvnode` and `kvctl` are placeholders that exit with an error. `go test ./raft` runs the leader election tests.

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

Every package below exists with its package documentation. Only `raft/` and `internal/storage/` have code so far.

- **`raft/`** is the core: roles, RPC handlers, the log, the commit rule, snapshots, and membership. It imports only the standard library, and the same seed and inputs always produce the same outputs. So far it implements leader election: the three roles, the randomized election timer, RequestVote, and heartbeats. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) describes its contract with the runtime.
- **`node/`** is the runtime. One goroutine feeds the core ticks, inbound messages, and proposals, then handles each `Ready` in order: persist and fsync, send, apply. A storage error stops the process.
- **`internal/storage/`** persists hard state, log entries, and snapshots. It holds an in-memory implementation for tests and a file implementation built on a segmented write-ahead log. So far only the in-memory implementation exists, and it keeps hard state.
- **`internal/transport/`** carries Raft messages between nodes as JSON over HTTP/1.1 ([ADR-0002](docs/adr/0002-http-json-transport.md)).
- **`internal/kv/`** is the replicated state machine: the command encoding, Get, Put, and Delete, and the per-client session table that makes retried writes apply once.
- **`internal/kvserver/`** is the client-facing HTTP API. It proposes commands, waits for them to apply, and points clients at the leader.
- **`client/`** is the Go client library: leader discovery, retries, client IDs, and sequence numbers.
- **`internal/sim/`** is the deterministic simulator: a virtual clock, a simulated network with injected faults, crashes and restarts, and an invariant checker that runs after every event.
- **`internal/linearizability/`** holds the Porcupine model and the history recorder.
- **`cmd/kvnode/`** and **`cmd/kvctl/`** are the server and CLI binaries.
- **`test/chaos/`** is the multi-process chaos test.

`raft/` and `node/` stay outside `internal/` on purpose. The MIT 6.5840 adapter compiles inside the course's own Go module, and Go forbids importing another module's `internal/` packages.

## Paper-to-code map

Each row names a concept or rule from the paper, the function that implements it, and the tests that cover it. The rows cover leader election: §5.1, §5.2, and the election rules of Figure 2. Every test named here is in `raft/`.

| Paper | Rule | Code | Tests |
| --- | --- | --- | --- |
| Figure 2, State | `currentTerm` and `votedFor` reach stable storage before the server responds to an RPC | [`Node.Ready`](raft/node.go) puts a vote and the reply granting it in one `Ready`, which the runtime persists before it sends ([ARCHITECTURE.md](docs/ARCHITECTURE.md#the-ready-contract)) | `TestVoteIsDurableBeforeReply` |
| Figure 2, State | `currentTerm` starts at 0 and only increases; `votedFor` is null until the server votes in the current term | [`becomeFollower`, `becomeCandidate`](raft/node.go) | `TestRequestVoteRejectsStaleTerm`, `TestHigherTermInRequestStepsDown` |
| §5.2 | Servers start up as followers, from the state they persisted | [`NewNode`](raft/node.go) | `TestNewNodeRestoresHardState`, `TestClusterReplacesFailedLeader` |
| Figure 2, RequestVote arguments | `term`, `candidateId`, `lastLogIndex`, `lastLogTerm` | [`RequestVoteRequest`](raft/types.go), sent by [`campaign`](raft/election.go) | `TestFollowerTimeoutStartsElection` |
| Figure 2, RequestVote receiver, step 1 | Reply false if `term < currentTerm` (§5.1) | [`handleRequestVote`](raft/election.go) | `TestRequestVoteRejectsStaleTerm` |
| Figure 2, RequestVote receiver, step 2 | Grant the vote only if `votedFor` is null or `candidateId`: one vote per term, first come, first served (§5.2) | [`handleRequestVote`](raft/election.go) | `TestOneVotePerTerm`, `TestNewNodeRestoresHardState` |
| Figure 2, RequestVote receiver, step 2; §5.4.1 | ... and only if the candidate's log is at least as up-to-date as the receiver's | [`isUpToDate`](raft/election.go) | `TestVoteRequiresUpToDateLog` |
| Figure 2, AppendEntries arguments | `term` and `leaderId`; the log fields arrive with log replication (§5.3) | [`AppendEntriesRequest`](raft/types.go) | `TestLeaderSendsHeartbeats` |
| Figure 2, AppendEntries receiver, step 1 | Reply false if `term < currentTerm` (§5.1) | [`handleAppendEntries`](raft/append_entries.go) | `TestAppendEntriesRejectsStaleTerm` |
| Figure 2, Rules for Servers, all servers | A request or response with term T > `currentTerm`: set `currentTerm` = T and convert to follower (§5.1) | [`Node.Step`](raft/node.go), before any handler | `TestHigherTermInRequestStepsDown`, `TestHigherTermInResponseStepsDown` |
| Figure 2, followers | Respond to RPCs from candidates and leaders | [`handleRequestVote`](raft/election.go), [`handleAppendEntries`](raft/append_entries.go) | `TestOneVotePerTerm`, `TestHeartbeatsSuppressElection` |
| Figure 2, followers | If the election timeout elapses without an AppendEntries from the current leader or a vote granted, convert to candidate | [`Node.Tick`](raft/node.go); the timer restarts only in [`handleAppendEntries`](raft/append_entries.go) and on a grant in [`handleRequestVote`](raft/election.go) | `TestFollowerTimeoutStartsElection`, `TestHeartbeatsSuppressElection`, `TestElectionTimerReset` |
| Figure 2, candidates | On conversion, start an election: increment `currentTerm`, vote for self, reset the election timer, send RequestVote to all other servers | [`becomeCandidate`](raft/node.go), [`campaign`](raft/election.go) | `TestFollowerTimeoutStartsElection` |
| Figure 2, candidates | Votes from a majority of servers: become leader | [`handleRequestVoteResponse`, `hasMajority`](raft/election.go) | `TestCandidateWinsWithMajority`, `TestDuplicateVoteResponseCountsOnce`, `TestSingleServerElectsItself` |
| Figure 2, candidates; §5.2 | AppendEntries from the new leader: convert to follower | [`handleAppendEntries`](raft/append_entries.go) | `TestCandidateStepsDownOnAppendEntriesFromLeader`, `TestAppendEntriesRejectsStaleTerm` |
| Figure 2, candidates | If the election timeout elapses, start a new election | [`Node.Tick`](raft/node.go), [`campaign`](raft/election.go) | `TestSplitVoteStartsNewElection` |
| Figure 2, leaders | Upon election, send an empty AppendEntries (heartbeat) to each server; repeat during idle periods to prevent election timeouts | [`becomeLeader`](raft/node.go), [`broadcastHeartbeat`](raft/append_entries.go) | `TestLeaderSendsHeartbeats`, `TestHeartbeatsSuppressElection` |
| §5.2 | Election timeouts are chosen at random from a fixed interval, [T, 2T), again for every election | [`resetElectionTimer`](raft/election.go) | `TestElectionTimeoutInRange` |
| §5.2; Figure 3, Election Safety | At most one leader per term, because each server votes once per term and a winner needs a majority | [`hasMajority`](raft/election.go); a leader that meets a rival of its term panics in [`handleAppendEntries`](raft/append_entries.go) | `TestClusterElectsOneLeader`, `TestTwoLeadersInOneTermPanics` |
| §5.2 | When the leader fails, the remaining majority elects a new one in a later term | The rules above, together | `TestClusterReplacesFailedLeader` |

## Verification

Six independent harnesses produce the evidence for correctness, and no single one is trusted alone. [docs/TESTING.md](docs/TESTING.md) covers each in depth.

| Harness | What it catches | How to run | State |
| --- | --- | --- | --- |
| Race detector | Data races in the runtime, transport, kvserver, and client | `make test` | Runs every test; the code so far is single-threaded |
| Deterministic simulator | Safety violations under drops, delays, reordering, duplication, partitions, crashes, and clock skew | `make sim SEEDS=N` | Not built |
| MIT 6.5840 Labs 3 and 4 | Bugs found by an independent test suite; timing and RPC-count limits | Private adapter | Not built |
| Porcupine | Client histories that are not linearizable | Inside the simulator and chaos tests | Not built |
| Fuzzing and disk faults | Decoder panics, torn writes, lost fsyncs, corrupted files | `make fuzz FUZZTIME=60s` | Wired; no targets yet |
| Chaos test | Bugs only real processes, sockets, `kill -9`, and disks expose | Not built | Not built |

Until the simulator exists, the evidence for leader election comes from unit tests that drive one core with `Tick` and `Step`, and from a synchronous three-node harness that runs whole elections, crashes and restarts the leader, and checks Election Safety after every step, over 1,000 seeds per test. [docs/TESTING.md](docs/TESTING.md#unit-tests-and-the-election-harness) describes both.

Once the simulator exists, every failing seed prints its trace, and `go test ./internal/sim -run TestRandomized -seed=N` replays it exactly.

## Deviations from the paper

None. Every deviation gets an ADR in [docs/adr/](docs/adr/) and a row here.

| Paper | This implementation | Reason | ADR |
| --- | --- | --- | --- |

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
| `scripts/` | Helper scripts for the Makefile |

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

**CI.** GitHub Actions runs `make check` on every push and pull request ([.github/workflows/ci.yml](.github/workflows/ci.yml)).

## Phase history

| Tag | Date | What landed |
| --- | --- | --- |
| `phase-0` | 2026-10-27 | Module and package layout, Makefile, lint configuration, CI, contributing and testing guides, ADR-0001 (deterministic core) and ADR-0002 (HTTP/JSON transport) |
| `phase-1` | 2026-10-27 | Core types and leader election: the three roles, the randomized election timer, RequestVote, heartbeats, in-memory storage, a three-node election harness, and `docs/ARCHITECTURE.md` |
