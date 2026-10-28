# Wilson

A linearizable key-value store built on the Raft consensus algorithm, written in Go from scratch and meant to be read beside the [extended Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf).

> **Status: foundations only.** The module layout, build, lint, CI, and design records are in place. The Raft core and everything built on it are not written yet. Each section below says what exists today.

## Overview and guarantees

Wilson is a replicated key-value store that keeps serving Get, Put, and Delete while a majority of its servers are up. It implements §5–§8 of the paper by Ongaro and Ousterhout: leader election, log replication, safety, membership changes, log compaction, and client interaction. The code is written to be read and maintained by people, with the paper open beside it.

The table lists the guarantees the system is built to provide and the evidence each one rests on. None holds yet, because no Raft code exists. The first five rows are the paper's Figure 3 properties.

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

There is no cluster to run yet: `kvnode` and `kvctl` are placeholders that exit with an error.

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

Every package below exists with its package documentation; none has code yet.

- **`raft/`** is the core: roles, RPC handlers, the log, the commit rule, snapshots, and membership. It imports only the standard library, and the same seed and inputs always produce the same outputs.
- **`node/`** is the runtime. One goroutine feeds the core ticks, inbound messages, and proposals, then handles each `Ready` in order: persist and fsync, send, apply. A storage error stops the process.
- **`internal/storage/`** persists hard state, log entries, and snapshots. It holds an in-memory implementation for tests and a file implementation built on a segmented write-ahead log.
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

Each row will name a concept or rule from the paper, the file and function that implement it, and the test that covers it. There are no rows yet.

| Paper | Rule | Code | Test |
| --- | --- | --- | --- |

## Verification

Six independent harnesses produce the evidence for correctness, and no single one is trusted alone. [docs/TESTING.md](docs/TESTING.md) covers each in depth.

| Harness | What it catches | How to run | State |
| --- | --- | --- | --- |
| Race detector | Data races in the runtime, transport, kvserver, and client | `make test` | Wired; no tests yet |
| Deterministic simulator | Safety violations under drops, delays, reordering, duplication, partitions, crashes, and clock skew | `make sim SEEDS=N` | Not built |
| MIT 6.5840 Labs 3 and 4 | Bugs found by an independent test suite; timing and RPC-count limits | Private adapter | Not built |
| Porcupine | Client histories that are not linearizable | Inside the simulator and chaos tests | Not built |
| Fuzzing and disk faults | Decoder panics, torn writes, lost fsyncs, corrupted files | `make fuzz FUZZTIME=60s` | Wired; no targets yet |
| Chaos test | Bugs only real processes, sockets, `kill -9`, and disks expose | Not built | Not built |

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
| `docs/` | [TESTING.md](docs/TESTING.md) and the decision records in [adr/](docs/adr/) |
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
