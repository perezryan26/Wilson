# Contributing

## Before you push

Install Go; `go.mod` pins the toolchain and downloads it if needed. Then run:

```sh
make check
```

It runs gofmt, go vet, golangci-lint, the tests with `-race`, a short simulator sweep, and a short fuzz run: everything CI runs. The [README](README.md#development) lists the individual targets.

## Ground rules

- **The paper is the authority** on algorithm behavior. A deviation needs an ADR in [docs/adr/](docs/adr/) and a row in the README's "Deviations from the paper" table.
- **Never weaken, skip, or delete a test to make it pass.** Never add sleeps or retries to hide a timing bug. Treat a flaky test as a code bug until proven otherwise.
- **Every bug found by a heavyweight harness** (the MIT suites, the chaos test, a random simulator seed) gets a small scripted regression test before the fix lands.
- **Standard library first.** A runtime dependency needs an approved ADR. Porcupine is the one pre-approved test dependency.
- **Never commit MIT 6.5840 course source.** [docs/TESTING.md](docs/TESTING.md#mit-65840-test-suites) explains why.

## Writing code

The code is written for a reader who knows Go and has the Raft paper open beside it.

1. Name things as the paper does: `currentTerm`, `votedFor`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`, `RequestVote`, `AppendEntries`, `InstallSnapshot`. A reader can search the paper for any of them.
2. Shape handlers like Figure 2. Each RPC receiver follows the figure's numbered steps in order. Rules that apply to every RPC, such as stepping down on a higher term, live in one helper.
3. Keep functions small and flat: one job, one level of abstraction, and early returns instead of nesting. The linter enforces cyclomatic complexity 10, 60 lines per function, and nesting depth 3. Exceeding a limit needs a `//nolint` comment with a stated reason.
4. Avoid cleverness: no reflection, no `unsafe`, no `init()` side effects, no package-level mutable state, and no naked returns. Use generics only where they plainly remove duplication.
5. Every goroutine has one owner and a documented way to stop.
6. Wrap errors with context, for example `fmt.Errorf("wal: read record at offset %d: %w", off, err)`. Export the sentinel errors that callers branch on.
7. Panic only when an internal invariant is broken, and name the invariant in the message.

## Comments

- Every package has a `doc.go` that explains its role and where it sits in the architecture.
- Every exported identifier has a doc comment saying what it does and why it exists. revive's `exported` rule enforces this.
- Inline comments explain why, never what: a non-obvious invariant, an ordering requirement, or the paper rule being enforced. Cite the paper as `§5.4.2` or `Figure 8`.
- No commented-out code, and no `TODO` or `FIXME` in merged code.

| Avoid | Prefer |
| --- | --- |
| `// increment the term` above `n.currentTerm++` | No comment; the code already says it |
| `// check the log` above the commit loop | `// §5.4.2: count replicas only for current-term entries; older ones commit indirectly (Figure 8).` |
| `// save state` before replying to a vote | `// The vote must be durable before the reply leaves, or a restart could vote twice in one term.` |

## Commits

Each commit is one logical change that builds, passes its tests, and says why it exists in a short subject and body.

**Format**

- Subject: `<area>: <imperative summary>`, at most 72 characters, with no trailing period. The area is the package or top-level directory, such as `raft`, `wal`, `sim`, `kvserver`, `docs`, or `build`.
- Body: wrapped at 72 columns, usually 2–6 lines. State the problem, why this approach, and any alternative you rejected. Name the tests that cover the change.
- Trailers: `Refs: Raft §x.y` on algorithm changes, and `Phase: N` on every commit, naming the milestone in the README's phase history.
- Omit the body only when the subject says everything, as with a typo fix.

**Rules**

- One logical change per commit. Refactors land separately from behavior changes.
- Tests land in the same commit as the code they cover, so every commit passes `go test ./...` and `git bisect` works.
- Never use subjects like "wip", "fix", "updates", "misc", or "address feedback", and never bundle unrelated changes.

**Example**

```text
raft: count replicas only for current-term entries

A leader that committed an entry from an earlier term by counting
replicas could be replaced by a leader whose log lacks that entry,
which would then overwrite it (Figure 8). Restrict the counting rule
to entries from the leader's current term; earlier entries commit
indirectly through Log Matching.

Covered by TestFigure8Scenario and the Leader Completeness invariant.

Refs: Raft §5.4.2, Figure 8
Phase: 3
```
