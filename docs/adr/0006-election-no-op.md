# ADR-0006: Election no-op toggle

- Status: Accepted
- Date: 2026-11-15

## Context

A new leader holds every committed entry (Figure 3, Leader Completeness), but at the start of its term it may not know which of its entries those are. §5.4.2 forbids it to commit an entry from an earlier term by counting replicas (Figure 8), so it learns what is committed only when an entry of its own term commits, which commits every entry before it. Until a client sends a command, that never happens. Two things wait on it:

- **Reads.** §8 serves a read-only request without writing to the log only once the leader knows its commit index is current. A leader that has committed nothing in its term must wait (ADR-0008).
- **Restarts.** A cluster restarted whole applies none of its committed entries until the first new command commits.

§8's answer is that "each leader commit a blank no-op entry into the log at the start of its term."

MIT 6.5840's Lab 3 tests, which this core must also pass, check which command sits at each log index, and a no-op that the tests did not ask for shifts every index after it.

## Decision

- `raft.EntryType` says what an entry holds: `EntryNormal` (0), a command for the state machine, or `EntryNoOp` (1), nothing. `Entry` gains a `Type` field. The WAL keeps it in the entry record's type byte, which [ADR-0003](0003-wal-format.md) reserved for it; recovery refuses any other value.
- `Config.CommitNoOpOnElection` turns the no-op on. `becomeLeader` resets `nextIndex` and `matchIndex` as before, then appends an entry of type `EntryNoOp` and of the new term after its last entry, so the first heartbeat carries it. In a one-server cluster it commits at once.
- `DefaultConfig` turns it on, and `kvnode` runs with it. `MITLabConfig` leaves it off; the MIT adapter turns it on for Lab 4, whose tests never look at log indexes, and keeps no-ops off the course's `applyCh`, whose messages carry no entry type.
- A no-op travels like any entry: persisted through `Ready`, replicated, committed, and released in `CommittedEntries`. State machines skip it: the key-value service's `StateMachine` hands only `EntryNormal` entries to its store.

## Alternatives considered

- **Always on, with no toggle.** The lab's tests would fail on every index they check.
- **An entry with an empty command instead of a type.** An empty command is a legitimate command, so the state machine could not tell the two apart, and neither could a reader of the WAL.
- **The runtime hides no-ops from the state machine.** Every runtime, the MIT adapter among them, would need the same filter, and configuration entries will need the runtime's attention anyway. Skipping types it does not apply is one rule for the state machine.
- **Commit a no-op only when a read arrives.** Reads would still wait a round trip after every election, a restarted cluster would still wait for a request, and the core would carry another trigger.

## Consequences

- One round trip after an election, a leader has committed every entry before its term, so reads can be served and a restarted cluster applies its log without waiting for a client.
- Every term that elects a leader adds one entry to the log, and indexes no longer count commands.
- With the no-op off, ReadIndex reads wait until a command of the leader's term commits.
- The deterministic simulator runs half its seeds with the no-op on and half with it off. The half without keeps Figure 8's hazard within reach: a leader holding earlier-term entries that it has not committed. Unit tests of Figure 2's rules turn it off, so that a leader's log is exactly what each test gave it; the no-op has tests of its own.
