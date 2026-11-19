# ADR-0008: Read path

- Status: Accepted
- Date: 2026-11-15

## Context

A read must reflect every write that completed before it began, or the store is not linearizable. The simplest way to guarantee that is to send reads through the log like writes: a read that is committed and applied in log order sees exactly the writes ordered before it. It costs a log entry, fsynced on a majority of servers, for every read.

§8 describes reads that skip the log, with two precautions:

1. "A leader must have the latest information on which entries are committed." A new leader holds every committed entry but does not know which ones are committed until it commits an entry of its own term, which the election no-op does at once ([ADR-0006](0006-election-no-op.md)).
2. "A leader must check whether it has been deposed before processing a read-only request", by exchanging heartbeats with a majority of the cluster. A leader cut off in a minority keeps believing it leads while the majority elects another and takes writes it never sees.

Ongaro's dissertation (§6.4) names the result ReadIndex: the leader notes its commit index as the read index, confirms with a round of heartbeats that it still leads, waits until its state machine has applied the read index, then answers from the state machine. A third approach, leases (dissertation §6.4.1), skips the heartbeat round by trusting bounded clock drift; it is extension E3 and not built.

Messages are one-way ([ADR-0002](0002-http-json-transport.md)), so a reply that arrives after a read began may answer a heartbeat sent before it, from a server that has since voted for another leader. A heartbeat round needs replies that say which request they answer.

## Decision

Both read paths exist, and `kvnode -reads` picks one: `index`, the default, or `log`. Each read is linearizable either way, so servers of one cluster may differ.

### Reads through the log

A Get is a command, `kv.OpGet` with no session, which the server proposes and answers once it applies on that server. A read has no side effects, so running it twice is harmless and it needs no session.

### ReadIndex

- **The core.** `Node.ReadIndex(id)` returns `ErrNotLeader` on any server but the leader. On the leader it starts a round of leadership checks: it increments its read round and sends every other server an AppendEntries, as a heartbeat would, that carries the round in `AppendEntriesRequest.ReadRound`. A follower echoes the round in `AppendEntriesResponse.ReadRound` of every reply in the leader's term, a rejection too.
- **Confirmation.** The leader records, for each server, the latest round it has answered in the leader's term. A read is confirmed once a majority, the leader included, has answered a round at or past the read's, and the leader has committed an entry of its term. `Ready.ReadStates` then releases it with the leader's commitIndex at that moment as its read index. A leader that steps down drops the reads it holds, and a new term starts its rounds over.
- **Why the commitIndex at confirmation.** Every write that completed before the read began was committed in a term no later than the leader's: no later leader can have been elected before the read began, since a majority answered the leader's term after that, and terms never decrease. So the write is in the leader's log (Leader Completeness), and once the leader has committed an entry of its own term, its commitIndex covers the write. A later commitIndex only adds writes concurrent with the read.
- **The runtime.** `node.ReadIndex(ctx)` gives each read an ID and answers it once a `Ready` confirms it, after applying that `Ready`'s committed entries, which reach the read index. It fails the read with `raft.ErrNotLeader` when the node stops leading the term it took the read in. The key-value server then reads its store.
- **InstallSnapshot carries no round.** A follower in the middle of a snapshot transfer does not count toward confirming reads until the transfer ends.

### The client-facing API

`kvserver` serves `POST /v1/sessions`, `GET`, `PUT`, and `DELETE /v1/kv/{key}`, and `GET /v1/status`. Writes carry `?session=S&seq=N` ([ADR-0007](0007-client-sessions.md)). A server that does not lead answers 421 with the leader's address, if it knows it. A request that cannot finish within the server's timeout, two seconds by default, answers 504: for a write that means the outcome is unknown, and the client must send it again with the same number. The status code carries the meaning a client acts on: 410 for an expired session, 409 for a write older than its session's last, 413 for a value too large to replicate, and 503 for a stopped node.

## Alternatives considered

- **Reads through the log only.** Simplest, and the fallback this ADR keeps, but every read costs a disk write on a majority.
- **Leases** (dissertation §6.4.1). Faster reads, at the cost of trusting clock drift; extension E3.
- **Counting any reply that arrives after the read began.** Unsafe, as above: it can count a server that voted for a new leader after answering an older heartbeat.
- **A separate heartbeat message carrying a request context**, as etcd's raft does. Heartbeats here are AppendEntries, so one field on each side of it does the same with no new message.
- **One round for every read waiting**, starting a new round only when none is in flight. Fewer messages under heavy read load, but more state; each read starts its own round until a measurement asks for more.
- **Reads served by followers**, which ask the leader for a read index (dissertation §6.4). Spreads read load; not needed by anything yet.

## Consequences

- A read through ReadIndex costs one AppendEntries to each follower and no disk write. A read through the log costs a log entry on a majority.
- A leader cut off in a minority never answers a read in either mode: its Get never commits, and no majority confirms its rounds. `TestPartitionedLeaderServesNoStaleRead` checks both in the simulator, and `TestCutOffLeaderServesNoRead` the runtime.
- With the election no-op off, ReadIndex reads wait until a command of the leader's term commits. `TestNewLeaderServesNoReadBeforeItsTermCommits` shows why that wait matters.
- Without CheckQuorum (extension E1), a leader cut off in a minority keeps the reads it was asked to confirm until it hears of a later term; their callers time out first.
- MIT 6.5840's labs make no reads through ReadIndex (Lab 3 makes none, and Lab 4's go through the log), so the adapter's AppendEntries carry round 0.
