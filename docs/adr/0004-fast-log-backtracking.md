# ADR-0004: Fast log backtracking

- Status: Accepted
- Date: 2026-11-03

## Context

A new leader does not know where each follower's log stops matching its own. Figure 2 has the leader find out one entry at a time: after each rejected AppendEntries it decrements the follower's `nextIndex` and retries. A follower that missed many entries, or holds many entries from terms that never committed (Figure 7(f), or an old leader back from a long partition), then costs one round trip per entry.

§5.3 sketches a remedy without fixing its details: the rejection carries the term of the conflicting entry and the first index the follower stores for that term, so the leader can skip a whole term per round trip.

The details matter here because messages are one-way (ADR-0002). A reply can arrive late, twice, or after a newer one, so the leader cannot assume a reply answers the request it sent last.

## Decision

`AppendEntriesResponse` carries three fields beyond Figure 2's `Term` and `Success`.

- **`MatchIndex`**, on success: `PrevLogIndex + len(Entries)` of the request being answered. The leader raises the follower's `matchIndex` to it and sets `nextIndex` to one past it, but never lowers `matchIndex`. It is not the follower's last index, because entries after the request's may still conflict with the leader's.
- **`ConflictTerm`** and **`ConflictIndex`**, on a rejection at step 2 (no entry at `PrevLogIndex`, or one of another term):
  - If the follower has no entry at `PrevLogIndex`: `ConflictTerm` is 0 and `ConflictIndex` is the follower's last index + 1. This carries the follower's log length, so no separate field is needed.
  - Otherwise: `ConflictTerm` is the term of the follower's entry at `PrevLogIndex`, and `ConflictIndex` is the first index of that term in the follower's log. Terms never decrease along a log, so the term's entries form one run.

On a rejection, the leader computes where to resume:

1. If `ConflictTerm` is not 0 and the leader's log holds entries of that term, resume just past the leader's last entry of that term.
2. Otherwise, resume at `ConflictIndex`.

It then sets `nextIndex = max(matchIndex + 1, min(nextIndex − 1, that index))` and retries at once. Taking the minimum with `nextIndex − 1` means every rejection backs up by at least one entry, so even a stale or duplicated reply cannot stall the search or move it forward. The bound `matchIndex + 1` keeps it from resending entries the follower is known to hold.

A rejection at step 1 (a stale term) carries no conflict information. Its `Term` is higher than the leader's, so the leader steps down before it looks at anything else.

## Alternatives considered

- **One entry per round trip**, as Figure 2 states it. Simplest, but a follower many entries behind takes as many round trips; the paper itself proposes this optimization.
- **Only the follower's log length.** Enough for a follower that is merely missing entries, but a follower with a long conflicting suffix still costs one round trip per entry.
- **Binary search over `nextIndex`.** Needs no extra fields beyond the length, but takes log₂(n) round trips even for a single conflicting term.

## Consequences

- A diverging follower converges in one round trip per conflicting term. `TestFigure7FollowersConverge` pins the count for each of Figure 7's follower logs, and `TestDivergentLogConvergesPerTerm` shows a 100-entry divergence resolved in one rejection where one-at-a-time takes fifty.
- The rules affect only how fast logs converge, never safety: the consistency check at step 2 still guards every append, so a wrong guess costs a round trip, not a wrong log.
- The three fields become part of the message format the transport encodes.
