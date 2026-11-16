# ADR-0007: Client sessions

- Status: Accepted
- Date: 2026-11-15

## Context

A client that sends a write and hears nothing back cannot tell whether the write was lost or only its reply. The leader may have committed and applied it, then crashed before answering. If the client sends it again, a store that runs every command it is given runs that write twice, and a history with one write applied twice is not linearizable. §8: "clients assign unique serial numbers to every command. Then, the state machine tracks the latest serial number processed for each client, along with the associated response. If it receives a command whose serial number has already been executed, it responds immediately without re-executing the request."

That table must be bounded, so sessions must sometimes be discarded, and a client whose session is gone must never have a retried write run a second time. Which side picks a client's ID decides whether that holds:

- **Clients pick random IDs and send no first command.** After its session is evicted, a client's retried write looks exactly like the first write of a new client, and it runs again.
- **Clients pick random IDs and register them with an idempotent `Register(clientID)`.** A write from an unknown ID is refused. But a late copy of `Register`, arriving after the session was evicted, re-creates it with nothing applied, and a late copy of the client's first write then runs a second time.
- **The cluster picks the ID** (Ongaro's dissertation, §6.3). Every `Register` makes a new session, so no session is ever re-created.

The state machine also needs a byte encoding for its commands and its snapshots. Keys and values are arbitrary bytes, and every command is stored and sent many times.

## Decision

### Sessions

- **Register.** A client's first command is `Register`, which carries no ID. The new session's ID is the log index of the `Register` entry, which no other entry will ever have. A repeated or late `Register` only starts another session, which nobody uses until eviction removes it.
- **Writes.** Each `Put` and `Delete` carries its session's ID and a sequence number, from 1. A client sends one write at a time and moves to the next number only after an answer, and it retries a write with the same number.
- **The record.** For each session the store keeps the last sequence number applied, that write's response (for a `Delete`, whether the key existed), and the log index of the session's last command.
- **Applying a write.** From a session the store does not hold, the write is refused with `ErrSessionExpired`: it does not run now, and whether it ran before is unknown. Numbered below the session's last, it is refused with `ErrStaleRequest`; only a client that broke the one-write-at-a-time rule, or a copy delayed past the next write, sends one. Numbered the same as the last, it is answered from the record and does not run. Numbered above, it runs, and the record takes its number and response.
- **Eviction.** A store keeps at most `MaxSessions` sessions; `kvnode` uses 10,000 and offers no flag, because every server of a cluster must use the same limit. A `Register` that would exceed it first evicts the session whose last command has the lowest log index. Every server applies the same entries in the same order, so every server evicts the same session, and no two sessions share a last index, since each index holds one command.
- **After expiry.** The client library reports the write's outcome as unknown and registers a new session for its next write. It never reuses an ID.
- **Reads.** Reads need no session: they change nothing, so running one twice is harmless. A `Get` through the log carries only a request ID, and a read through ReadIndex never reaches the log ([ADR-0008](0008-read-path.md)).
- **Request IDs.** The server that proposes a command picks a random 64-bit request ID and waits for the result that carries it. The store only copies it into the result.
- **Snapshots** hold every session along with every key (§7), so a server restored from one goes on answering retries from the same records and evicts in the same order.

### Commands

A command is laid out as

```text
[version u8][op u8][request u64][session u64][seq u64][key length u32][key][value]
```

in little-endian byte order, the value running to the end. The ops are `Register` (1), `Get` (2), `Put` (3), and `Delete` (4); 0 is none, so zero bytes never read as a command. Every field is present in every command, and those its op does not use are zero. A log entry that does not decode as a command, a version this code does not know among them, changes nothing on any server.

### Snapshots

The store's snapshot is laid out as

```text
[version u8][pair count u32]{[key length u32][key][value length u32][value]}
[session count u32]{[id u64][seq u64][last index u64][found u8]}
```

with pairs in increasing order of key and sessions in increasing order of ID, so that stores holding the same state encode it identically. The decoder refuses anything the encoder would not write: a cut-short field, another version, leftover bytes, keys or sessions out of order, or a `found` byte other than 0 or 1.

## Alternatives considered

- **Client-picked random IDs**, with or without an idempotent `Register`, as above: either leaves a way for a write to run twice.
- **Expire sessions by time**, as the dissertation's LogCabin does with timestamps the leader writes into entries. The state machine would need a clock value in every entry to stay deterministic; a count-based limit needs none.
- **Keep a response for every sequence number until the client acknowledges it**, so a client can have several writes outstanding. One write at a time per session keeps the record to one response.
- **JSON** for commands and snapshots, as the transport uses. It would base64 values, a third larger, and it cannot carry a key that is not valid UTF-8. Fixed layouts, fuzzed like the WAL's, cost two small codecs.

## Consequences

- At-most-once holds with no exception: no write runs twice. When a session is evicted between a write and its retry, the client learns that the outcome is unknown instead of learning the result.
- A new client costs one extra round trip, its `Register`, before its first write.
- A cluster with more than `MaxSessions` clients writing at once evicts sessions that are still in use. Their next writes fail with `ErrSessionExpired`, which the client sees; nothing runs twice.
- `FuzzCommandDecode` and `FuzzStateDecode` fuzz the two decoders. Each must refuse every byte string its encoder would not write, so a command or a state decodes from exactly one byte string.
