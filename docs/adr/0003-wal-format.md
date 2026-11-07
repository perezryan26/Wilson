# ADR-0003: WAL format

- Status: Accepted
- Date: 2026-11-06

## Context

Figure 2 requires `currentTerm`, `votedFor`, and the log to be "updated on stable storage before responding to RPCs." The runtime meets that rule by persisting each `Ready`'s hard state and entries, and fsyncing them, before it sends the `Ready`'s messages ([ADR-0001](0001-deterministic-core.md)). The storage underneath decides whether the rule survives a crash:

- A crash can strike at any instant: in the middle of a write, before or after an fsync, or before a new file's directory entry is durable. Whatever was written but not yet fsynced may be lost, in part or in full.
- Nothing an fsync made durable may be lost. A server that forgets its vote can vote twice in a term and help elect two leaders; one that forgets an entry it acknowledged can lose a committed command.
- Damage must not pass for a crash. Recovery that quietly drops every record after a bad one would forget exactly those votes and entries.
- §5.5: a server that crashes after completing an RPC but before responding receives the same RPC again once it restarts, so it must restart from exactly the state the crash left.

## Decision

A node keeps its hard state and log in a write-ahead log (WAL): records appended to segment files in one directory.

### Records

Each record is laid out as follows, in little-endian byte order:

```text
[length u32][crc32c u32][type u8][payload]
```

- `length` counts the type byte and the payload, which are exactly the bytes the checksum covers, so a record takes 8 + `length` bytes. A `length` of 0 is invalid, so a run of zero bytes never reads as a record.
- The checksum is CRC-32C (Castagnoli). It detects more errors than the IEEE polynomial at these lengths, and amd64 and arm64 compute it in hardware.

| Type | Record | Payload |
| --- | --- | --- |
| 1 | hard state | `currentTerm u64`, `votedFor u64` (0 for none) |
| 2 | entry | `index u64`, `term u64`, `entry type u8`, then the command, to the end of the record |
| 3 | truncation | `index u64`: every entry from `index` on is discarded |

Type 0 is invalid. The entry type byte is 0, a normal entry, in every record written today. The election no-op and configuration entries will take other values, so they need no new layout.

### Saving a Ready

`Save` turns a `Ready` into records, writes them with one write, and fsyncs before it returns. The order is fixed:

1. the hard state, if it changed;
2. a truncation at `Entries[0].Index`, if that is at or below the last stored index, because the node is overwriting a conflicting suffix (Figure 2, AppendEntries step 3);
3. one record per entry.

A crash may keep any prefix of these records, and every prefix is a state the server can restart from. The hard state comes first, so no prefix holds an entry from a term later than `currentTerm`. A truncation without the entries that follow it only shortens the log by entries that conflict with the current leader's, which are therefore not committed. No message of the `Ready` was sent, so nothing depends on the records that were lost.

After a write or fsync fails, every later `Save` fails too. What the disk holds is then unknown, and on Linux a retried fsync can report success for data it already dropped; the runtime must stop.

### Segments

- A segment is named after the sequence number of its first record, as 16 lowercase hex digits plus `.wal`, such as `000000000000002a.wal`. Records are numbered from 0 over the whole log, and the fixed width makes names sort in log order.
- Before a write, a segment that has reached the size limit (64 MiB by default) is closed and a new one is created, named after the next record. The directory is fsynced before any record goes into the new file: until then the file, and everything in it, could vanish in a crash.

### Recovery

Opening the WAL replays every segment in order and applies each record.

- **Gaps.** Each segment must begin with the record after the previous segment's last, so a missing segment, or one cut short at a record boundary, shows as a gap. The first segment must begin with record 0 until log compaction starts deleting old segments.
- **A torn final record.** In the last segment only, a record that runs past the end of the file, its header or its body cut short, is a write torn by a crash. No `Save` returned for it. Recovery truncates the file where the record begins, fsyncs, and logs the segment, the offset, and the bytes dropped, so new records never follow the remains.
- **Everything else refuses to start**, with an error wrapping `ErrCorrupt` that names the segment and offset: a checksum mismatch, an empty record or one of unknown type, a payload of the wrong size, an entry that is not the next index, a truncation outside the log, a record cut short in an earlier segment, or a `.wal` file whose name is not exactly a segment name.

## Alternatives considered

- **One record per `Ready`.** It would make every `Ready` atomic, but records would grow with each batch, and the write order above already makes a partial `Ready` safe. A record per entry keeps records small and the decoder simple.
- **Truncation implied by an entry's index.** Replay could treat an entry at or below the last index as an overwrite. An explicit truncation record lets replay demand that every entry is exactly the next index, so a misplaced record is caught instead of quietly rewriting the log.
- **End the log at the first bad record**, as SQLite's and PostgreSQL's logs do. It never refuses to start, but it silently drops everything after damage in the middle of the log, which is the failure this design exists to prevent.
- **etcd's WAL**: 8-byte frames with padding, protobuf records, a checksum chained from record to record, and preallocated segments. Protobuf would be a dependency, and the chain and preallocation add machinery this project does not need.
- **A checksum over the length field.** A damaged length that points past the end of the file still cannot be told from a torn write, because the bytes a checksum would cover are not there.
- **A format version in each segment.** New record types and entry types are additions that need none, and an incompatible change can use a new file extension.

## Consequences

- Durability assumes what common file systems give appends to one file: after a crash the file holds what was fsynced plus some prefix of what came after. Recovery refuses to start rather than guess when that assumption fails.
- As in etcd, a damaged length field in the last segment that points past the end of the file looks exactly like a torn write, and recovery truncates the records from there on, logging that it did.
- Startup replays the whole log and keeps a copy in memory. Log compaction bounds both.
- File storage reaches the disk only through a small file-system interface. Crash tests and the simulator run it on an in-memory file system that crashes during any chosen write, truncation, file creation, or fsync, keeping what was synced and a random prefix of the rest.
- Fuzzing covers the record decoder (`FuzzWALRecordDecode`); unit tests cover every prefix of a torn final record, damage to every byte of every record, missing segments, and a crash at every write boundary.
