# ADR-0005: Snapshot format

- Status: Accepted
- Date: 2026-11-14

## Context

Without compaction the log grows without bound, and so does the time a restart takes to replay it. §7 bounds both with snapshots. Each server writes its state machine's state as of an index it has applied, with the index and term of the last entry the snapshot includes (Figure 12), and then discards the log up to that entry. A leader that has discarded entries a follower still needs sends it the snapshot with InstallSnapshot (Figure 13), in chunks if it is large.

Several decisions follow from that:

- **The snapshot's own format.** The file must survive a crash at any instant, leaving either the previous snapshot or the new one usable. It must also hold the cluster configuration once membership changes exist (§6).
- **How the WAL ([ADR-0003](0003-wal-format.md)) shrinks.** Segments may be deleted only once a durable snapshot covers them, and a restart must rebuild exactly the snapshot plus the log after it.
- **What InstallSnapshot carries.** Messages are one-way ([ADR-0002](0002-http-json-transport.md)), so a reply can arrive late, twice, or out of order, and the leader cannot assume a reply answers the chunk it sent last.
- **How Figure 13's receiver reads.** Taken literally, step 6 ("retain log entries following it and reply") stops before step 8 (reset the state machine). But the snapshot has replaced the entries it covers, so a follower that had not yet applied them would skip them.

## Decision

### Snapshots in the core

- `raft.Snapshot` holds `LastIncludedIndex`, `LastIncludedTerm`, and `Data`, the state machine's state as the service encoded it. A snapshot covers only committed entries.
- The log's sentinel entry stands for the snapshot's last included entry, so the log always knows the term of the entry before its first. `term(i)` reports nothing below it.
- The service calls `Node.Compact(index, data)` once its state machine has applied `index`. The index must lie past the current snapshot and at or before `lastApplied`. The entries after it stay, as step 6 keeps them.
- `Ready.Snapshot` hands storage a snapshot to persist, whether `Compact` took it or the leader sent it. `Ready.Entries` then hold every entry after it, and the two replace the whole stored log, so storage needs no Figure 13 logic of its own.
- The runtime restores the state machine from `Ready.Snapshot` exactly when the snapshot is past the last entry the state machine applied. That is always true of a snapshot from the leader and never of one from `Compact`. At startup it restores from `RestoredState.Snapshot`, and `commitIndex` and `lastApplied` start at its last included index.

### The snapshot file

A snapshot is saved in one file in the WAL's directory, named after its last included index as 16 lowercase hex digits plus `.snap`, such as `0000000000000007.snap`. Every number is little-endian.

```text
[crc32c u32][lastIncludedIndex u64][lastIncludedTerm u64][config length u32][config][data]
```

- The CRC-32C covers everything after it. The data runs to the end of the file.
- `config` is reserved for the cluster configuration as of the last included index, which §7 says a snapshot must hold once membership changes exist. Until then its length is 0, and a decoder refuses any other length, as the WAL refuses an entry type it does not know. Membership changes will fill it without a new layout.
- The file is written under a temporary name ending in `.tmp`, fsynced, renamed into place, and its directory fsynced. It therefore appears whole or not at all, so any damage means corruption, never a torn write.

### Starting the WAL over

WAL record type 4, the snapshot record, holds `currentTerm u64`, `votedFor u64`, `lastIncludedIndex u64`, and `lastIncludedTerm u64`. It appears only as the first record of a segment, and such a segment replaces every segment before it.

Saving a `Ready` with a snapshot:

1. writes the snapshot file;
2. writes a new segment, named after the next record's sequence number: a snapshot record with the hard state, then one entry record per entry after the snapshot. Like the snapshot file, it is written to a temporary file, fsynced, renamed, and its directory fsynced;
3. removes the segments before the new one, and every other snapshot file.

A crash before step 2's rename leaves the old WAL and old snapshot, and recovery removes the new snapshot file. After it, the new segment and its snapshot are the whole state, and recovery removes whatever step 3 had not.

### Recovery

1. Find the newest segment whose first record is a snapshot record, and replay from it. If no segment begins with one, replay from segment 0; a WAL that does not begin there is missing segments, as in ADR-0003.
2. A snapshot record loads the snapshot file it names. That file must exist, decode, and hold the same index and term, or recovery refuses to start with `ErrCorrupt`: the entries it replaced are gone, so nothing else can stand in for it.
3. Replay then continues as before, with entries numbered from the one after the snapshot. A snapshot record anywhere but at the start of the log refuses to start.
4. Remove the files a crash or a newer snapshot left behind: segments before the starting one, which recovery never reads, other snapshot files, and `.tmp` files.

### InstallSnapshot

- **Chunks.** `InstallSnapshotRequest` has Figure 13's fields: `Term`, `LeaderID`, `LastIncludedIndex`, `LastIncludedTerm`, `Offset`, `Data`, and `Done`. `Config.SnapshotChunkSize` bounds a chunk; 0 sends the whole snapshot in one message. `DefaultConfig` uses 1 MiB, and `MITLabConfig`, for the MIT 6.5840 tests, uses 0, as the lab requires.
- **Replies.** `InstallSnapshotResponse` adds three fields to Figure 13's `Term`, so the reply names what it acknowledges, as `AppendEntriesResponse.MatchIndex` does ([ADR-0004](0004-fast-log-backtracking.md)):
  - `LastIncludedIndex` names the snapshot the reply answers.
  - `Offset` says how many of its bytes the follower holds.
  - `Done` says the follower's log now matches the leader's through `LastIncludedIndex`, either because it installed the snapshot or because it already had every entry the snapshot covers.
- **The leader's side.**
  - It sends a snapshot when a follower's `nextIndex` is at or below the snapshot's last included index, because the next entry it needs is gone.
  - It sends a chunk from the follower's known offset, resends it at every heartbeat, and sends the next chunk as soon as a reply shows progress. A reply that shows none waits for the heartbeat, so duplicated replies never start a second stream of chunks.
  - A transfer keeps the snapshot it began with until the follower holds all of it, even if the leader compacts again meanwhile.
  - `Done` counts as a successful AppendEntries through `LastIncludedIndex`.
- **The follower's side.** It assembles chunks in memory, keyed by the leader's term and the snapshot's index. A chunk that does not continue the snapshot arriving is a repeat, or belongs to an earlier transfer. It is dropped, and the reply says how many bytes the follower holds. Only the complete snapshot reaches storage, in step 5.

### Figure 13's receiver

1. Reply immediately if the term is stale (step 1). Otherwise the sender leads this term: InstallSnapshot restarts the election timer as AppendEntries does, and a candidate of that term concedes.
2. A snapshot at or below `commitIndex` is ignored, with a reply that is `Done`. The follower already holds every entry it covers, and installing it would move the state machine backwards.
3. Steps 2–4 assemble the chunks. Step 5 hands the snapshot to storage through `Ready`, which keeps only the newest.
4. Step 6 keeps the entries after a matching last included entry; step 7 otherwise discards the log.
5. Step 8 runs in both cases: `commitIndex` and `lastApplied` move to the last included index, and the runtime resets the state machine. Only snapshots past `commitIndex` reach this point, so the state machine has not applied every entry the snapshot covers, and the log no longer holds them. The README's deviations table lists this.

An AppendEntries whose `PrevLogIndex` comes before the snapshot's last included entry matches there. The entries the snapshot replaced are committed, so they match every leader's (Leader Completeness), and only the request's entries after the snapshot reach the log.

## Alternatives considered

- **Keep the segments that still hold entries after the snapshot**, as etcd does by naming segments after their first Raft index. Replay must then start in the middle of the log. There, a kept truncation or entry may refer to indexes the snapshot replaced, so every record type needs rules for what lies inside the snapshot. Restating the entries after the snapshot gives recovery one starting point and no such rules. The cost is rewriting a tail that is short, since the service snapshots what it has applied.
- **A snapshot record holding the data, with no separate file.** It ties a snapshot's size to the record's 4-byte length and rewrites the data through the WAL. A file of its own is written once.
- **Partial chunks on disk**, as Figure 13's steps 2 and 3 describe. A restarted follower could resume a transfer, but the core would need a `Ready` field for partial writes. A crash mid-transfer only costs sending the snapshot again.
- **etcd's step 6**: when the log already holds the snapshot's last included entry, commit through it and apply from the log instead of installing. It avoids resetting the state machine, but saves no snapshot in that case, and steps away from Figure 13's shape.
- **Restart each transfer with the newest snapshot.** The randomized simulator showed that a leader compacting every two entries outran 16-byte chunks, and a lagging follower never caught up.
- **A format version in the file.** As with the WAL, an addition such as the configuration needs none, and an incompatible change can use a new name.

## Consequences

- Logs stay bounded. A restart replays one snapshot and the segments after it, however long the server has run.
- Each compaction rewrites the entries after the snapshot into a new segment and removes the old ones. Storage also keeps the snapshot in memory to return from `Load`, the core keeps it to send to followers, and a leader keeps the snapshot each transfer under way began with.
- Safety rests on two atomic renames, each followed by a directory fsync. Crash tests strike every write, fsync, rename, and removal of a snapshot save and of the recovery after it.
- `FuzzSnapshotDecode` fuzzes the file decoder, and `FuzzWALRecordDecode` covers the new record. The simulator checks every compacted, installed, or restored snapshot against the commands applied through its last included index.
