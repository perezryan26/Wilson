package raft

// sendSnapshot sends server the next chunk of the snapshot, because the
// leader has replaced the entries the server needs next (§7). The chunk
// begins where the server's copy is known to end, and holds at most
// snapshotChunkSize bytes, or the rest of the snapshot if that is 0.
func (n *Node) sendSnapshot(to NodeID) {
	s, offset := n.snapshot, n.snapshotOffset[to]
	end := len(s.Data)
	if n.snapshotChunkSize > 0 {
		end = min(end, offset+n.snapshotChunkSize)
	}
	n.send(Message{To: to, InstallSnapshot: &InstallSnapshotRequest{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: s.LastIncludedIndex,
		LastIncludedTerm:  s.LastIncludedTerm,
		Offset:            offset,
		Data:              s.Data[offset:end],
		Done:              end == len(s.Data),
	}})
}

// handleInstallSnapshot is Figure 13's InstallSnapshot receiver. Step has
// already adopted a later term, so req.Term is at most currentTerm here.
func (n *Node) handleInstallSnapshot(req *InstallSnapshotRequest) {
	// 1. Reply immediately if term < currentTerm.
	if req.Term < n.currentTerm {
		n.replySnapshot(req.LeaderID, InstallSnapshotResponse{})
		return
	}
	// The leader sends a snapshot in place of AppendEntries, so it counts
	// as hearing from the leader (Figure 2, followers).
	n.followLeader(req.LeaderID)
	// Every entry the snapshot covers is committed here already, and this
	// server holds each in its log or its own snapshot, so the snapshot
	// adds nothing; installing it would move the state machine backwards.
	if req.LastIncludedIndex <= n.commitIndex {
		n.replySnapshot(req.LeaderID, InstallSnapshotResponse{LastIncludedIndex: req.LastIncludedIndex, Done: true})
		return
	}
	if !n.receiveChunk(req) {
		// 4. Reply and wait for more data chunks if done is false.
		n.replySnapshot(req.LeaderID, InstallSnapshotResponse{LastIncludedIndex: req.LastIncludedIndex, Offset: n.received(req)})
		return
	}
	n.installSnapshot()
	n.replySnapshot(req.LeaderID, InstallSnapshotResponse{LastIncludedIndex: req.LastIncludedIndex, Done: true})
}

// receiveChunk carries out Figure 13's steps 2 and 3 for one chunk and
// reports whether the snapshot is now complete. The partial snapshot
// lives in memory: a crash loses it, and the leader starts again. The
// leader sends each chunk once the previous one is acknowledged, so a
// chunk that does not continue the snapshot arriving is a repeat or
// belongs to an earlier transfer, and is dropped.
func (n *Node) receiveChunk(req *InstallSnapshotRequest) bool {
	// 2. Create new snapshot file if first chunk (offset is 0), unless it
	// repeats the first chunk of the snapshot already arriving.
	if req.Offset == 0 && !n.receiving(req) {
		n.partial = &Snapshot{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}
		n.partialTerm = req.Term
	}
	// 3. Write data into snapshot file at given offset.
	if req.Offset != n.received(req) {
		return false
	}
	n.partial.Data = append(n.partial.Data, req.Data...)
	return req.Done
}

// receiving reports whether req belongs to the snapshot arriving: the one
// through the same index, from the leader of the same term.
func (n *Node) receiving(req *InstallSnapshotRequest) bool {
	return n.partial != nil && n.partialTerm == req.Term && n.partial.LastIncludedIndex == req.LastIncludedIndex
}

// received returns how many bytes of req's snapshot have arrived.
func (n *Node) received(req *InstallSnapshotRequest) int {
	if !n.receiving(req) {
		return 0
	}
	return len(n.partial.Data)
}

// installSnapshot carries out Figure 13's steps 5 to 8 for the snapshot
// that has fully arrived.
func (n *Node) installSnapshot() {
	// 5. Save snapshot file, discard any existing or partial snapshot with
	// a smaller index: Ready hands the snapshot to storage, which keeps
	// only the newest.
	n.snapshot, n.partial = n.partial, nil
	n.snapshotUnstable = true
	// 6. If existing log entry has same index and term as snapshot's last
	// included entry, retain log entries following it. 7. Otherwise,
	// discard the entire log. Either way the snapshot and the entries
	// after it replace what storage holds.
	n.log.restore(n.snapshot)
	n.unstable = n.snapshot.LastIncludedIndex + 1
	// 8. Reset state machine using snapshot contents. Step 6 replies
	// without this, but the entries the snapshot covers are gone from the
	// log, and the state machine has not applied them all: Ready has the
	// runtime restore it from the snapshot.
	n.commitIndex = n.snapshot.LastIncludedIndex
	n.lastApplied = n.commitIndex
}

// replySnapshot sends resp, in the current term, to answer an
// InstallSnapshot.
func (n *Node) replySnapshot(to NodeID, resp InstallSnapshotResponse) {
	resp.Term = n.currentTerm
	n.send(Message{To: to, InstallSnapshotResponse: &resp})
}

// handleInstallSnapshotResponse moves a snapshot transfer along. Once the
// follower is done, its log matches the leader's through the snapshot, as
// after a successful AppendEntries, and the leader sends the entries that
// follow. Until then, the reply says how much of the snapshot the
// follower holds, and the leader sends the next chunk from there.
func (n *Node) handleInstallSnapshotResponse(from NodeID, resp *InstallSnapshotResponse) {
	// A reply from an earlier term answers an earlier leader. Step has
	// already stepped down for a later one.
	if n.role != Leader || resp.Term != n.currentTerm {
		return
	}
	if resp.Done {
		n.acknowledge(from, resp.LastIncludedIndex)
		return
	}
	// A reply about another snapshot answers an earlier transfer.
	s := n.snapshot
	if s == nil || resp.LastIncludedIndex != s.LastIncludedIndex || resp.Offset > len(s.Data) {
		return
	}
	prev := n.snapshotOffset[from]
	n.snapshotOffset[from] = resp.Offset
	// A reply that moves the transfer forward gets the next chunk at once.
	// Any other waits for the next heartbeat, so a duplicated reply never
	// starts a second stream of chunks.
	if resp.Offset > prev && n.nextIndex[from] <= s.LastIncludedIndex {
		n.sendSnapshot(from)
	}
}
