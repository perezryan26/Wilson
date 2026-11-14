package raft

// sendSnapshot sends server the snapshot, because the leader has replaced
// the entries the server needs next (§7).
func (n *Node) sendSnapshot(to NodeID) {
	n.send(Message{To: to, InstallSnapshot: &InstallSnapshotRequest{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: n.snapshot.LastIncludedIndex,
		LastIncludedTerm:  n.snapshot.LastIncludedTerm,
		Data:              n.snapshot.Data,
	}})
}

// handleInstallSnapshot is Figure 13's InstallSnapshot receiver. Step has
// already adopted a later term, so req.Term is at most currentTerm here.
func (n *Node) handleInstallSnapshot(req *InstallSnapshotRequest) {
	// 1. Reply immediately if term < currentTerm.
	if req.Term < n.currentTerm {
		n.replySnapshot(req.LeaderID, 0)
		return
	}
	// The leader sends a snapshot in place of AppendEntries, so it counts
	// as hearing from the leader (Figure 2, followers).
	n.followLeader(req.LeaderID)
	// Every entry the snapshot covers is committed here already, and this
	// server holds each in its log or its own snapshot, so the snapshot
	// adds nothing; installing it would move the state machine backwards.
	if req.LastIncludedIndex <= n.commitIndex {
		n.replySnapshot(req.LeaderID, req.LastIncludedIndex)
		return
	}
	// 5. Save snapshot file, discard any existing or partial snapshot with
	// a smaller index: Ready hands the snapshot to storage, which keeps
	// only the newest.
	n.snapshot = &Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	}
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
	n.replySnapshot(req.LeaderID, n.snapshot.LastIncludedIndex)
}

// replySnapshot answers an InstallSnapshot, acknowledging that this log
// matches the leader's through index, or nothing if index is 0.
func (n *Node) replySnapshot(to NodeID, index Index) {
	n.send(Message{To: to, InstallSnapshotResponse: &InstallSnapshotResponse{
		Term:              n.currentTerm,
		LastIncludedIndex: index,
	}})
}

// handleInstallSnapshotResponse records that a follower's log matches the
// leader's through the snapshot it acknowledges, as a successful
// AppendEntries would, and sends it the entries that follow.
func (n *Node) handleInstallSnapshotResponse(from NodeID, resp *InstallSnapshotResponse) {
	// A reply from an earlier term answers an earlier leader. Step has
	// already stepped down for a later one.
	if n.role != Leader || resp.Term != n.currentTerm {
		return
	}
	n.acknowledge(from, resp.LastIncludedIndex)
}
