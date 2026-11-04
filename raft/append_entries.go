package raft

import (
	"fmt"
	"slices"
)

// broadcastAppend sends AppendEntries to every other server.
func (n *Node) broadcastAppend() {
	n.heartbeatElapsed = 0
	for _, id := range n.peers {
		if id != n.id {
			n.sendAppend(id)
		}
	}
}

// sendAppend sends server to an AppendEntries carrying every entry from its
// nextIndex on (Figure 2, leaders), or none when it already has them all,
// which makes it a heartbeat (§5.2). Every heartbeat therefore also retries
// whatever a lost message failed to deliver.
func (n *Node) sendAppend(to NodeID) {
	next := n.nextIndex[to]
	prevTerm, _ := n.log.term(next - 1)
	n.send(Message{To: to, AppendEntries: &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: next - 1,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.from(next),
		LeaderCommit: n.commitIndex,
	}})
}

// handleAppendEntries is Figure 2's AppendEntries receiver. Step has already
// adopted a later term, so req.Term is at most currentTerm here.
func (n *Node) handleAppendEntries(req *AppendEntriesRequest) {
	// 1. Reply false if term < currentTerm (§5.1).
	if req.Term < n.currentTerm {
		n.rejectAppend(req.LeaderID)
		return
	}
	if n.role == Leader {
		panic(fmt.Sprintf("raft: Election Safety violated: servers %d and %d both lead term %d",
			n.id, req.LeaderID, n.currentTerm))
	}
	// The sender won this term's election, so a candidate concedes (§5.2),
	// and hearing from the current leader restarts a follower's election
	// timer (Figure 2, followers). The vote cast in this term stands.
	n.becomeFollower(n.currentTerm, req.LeaderID)
	n.resetElectionTimer()
	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose
	// term matches prevLogTerm (§5.3).
	if term, ok := n.log.term(req.PrevLogIndex); !ok || term != req.PrevLogTerm {
		n.rejectConflict(req.LeaderID, req.PrevLogIndex)
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow it.
	// 4. Append any new entries not already in the log.
	n.unstable = min(n.unstable, n.log.merge(req.Entries))
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
	// index of last new entry). That is the request's last entry, not the
	// log's: any entries after it may still be overwritten.
	lastNew := req.PrevLogIndex + Index(len(req.Entries))
	n.commitTo(min(req.LeaderCommit, lastNew))
	// The new entries leave in the same Ready as this reply, and the runtime
	// persists them before it sends it.
	n.send(Message{To: req.LeaderID, AppendEntriesResponse: &AppendEntriesResponse{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: lastNew,
	}})
}

func (n *Node) rejectAppend(to NodeID) {
	n.send(Message{To: to, AppendEntriesResponse: &AppendEntriesResponse{Term: n.currentTerm}})
}

// rejectConflict refuses an AppendEntries whose PrevLogIndex entry this log
// lacks or holds with another term, and says where the logs part
// (docs/adr/0004-fast-log-backtracking.md).
func (n *Node) rejectConflict(to NodeID, prevIndex Index) {
	resp := &AppendEntriesResponse{Term: n.currentTerm, ConflictIndex: n.log.lastIndex() + 1}
	if prevIndex <= n.log.lastIndex() {
		resp.ConflictTerm, _ = n.log.term(prevIndex)
		resp.ConflictIndex = n.log.firstIndexOf(resp.ConflictTerm, prevIndex)
	}
	n.send(Message{To: to, AppendEntriesResponse: resp})
}

// handleAppendEntriesResponse applies Figure 2's rules for a leader that
// hears back from a follower: on success it moves the follower's matchIndex
// and nextIndex forward, and on a rejection it backs nextIndex up and
// retries.
func (n *Node) handleAppendEntriesResponse(from NodeID, resp *AppendEntriesResponse) {
	// A reply from an earlier term answers an earlier leader. Step has
	// already stepped down for a later one.
	if n.role != Leader || resp.Term != n.currentTerm {
		return
	}
	if !resp.Success {
		// The follower's log lacks or contradicts the entry at PrevLogIndex
		// (§5.3). Back up past the whole conflict, but always by at least
		// one entry, so even an odd reply cannot stall the search, and never
		// to entries the follower is known to hold.
		next := min(n.nextIndex[from]-1, n.backtrack(resp))
		n.nextIndex[from] = max(next, n.matchIndex[from]+1)
		n.sendAppend(from)
		return
	}
	// A late or repeated reply acknowledges no more than one already
	// counted, and matchIndex never moves back.
	if resp.MatchIndex <= n.matchIndex[from] {
		return
	}
	n.matchIndex[from] = resp.MatchIndex
	n.nextIndex[from] = resp.MatchIndex + 1
	n.maybeCommit()
	if n.nextIndex[from] <= n.log.lastIndex() {
		n.sendAppend(from)
	}
}

// backtrack returns the nextIndex a rejection points to: just past the
// leader's last entry of ConflictTerm if it has that term, and otherwise
// ConflictIndex, where the follower's run of that term begins or its log
// ends (docs/adr/0004-fast-log-backtracking.md).
func (n *Node) backtrack(resp *AppendEntriesResponse) Index {
	if resp.ConflictTerm != 0 {
		if last := n.log.lastIndexOf(resp.ConflictTerm); last > 0 {
			return last + 1
		}
	}
	return resp.ConflictIndex
}

// maybeCommit sets commitIndex to the highest index a majority of servers
// store, the leader's own log included, if that entry is from the current
// term (Figure 2, leaders). §5.4.2: an entry from an earlier term is never
// committed by counting replicas, because a later leader could still
// overwrite it (Figure 8); it commits with the first entry of this term.
func (n *Node) maybeCommit() {
	matched := make([]Index, len(n.peers))
	for i, id := range n.peers {
		matched[i] = n.matchIndex[id]
		if id == n.id {
			matched[i] = n.log.lastIndex()
		}
	}
	slices.Sort(matched)
	// A majority of servers have matched at least this far.
	index := matched[(len(matched)-1)/2]
	if term, _ := n.log.term(index); term == n.currentTerm {
		n.commitTo(index)
	}
}

// commitTo raises commitIndex to i, never lowering it (Figure 2: it
// increases monotonically).
func (n *Node) commitTo(i Index) {
	if i > n.commitIndex {
		n.commitIndex = i
	}
}
