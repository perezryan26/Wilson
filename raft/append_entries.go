package raft

import "fmt"

// broadcastHeartbeat sends an AppendEntries with no entries to every other
// server, so that followers keep recognizing this leader and do not start
// an election (§5.2).
func (n *Node) broadcastHeartbeat() {
	n.heartbeatElapsed = 0
	for _, id := range n.peers {
		if id == n.id {
			continue
		}
		n.send(Message{To: id, AppendEntries: &AppendEntriesRequest{
			Term:     n.currentTerm,
			LeaderID: n.id,
		}})
	}
}

// handleAppendEntries is Figure 2's AppendEntries receiver for a heartbeat.
// Step has already adopted a later term, so req.Term is at most currentTerm
// here.
func (n *Node) handleAppendEntries(req *AppendEntriesRequest) {
	// 1. Reply false if term < currentTerm (§5.1).
	if req.Term < n.currentTerm {
		n.replyAppendEntries(req.LeaderID, false)
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
	n.replyAppendEntries(req.LeaderID, true)
}

func (n *Node) replyAppendEntries(to NodeID, success bool) {
	n.send(Message{To: to, AppendEntriesResponse: &AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: success,
	}})
}
