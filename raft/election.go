package raft

// resetElectionTimer restarts the election timer with a timeout drawn from
// [electionTick, 2*electionTick). Drawing again on every reset spreads the
// servers out, so split votes are rare and resolve quickly (§5.2).
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.electionTick + n.rand.IntN(n.electionTick)
}

// campaign starts an election (Figure 2, candidates): the node becomes a
// candidate for the next term and asks every other server for its vote. A
// server that is a majority on its own, the only server of its cluster,
// wins at once.
func (n *Node) campaign() {
	n.becomeCandidate()
	lastIndex, lastTerm := n.lastLog()
	for _, id := range n.peers {
		if id == n.id {
			continue
		}
		n.send(Message{To: id, RequestVote: &RequestVoteRequest{
			Term:         n.currentTerm,
			CandidateID:  n.id,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}})
	}
	if n.hasMajority() {
		n.becomeLeader()
	}
}

// handleRequestVote is Figure 2's RequestVote receiver. Step has already
// adopted a later term, so req.Term is at most currentTerm here.
func (n *Node) handleRequestVote(req *RequestVoteRequest) {
	// 1. Reply false if term < currentTerm (§5.1).
	if req.Term < n.currentTerm {
		n.replyVote(req.CandidateID, false)
		return
	}
	// 2. If votedFor is null or candidateId, and the candidate's log is at
	// least as up-to-date as the receiver's log, grant the vote (§5.2, §5.4).
	free := n.votedFor == None || n.votedFor == req.CandidateID
	if !free || !n.isUpToDate(req.LastLogIndex, req.LastLogTerm) {
		n.replyVote(req.CandidateID, false)
		return
	}
	n.votedFor = req.CandidateID
	// Figure 2, followers: granting a vote restarts the election timer, and
	// nothing else a candidate sends does, so a candidate that cannot win
	// cannot hold off the election of one that can.
	n.resetElectionTimer()
	// The vote and this reply leave in the same Ready, which the runtime
	// persists before it sends, so a restart can never vote twice in a term.
	n.replyVote(req.CandidateID, true)
}

func (n *Node) replyVote(to NodeID, granted bool) {
	n.send(Message{To: to, RequestVoteResponse: &RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: granted,
	}})
}

// handleRequestVoteResponse counts a granted vote and makes the candidate
// leader once a majority has voted for it (Figure 2, candidates). Votes are
// recorded by voter, so a duplicated response counts once.
func (n *Node) handleRequestVoteResponse(from NodeID, resp *RequestVoteResponse) {
	// A vote from an earlier election, or one that arrives after this
	// election is decided, has nothing left to count toward.
	if n.role != Candidate || resp.Term != n.currentTerm || !resp.VoteGranted {
		return
	}
	n.votes[from] = true
	if n.hasMajority() {
		n.becomeLeader()
	}
}

// hasMajority reports whether a majority of the cluster granted this
// candidate its vote. It counts over the sorted peers, so a vote from a
// server outside the cluster never counts.
func (n *Node) hasMajority() bool {
	granted := 0
	for _, id := range n.peers {
		if n.votes[id] {
			granted++
		}
	}
	return granted > len(n.peers)/2
}

// lastLog returns the index and term of the last log entry, or 0 and 0 when
// the log is empty.
func (n *Node) lastLog() (Index, Term) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// isUpToDate reports whether a log that ends at lastIndex and lastTerm is
// at least as up-to-date as this server's (§5.4.1): the later last term
// wins, and between equal last terms the longer log wins.
func (n *Node) isUpToDate(lastIndex Index, lastTerm Term) bool {
	ownIndex, ownTerm := n.lastLog()
	if lastTerm != ownTerm {
		return lastTerm > ownTerm
	}
	return lastIndex >= ownIndex
}
