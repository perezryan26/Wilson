package raft

// resetElectionTimer restarts the election timer with a timeout drawn from
// [electionTick, 2*electionTick). Drawing again on every reset spreads the
// servers out, so split votes are rare and resolve quickly (§5.2).
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.electionTick + n.rand.IntN(n.electionTick)
}

// campaign starts an election (Figure 2, candidates). A server that is a
// majority on its own, the only server of its cluster, wins at once.
func (n *Node) campaign() {
	n.becomeCandidate()
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
