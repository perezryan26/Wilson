package sim

import (
	"fmt"

	"github.com/perezryan26/wilson/raft"
)

// checkElectionSafety fails the run if two servers ever lead the same term
// (Figure 3), even at different times.
func (c *Cluster) checkElectionSafety(n *node, st raft.Status) {
	if st.Role != raft.Leader {
		return
	}
	if prev, ok := c.leaders[st.Term]; ok && prev != n.id {
		c.fail(fmt.Errorf("two leaders in term %d, servers %d and %d (Election Safety)", st.Term, prev, n.id))
		return
	}
	c.leaders[st.Term] = n.id
}

// checkTerm fails the run if n's currentTerm ever decreases, even across a
// restart (Figure 2: it increases monotonically).
func (c *Cluster) checkTerm(n *node, st raft.Status) {
	if st.Term < n.maxTerm {
		c.fail(fmt.Errorf("server %d: currentTerm fell from %d to %d", n.id, n.maxTerm, st.Term))
		return
	}
	n.maxTerm = st.Term
}

// checkVote fails the run if the hard state n is about to save votes for a
// second candidate in a term, even after a restart.
func (c *Cluster) checkVote(n *node, hs raft.HardState) {
	if hs.VotedFor == raft.None {
		return
	}
	if prev, ok := n.votes[hs.CurrentTerm]; ok && prev != hs.VotedFor {
		c.fail(fmt.Errorf("server %d voted for both %d and %d in term %d", n.id, prev, hs.VotedFor, hs.CurrentTerm))
		return
	}
	n.votes[hs.CurrentTerm] = hs.VotedFor
}
