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
// restart (Figure 2: it increases monotonically). It runs after every
// event, when every running server's term is durable. A term lost to a
// crash in the middle of its Save is never seen here, and never left in a
// message.
func (c *Cluster) checkTerm(n *node, st raft.Status) {
	if st.Term < n.maxTerm {
		c.fail(fmt.Errorf("server %d: currentTerm fell from %d to %d", n.id, n.maxTerm, st.Term))
		return
	}
	n.maxTerm = st.Term
}

// checkVote fails the run if n votes for a second candidate in a term,
// even after a restart. It sees each vote once it is durable: when its
// Save returns, and when a restart loads it. A vote lost to a crash in the
// middle of its Save never left in a reply, so it binds no one.
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

// checkRestored fails the run if a restarting server lost state that a
// Save had made durable: if it comes back in a term below the highest it
// saved, or with a vote other than the one it saved for that term. A
// crash in the middle of a Save may lose that Save's writes, but nothing
// saved before.
func (c *Cluster) checkRestored(n *node, hs raft.HardState) {
	if hs.CurrentTerm < n.maxTerm {
		c.fail(fmt.Errorf("server %d restarted in term %d after saving term %d", n.id, hs.CurrentTerm, n.maxTerm))
		return
	}
	c.checkVote(n, hs)
}

// logKey names an entry by index and term, which Log Matching says name it
// in every log that holds it.
type logKey struct {
	index raft.Index
	term  raft.Term
}

// entryInfo is what every stored copy of an entry must agree on.
type entryInfo struct {
	command  string
	prevTerm raft.Term // the term of the entry before it
}

// commitment is an entry known to be committed: its term, and the term of
// the first server to know it committed. Every leader of a later term must
// hold it (Leader Completeness).
type commitment struct {
	term, inTerm raft.Term
}

// store records entries that n just saved in the simulator's copy of n's
// log, checking Leader Append-Only and Log Matching on the way.
func (c *Cluster) store(n *node, entries []raft.Entry) {
	if len(entries) == 0 {
		return
	}
	first := entries[0].Index
	// Leader Append-Only (Figure 3): a leader never overwrites or deletes
	// entries in its own log.
	if n.core.Status().Role == raft.Leader && first <= raft.Index(len(n.log)) {
		c.fail(fmt.Errorf("leader %d rewrote its log from index %d (Leader Append-Only)", n.id, first))
		return
	}
	n.log = append(n.log[:first-1], entries...)
	for _, e := range entries {
		c.checkLogMatching(n, e)
	}
}

// checkLogMatching fails the run if e differs, in command or in the term of
// the entry before it, from any entry ever stored with e's index and term.
// With the term before every entry pinned, two logs that share an entry's
// index and term share every entry up to it: Log Matching (Figure 3).
func (c *Cluster) checkLogMatching(n *node, e raft.Entry) {
	var prevTerm raft.Term
	if e.Index > 1 {
		prevTerm = n.log[e.Index-2].Term
	}
	key, info := logKey{e.Index, e.Term}, entryInfo{string(e.Data), prevTerm}
	if seen, ok := c.entries[key]; ok && seen != info {
		c.fail(fmt.Errorf("server %d stored a different entry at index %d of term %d (Log Matching)", n.id, e.Index, e.Term))
		return
	}
	c.entries[key] = info
}

// apply records that n applied e. The run fails unless e follows the last
// entry n applied since it booted, and is the entry every other server
// applied at that index (State Machine Safety, Figure 3).
func (c *Cluster) apply(n *node, e raft.Entry) {
	if e.Index != n.applied+1 {
		c.fail(fmt.Errorf("server %d applied index %d right after index %d", n.id, e.Index, n.applied))
		return
	}
	n.applied = e.Index
	if e.Index > raft.Index(len(c.applied)) {
		c.applied = append(c.applied, e)
		return
	}
	if was := c.applied[e.Index-1]; was.Term != e.Term || string(was.Data) != string(e.Data) {
		c.fail(fmt.Errorf("server %d applied a different entry at index %d (State Machine Safety)", n.id, e.Index))
	}
}

// checkCommits records the entries n has newly learned are committed. The
// run fails unless lastApplied ≤ commitIndex ≤ n's last log index, if two
// different entries are ever committed at one index, or if a current leader
// lacks a newly committed entry.
func (c *Cluster) checkCommits(n *node, st raft.Status) {
	if st.LastApplied > st.CommitIndex || st.CommitIndex > raft.Index(len(n.log)) {
		c.fail(fmt.Errorf("server %d has lastApplied %d, commitIndex %d, and last log index %d",
			n.id, st.LastApplied, st.CommitIndex, len(n.log)))
		return
	}
	for i := n.status.CommitIndex + 1; i <= st.CommitIndex; i++ {
		term := n.log[i-1].Term
		if i <= raft.Index(len(c.committed)) {
			if c.committed[i-1].term != term {
				c.fail(fmt.Errorf("server %d committed a different entry at index %d", n.id, i))
				return
			}
			continue
		}
		c.committed = append(c.committed, commitment{term: term, inTerm: st.Term})
		c.checkLeadersHold(i)
	}
}

// checkLeadersHold fails the run if a current leader of a term after the
// one in which entry i was committed lacks the entry (Leader Completeness,
// Figure 3).
func (c *Cluster) checkLeadersHold(i raft.Index) {
	for _, l := range c.nodes {
		if l.core == nil {
			continue
		}
		if st := l.core.Status(); st.Role == raft.Leader && lacks(l, st.Term, i, c.committed[i-1]) {
			c.fail(fmt.Errorf("leader %d of term %d lacks committed entry %d (Leader Completeness)", l.id, st.Term, i))
			return
		}
	}
}

// checkNewLeader fails the run if n has just become leader without every
// entry committed in an earlier term (Leader Completeness, Figure 3).
func (c *Cluster) checkNewLeader(n *node, st raft.Status) {
	if st.Role != raft.Leader || (n.status.Role == raft.Leader && n.status.Term == st.Term) {
		return
	}
	for i, com := range c.committed {
		if lacks(n, st.Term, raft.Index(i+1), com) {
			c.fail(fmt.Errorf("server %d became leader of term %d without committed entry %d (Leader Completeness)", n.id, st.Term, i+1))
			return
		}
	}
}

// lacks reports whether n, leader of term, lacks committed entry i, which
// every leader of a term after com.inTerm must hold.
func lacks(n *node, term raft.Term, i raft.Index, com commitment) bool {
	return term > com.inTerm && (i > raft.Index(len(n.log)) || n.log[i-1].Term != com.term)
}
