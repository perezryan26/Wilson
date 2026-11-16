package raft

import "slices"

// ReadState says that a read the runtime asked about with ReadIndex may
// be served from the state machine without going through the log: once
// the state machine has applied Index, it reflects every write that
// completed before the read began (§8).
type ReadState struct {
	ID    uint64 // the ID the read was asked about with
	Index Index  // the read index: the leader's commitIndex when it confirmed the read
}

// pendingRead is a read that a leader has yet to confirm, and the round of
// leadership checks that began with it.
type pendingRead struct {
	id    uint64
	round uint64
}

// ReadIndex asks whether the read named id may be served without going
// through the log (§8). The leader starts a round of leadership checks,
// sending every other server an AppendEntries that carries the round, as
// a heartbeat would. Once a majority, itself included, has answered a
// request of that round or a later one, no other leader can have been
// elected before the read arrived. Once, too, the leader has committed an
// entry of its term, its commitIndex covers every write that completed
// before then. A ReadState then names the read, with that commitIndex.
// ReadIndex returns ErrNotLeader on a server that is not the leader, and
// a leader that loses its leadership first drops the read.
func (n *Node) ReadIndex(id uint64) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	n.readRound++
	n.reads = append(n.reads, pendingRead{id: id, round: n.readRound})
	n.broadcastAppend()
	// A leader that is a majority on its own confirms the read at once.
	n.confirmReads()
	return nil
}

// ackRead records that server answered, in this term, an AppendEntries of
// round, and confirms the reads that answer completes.
func (n *Node) ackRead(server NodeID, round uint64) {
	if round > n.readAcks[server] {
		n.readAcks[server] = round
		n.confirmReads()
	}
}

// confirmReads hands the runtime, as ReadStates, the reads whose round a
// majority has answered, provided the leader has committed an entry of
// its term. Until then, it may not know which entries are committed
// (§8), so every read waits.
func (n *Node) confirmReads() {
	if term, _ := n.log.term(n.commitIndex); len(n.reads) == 0 || term != n.currentTerm {
		return
	}
	// Reads wait in the order they arrived, which is the order of their
	// rounds, so the confirmed ones come first.
	confirmed := n.quorumRound()
	i := 0
	for ; i < len(n.reads) && n.reads[i].round <= confirmed; i++ {
		n.readStates = append(n.readStates, ReadState{ID: n.reads[i].id, Index: n.commitIndex})
	}
	n.reads = n.reads[i:]
}

// quorumRound returns the latest round of leadership checks that a
// majority of servers, the leader included, has answered.
func (n *Node) quorumRound() uint64 {
	rounds := make([]uint64, len(n.peers))
	for i, id := range n.peers {
		rounds[i] = n.readAcks[id]
		if id == n.id {
			rounds[i] = n.readRound
		}
	}
	slices.Sort(rounds)
	// A majority of servers have answered at least this round.
	return rounds[(len(rounds)-1)/2]
}
