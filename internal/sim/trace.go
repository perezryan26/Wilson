package sim

import (
	"fmt"
	"strings"
	"time"

	"github.com/perezryan26/wilson/raft"
)

// tracef adds one line to the trace, stamped with the virtual time.
func (c *Cluster) tracef(format string, args ...any) {
	fmt.Fprintf(&c.trace, "t=%s ", ms(c.now))
	fmt.Fprintf(&c.trace, format, args...)
	c.trace.WriteByte('\n')
}

// traceMessage traces what happened to m, if message tracing is on. what
// is empty for a delivery.
func (c *Cluster) traceMessage(what string, m raft.Message) {
	if c.traceMessages {
		c.tracef("%s→%s %s%s", name(m.From), name(m.To), what, describe(m))
	}
}

// ms prints d in milliseconds to the microsecond, the simulator's
// resolution.
func ms(d time.Duration) string {
	return fmt.Sprintf("%d.%03dms", d/time.Millisecond, d%time.Millisecond/time.Microsecond)
}

// name prints a server as n1, n2, and so on, or "none".
func name(id raft.NodeID) string {
	if id == raft.None {
		return "none"
	}
	return fmt.Sprintf("n%d", id)
}

// change prints a server's status, marking each field that changed since
// was as old→new.
func change(was, now raft.Status) string {
	role, term, leader := now.Role.String(), fmt.Sprint(now.Term), name(now.Leader)
	if was.Role != now.Role {
		role = was.Role.String() + "→" + role
	}
	if was.Term != now.Term {
		term = fmt.Sprintf("%d→%d", was.Term, now.Term)
	}
	if was.Leader != now.Leader {
		leader = name(was.Leader) + "→" + leader
	}
	return fmt.Sprintf("%s term=%s leader=%s", role, term, leader)
}

// describe prints m's RPC with the paper's field names.
func describe(m raft.Message) string {
	switch {
	case m.RequestVote != nil:
		r := m.RequestVote
		return fmt.Sprintf("RequestVote term=%d candidateId=%d lastLogIndex=%d lastLogTerm=%d",
			r.Term, r.CandidateID, r.LastLogIndex, r.LastLogTerm)
	case m.RequestVoteResponse != nil:
		r := m.RequestVoteResponse
		return fmt.Sprintf("RequestVoteResponse term=%d voteGranted=%t", r.Term, r.VoteGranted)
	case m.AppendEntries != nil:
		r := m.AppendEntries
		return fmt.Sprintf("AppendEntries term=%d leaderId=%d", r.Term, r.LeaderID)
	case m.AppendEntriesResponse != nil:
		r := m.AppendEntriesResponse
		return fmt.Sprintf("AppendEntriesResponse term=%d success=%t", r.Term, r.Success)
	}
	return "empty message"
}

// groupNames prints partition groups as "n1 n2 | n3 n4 n5".
func groupNames(groups [][]raft.NodeID) string {
	parts := make([]string, len(groups))
	for i, g := range groups {
		names := make([]string, len(g))
		for j, id := range g {
			names[j] = name(id)
		}
		parts[i] = strings.Join(names, " ")
	}
	return strings.Join(parts, " | ")
}
