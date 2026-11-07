package sim

import (
	"fmt"
	"log/slog"
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

// logger returns a logger whose lines, such as recovery's report of a
// record torn by a crash, go into the trace as server id's.
func (c *Cluster) logger(id raft.NodeID) *slog.Logger {
	return slog.New(slog.NewTextHandler(traceWriter{c, id}, &slog.HandlerOptions{ReplaceAttr: dropTimeAndLevel}))
}

// traceWriter adds each line written to it to the trace, as server id's.
type traceWriter struct {
	c  *Cluster
	id raft.NodeID
}

func (w traceWriter) Write(p []byte) (int, error) {
	w.c.tracef("%s %s", name(w.id), strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

// dropTimeAndLevel leaves the wall-clock time out of log lines, which
// would make traces differ between runs, and the level, which adds
// nothing there.
func dropTimeAndLevel(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && (a.Key == slog.TimeKey || a.Key == slog.LevelKey) {
		return slog.Attr{}
	}
	return a
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
// was as old→new. commitIndex and lastApplied appear only when they moved.
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
	s := fmt.Sprintf("%s term=%s leader=%s", role, term, leader)
	if was.CommitIndex != now.CommitIndex {
		s += fmt.Sprintf(" commitIndex=%d→%d", was.CommitIndex, now.CommitIndex)
	}
	if was.LastApplied != now.LastApplied {
		s += fmt.Sprintf(" lastApplied=%d→%d", was.LastApplied, now.LastApplied)
	}
	return s
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
		return fmt.Sprintf("AppendEntries term=%d leaderId=%d prevLogIndex=%d prevLogTerm=%d entries=%s leaderCommit=%d",
			r.Term, r.LeaderID, r.PrevLogIndex, r.PrevLogTerm, entryNames(r.Entries), r.LeaderCommit)
	case m.AppendEntriesResponse != nil:
		r := m.AppendEntriesResponse
		if r.Success {
			return fmt.Sprintf("AppendEntriesResponse term=%d success=true matchIndex=%d", r.Term, r.MatchIndex)
		}
		return fmt.Sprintf("AppendEntriesResponse term=%d success=false conflictTerm=%d conflictIndex=%d",
			r.Term, r.ConflictTerm, r.ConflictIndex)
	}
	return "empty message"
}

// entryNames prints entries as [index@term ...], such as [8@4 9@4].
func entryNames(entries []raft.Entry) string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = fmt.Sprintf("%d@%d", e.Index, e.Term)
	}
	return "[" + strings.Join(names, " ") + "]"
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
