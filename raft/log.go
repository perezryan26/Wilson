package raft

import (
	"fmt"
	"slices"
)

// raftLog is Figure 2's log[]: entries indexed from 1, after a sentinel
// entry at index 0 with term 0, so the entry before the first one always
// exists and has a term. It is the only code that turns a log index into a
// slice position.
type raftLog struct {
	entries []Entry // entries[0] is the sentinel
}

// newLog returns a log holding restored, which must run from index 1
// without gaps.
func newLog(restored []Entry) raftLog {
	for i, e := range restored {
		if e.Index != Index(i+1) {
			panic(fmt.Sprintf("raft: a restored log must run from index 1 without gaps, but entry %d has index %d", i+1, e.Index))
		}
	}
	return raftLog{entries: append([]Entry{{}}, restored...)}
}

// lastIndex returns the index of the last entry, or 0 if the log is empty.
func (l *raftLog) lastIndex() Index { return l.entries[len(l.entries)-1].Index }

// lastTerm returns the term of the last entry, or 0 if the log is empty.
func (l *raftLog) lastTerm() Term { return l.entries[len(l.entries)-1].Term }

// append adds e, which follows the last entry, to the log.
func (l *raftLog) append(e Entry) { l.entries = append(l.entries, e) }

// from returns a copy of the entries from index i to the end. A copy, so
// that a later truncation of the log cannot rewrite entries already handed
// out in a message or a Ready.
func (l *raftLog) from(i Index) []Entry { return slices.Clone(l.entries[i-l.entries[0].Index:]) }

// term returns the term of the entry at index i, and false if the log has
// no entry there. Index 0 is the sentinel, of term 0.
func (l *raftLog) term(i Index) (Term, bool) {
	if i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.entries[0].Index].Term, true
}
