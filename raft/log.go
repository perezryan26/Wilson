package raft

import (
	"fmt"
	"slices"
)

// raftLog is Figure 2's log[]: entries indexed from 1, after a sentinel
// entry that stands for the last entry a snapshot replaced (§7), or, with
// no snapshot, for index 0 with term 0. The entry before the first one
// therefore always exists and has a term. It is the only code that turns
// a log index into a slice position.
type raftLog struct {
	entries []Entry // entries[0] is the sentinel
}

// newLog returns a log holding restored, which must run without gaps from
// the entry after snap's last included entry, or from index 1 if snap is
// nil.
func newLog(snap *Snapshot, restored []Entry) raftLog {
	var sentinel Entry
	if snap != nil {
		sentinel = Entry{Index: snap.LastIncludedIndex, Term: snap.LastIncludedTerm}
	}
	for i, e := range restored {
		if want := sentinel.Index + Index(i+1); e.Index != want {
			panic(fmt.Sprintf("raft: a restored log must run from index %d without gaps, but entry %d has index %d",
				sentinel.Index+1, want, e.Index))
		}
	}
	return raftLog{entries: append([]Entry{sentinel}, restored...)}
}

// snapshotIndex returns the index of the last entry a snapshot replaced,
// lastIncludedIndex, or 0 if there is no snapshot.
func (l *raftLog) snapshotIndex() Index { return l.entries[0].Index }

// lastIndex returns the index of the last entry, or 0 if the log is empty.
func (l *raftLog) lastIndex() Index { return l.entries[len(l.entries)-1].Index }

// lastTerm returns the term of the last entry, or 0 if the log is empty.
func (l *raftLog) lastTerm() Term { return l.entries[len(l.entries)-1].Term }

// append adds e, which follows the last entry, to the log.
func (l *raftLog) append(e Entry) { l.entries = append(l.entries, e) }

// from returns a copy of the entries from index i to the end. A copy, so
// that a later truncation of the log cannot rewrite entries already handed
// out in a message or a Ready.
func (l *raftLog) from(i Index) []Entry { return slices.Clone(l.entries[i-l.snapshotIndex():]) }

// between returns a copy of the entries from index lo to index hi, both
// included.
func (l *raftLog) between(lo, hi Index) []Entry {
	off := l.snapshotIndex()
	return slices.Clone(l.entries[lo-off : hi-off+1])
}

// firstIndexOf returns the first index of the run of entries of term that
// includes index i, looking no further back than the first entry after
// the snapshot. Terms never decrease along a log, so the run is the
// term's only one.
func (l *raftLog) firstIndexOf(term Term, i Index) Index {
	for i > l.snapshotIndex()+1 {
		if t, _ := l.term(i - 1); t != term {
			break
		}
		i--
	}
	return i
}

// lastIndexOf returns the index of the last entry of term, the sentinel
// included, or 0 if the log holds none.
func (l *raftLog) lastIndexOf(term Term) Index {
	for i := len(l.entries) - 1; i >= 0; i-- {
		switch e := l.entries[i]; {
		case e.Term == term:
			return e.Index
		case e.Term < term:
			return 0
		}
	}
	return 0
}

// merge carries out AppendEntries receiver steps 3 and 4 for entries, which
// follow an entry the log already matches. It keeps every entry the log
// already holds, truncates the log at the first entry that conflicts, and
// appends the rest, so a stale or repeated request truncates nothing. It
// returns the index of the first entry it wrote, or lastIndex+1 if none.
func (l *raftLog) merge(entries []Entry) Index {
	for i, e := range entries {
		// An entry a snapshot replaced is committed, so it matches the
		// leader's (§7).
		if e.Index <= l.snapshotIndex() {
			continue
		}
		if term, ok := l.term(e.Index); ok && term == e.Term {
			continue
		}
		l.entries = append(l.entries[:e.Index-l.snapshotIndex()], entries[i:]...)
		return e.Index
	}
	return l.lastIndex() + 1
}

// term returns the term of the entry at index i, and false if the log has
// no entry there, either past its end or replaced by the snapshot. The
// sentinel has a term: lastIncludedTerm, or 0 at index 0.
func (l *raftLog) term(i Index) (Term, bool) {
	if i < l.snapshotIndex() || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.snapshotIndex()].Term, true
}

// restore makes the log begin after snapshot s (Figure 13): if the log
// holds s's last included entry, the entries that follow it stay (step
// 6); otherwise the whole log goes (step 7).
func (l *raftLog) restore(s *Snapshot) {
	var rest []Entry
	if term, ok := l.term(s.LastIncludedIndex); ok && term == s.LastIncludedTerm {
		rest = l.entries[s.LastIncludedIndex-l.snapshotIndex()+1:]
	}
	l.entries = append([]Entry{{Index: s.LastIncludedIndex, Term: s.LastIncludedTerm}}, rest...)
}
