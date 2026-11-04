package raft

import "testing"

// The log is 1-indexed behind a sentinel at index 0 of term 0, so even an
// empty log has a term for the entry before its first.
func TestLogSentinel(t *testing.T) {
	empty := newLog(nil)
	if empty.lastIndex() != 0 || empty.lastTerm() != 0 {
		t.Errorf("empty log ends at index %d, term %d; want 0 and 0", empty.lastIndex(), empty.lastTerm())
	}
	l := newLog([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}})
	for _, tt := range []struct {
		index Index
		term  Term
		ok    bool
	}{{0, 0, true}, {1, 1, true}, {2, 3, true}, {3, 0, false}} {
		if term, ok := l.term(tt.index); term != tt.term || ok != tt.ok {
			t.Errorf("term(%d) = %d, %t; want %d, %t", tt.index, term, ok, tt.term, tt.ok)
		}
	}
	if l.lastIndex() != 2 || l.lastTerm() != 3 {
		t.Errorf("log ends at index %d, term %d; want 2 and 3", l.lastIndex(), l.lastTerm())
	}
}

// Every index lookup assumes the log runs from 1 without gaps, so a
// restored log that does not is refused at once.
func TestNewLogRejectsGaps(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("newLog accepted a log that skips index 2")
		}
	}()
	newLog([]Entry{{Index: 1, Term: 1}, {Index: 3, Term: 1}})
}
