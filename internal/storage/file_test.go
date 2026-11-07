package storage

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

// ent returns an entry whose command names its index and term.
func ent(index raft.Index, term raft.Term) raft.Entry {
	return raft.Entry{Index: index, Term: term, Data: fmt.Appendf(nil, "%d@%d", index, term)}
}

func hardState(term raft.Term, votedFor raft.NodeID) *raft.HardState {
	return &raft.HardState{CurrentTerm: term, VotedFor: votedFor}
}

// storageSteps returns Readies as a node saves them: hard state alone,
// entries alone, both at once, nothing, and two overwrites of a
// conflicting suffix.
func storageSteps() []raft.Ready {
	return []raft.Ready{
		{HardState: hardState(1, 1)},
		{Entries: []raft.Entry{ent(1, 1), ent(2, 1), ent(3, 1)}},
		{},
		{HardState: hardState(2, 0), Entries: []raft.Entry{ent(3, 2), ent(4, 2)}},
		{Entries: []raft.Entry{ent(5, 2)}},
		{HardState: hardState(2, 3)},
		{HardState: hardState(3, 0), Entries: []raft.Entry{ent(2, 3)}},
		{Entries: []raft.Entry{ent(3, 3), ent(4, 3), ent(5, 3), ent(6, 3)}},
	}
}

func sameState(a, b raft.RestoredState) bool {
	return a.HardState == b.HardState && slices.EqualFunc(a.Entries, b.Entries, func(x, y raft.Entry) bool {
		return x.Index == y.Index && x.Term == y.Term && bytes.Equal(x.Data, y.Data)
	})
}

func openFileStorage(t *testing.T, fsys FS, dir string) *FileStorage {
	t.Helper()
	s, err := OpenFileStorage(fsys, dir, FileOptions{SegmentSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func load(t *testing.T, s Storage) raft.RestoredState {
	t.Helper()
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// FileStorage keeps exactly what Memory, the reference, keeps: after each
// Save of hard states, appends, and overwrites of a conflicting suffix,
// Load returns the same state from both, and so does a FileStorage
// reopened from its WAL, which by then spans several segments.
func TestFileStorageMatchesMemory(t *testing.T) {
	dir := t.TempDir()
	var mem Memory
	s := openFileStorage(t, OSFS{}, dir)
	for i, rd := range storageSteps() {
		if err := mem.Save(rd); err != nil {
			t.Fatalf("step %d: Memory.Save: %v", i, err)
		}
		if err := s.Save(rd); err != nil {
			t.Fatalf("step %d: FileStorage.Save: %v", i, err)
		}
		want := load(t, &mem)
		if got := load(t, s); !sameState(got, want) {
			t.Errorf("step %d: loaded %+v, want %+v", i, got, want)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s = openFileStorage(t, OSFS{}, dir)
		if got := load(t, s); !sameState(got, want) {
			t.Errorf("step %d: reopened, loaded %+v, want %+v", i, got, want)
		}
	}
	if segs := readSegments(t, dir); len(segs) < 3 {
		t.Errorf("the WAL spans %d segments, want at least 3", len(segs))
	}
}

// Save refuses entries that would leave a gap after the stored log, or
// that do not run in index order, and changes nothing, in memory or on
// disk.
func TestFileStorageRefusesGaps(t *testing.T) {
	dir := t.TempDir()
	s := openFileStorage(t, OSFS{}, dir)
	if err := s.Save(raft.Ready{Entries: []raft.Entry{ent(1, 1), ent(2, 1)}}); err != nil {
		t.Fatal(err)
	}
	want := load(t, s)
	for _, entries := range [][]raft.Entry{
		{ent(4, 1)},
		{ent(0, 1)},
		{ent(3, 1), ent(5, 1)},
		{ent(2, 2), ent(2, 2)},
	} {
		if err := s.Save(raft.Ready{HardState: hardState(9, 9), Entries: entries}); err == nil {
			t.Errorf("saving entries %v after entry 2 succeeded", entries)
		}
		if got := load(t, s); !sameState(got, want) {
			t.Errorf("a refused Save of %v changed the state to %+v", entries, got)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := load(t, openFileStorage(t, OSFS{}, dir)); !sameState(got, want) {
		t.Errorf("refused Saves reached the disk: reopened to %+v", got)
	}
}

// A WAL of well-formed records that cannot follow one another, which Save
// never writes, refuses to open with an error wrapping ErrCorrupt that
// names the segment and the offset of the record at fault.
func TestFileStorageRefusesImpossibleLog(t *testing.T) {
	entry := func(i raft.Index) record { return record{typ: recordEntry, entry: ent(i, 1)} }
	truncation := func(i raft.Index) record { return record{typ: recordTruncate, index: i} }
	cases := map[string][]record{
		"entry 3 after entry 1":              {entry(1), entry(3)},
		"first entry 2":                      {entry(2)},
		"entry 2 again without a truncation": {entry(1), entry(2), entry(2)},
		"truncation past the end":            {entry(1), truncation(3)},
		"truncation at index 0":              {entry(1), truncation(0)},
	}
	for name, recs := range cases {
		dir := t.TempDir()
		w := &wal{fs: OSFS{}, dir: dir, segmentSize: noRotation}
		if err := w.append(recs); err != nil {
			t.Fatal(err)
		}
		if err := w.close(); err != nil {
			t.Fatal(err)
		}
		var before []byte // the records ahead of the last, which is at fault
		for _, r := range recs[:len(recs)-1] {
			before = appendRecord(before, r)
		}
		at := len(before)
		_, err := OpenFileStorage(OSFS{}, dir, FileOptions{})
		if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), fmt.Sprintf("offset %d:", at)) {
			t.Errorf("%s: error %v, want ErrCorrupt at offset %d", name, err, at)
		}
	}
}
