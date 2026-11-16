package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
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

// snap returns a snapshot through index, whose entry there has term,
// with data that names them.
func snap(index raft.Index, term raft.Term) *raft.Snapshot {
	return &raft.Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: fmt.Appendf(nil, "state@%d", index)}
}

// storageSteps returns Readies as a node saves them: hard state alone,
// entries alone, both at once, nothing, overwrites of a conflicting
// suffix, a compaction that keeps the entries after it, and a snapshot
// from a leader that replaces the whole log.
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
		{Snapshot: snap(4, 3), Entries: []raft.Entry{ent(5, 3), ent(6, 3)}},
		{HardState: hardState(4, 2), Entries: []raft.Entry{ent(6, 4), ent(7, 4)}},
		{HardState: hardState(5, 0), Snapshot: snap(9, 5)},
		{Entries: []raft.Entry{ent(10, 5), ent(11, 5)}},
	}
}

func sameState(a, b raft.RestoredState) bool {
	sameSnap := a.Snapshot == nil && b.Snapshot == nil ||
		a.Snapshot != nil && b.Snapshot != nil && sameSnapshot(*a.Snapshot, *b.Snapshot)
	return a.HardState == b.HardState && sameSnap && slices.EqualFunc(a.Entries, b.Entries, func(x, y raft.Entry) bool {
		return x.Index == y.Index && x.Term == y.Term && bytes.Equal(x.Data, y.Data)
	})
}

// lastIndex returns the index of st's last entry, or of its snapshot's
// last included entry if no entry follows it.
func lastIndex(st raft.RestoredState) raft.Index {
	return snapshotIndex(st.Snapshot) + raft.Index(len(st.Entries))
}

// openFileStorage opens file storage with 64-byte segments, discarding
// recovery's log lines.
func openFileStorage(t *testing.T, fsys FS, dir string) *FileStorage {
	t.Helper()
	s, err := OpenFileStorage(fsys, dir, FileOptions{SegmentSize: 64, Logger: slog.New(slog.DiscardHandler)})
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
// Save of hard states, appends, overwrites of a conflicting suffix, and
// snapshots, Load returns the same state from both, and so does a
// FileStorage reopened from its WAL, which spans several segments.
func TestFileStorageMatchesMemory(t *testing.T) {
	dir := t.TempDir()
	var mem Memory
	s := openFileStorage(t, OSFS{}, dir)
	most := 0 // the most segments the WAL spanned
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
		most = max(most, len(readSegments(t, dir)))
	}
	if most < 3 {
		t.Errorf("the WAL spanned at most %d segments, want 3 or more", most)
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
		"snapshot record after an entry":     {entry(1), {typ: recordSnapshot, index: 1, term: 1}},
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
		// The snapshot a misplaced snapshot record names exists, so only
		// the record's place is at fault.
		writeFile(t, filepath.Join(dir, snapshotName(1)), encodeSnapshot(raft.Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1}))
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

// names returns the names of the files in dir.
func names(t *testing.T, dir string) []string {
	t.Helper()
	got, err := OSFS{}.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// saveSteps opens file storage in a new directory, saves steps in it, and
// returns the directory and the state they leave, with the storage
// closed.
func saveSteps(t *testing.T, steps []raft.Ready) (string, raft.RestoredState) {
	t.Helper()
	dir := t.TempDir()
	s := openFileStorage(t, OSFS{}, dir)
	for i, rd := range steps {
		if err := s.Save(rd); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	st := load(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, st
}

// Once a snapshot is saved, it replaces the earlier snapshot and every
// WAL segment before the one that starts the log over from it: the
// directory then holds just that snapshot, and a WAL whose first segment
// begins with a snapshot record.
func TestSnapshotRemovesWhatItReplaces(t *testing.T) {
	for n, rd := range storageSteps() {
		if rd.Snapshot == nil {
			continue
		}
		dir, _ := saveSteps(t, storageSteps()[:n+1])
		snaps := slices.DeleteFunc(names(t, dir), func(name string) bool { return !strings.HasSuffix(name, ".snap") })
		if want := snapshotName(rd.Snapshot.LastIncludedIndex); !slices.Equal(snaps, []string{want}) {
			t.Errorf("after the snapshot through %d the directory holds snapshots %q, want just %s", rd.Snapshot.LastIncludedIndex, snaps, want)
		}
		if segs := readSegments(t, dir); len(segs) == 0 || !startsOver(segs[0].data) || len(names(t, dir)) != len(segs)+1 {
			t.Errorf("after the snapshot through %d the directory holds %q, want the snapshot and a WAL that starts over from it",
				rd.Snapshot.LastIncludedIndex, names(t, dir))
		}
	}
}

// Recovery removes what a crash left behind: segments before the one the
// log starts over in, which it never reads, older snapshots, and files a
// crash caught before their rename.
func TestRecoveryRemovesLeftovers(t *testing.T) {
	dir, want := saveSteps(t, storageSteps()[:9])
	kept := names(t, dir)
	for _, name := range []string{segmentName(0), snapshotName(2), snapshotName(4) + tmpSuffix, segmentName(99) + tmpSuffix} {
		writeFile(t, filepath.Join(dir, name), []byte("leftover"))
	}
	s := openFileStorage(t, OSFS{}, dir)
	if got := load(t, s); !sameState(got, want) {
		t.Errorf("loaded %+v, want %+v", got, want)
	}
	if got := names(t, dir); !slices.Equal(got, kept) {
		t.Errorf("after recovery the directory holds %q, want %q", got, kept)
	}
}

// A WAL that starts over from a snapshot refuses to open if that
// snapshot's file is missing, damaged, or holds another snapshot: the
// entries it replaced are gone, so nothing else can stand in for it.
// Here no entry follows the snapshot, whose index and term only the file
// itself can confirm.
func TestBrokenSnapshotRefusesToStart(t *testing.T) {
	for name, breakIt := range map[string]func(path string){
		"missing": func(path string) { _ = os.Remove(path) },
		"damaged": func(path string) {
			b, _ := os.ReadFile(path)
			b[len(b)-1] ^= 1
			writeFile(t, path, b)
		},
		"another term":  func(path string) { writeFile(t, path, encodeSnapshot(*snap(9, 4))) },
		"another index": func(path string) { writeFile(t, path, encodeSnapshot(*snap(10, 5))) },
	} {
		dir, _ := saveSteps(t, storageSteps()[:11])
		breakIt(filepath.Join(dir, snapshotName(9)))
		if _, err := OpenFileStorage(OSFS{}, dir, FileOptions{}); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s snapshot: error %v, want ErrCorrupt", name, err)
		}
	}
}

// Save refuses a snapshot no newer than the stored one, entries inside
// the snapshot, and entries that do not start right after a snapshot
// saved with them, changing nothing, in memory or on disk.
func TestFileStorageRefusesStaleSnapshots(t *testing.T) {
	dir, want := saveSteps(t, storageSteps()[:9])
	s := openFileStorage(t, OSFS{}, dir)
	for _, rd := range []raft.Ready{
		{Snapshot: snap(4, 3)},
		{Snapshot: snap(3, 3)},
		{Entries: []raft.Entry{ent(4, 3)}},
		{Snapshot: snap(6, 3), Entries: []raft.Entry{ent(8, 3)}},
	} {
		rd.HardState = hardState(9, 9)
		if err := s.Save(rd); err == nil {
			t.Errorf("saving %+v after the snapshot through 4 succeeded", rd)
		}
		if got := load(t, s); !sameState(got, want) {
			t.Errorf("a refused Save of %+v changed the state to %+v", rd, got)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := load(t, openFileStorage(t, OSFS{}, dir)); !sameState(got, want) {
		t.Errorf("refused Saves reached the disk: reopened to %+v", got)
	}
}

// replica is a cluster of one server whose state machine is the list of
// commands it applied, in order, and whose snapshot is that list in JSON.
// Everything else it keeps in file storage.
type replica struct {
	storage  *FileStorage
	node     *raft.Node
	state    []string   // the commands applied, in order; no-ops are skipped
	applied  raft.Index // the last index applied
	snapshot raft.Index // the last index the latest snapshot includes
	restored []string   // the commands the snapshot it restarted from held
}

// startReplica restarts a replica from the file storage on fsys: its
// state machine from the snapshot, then its node, which it runs until it
// leads.
func startReplica(t *testing.T, fsys FS, seed uint64) *replica {
	t.Helper()
	r := &replica{storage: openFileStorage(t, fsys, "wal")}
	restored := load(t, r.storage)
	if s := restored.Snapshot; s != nil {
		if err := json.Unmarshal(s.Data, &r.state); err != nil {
			t.Fatal(err)
		}
		r.applied, r.snapshot, r.restored = s.LastIncludedIndex, s.LastIncludedIndex, slices.Clone(r.state)
	}
	cfg, _ := raft.DefaultConfig()
	cfg.ID, cfg.Peers, cfg.Rand = 1, []raft.NodeID{1}, rand.New(rand.NewPCG(seed, 1))
	r.node = raft.NewNode(cfg, restored)
	for r.node.Status().Role != raft.Leader {
		r.node.Tick()
		r.handleReady(t)
	}
	return r
}

// handleReady saves the node's Ready and applies its committed entries.
func (r *replica) handleReady(t *testing.T) {
	t.Helper()
	rd := r.node.Ready()
	if err := r.storage.Save(rd); err != nil {
		t.Fatal(err)
	}
	for _, e := range rd.CommittedEntries {
		if e.Index != r.applied+1 {
			t.Fatalf("applied entry %d after entry %d", e.Index, r.applied)
		}
		r.applied = e.Index
		if e.Type == raft.EntryNormal {
			r.state = append(r.state, string(e.Data))
		}
	}
}

// compact snapshots the state machine, if it applied anything since the
// last snapshot.
func (r *replica) compact(t *testing.T) {
	t.Helper()
	index := r.applied
	if index == r.snapshot {
		return
	}
	data, err := json.Marshal(r.state)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.node.Compact(index, data); err != nil {
		t.Fatal(err)
	}
	r.snapshot = index
	r.handleReady(t)
}

// §7: a server that restarts from its snapshot and the log after it ends
// up with the state that replaying the whole log gives. A replica
// proposes commands, compacts after some of them, and restarts at random
// from a disk that crashed, rebuilding its state machine from its
// snapshot and applying the entries after it, among them the no-op each
// of its terms begins with. Every restart's snapshot must hold exactly
// the commands it includes, and at the end the state machine must hold
// every command, in order.
func TestRestoreEqualsFullReplay(t *testing.T) {
	restarts := 0
	for seed := range uint64(20) {
		pick := rand.New(rand.NewPCG(seed, 2))
		fsys := NewFaultFS(rand.New(rand.NewPCG(seed, 3)))
		r := startReplica(t, fsys, seed)
		var proposed []string
		var at []raft.Index // the index of each proposed command
		for i := range 150 {
			cmd := fmt.Sprintf("c%d", i)
			index, _, err := r.node.Propose([]byte(cmd))
			if err != nil {
				t.Fatal(err)
			}
			proposed, at = append(proposed, cmd), append(at, index)
			r.handleReady(t)
			if pick.IntN(4) == 0 {
				r.compact(t)
			}
			if pick.IntN(15) == 0 {
				fsys.Crash()
				r = startReplica(t, fsys, seed)
				restarts++
				included := proposed[:len(at)-len(slices.DeleteFunc(slices.Clone(at), func(i raft.Index) bool { return i <= r.snapshot }))]
				if !slices.Equal(r.restored, included) {
					t.Fatalf("seed %d: restarted from a snapshot through %d holding %v", seed, r.snapshot, r.restored)
				}
				if !slices.Equal(r.state, proposed[:len(r.state)]) {
					t.Fatalf("seed %d: restarted and applied %v", seed, r.state)
				}
			}
		}
		if _, _, err := r.node.Propose([]byte("last")); err != nil {
			t.Fatal(err)
		}
		r.handleReady(t)
		if want := append(proposed, "last"); !slices.Equal(r.state, want) {
			t.Errorf("seed %d: the state machine holds %v, want %v", seed, r.state, want)
		}
	}
	t.Logf("%d restarts from a snapshot and the log after it", restarts)
}
