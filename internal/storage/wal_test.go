package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

// openTestWAL opens the WAL in dir with segments of segmentSize bytes and
// returns it with every record it holds.
func openTestWAL(t *testing.T, fsys FS, dir string, segmentSize int64) (*wal, []record) {
	t.Helper()
	w := &wal{fs: fsys, dir: dir, segmentSize: segmentSize}
	var recs []record
	if err := w.open(func(r record) error { recs = append(recs, r); return nil }); err != nil {
		t.Fatal(err)
	}
	return w, recs
}

// testRecords returns n different records, of every type in turn.
func testRecords(n int) []record {
	recs := make([]record, n)
	for i := range recs {
		switch i % 3 {
		case 0:
			recs[i] = record{typ: recordHardState, hardState: raft.HardState{CurrentTerm: raft.Term(i), VotedFor: raft.NodeID(i % 5)}}
		case 1:
			recs[i] = record{typ: recordEntry, entry: raft.Entry{Index: raft.Index(i), Term: raft.Term(i), Data: fmt.Appendf(nil, "command %d", i)}}
		default:
			recs[i] = record{typ: recordTruncate, index: raft.Index(i)}
		}
	}
	return recs
}

// appendInBatches appends recs to w in batches of one, two, and three
// records in turn.
func appendInBatches(t *testing.T, w *wal, recs []record) {
	t.Helper()
	for i, n := 0, 1; i < len(recs); i, n = i+n, n%3+1 {
		if err := w.append(recs[i:min(i+n, len(recs))]); err != nil {
			t.Fatal(err)
		}
	}
}

func checkRecords(t *testing.T, got, want []record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !sameRecord(got[i], want[i]) {
			t.Errorf("record %d is %+v, want %+v", i, got[i], want[i])
		}
	}
}

// Records appended in batches across many segments read back in order
// after a restart, and appends after a restart continue the log.
func TestWALRoundTripAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	recs := testRecords(40)
	w, got := openTestWAL(t, OSFS{}, dir, 100)
	if len(got) != 0 {
		t.Fatalf("a new WAL holds %d records", len(got))
	}
	appendInBatches(t, w, recs[:30])
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	w, got = openTestWAL(t, OSFS{}, dir, 100)
	checkRecords(t, got, recs[:30])
	appendInBatches(t, w, recs[30:])
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	_, got = openTestWAL(t, OSFS{}, dir, 100)
	checkRecords(t, got, recs)
}

// segmentFile is one segment as a test finds it on disk.
type segmentFile struct {
	name    string
	data    []byte
	offsets []int // where each record starts
}

// readSegments returns every file in dir with its records' offsets.
func readSegments(t *testing.T, dir string) []segmentFile {
	t.Helper()
	names, err := OSFS{}.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	segs := make([]segmentFile, len(names))
	for i, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		segs[i] = segmentFile{name: name, data: data}
		for off := 0; off < len(data); {
			segs[i].offsets = append(segs[i].offsets, off)
			_, n, err := decodeRecord(data[off:])
			if err != nil {
				t.Fatalf("segment %s, offset %d: %v", name, off, err)
			}
			off += n
		}
	}
	return segs
}

// Each segment is named after the sequence number of its first record,
// and every segment but the last had reached the size limit before the
// next one began.
func TestWALSegmentsRotateAtSizeLimit(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTestWAL(t, OSFS{}, dir, 100)
	appendInBatches(t, w, testRecords(40))
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	segs := readSegments(t, dir)
	if len(segs) < 5 {
		t.Fatalf("40 records in 100-byte segments made only %d segments", len(segs))
	}
	seq := 0
	for i, seg := range segs {
		if seg.name != segmentName(uint64(seq)) {
			t.Errorf("segment %d is %s, but its first record is number %d", i, seg.name, seq)
		}
		if i < len(segs)-1 && len(seg.data) < 100 {
			t.Errorf("segment %s holds %d bytes, but the next one began", seg.name, len(seg.data))
		}
		seq += len(seg.offsets)
	}
	if seq != 40 {
		t.Errorf("the segments hold %d records, want 40", seq)
	}
}
