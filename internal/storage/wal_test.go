package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

// openTestWAL opens the WAL in dir with segments of segmentSize bytes and
// returns it with every record it holds.
func openTestWAL(t *testing.T, fsys FS, dir string, segmentSize int64) (*wal, []record) {
	t.Helper()
	w, recs, err := tryOpenWAL(fsys, dir, segmentSize, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return w, recs
}

// tryOpenWAL opens the WAL in dir and returns it with every record it
// holds, or the error that refused it.
func tryOpenWAL(fsys FS, dir string, segmentSize int64, logger *slog.Logger) (*wal, []record, error) {
	w := &wal{fs: fsys, dir: dir, segmentSize: segmentSize, logger: logger}
	var recs []record
	err := w.open(func(r record) error { recs = append(recs, r); return nil })
	return w, recs, err
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

// writeTestWAL writes 12 records to a new WAL in 50-byte segments, which
// makes four, and returns its directory, the records, and the segments.
func writeTestWAL(t *testing.T) (string, []record, []segmentFile) {
	t.Helper()
	dir := t.TempDir()
	recs := testRecords(12)
	w, _ := openTestWAL(t, OSFS{}, dir, 50)
	appendInBatches(t, w, recs)
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	return dir, recs, readSegments(t, dir)
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// noRotation is a segment size no test reaches, so that appends stay in
// the last segment.
const noRotation = 1 << 30

// A crash mid-write leaves some prefix of the final record. For every
// prefix length, recovery returns the records before it, truncates the
// segment where the torn record began, logs that with the segment and
// offset, and appends after it. With no byte of the record written there
// is nothing to truncate or log.
func TestTornFinalRecordIsTruncated(t *testing.T) {
	dir, recs, segs := writeTestWAL(t)
	last := segs[len(segs)-1]
	start := last.offsets[len(last.offsets)-1]
	path := filepath.Join(dir, last.name)
	for n := start; n < len(last.data); n++ {
		writeFile(t, path, last.data[:n])
		var logged bytes.Buffer
		w, got, err := tryOpenWAL(OSFS{}, dir, noRotation, slog.New(slog.NewTextHandler(&logged, nil)))
		if err != nil {
			t.Fatalf("final record cut to %d bytes: %v", n-start, err)
		}
		checkRecords(t, got, recs[:len(recs)-1])
		if data, err := os.ReadFile(path); err != nil || len(data) != start {
			t.Errorf("final record cut to %d bytes: the segment holds %d bytes after recovery (%v), want %d", n-start, len(data), err, start)
		}
		wantLog := fmt.Sprintf("segment=%s offset=%d bytes=%d", last.name, start, n-start)
		if torn := n > start; torn != strings.Contains(logged.String(), wantLog) {
			t.Errorf("final record cut to %d bytes: logged %q", n-start, logged.String())
		}
		if err := w.append(recs[len(recs)-1:]); err != nil {
			t.Fatal(err)
		}
		if err := w.close(); err != nil {
			t.Fatal(err)
		}
		_, got = openTestWAL(t, OSFS{}, dir, noRotation)
		checkRecords(t, got, recs)
	}
}

// Damage that no crash explains refuses to start with an error wrapping
// ErrCorrupt that names the segment and the damaged record's offset: any
// byte of any record's checksum, type, or payload changed, the final
// record's included, and any byte of a length changed in a segment before
// the last. A length changed in the last segment so that the record runs
// past the end of the file looks exactly like a torn write, so recovery
// keeps the records before it and truncates the rest (ADR-0003).
func TestDamagedRecordRefusesToStart(t *testing.T) {
	dir, recs, segs := writeTestWAL(t)
	seq := 0
	for i, seg := range segs {
		path := filepath.Join(dir, seg.name)
		for j, off := range seg.offsets {
			end := len(seg.data)
			if j+1 < len(seg.offsets) {
				end = seg.offsets[j+1]
			}
			for b := off; b < end; b++ {
				damaged := bytes.Clone(seg.data)
				damaged[b] ^= 0xff
				writeFile(t, path, damaged)
				length := int(binary.LittleEndian.Uint32(damaged[off:]))
				tornLooking := i == len(segs)-1 && off+headerSize+length > len(damaged)
				checkDamage(t, dir, seg.name, off, b-off, tornLooking, recs[:seq+j])
			}
		}
		writeFile(t, path, seg.data)
		seq += len(seg.offsets)
	}
}

// checkDamage opens the WAL in dir, whose record at off in segment name
// has byte at changed. If the damage looks like a torn write, the WAL must
// open with the records before; otherwise it must be refused.
func checkDamage(t *testing.T, dir, name string, off, at int, tornLooking bool, before []record) {
	t.Helper()
	w, got, err := tryOpenWAL(OSFS{}, dir, 50, slog.New(slog.DiscardHandler))
	if tornLooking {
		if err != nil {
			t.Fatalf("%s, offset %d, byte %d changed: %v", name, off, at, err)
		}
		checkRecords(t, got, before)
		_ = w.close()
		return
	}
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), fmt.Sprintf("segment %s, offset %d:", name, off)) {
		t.Fatalf("%s, offset %d, byte %d changed: error %v, want ErrCorrupt naming the segment and offset", name, off, at, err)
	}
}

// Segments missing from the log refuse to start with an error wrapping
// ErrCorrupt, because each segment must begin with the record after the
// previous one's last: the first or a middle segment deleted, or a middle
// one cut short, at a record boundary or mid-record. So does a .wal file
// whose name no segment has. A file without the .wal suffix is not part
// of the log and changes nothing.
func TestMissingSegmentRefusesToStart(t *testing.T) {
	_, recs, segs := writeTestWAL(t)
	if len(segs) < 3 {
		t.Fatalf("the test WAL has %d segments, want at least 3", len(segs))
	}
	mid := segs[1]
	cut := func(n int) []segmentFile {
		return append(append(segs[:1:1], segmentFile{name: mid.name, data: mid.data[:n]}), segs[2:]...)
	}
	// It parses as the number of the next segment, so only the exact name
	// tells it from one.
	upperHex := strings.ToUpper(strings.TrimSuffix(segmentName(uint64(len(recs))), ".wal")) + ".wal"
	cases := map[string][]segmentFile{
		"first deleted":                     segs[1:],
		"middle deleted":                    append(segs[:1:1], segs[2:]...),
		"middle cut at its last record":     cut(mid.offsets[len(mid.offsets)-1]),
		"middle cut mid-record":             cut(len(mid.data) - 1),
		"stray .wal file":                   append(segs[:len(segs):len(segs)], segmentFile{name: "stray.wal"}),
		"next segment's name in upper case": append(segs[:len(segs):len(segs)], segmentFile{name: upperHex}),
	}
	for name, files := range cases {
		if _, _, err := tryOpenWAL(OSFS{}, writeSegments(t, files), 50, slog.New(slog.DiscardHandler)); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: error %v, want ErrCorrupt", name, err)
		}
	}
	notes := append(segs[:len(segs):len(segs)], segmentFile{name: "notes.txt", data: []byte("hello")})
	_, got, err := tryOpenWAL(OSFS{}, writeSegments(t, notes), 50, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("with an unrelated file beside the WAL: %v", err)
	}
	checkRecords(t, got, recs)
}

// writeSegments writes files to a new directory and returns it.
func writeSegments(t *testing.T, files []segmentFile) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range files {
		writeFile(t, filepath.Join(dir, f.name), f.data)
	}
	return dir
}
