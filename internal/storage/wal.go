package storage

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ErrCorrupt is the error that opening file storage wraps, naming the
// segment and offset, when the WAL is damaged in a way no crash explains:
// a record that fails its checksum or cannot follow the records before
// it, a record cut short anywhere but at the end of the log, or a missing
// segment. Starting anyway could make the server vote twice in a term or
// forget entries it acknowledged, so it refuses.
var ErrCorrupt = errors.New("storage: corrupt WAL")

// wal is a write-ahead log of records in segment files in one directory.
// Each segment is named after the sequence number of its first record,
// counting from 0 over the whole log. Records are only ever appended, and
// a new segment begins once the last one has reached segmentSize bytes.
// The log starts over in a segment that begins with a snapshot record,
// which replaces every segment before it. A wal is not safe for
// concurrent use.
type wal struct {
	fs          FS
	dir         string
	segmentSize int64
	logger      *slog.Logger // receives the report of a truncated torn record

	file     File   // the last segment, open for appending; nil if there is none
	name     string // the last segment's file name
	size     int64  // the last segment's size in bytes
	firstSeq uint64 // the sequence number of the log's first record
	nextSeq  uint64 // the sequence number of the next record
}

// segmentName returns the file name of the segment whose first record has
// sequence number seq. The fixed width makes names sort in log order.
func segmentName(seq uint64) string { return fmt.Sprintf("%016x.wal", seq) }

// tmpSuffix ends the name of a file being written, which becomes a
// segment or a snapshot only once it is renamed whole.
const tmpSuffix = ".tmp"

// open reads the log's segments in order, handing each record to apply,
// and leaves w ready to append after the last record. The log begins at
// the newest segment that begins with a snapshot record, or else at
// segment 0; any segment before it is obsolete, left for removal. A
// record that runs past the end of the last segment was torn by a crash
// before its append could return: open truncates and logs it. Every other
// flaw is an error wrapping ErrCorrupt, an error from apply among them.
func (w *wal) open(apply func(record) error) error {
	seqs, err := w.segments()
	if err != nil {
		return err
	}
	data, err := w.readFromStart(seqs)
	if err != nil {
		return err
	}
	seqs = seqs[len(seqs)-len(data):]
	if len(seqs) > 0 && startsOver(data[0]) {
		w.firstSeq, w.nextSeq = seqs[0], seqs[0]
	}
	for i, seq := range seqs {
		if err := w.replay(seq, data[i], i == len(seqs)-1, apply); err != nil {
			return err
		}
	}
	return nil
}

// readFromStart returns the contents of the segments the log consists of:
// those from the newest that starts the log over, or all of them if none
// does.
func (w *wal) readFromStart(seqs []uint64) ([][]byte, error) {
	var data [][]byte
	for i := len(seqs) - 1; i >= 0; i-- {
		b, err := w.read(segmentName(seqs[i]))
		if err != nil {
			return nil, err
		}
		data = append([][]byte{b}, data...)
		if startsOver(b) {
			break
		}
	}
	return data, nil
}

// startsOver reports whether a segment holding b begins with a snapshot
// record, which starts the log over.
func startsOver(b []byte) bool {
	r, _, err := decodeRecord(b)
	return err == nil && r.typ == recordSnapshot
}

// segments returns the sequence numbers of the segments in w.dir, in
// order. Files without the .wal suffix are not part of the log, but one
// with the suffix and some other name is refused: skipping it could skip
// records.
func (w *wal) segments() ([]uint64, error) {
	names, err := w.fs.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("storage: list %s: %w", w.dir, err)
	}
	var seqs []uint64
	for _, name := range names {
		if !strings.HasSuffix(name, ".wal") {
			continue
		}
		seq, ok := parseSegmentName(name)
		if !ok {
			return nil, fmt.Errorf("%w: %s in %s is not a segment name", ErrCorrupt, name, w.dir)
		}
		seqs = append(seqs, seq)
	}
	return seqs, nil
}

// parseSegmentName returns the sequence number segment name is named
// after, and false if name is not exactly a segment name.
func parseSegmentName(name string) (uint64, bool) {
	hex, ok := strings.CutSuffix(name, ".wal")
	seq, err := strconv.ParseUint(hex, 16, 64)
	return seq, ok && err == nil && segmentName(seq) == name
}

// replay hands each record in data, the contents of segment seq, to
// apply, then opens the segment for appending if it is the last. The
// segment must begin with the record after the previous segment's last,
// so a missing or cut-short segment shows as a gap.
func (w *wal) replay(seq uint64, data []byte, last bool, apply func(record) error) error {
	name := segmentName(seq)
	if seq != w.nextSeq {
		return fmt.Errorf("%w: segment %s should begin with record %d", ErrCorrupt, name, w.nextSeq)
	}
	off := 0
	for off < len(data) {
		r, n, err := decodeRecord(data[off:])
		if last && errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err == nil {
			err = w.replayRecord(r, apply)
		}
		if err != nil {
			return fmt.Errorf("%w: segment %s, offset %d: %w", ErrCorrupt, name, off, err)
		}
		off += n
		w.nextSeq++
	}
	if !last {
		return nil
	}
	return w.openTail(name, off, len(data))
}

// replayRecord hands r to apply. A snapshot record is valid only as the
// first record of the log, the only place startOver writes one.
func (w *wal) replayRecord(r record, apply func(record) error) error {
	if r.typ == recordSnapshot && w.nextSeq != w.firstSeq {
		return errors.New("a snapshot record that does not begin the log")
	}
	return apply(r)
}

// read returns the contents of segment name.
func (w *wal) read(name string) ([]byte, error) {
	f, err := w.fs.OpenFile(filepath.Join(w.dir, name), os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", name, err)
	}
	data, err := io.ReadAll(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("storage: read %s: %w", name, err)
	}
	return data, nil
}

// openTail opens segment name, the last, to append after its first valid
// bytes. The rest of its size bytes, if any, are a record torn by a crash:
// openTail truncates them and logs it.
func (w *wal) openTail(name string, valid, size int) error {
	f, err := w.fs.OpenFile(filepath.Join(w.dir, name), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("storage: open %s: %w", name, err)
	}
	if valid < size {
		// The torn bytes must be gone before new records follow them, or
		// the next recovery would find them mid-log and refuse to start.
		if err := truncate(f, valid); err != nil {
			_ = f.Close()
			return fmt.Errorf("storage: truncate %s: %w", name, err)
		}
		w.logger.Warn("storage: truncated a record torn by a crash at the end of the WAL",
			"segment", name, "offset", valid, "bytes", size-valid)
	}
	w.file, w.name, w.size = f, name, int64(valid)
	return nil
}

// truncate cuts f to size bytes and makes that durable.
func truncate(f File, size int) error {
	if err := f.Truncate(int64(size)); err != nil {
		return err
	}
	return f.Sync()
}

// append writes recs at the end of the log in one write and one fsync,
// starting a new segment first if the last one is full. The records are
// durable once it returns.
func (w *wal) append(recs []record) error {
	if w.file == nil || w.size >= w.segmentSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	var buf []byte
	for _, r := range recs {
		buf = appendRecord(buf, r)
	}
	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("storage: write %s: %w", w.name, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("storage: fsync %s: %w", w.name, err)
	}
	w.size += int64(len(buf))
	w.nextSeq += uint64(len(recs))
	return nil
}

// startOver makes recs, which begin with a snapshot record, the whole
// log: it writes them to a new segment named after the next record's
// sequence number, which appears whole or not at all, and appends to it
// from then on. The segments before it are obsolete, left for removal.
func (w *wal) startOver(recs []record) error {
	if err := w.close(); err != nil {
		return err
	}
	var buf []byte
	for _, r := range recs {
		buf = appendRecord(buf, r)
	}
	name := segmentName(w.nextSeq)
	if err := writeFileAtomic(w.fs, w.dir, name, buf); err != nil {
		return err
	}
	f, err := w.fs.OpenFile(filepath.Join(w.dir, name), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("storage: open %s: %w", name, err)
	}
	w.file, w.name, w.size = f, name, int64(len(buf))
	w.firstSeq = w.nextSeq
	w.nextSeq += uint64(len(recs))
	return nil
}

// obsolete reports whether name is a segment before the log's first.
func (w *wal) obsolete(name string) bool {
	seq, ok := parseSegmentName(name)
	return ok && seq < w.firstSeq
}

// writeFileAtomic writes data to a new file name in dir that appears
// whole or not at all: it writes a temporary file, fsyncs it, renames it
// to name, and fsyncs dir, so that the rename survives a crash.
func writeFileAtomic(fsys FS, dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+tmpSuffix)
	f, err := fsys.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("storage: create %s: %w", tmp, err)
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = fsys.Rename(tmp, filepath.Join(dir, name))
	}
	if err == nil {
		err = fsys.SyncDir(dir)
	}
	if err != nil {
		return fmt.Errorf("storage: write %s: %w", name, err)
	}
	return nil
}

// rotate closes the last segment and starts a new one, named after the
// next record's sequence number.
func (w *wal) rotate() error {
	if err := w.close(); err != nil {
		return err
	}
	name := segmentName(w.nextSeq)
	f, err := w.fs.OpenFile(filepath.Join(w.dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("storage: create %s: %w", name, err)
	}
	w.file, w.name, w.size = f, name, 0
	// A new file's directory entry survives a crash only once the directory
	// is synced; until then the segment, and every record written to it,
	// could vanish.
	if err := w.fs.SyncDir(w.dir); err != nil {
		return fmt.Errorf("storage: fsync %s: %w", w.dir, err)
	}
	return nil
}

// close closes the last segment's file, if it is open.
func (w *wal) close() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	if err != nil {
		return fmt.Errorf("storage: close %s: %w", w.name, err)
	}
	return nil
}
