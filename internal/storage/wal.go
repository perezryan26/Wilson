package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// wal is a write-ahead log of records in segment files in one directory.
// Each segment is named after the sequence number of its first record,
// counting from 0 over the whole log. Records are only ever appended, and
// a new segment begins once the last one has reached segmentSize bytes. A
// wal is not safe for concurrent use.
type wal struct {
	fs          FS
	dir         string
	segmentSize int64

	file    File   // the last segment, open for appending; nil if there is none
	name    string // the last segment's file name
	size    int64  // the last segment's size in bytes
	nextSeq uint64 // the sequence number of the next record
}

// segmentName returns the file name of the segment whose first record has
// sequence number seq. The fixed width makes names sort in log order.
func segmentName(seq uint64) string { return fmt.Sprintf("%016x.wal", seq) }

// open reads every segment in w.dir, in order, handing each record to
// apply, and leaves w ready to append after the last record.
func (w *wal) open(apply func(record) error) error {
	seqs, err := w.segments()
	if err != nil {
		return err
	}
	for i, seq := range seqs {
		if err := w.replay(seq, i == len(seqs)-1, apply); err != nil {
			return err
		}
	}
	return nil
}

// segments returns the sequence numbers of the segments in w.dir, in
// order. Files without the .wal suffix are not part of the log.
func (w *wal) segments() ([]uint64, error) {
	names, err := w.fs.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("storage: list %s: %w", w.dir, err)
	}
	var seqs []uint64
	for _, name := range names {
		hex, ok := strings.CutSuffix(name, ".wal")
		if !ok {
			continue
		}
		seq, err := strconv.ParseUint(hex, 16, 64)
		if err != nil || segmentName(seq) != name {
			return nil, fmt.Errorf("storage: %s in %s is not a segment name", name, w.dir)
		}
		seqs = append(seqs, seq)
	}
	return seqs, nil
}

// replay hands each record in segment seq to apply, then opens the
// segment for appending if it is the last.
func (w *wal) replay(seq uint64, last bool, apply func(record) error) error {
	name := segmentName(seq)
	data, err := w.read(name)
	if err != nil {
		return err
	}
	w.nextSeq = seq
	off := 0
	for off < len(data) {
		r, n, err := decodeRecord(data[off:])
		if err == nil {
			err = apply(r)
		}
		if err != nil {
			return fmt.Errorf("storage: segment %s, offset %d: %w", name, off, err)
		}
		off += n
		w.nextSeq++
	}
	if !last {
		return nil
	}
	return w.openTail(name, off)
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

// openTail opens segment name, the last, to append after its size bytes.
func (w *wal) openTail(name string, size int) error {
	f, err := w.fs.OpenFile(filepath.Join(w.dir, name), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("storage: open %s: %w", name, err)
	}
	w.file, w.name, w.size = f, name, int64(size)
	return nil
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
