package storage

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/perezryan26/wilson/raft"
)

// FileStorage keeps a node's hard state, log, and latest snapshot on disk:
// the hard state and log in a write-ahead log of records
// (docs/adr/0003-wal-format.md), and the snapshot in a file of its own
// (docs/adr/0005-snapshot-format.md). It keeps a copy of all three in
// memory for Load. It is not safe for concurrent use.
type FileStorage struct {
	wal   wal
	state raft.RestoredState // what the disk holds
	err   error              // the failed write, if one failed; every later Save returns it
}

// FileOptions tune OpenFileStorage. The zero value is ready to use.
type FileOptions struct {
	// SegmentSize is the size a WAL segment reaches before the next one
	// begins. Zero means 64 MiB.
	SegmentSize int64
	// Logger receives recovery's report of a record torn by a crash. Nil
	// means slog.Default().
	Logger *slog.Logger
}

// OpenFileStorage opens the WAL and snapshot in dir, an existing
// directory, and replays them. A record torn by a crash at the end of the
// WAL is truncated and logged, and files that a crash or a newer snapshot
// left behind are removed. Any other damage is refused with an error
// wrapping ErrCorrupt that names the segment and offset.
func OpenFileStorage(fsys FS, dir string, opts FileOptions) (*FileStorage, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 64 << 20
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	s := &FileStorage{wal: wal{fs: fsys, dir: dir, segmentSize: opts.SegmentSize, logger: opts.Logger}}
	if err := s.wal.open(s.replay); err != nil {
		return nil, err
	}
	if err := s.removeObsolete(); err != nil {
		_ = s.wal.close()
		return nil, err
	}
	return s, nil
}

// Save writes rd's hard state, snapshot, and entries to disk and returns
// once they are fsynced, so that no reply leaves before the state it
// reflects is durable (Figure 2). A snapshot, with the entries that
// follow it, replaces the whole stored log; otherwise entries replace
// every stored entry from rd.Entries[0].Index on. Save refuses, changing
// nothing, what no correct node asks for: entries that would leave a gap,
// or a snapshot no newer than the stored one. Once a write or fsync has
// failed, what the disk holds is unknown, so that Save and every later
// one fail: the runtime must stop.
func (s *FileStorage) Save(rd raft.Ready) error {
	if s.err != nil {
		return s.err
	}
	if rd.Snapshot != nil {
		return s.saveSnapshot(rd)
	}
	recs, err := s.records(rd)
	if err != nil || len(recs) == 0 {
		return err
	}
	if err := s.wal.append(recs); err != nil {
		s.err = err
		return err
	}
	if rd.HardState != nil {
		s.state.HardState = *rd.HardState
	}
	if len(rd.Entries) > 0 {
		s.state.Entries = append(s.state.Entries[:rd.Entries[0].Index-s.snapshotIndex()-1], rd.Entries...)
	}
	return nil
}

// records returns the WAL records that persist rd, in the order recovery
// must find them: the hard state first, so that no prefix of them leaves
// an entry from a term after currentTerm; then a truncation, if rd
// overwrites stored entries; then each entry.
func (s *FileStorage) records(rd raft.Ready) ([]record, error) {
	var recs []record
	if rd.HardState != nil {
		recs = append(recs, record{typ: recordHardState, hardState: *rd.HardState})
	}
	if len(rd.Entries) == 0 {
		return recs, nil
	}
	first, last := rd.Entries[0].Index, s.lastIndex()
	if err := checkEntries(rd.Entries, s.snapshotIndex(), last); err != nil {
		return nil, err
	}
	if first <= last {
		recs = append(recs, record{typ: recordTruncate, index: first})
	}
	for _, e := range rd.Entries {
		recs = append(recs, record{typ: recordEntry, entry: e})
	}
	return recs, nil
}

// checkEntries returns an error unless entries run in index order from a
// first index that follows the snapshot through base and is at most one
// past last, so that saving them leaves no gap.
func checkEntries(entries []raft.Entry, base, last raft.Index) error {
	first := entries[0].Index
	if first <= base || first > last+1 {
		return fmt.Errorf("storage: entries start at index %d, but the log runs from %d to %d", first, base+1, last)
	}
	for i, e := range entries {
		if want := first + raft.Index(i); e.Index != want {
			return fmt.Errorf("storage: entry %d where entry %d belongs", e.Index, want)
		}
	}
	return nil
}

// saveSnapshot saves rd, whose snapshot and entries replace the whole
// stored log. It writes the snapshot file, then starts the WAL over with
// a segment that holds the hard state, the snapshot's last included index
// and term, and the entries; each appears whole or not at all, so a crash
// leaves either the old state or the new one. Only then does it remove
// the WAL segments and the snapshot the new ones replace.
func (s *FileStorage) saveSnapshot(rd raft.Ready) error {
	snap := *rd.Snapshot
	next := raft.RestoredState{HardState: s.state.HardState, Snapshot: &snap, Entries: slices.Clone(rd.Entries)}
	if rd.HardState != nil {
		next.HardState = *rd.HardState
	}
	if snap.LastIncludedIndex <= s.snapshotIndex() {
		return fmt.Errorf("storage: a snapshot through index %d is no newer than the stored one through %d",
			snap.LastIncludedIndex, s.snapshotIndex())
	}
	if len(rd.Entries) > 0 {
		if err := checkEntries(rd.Entries, snap.LastIncludedIndex, snap.LastIncludedIndex); err != nil {
			return err
		}
	}
	recs := []record{{typ: recordSnapshot, hardState: next.HardState, index: snap.LastIncludedIndex, term: snap.LastIncludedTerm}}
	for _, e := range rd.Entries {
		recs = append(recs, record{typ: recordEntry, entry: e})
	}
	err := writeFileAtomic(s.wal.fs, s.wal.dir, snapshotName(snap.LastIncludedIndex), encodeSnapshot(snap))
	if err == nil {
		err = s.wal.startOver(recs)
	}
	if err == nil {
		s.state = next
		err = s.removeObsolete()
	}
	if err != nil {
		s.err = err
	}
	return err
}

// snapshotName returns the file name of the snapshot through index.
func snapshotName(index raft.Index) string { return fmt.Sprintf("%016x.snap", index) }

// removeObsolete removes the files that a newer snapshot or a crash left
// behind: WAL segments before the log's first, snapshots other than the
// stored one, and files a crash interrupted the writing of.
func (s *FileStorage) removeObsolete() error {
	names, err := s.wal.fs.ReadDir(s.wal.dir)
	if err != nil {
		return fmt.Errorf("storage: list %s: %w", s.wal.dir, err)
	}
	for _, name := range names {
		obsolete := strings.HasSuffix(name, tmpSuffix) || s.wal.obsolete(name) ||
			strings.HasSuffix(name, ".snap") && name != snapshotName(s.snapshotIndex())
		if !obsolete {
			continue
		}
		if err := s.wal.fs.Remove(filepath.Join(s.wal.dir, name)); err != nil {
			return fmt.Errorf("storage: remove %s: %w", name, err)
		}
	}
	return nil
}

// replay applies one record read from the WAL to s.state. Save never
// writes a record that cannot follow the ones before it, so such a record
// means the WAL is damaged.
func (s *FileStorage) replay(r record) error {
	switch r.typ {
	case recordHardState:
		s.state.HardState = r.hardState
	case recordSnapshot:
		snap, err := s.readSnapshot(r.index)
		if err != nil {
			return err
		}
		if snap.LastIncludedTerm != r.term {
			return fmt.Errorf("the snapshot through index %d has term %d, not %d", r.index, snap.LastIncludedTerm, r.term)
		}
		s.state = raft.RestoredState{HardState: r.hardState, Snapshot: &snap}
	case recordTruncate:
		if r.index <= s.snapshotIndex() || r.index > s.lastIndex() {
			return fmt.Errorf("truncation at index %d of a log that runs from %d to %d", r.index, s.snapshotIndex()+1, s.lastIndex())
		}
		s.state.Entries = s.state.Entries[:r.index-s.snapshotIndex()-1]
	case recordEntry:
		if want := s.lastIndex() + 1; r.entry.Index != want {
			return fmt.Errorf("entry %d where entry %d belongs", r.entry.Index, want)
		}
		s.state.Entries = append(s.state.Entries, r.entry)
	}
	return nil
}

// readSnapshot reads and decodes the snapshot file through index.
func (s *FileStorage) readSnapshot(index raft.Index) (raft.Snapshot, error) {
	name := snapshotName(index)
	f, err := s.wal.fs.OpenFile(filepath.Join(s.wal.dir, name), os.O_RDONLY, 0)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("open %s: %w", name, err)
	}
	data, err := io.ReadAll(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("read %s: %w", name, err)
	}
	snap, err := decodeSnapshot(data)
	if err == nil && snap.LastIncludedIndex != index {
		err = fmt.Errorf("it holds the snapshot through index %d", snap.LastIncludedIndex)
	}
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("snapshot %s: %w", name, err)
	}
	return snap, nil
}

// snapshotIndex returns the last index the stored snapshot includes, or 0.
func (s *FileStorage) snapshotIndex() raft.Index { return snapshotIndex(s.state.Snapshot) }

// lastIndex returns the index of the last stored entry, or the snapshot's
// last included index if no entry follows it. The stored entries run
// without gaps from the one after the snapshot.
func (s *FileStorage) lastIndex() raft.Index {
	return s.snapshotIndex() + raft.Index(len(s.state.Entries))
}

// Load returns the hard state, snapshot, and log the disk holds. The
// entries are a copy, so the node cannot change what is stored.
func (s *FileStorage) Load() (raft.RestoredState, error) {
	return raft.RestoredState{HardState: s.state.HardState, Snapshot: s.state.Snapshot, Entries: slices.Clone(s.state.Entries)}, nil
}

// Close closes the WAL's open segment.
func (s *FileStorage) Close() error { return s.wal.close() }
