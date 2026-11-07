package storage

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/perezryan26/wilson/raft"
)

// FileStorage keeps a node's hard state and log on disk, in a write-ahead
// log of records (docs/adr/0003-wal-format.md), with a copy in memory for
// Load. It is not safe for concurrent use.
type FileStorage struct {
	wal   wal
	state raft.RestoredState // what the WAL holds
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

// OpenFileStorage opens the WAL in dir, an existing directory, and
// replays it. A record torn by a crash at the end of the WAL is truncated
// and logged. Any other damage is refused with an error wrapping
// ErrCorrupt that names the segment and offset.
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
	return s, nil
}

// Save writes rd's hard state and entries to the WAL and returns once
// they are fsynced, so that no reply leaves before the state it reflects
// is durable (Figure 2). Entries replace every stored entry from
// rd.Entries[0].Index on. Save refuses entries that would leave a gap,
// changing nothing. Once a write or fsync has failed, what the disk holds
// is unknown, so that Save and every later one fail: the runtime must
// stop.
func (s *FileStorage) Save(rd raft.Ready) error {
	if s.err != nil {
		return s.err
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
		first := rd.Entries[0].Index
		s.state.Entries = append(s.state.Entries[:first-1], rd.Entries...)
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
	if first < 1 || first > last+1 {
		return nil, fmt.Errorf("storage: entries start at index %d, but the log ends at index %d", first, last)
	}
	if first <= last {
		recs = append(recs, record{typ: recordTruncate, index: first})
	}
	for i, e := range rd.Entries {
		if want := first + raft.Index(i); e.Index != want {
			return nil, fmt.Errorf("storage: entry %d where entry %d belongs", e.Index, want)
		}
		recs = append(recs, record{typ: recordEntry, entry: e})
	}
	return recs, nil
}

// replay applies one record read from the WAL to s.state. Save never
// writes a record that cannot follow the ones before it, so such a record
// means the WAL is damaged.
func (s *FileStorage) replay(r record) error {
	switch r.typ {
	case recordHardState:
		s.state.HardState = r.hardState
	case recordTruncate:
		if r.index < 1 || r.index > s.lastIndex() {
			return fmt.Errorf("truncation at index %d of a log that ends at index %d", r.index, s.lastIndex())
		}
		s.state.Entries = s.state.Entries[:r.index-1]
	case recordEntry:
		if want := s.lastIndex() + 1; r.entry.Index != want {
			return fmt.Errorf("entry %d where entry %d belongs", r.entry.Index, want)
		}
		s.state.Entries = append(s.state.Entries, r.entry)
	}
	return nil
}

// lastIndex returns the index of the last stored entry, or 0. The stored
// entries run from index 1 without gaps.
func (s *FileStorage) lastIndex() raft.Index { return raft.Index(len(s.state.Entries)) }

// Load returns the hard state and log the WAL holds. The entries are a
// copy, so the node cannot change what is stored.
func (s *FileStorage) Load() (raft.RestoredState, error) {
	return raft.RestoredState{HardState: s.state.HardState, Entries: slices.Clone(s.state.Entries)}, nil
}

// Close closes the WAL's open segment.
func (s *FileStorage) Close() error { return s.wal.close() }
