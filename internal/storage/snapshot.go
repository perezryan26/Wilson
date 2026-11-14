package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/perezryan26/wilson/raft"
)

// A snapshot file is laid out as
//
//	[crc32c u32][lastIncludedIndex u64][lastIncludedTerm u64][config length u32][config][data]
//
// in little-endian byte order. The CRC-32C checksum covers everything
// after it, and the state machine's data runs to the end of the file.
// config is reserved for the cluster configuration, which membership
// changes add; until then it is always empty
// (docs/adr/0005-snapshot-format.md).
const snapshotHeaderSize = 4 + 8 + 8 + 4

// encodeSnapshot returns the contents of the file that holds s.
func encodeSnapshot(s raft.Snapshot) []byte {
	le := binary.LittleEndian
	b := make([]byte, 4, snapshotHeaderSize+len(s.Data))
	b = le.AppendUint64(b, uint64(s.LastIncludedIndex))
	b = le.AppendUint64(b, uint64(s.LastIncludedTerm))
	b = le.AppendUint32(b, 0) // no configuration
	b = append(b, s.Data...)
	le.PutUint32(b, checksum(b[4:]))
	return b
}

// decodeSnapshot decodes the contents of a snapshot file. It refuses a
// file that fails its checksum or does not fit the layout.
func decodeSnapshot(b []byte) (raft.Snapshot, error) {
	if len(b) < snapshotHeaderSize {
		return raft.Snapshot{}, fmt.Errorf("%d bytes, fewer than a snapshot's %d-byte header", len(b), snapshotHeaderSize)
	}
	le := binary.LittleEndian
	if stored, computed := le.Uint32(b), checksum(b[4:]); stored != computed {
		return raft.Snapshot{}, fmt.Errorf("checksum mismatch: stored %#08x, computed %#08x", stored, computed)
	}
	if n := le.Uint32(b[20:]); n != 0 {
		return raft.Snapshot{}, fmt.Errorf("a %d-byte configuration, which no snapshot holds yet", n)
	}
	return raft.Snapshot{
		LastIncludedIndex: raft.Index(le.Uint64(b[4:])),
		LastIncludedTerm:  raft.Term(le.Uint64(b[12:])),
		Data:              bytes.Clone(b[snapshotHeaderSize:]),
	}, nil
}
