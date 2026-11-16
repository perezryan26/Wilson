package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/perezryan26/wilson/raft"
)

// A WAL record is laid out as
//
//	[length u32][crc32c u32][type u8][payload]
//
// in little-endian byte order. length counts the type byte and the
// payload, which are exactly the bytes the CRC-32C checksum covers, so a
// record takes headerSize + length bytes (docs/adr/0003-wal-format.md).
const headerSize = 8

// recordType says what a record's payload holds. Type 0 is unused, so a
// run of zero bytes never reads as a record.
type recordType uint8

const (
	// recordHardState holds currentTerm u64 and votedFor u64.
	recordHardState recordType = 1
	// recordEntry holds one log entry: index u64, term u64, the entry's
	// type as a byte, and the command, which runs to the end of the
	// record.
	recordEntry recordType = 2
	// recordTruncate holds index u64: every entry from index on is
	// discarded, because the entries that follow overwrite them.
	recordTruncate recordType = 3
	// recordSnapshot begins a segment that replaces every segment before
	// it: the log starts over after a snapshot. It holds currentTerm u64
	// and votedFor u64, then the snapshot's lastIncludedIndex u64 and
	// lastIncludedTerm u64; the entry records that follow it hold the log
	// after the snapshot.
	recordSnapshot recordType = 4
)

// record is one WAL record. Which fields hold its payload depends on typ.
type record struct {
	typ       recordType
	hardState raft.HardState // recordHardState, recordSnapshot
	entry     raft.Entry     // recordEntry
	index     raft.Index     // recordTruncate: the first index discarded; recordSnapshot: lastIncludedIndex
	term      raft.Term      // recordSnapshot: lastIncludedTerm
}

// appendRecord appends r, framed, to buf and returns the extended buffer.
func appendRecord(buf []byte, r record) []byte {
	start := len(buf)
	buf = append(buf, make([]byte, headerSize)...)
	buf = append(buf, byte(r.typ))
	le := binary.LittleEndian
	switch r.typ {
	case recordHardState:
		buf = le.AppendUint64(buf, uint64(r.hardState.CurrentTerm))
		buf = le.AppendUint64(buf, uint64(r.hardState.VotedFor))
	case recordEntry:
		buf = le.AppendUint64(buf, uint64(r.entry.Index))
		buf = le.AppendUint64(buf, uint64(r.entry.Term))
		buf = append(buf, byte(r.entry.Type))
		buf = append(buf, r.entry.Data...)
	case recordTruncate:
		buf = le.AppendUint64(buf, uint64(r.index))
	case recordSnapshot:
		buf = le.AppendUint64(buf, uint64(r.hardState.CurrentTerm))
		buf = le.AppendUint64(buf, uint64(r.hardState.VotedFor))
		buf = le.AppendUint64(buf, uint64(r.index))
		buf = le.AppendUint64(buf, uint64(r.term))
	}
	body := buf[start+headerSize:]
	le.PutUint32(buf[start:], uint32(len(body)))
	le.PutUint32(buf[start+4:], checksum(body))
	return buf
}

// decodeRecord decodes the record at the start of b and returns it with
// its size in bytes. It returns io.ErrUnexpectedEOF if b ends before the
// record does, which is how a write cut short by a crash looks, and
// another error if the record is damaged.
func decodeRecord(b []byte) (record, int, error) {
	if len(b) < headerSize {
		return record{}, 0, io.ErrUnexpectedEOF
	}
	length := binary.LittleEndian.Uint32(b)
	if uint64(length) > uint64(len(b)-headerSize) {
		return record{}, 0, io.ErrUnexpectedEOF
	}
	if length == 0 {
		// The checksum of no bytes is 0, so only this check stops a run of
		// zero bytes from reading as a record.
		return record{}, 0, errors.New("empty record")
	}
	size := headerSize + int(length)
	body := b[headerSize:size]
	if stored, computed := binary.LittleEndian.Uint32(b[4:]), checksum(body); stored != computed {
		return record{}, 0, fmt.Errorf("checksum mismatch: stored %#08x, computed %#08x", stored, computed)
	}
	r, err := decodePayload(recordType(body[0]), body[1:])
	if err != nil {
		return record{}, 0, err
	}
	return r, size, nil
}

// decodePayload decodes p, the payload of a record of type t.
func decodePayload(t recordType, p []byte) (record, error) {
	le := binary.LittleEndian
	switch t {
	case recordHardState:
		if len(p) != 16 {
			return record{}, payloadSizeError(t, p)
		}
		hs := raft.HardState{CurrentTerm: raft.Term(le.Uint64(p)), VotedFor: raft.NodeID(le.Uint64(p[8:]))}
		return record{typ: t, hardState: hs}, nil
	case recordEntry:
		e, err := decodeEntry(p)
		return record{typ: t, entry: e}, err
	case recordTruncate:
		if len(p) != 8 {
			return record{}, payloadSizeError(t, p)
		}
		return record{typ: t, index: raft.Index(le.Uint64(p))}, nil
	case recordSnapshot:
		if len(p) != 32 {
			return record{}, payloadSizeError(t, p)
		}
		hs := raft.HardState{CurrentTerm: raft.Term(le.Uint64(p)), VotedFor: raft.NodeID(le.Uint64(p[8:]))}
		return record{typ: t, hardState: hs, index: raft.Index(le.Uint64(p[16:])), term: raft.Term(le.Uint64(p[24:]))}, nil
	}
	return record{}, fmt.Errorf("unknown record type %d", t)
}

// decodeEntry decodes p, the payload of an entry record.
func decodeEntry(p []byte) (raft.Entry, error) {
	if len(p) < 17 {
		return raft.Entry{}, payloadSizeError(recordEntry, p)
	}
	le := binary.LittleEndian
	e := raft.Entry{Index: raft.Index(le.Uint64(p)), Term: raft.Term(le.Uint64(p[8:])), Type: raft.EntryType(p[16]), Data: bytes.Clone(p[17:])}
	if e.Type != raft.EntryNormal && e.Type != raft.EntryNoOp {
		return raft.Entry{}, fmt.Errorf("entry of unknown type %d", e.Type)
	}
	return e, nil
}

func payloadSizeError(t recordType, p []byte) error {
	return fmt.Errorf("record of type %d with a %d-byte payload", t, len(p))
}

// checksum returns the CRC-32C of b. The Castagnoli polynomial detects
// more errors than IEEE's at these lengths, and amd64 and arm64 compute
// it in hardware.
func checksum(b []byte) uint32 {
	return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))
}
