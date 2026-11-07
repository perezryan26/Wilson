package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

// sampleRecords returns records of every type, including entries with no
// command, a long command, and indexes and terms that need all eight bytes.
func sampleRecords() []record {
	return []record{
		{typ: recordHardState, hardState: raft.HardState{CurrentTerm: 7, VotedFor: 3}},
		{typ: recordHardState},
		{typ: recordEntry, entry: raft.Entry{Index: 1, Term: 1}},
		{typ: recordEntry, entry: raft.Entry{Index: 42, Term: 9, Data: []byte("put x=1")}},
		{typ: recordEntry, entry: raft.Entry{Index: 1 << 40, Term: 1 << 33, Data: bytes.Repeat([]byte{0xab}, 1000)}},
		{typ: recordTruncate, index: 5},
	}
}

func sameRecord(a, b record) bool {
	return a.typ == b.typ && a.hardState == b.hardState && a.index == b.index &&
		a.entry.Index == b.entry.Index && a.entry.Term == b.entry.Term && bytes.Equal(a.entry.Data, b.entry.Data)
}

// frame returns body as a record with a correct length and checksum, so a
// test can build records that are damaged in their payload only.
func frame(body []byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	b = binary.LittleEndian.AppendUint32(b, checksum(body))
	return append(b, body...)
}

// Records encoded back to back decode to the same records, each reporting
// its own size.
func TestRecordRoundTrip(t *testing.T) {
	var buf []byte
	for _, r := range sampleRecords() {
		buf = appendRecord(buf, r)
	}
	for i, want := range sampleRecords() {
		got, n, err := decodeRecord(buf)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !sameRecord(got, want) {
			t.Errorf("record %d decoded as %+v, want %+v", i, got, want)
		}
		buf = buf[n:]
	}
	if len(buf) != 0 {
		t.Errorf("%d bytes left over", len(buf))
	}
}

// Every proper prefix of a record, as a crash mid-write leaves it, decodes
// as io.ErrUnexpectedEOF: the record is incomplete, not damaged.
func TestShortRecordIsUnexpectedEOF(t *testing.T) {
	for i, r := range sampleRecords() {
		enc := appendRecord(nil, r)
		for n := range len(enc) {
			if _, _, err := decodeRecord(enc[:n]); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("record %d cut to %d of %d bytes: error %v, want io.ErrUnexpectedEOF", i, n, len(enc), err)
			}
		}
	}
}

// Flipping any one bit of a record, in its length, checksum, type, or
// payload, never decodes as a record.
func TestFlippedBitIsCaught(t *testing.T) {
	for i, r := range sampleRecords() {
		enc := appendRecord(nil, r)
		for bit := range 8 * len(enc) {
			damaged := bytes.Clone(enc)
			damaged[bit/8] ^= 1 << (bit % 8)
			if got, _, err := decodeRecord(damaged); err == nil {
				t.Errorf("record %d with bit %d flipped decoded as %+v", i, bit, got)
			}
		}
	}
}

// Records whose checksum is right but whose content no writer produces are
// damaged, not incomplete: an empty record, an unknown type, a payload of
// the wrong size, or an entry of an unknown entry type.
func TestMalformedRecordIsDamaged(t *testing.T) {
	entry := func(entryType byte) []byte {
		b := append([]byte{byte(recordEntry)}, make([]byte, 16)...)
		return append(b, entryType, 'x')
	}
	if _, _, err := decodeRecord(frame(entry(entryNormal))); err != nil {
		t.Fatalf("a well-formed entry failed to decode: %v", err)
	}
	cases := map[string][]byte{
		"empty":                       make([]byte, headerSize),
		"type 0":                      frame([]byte{0}),
		"type 4":                      frame([]byte{4, 1, 2, 3}),
		"hard state, 15-byte payload": frame(append([]byte{byte(recordHardState)}, make([]byte, 15)...)),
		"hard state, 17-byte payload": frame(append([]byte{byte(recordHardState)}, make([]byte, 17)...)),
		"truncation, 9-byte payload":  frame(append([]byte{byte(recordTruncate)}, make([]byte, 9)...)),
		"entry, 16-byte payload":      frame(append([]byte{byte(recordEntry)}, make([]byte, 16)...)),
		"entry of unknown entry type": frame(entry(1)),
	}
	for name, b := range cases {
		if _, _, err := decodeRecord(b); err == nil || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%s: error %v, want a damaged-record error", name, err)
		}
	}
}

// FuzzWALRecordDecode feeds arbitrary bytes to the record decoder, which
// must never panic, and requires every record it accepts to encode back to
// the very bytes it came from, so no two byte strings decode to one record.
func FuzzWALRecordDecode(f *testing.F) {
	var all []byte
	for _, r := range sampleRecords() {
		enc := appendRecord(nil, r)
		f.Add(enc)
		all = append(all, enc...)
	}
	f.Add(all)
	f.Add(all[:len(all)-3])
	f.Fuzz(func(t *testing.T, b []byte) {
		var again []byte
		off := 0
		for off < len(b) {
			r, n, err := decodeRecord(b[off:])
			if err != nil {
				break
			}
			again = appendRecord(again, r)
			off += n
		}
		if !bytes.Equal(again, b[:off]) {
			t.Fatalf("records decoded from %x encode as %x", b[:off], again)
		}
	})
}
