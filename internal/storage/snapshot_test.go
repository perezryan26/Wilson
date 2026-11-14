package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

// sampleSnapshots returns snapshots with no data, a little, and a lot, and
// indexes and terms that need all eight bytes.
func sampleSnapshots() []raft.Snapshot {
	return []raft.Snapshot{
		{LastIncludedIndex: 1, LastIncludedTerm: 1},
		{LastIncludedIndex: 42, LastIncludedTerm: 9, Data: []byte(`{"x":"1"}`)},
		{LastIncludedIndex: 1 << 40, LastIncludedTerm: 1 << 33, Data: bytes.Repeat([]byte{0xcd}, 5000)},
	}
}

func sameSnapshot(a, b raft.Snapshot) bool {
	return a.LastIncludedIndex == b.LastIncludedIndex && a.LastIncludedTerm == b.LastIncludedTerm && bytes.Equal(a.Data, b.Data)
}

// A snapshot file decodes to the snapshot it was encoded from.
func TestSnapshotRoundTrip(t *testing.T) {
	for _, want := range sampleSnapshots() {
		got, err := decodeSnapshot(encodeSnapshot(want))
		if err != nil || !sameSnapshot(got, want) {
			t.Errorf("snapshot through %d decoded as %+v, %v", want.LastIncludedIndex, got, err)
		}
	}
}

// The checksum catches every flipped bit, a file cut short anywhere is
// refused, and so is a configuration, until membership changes define one.
func TestDamagedSnapshotIsRefused(t *testing.T) {
	enc := encodeSnapshot(sampleSnapshots()[1])
	for bit := range 8 * len(enc) {
		b := bytes.Clone(enc)
		b[bit/8] ^= 1 << (bit % 8)
		if _, err := decodeSnapshot(b); err == nil {
			t.Fatalf("flipping bit %d went unnoticed", bit)
		}
	}
	for n := range len(enc) {
		if _, err := decodeSnapshot(enc[:n]); err == nil {
			t.Fatalf("a snapshot cut to %d of its %d bytes decoded", n, len(enc))
		}
	}
	withConfig := encodeSnapshot(raft.Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1})
	withConfig = binary.LittleEndian.AppendUint32(withConfig[:20], 1)
	withConfig = append(withConfig, 7)
	binary.LittleEndian.PutUint32(withConfig, checksum(withConfig[4:]))
	if _, err := decodeSnapshot(withConfig); err == nil || !strings.Contains(err.Error(), "configuration") {
		t.Errorf("a snapshot with a configuration: error %v, want one about the configuration", err)
	}
}

// FuzzSnapshotDecode feeds arbitrary bytes to the snapshot decoder. It
// must never panic, and a snapshot it accepts must encode back to exactly
// the bytes it came from.
func FuzzSnapshotDecode(f *testing.F) {
	for _, s := range sampleSnapshots() {
		enc := encodeSnapshot(s)
		f.Add(enc)
		f.Add(enc[:len(enc)-1])
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := decodeSnapshot(b)
		if err != nil {
			return
		}
		if again := encodeSnapshot(s); !bytes.Equal(again, b) {
			t.Fatalf("snapshot decoded from %x encodes as %x", b, again)
		}
	})
}

// The snapshot file's layout is fixed (docs/adr/0005-snapshot-format.md).
// These are the bytes docs/ARCHITECTURE.md shows; an encoder that changed
// them would no longer read the snapshots already on disk.
func TestSnapshotBytes(t *testing.T) {
	s := raft.Snapshot{LastIncludedIndex: 7, LastIncludedTerm: 3, Data: []byte(`{"x":"1"}`)}
	want := "7af9f7c0 0700000000000000 0300000000000000 00000000 7b2278223a2231227d"
	if got := hex.EncodeToString(encodeSnapshot(s)); got != strings.ReplaceAll(want, " ", "") {
		t.Errorf("snapshot bytes = %s, want %s", got, want)
	}
}
