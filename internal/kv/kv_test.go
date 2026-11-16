package kv

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// testLog applies commands to a store at successive log indexes, from 1,
// as the log would.
type testLog struct {
	t     *testing.T
	s     *Store
	index uint64
}

func newTestLog(t *testing.T, maxSessions int) *testLog {
	return &testLog{t: t, s: NewStore(maxSessions)}
}

func (l *testLog) apply(c Command) Result {
	l.index++
	return l.s.Apply(l.index, c.Encode())
}

func (l *testLog) register() uint64 {
	l.t.Helper()
	r := l.apply(Command{Op: OpRegister})
	if r.Err != nil || r.Session != l.index {
		l.t.Fatalf("Register at index %d = %+v, want session %d", l.index, r, l.index)
	}
	return r.Session
}

func (l *testLog) put(session, seq uint64, key, value string) Result {
	return l.apply(Command{Op: OpPut, Session: session, Seq: seq, Key: key, Value: []byte(value)})
}

func (l *testLog) del(session, seq uint64, key string) Result {
	return l.apply(Command{Op: OpDelete, Session: session, Seq: seq, Key: key})
}

// value returns key's value, or "(none)" if it does not exist.
func (l *testLog) value(key string) string {
	v, ok := l.s.Get(key)
	if !ok {
		return "(none)"
	}
	return string(v)
}

func wantValue(t *testing.T, l *testLog, key, want string) {
	t.Helper()
	if got := l.value(key); got != want {
		t.Errorf("%s = %s, want %s", key, got, want)
	}
}

// Put sets a key, Get reads it, through the log or not, and Delete
// removes it and says whether it existed.
func TestPutGetDelete(t *testing.T) {
	l := newTestLog(t, 10)
	s := l.register()
	if r := l.put(s, 1, "x", "1"); r.Err != nil {
		t.Fatal(r.Err)
	}
	wantValue(t, l, "x", "1")
	if r := l.apply(Command{Op: OpGet, Request: 9, Key: "x"}); r.Request != 9 || !r.Found || string(r.Value) != "1" {
		t.Errorf("Get through the log = %+v, want request 9 and x=1", r)
	}
	if r := l.del(s, 2, "x"); r.Err != nil || !r.Found {
		t.Errorf("Delete of an existing key = %+v, want found", r)
	}
	wantValue(t, l, "x", "(none)")
	if r := l.del(s, 3, "x"); r.Err != nil || r.Found {
		t.Errorf("Delete of a missing key = %+v, want not found", r)
	}
	if r := l.apply(Command{Op: OpGet, Key: "x"}); r.Found {
		t.Errorf("Get of a missing key through the log = %+v, want not found", r)
	}
}

// §8: a write whose session has applied it already is answered from the
// session's record and never runs again, even after other writes to the
// same key. A retried Delete answers what the first run found.
func TestRetriedWriteAppliesOnce(t *testing.T) {
	l := newTestLog(t, 10)
	a, b := l.register(), l.register()
	l.put(a, 1, "x", "a1")
	l.put(b, 1, "x", "b1")
	if r := l.put(a, 1, "x", "a1"); r.Err != nil {
		t.Errorf("retried Put = %+v, want the first run's answer", r)
	}
	wantValue(t, l, "x", "b1")
	if r := l.del(a, 2, "x"); !r.Found {
		t.Fatalf("Delete = %+v, want found", r)
	}
	l.put(b, 2, "x", "b2")
	if r := l.del(a, 2, "x"); r.Err != nil || !r.Found {
		t.Errorf("retried Delete = %+v, want the first run's answer, found", r)
	}
	wantValue(t, l, "x", "b2")
}

// A write numbered below its session's latest has lost its result and is
// refused, changing nothing.
func TestWriteOlderThanSessionsLatestIsRefused(t *testing.T) {
	l := newTestLog(t, 10)
	s := l.register()
	l.put(s, 1, "x", "1")
	l.put(s, 2, "x", "2")
	if r := l.put(s, 1, "x", "1"); !errors.Is(r.Err, ErrStaleRequest) {
		t.Errorf("a write older than the latest = %+v, want ErrStaleRequest", r)
	}
	wantValue(t, l, "x", "2")
}

// A write from a session the store does not hold is refused with
// ErrSessionExpired, changing nothing.
func TestUnknownSessionIsExpired(t *testing.T) {
	l := newTestLog(t, 10)
	if r := l.put(42, 1, "x", "1"); !errors.Is(r.Err, ErrSessionExpired) {
		t.Errorf("Put from an unknown session = %+v, want ErrSessionExpired", r)
	}
	wantValue(t, l, "x", "(none)")
}

// Past MaxSessions, a Register evicts the session whose last command is
// oldest. A write from the evicted session never runs again, not even
// after a late copy of a Register, which only starts a new session with
// an ID of its own.
func TestEvictsLeastRecentlyActiveSession(t *testing.T) {
	l := newTestLog(t, 2)
	a, b := l.register(), l.register()
	l.put(b, 1, "x", "b1")
	l.put(a, 1, "x", "a1") // a is now the more recently active
	c := l.register()      // evicts b
	if r := l.put(b, 1, "x", "b1"); !errors.Is(r.Err, ErrSessionExpired) {
		t.Errorf("b's retried write after eviction = %+v, want ErrSessionExpired", r)
	}
	wantValue(t, l, "x", "a1")
	if d := l.register(); d == b {
		t.Fatalf("a new Register reused the evicted session's ID %d", b)
	}
	if r := l.put(b, 1, "x", "b1"); !errors.Is(r.Err, ErrSessionExpired) {
		t.Errorf("b's write after another Register = %+v, want ErrSessionExpired", r)
	}
	if r := l.put(c, 1, "y", "c1"); r.Err != nil {
		t.Errorf("c's write = %+v, want it applied", r)
	}
	wantValue(t, l, "x", "a1")
}

// §7: sessions are part of the snapshot. A store restored from one still
// answers a retried write from its record, and evicts in the same order.
func TestSessionsSurviveSnapshotAndRestore(t *testing.T) {
	l := newTestLog(t, 2)
	a, b := l.register(), l.register()
	l.put(a, 1, "x", "a1")
	l.del(b, 1, "x")
	l.put(a, 2, "y", "a2")
	restored := NewStore(2)
	if err := restored.Restore(l.s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	l.s = restored
	if r := l.del(b, 1, "x"); r.Err != nil || !r.Found {
		t.Errorf("retried Delete after a restore = %+v, want the first run's answer, found", r)
	}
	l.put(a, 3, "z", "a3")
	l.register() // evicts b, whose last command is older than a's
	if r := l.put(b, 2, "x", "b2"); !errors.Is(r.Err, ErrSessionExpired) {
		t.Errorf("b's write = %+v, want ErrSessionExpired", r)
	}
	if r := l.put(a, 4, "x", "a4"); r.Err != nil {
		t.Errorf("a's write = %+v, want it applied", r)
	}
}

// Stores that hold the same state encode it the same way, whatever order
// it was built in, so the snapshots of servers that applied the same
// entries are equal byte for byte.
func TestSnapshotEncodesEqualStatesEqually(t *testing.T) {
	l := newTestLog(t, 10)
	s := l.register()
	for i, key := range strings.Fields("m b z a q") {
		l.put(s, uint64(i+1), key, key+"-value")
	}
	want := l.s.Snapshot()
	for range 20 {
		if got := l.s.Snapshot(); !bytes.Equal(got, want) {
			t.Fatal("one store encoded its state two ways")
		}
	}
	restored := NewStore(10)
	if err := restored.Restore(want); err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); !bytes.Equal(got, want) {
		t.Error("a restored store encodes its state differently")
	}
}

// A log entry that does not decode as a command changes nothing, on every
// server alike.
func TestMalformedCommandChangesNothing(t *testing.T) {
	l := newTestLog(t, 10)
	l.put(l.register(), 1, "x", "1")
	before := l.s.Snapshot()
	for _, data := range [][]byte{nil, []byte("x"), make([]byte, commandHeaderSize)} {
		if r := l.s.Apply(9, data); !errors.Is(r.Err, ErrMalformedCommand) {
			t.Errorf("Apply(%x) = %+v, want ErrMalformedCommand", data, r)
		}
	}
	if !bytes.Equal(l.s.Snapshot(), before) {
		t.Error("a malformed command changed the store")
	}
}

// Commands that no encoder writes are refused: shorter than the header,
// another version, an unknown op, or a key longer than the rest.
func TestDamagedCommandIsRefused(t *testing.T) {
	good := Command{Op: OpPut, Request: 7, Session: 5, Seq: 2, Key: "key", Value: []byte("v")}.Encode()
	if _, err := DecodeCommand(good); err != nil {
		t.Fatal(err)
	}
	with := func(i int, v byte) []byte {
		b := bytes.Clone(good)
		b[i] = v
		return b
	}
	cases := map[string][]byte{
		"no bytes":            nil,
		"one byte short":      good[:commandHeaderSize-1],
		"version 2":           with(0, 2),
		"op 0":                with(1, 0),
		"op 5":                with(1, 5),
		"key past the end":    with(26, 5),
		"key cut short":       good[:commandHeaderSize+2],
		"a key of 2^32 bytes": with(29, 0xff),
	}
	for name, b := range cases {
		if c, err := DecodeCommand(b); err == nil {
			t.Errorf("%s decoded as %+v", name, c)
		}
	}
}

// The command layout is fixed (docs/adr/0007-client-sessions.md). These
// are the bytes docs/ARCHITECTURE.md shows.
func TestCommandBytes(t *testing.T) {
	c := Command{Op: OpPut, Request: 7, Session: 5, Seq: 2, Key: "x", Value: []byte("1")}
	want := "01 03 0700000000000000 0500000000000000 0200000000000000 01000000 78 31"
	if got := c.Encode(); !bytes.Equal(got, unhex(t, want)) {
		t.Errorf("%+v encodes as % x, want %s", c, got, want)
	}
}

// The snapshot layout is fixed too. These are the bytes
// docs/ARCHITECTURE.md shows: x=1, and session 5, whose second write,
// at index 9, deleted a key that existed.
func TestStateBytes(t *testing.T) {
	s := NewStore(10)
	s.data["x"] = []byte("1")
	s.sessions[5] = &session{seq: 2, found: true, lastIndex: 9}
	want := "01 01000000 01000000 78 01000000 31 01000000 0500000000000000 0200000000000000 0900000000000000 01"
	if got := s.Snapshot(); !bytes.Equal(got, unhex(t, want)) {
		t.Errorf("the state encodes as % x, want %s", got, want)
	}
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sampleState returns a snapshot with two keys, one of them empty, and
// two sessions.
func sampleState() []byte {
	s := NewStore(10)
	for i, c := range []Command{
		{Op: OpRegister},
		{Op: OpRegister},
		{Op: OpPut, Session: 1, Seq: 1, Key: "x", Value: []byte("1")},
		{Op: OpPut, Session: 2, Seq: 1, Key: "", Value: []byte("")},
		{Op: OpDelete, Session: 2, Seq: 2, Key: "x"},
		{Op: OpPut, Session: 1, Seq: 2, Key: "y", Value: bytes.Repeat([]byte("v"), 300)},
	} {
		s.Apply(uint64(i+1), c.Encode())
	}
	return s.Snapshot()
}

// Snapshots that Snapshot never writes are refused: cut short anywhere,
// another version, bytes left over, keys or sessions out of order, or a
// found byte other than 0 or 1.
func TestDamagedStateIsRefused(t *testing.T) {
	good := sampleState()
	if _, _, err := decodeState(good); err != nil {
		t.Fatal(err)
	}
	for n := range len(good) {
		if _, _, err := decodeState(good[:n]); err == nil {
			t.Errorf("the state cut to %d of %d bytes decoded", n, len(good))
		}
	}
	twoKeys := unhex(t, "01 02000000 01000000 79 00000000 01000000 78 00000000 00000000")
	twoSessions := unhex(t, "01 00000000 02000000 0200000000000000 0000000000000000 0000000000000000 00 0100000000000000 0000000000000000 0000000000000000 00")
	cases := map[string][]byte{
		"version 2":              append([]byte{2}, good[1:]...),
		"a byte after the state": append(bytes.Clone(good), 0),
		"keys out of order":      twoKeys,
		"sessions out of order":  twoSessions,
		"a found byte of 2":      append(bytes.Clone(good[:len(good)-1]), 2),
	}
	for name, b := range cases {
		if _, _, err := decodeState(b); err == nil {
			t.Errorf("%s decoded", name)
		}
	}
	if err := NewStore(1).Restore(twoKeys); err == nil {
		t.Error("Restore accepted a damaged snapshot")
	}
}

// FuzzCommandDecode feeds arbitrary bytes to the command decoder, which
// must never panic, and requires every command it accepts to encode back
// to the very bytes it came from.
func FuzzCommandDecode(f *testing.F) {
	for _, c := range []Command{
		{Op: OpRegister, Request: 1},
		{Op: OpGet, Request: 2, Key: "x"},
		{Op: OpPut, Request: 3, Session: 4, Seq: 5, Key: "key", Value: []byte("value")},
		{Op: OpDelete, Request: 1 << 63, Session: 1 << 40, Seq: 1 << 33, Key: strings.Repeat("k", 100)},
	} {
		f.Add(c.Encode())
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := DecodeCommand(b)
		if err != nil {
			return
		}
		if again := c.Encode(); !bytes.Equal(again, b) {
			t.Fatalf("command decoded from %x encodes as %x", b, again)
		}
	})
}

// FuzzStateDecode feeds arbitrary bytes to the snapshot decoder, which
// must never panic, and requires every state it accepts to encode back to
// the very bytes it came from, so no two byte strings restore one state.
func FuzzStateDecode(f *testing.F) {
	f.Add(sampleState())
	f.Add(NewStore(1).Snapshot())
	f.Fuzz(func(t *testing.T, b []byte) {
		s := NewStore(1)
		if err := s.Restore(b); err != nil {
			return
		}
		if again := s.Snapshot(); !bytes.Equal(again, b) {
			t.Fatalf("state decoded from %x encodes as %x", b, again)
		}
	})
}
