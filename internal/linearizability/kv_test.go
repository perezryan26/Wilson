package linearizability

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// clock is a fake clock that a test moves by hand.
type clock struct{ now int64 }

func (c *clock) read() int64 { return c.now }

// op is one operation of a scripted history: its client, request, and
// answer, and when it was called and returned. A return time of 0 means
// it never returned.
type op struct {
	client    int
	in        Input
	out       Output
	call, ret int64
}

// check records ops, which must be in order of call time, and checks the
// history.
func check(t *testing.T, ops ...op) error {
	t.Helper()
	c := &clock{}
	r := NewRecorder(c.read)
	ids := make([]int, len(ops))
	type event struct {
		at   int64
		i    int
		call bool
	}
	var events []event
	for i, o := range ops {
		events = append(events, event{o.call, i, true})
		if o.ret != 0 {
			events = append(events, event{o.ret, i, false})
		}
	}
	for at := int64(0); at <= 100; at++ {
		c.now = at
		for _, e := range events {
			switch {
			case e.at != at:
			case e.call:
				ids[e.i] = r.Call(ops[e.i].client, ops[e.i].in)
			}
		}
		for _, e := range events {
			if e.at == at && !e.call {
				r.Return(ids[e.i], ops[e.i].out)
			}
		}
	}
	return r.Check(time.Second, t.TempDir())
}

func put(key, value string) Input { return Input{Op: Put, Key: key, Value: value} }
func get(key string) Input        { return Input{Op: Get, Key: key} }
func del(key string) Input        { return Input{Op: Delete, Key: key} }
func found(value string) Output   { return Output{Value: value, Found: true} }

// Operations one after another are linearizable exactly when each read
// returns the last value written, and a Delete says whether the key
// existed.
func TestSequentialHistory(t *testing.T) {
	ok := []op{
		{0, get("x"), Output{}, 1, 2},
		{0, put("x", "1"), Output{}, 3, 4},
		{1, get("x"), found("1"), 5, 6},
		{1, del("x"), Output{Found: true}, 7, 8},
		{0, del("x"), Output{}, 9, 10},
		{0, get("x"), Output{}, 11, 12},
	}
	if err := check(t, ok...); err != nil {
		t.Fatalf("a correct history: %v", err)
	}
	for name, bad := range map[string]op{
		"a stale read":          {1, get("x"), Output{}, 5, 6},
		"a read of no write":    {1, get("x"), found("2"), 5, 6},
		"a Delete of a missing": {1, del("x"), Output{}, 7, 8},
	} {
		history := append([]op(nil), ok...)
		for i := range history {
			if history[i].call == bad.call {
				history[i] = bad
			}
		}
		if err := check(t, history...); !errors.Is(err, ErrNotLinearizable) {
			t.Errorf("%s: error %v, want ErrNotLinearizable", name, err)
		}
	}
}

// A read concurrent with a write may return the old value or the new one,
// but once any read has returned the new one, no later read may return
// the old.
func TestConcurrentHistory(t *testing.T) {
	for _, value := range []Output{{}, found("1")} {
		err := check(t, op{0, put("x", "1"), Output{}, 1, 10}, op{1, get("x"), value, 2, 3})
		if err != nil {
			t.Errorf("a read concurrent with a write answered %+v: %v", value, err)
		}
	}
	err := check(t,
		op{0, put("x", "1"), Output{}, 1, 10},
		op{1, get("x"), found("1"), 2, 3},
		op{2, get("x"), Output{}, 4, 5},
	)
	if !errors.Is(err, ErrNotLinearizable) {
		t.Errorf("an old value read after the new one: error %v, want ErrNotLinearizable", err)
	}
}

// A write whose outcome the client never learned may take effect at any
// time after its call, or never: either answer of a later read is
// linearizable, but a read cannot see it and a still later one not.
func TestOpenWriteMayTakeEffectAnyTime(t *testing.T) {
	for _, value := range []Output{{}, found("1")} {
		if err := check(t, op{0, put("x", "1"), Output{}, 1, 0}, op{1, get("x"), value, 20, 21}); err != nil {
			t.Errorf("a read after an open write answered %+v: %v", value, err)
		}
	}
	err := check(t,
		op{0, put("x", "1"), Output{}, 1, 0},
		op{1, get("x"), found("1"), 20, 21},
		op{1, get("x"), Output{}, 30, 31},
	)
	if !errors.Is(err, ErrNotLinearizable) {
		t.Errorf("the open write seen, then unseen: error %v, want ErrNotLinearizable", err)
	}
}

// Keys are checked apart: a write to one never explains a read of
// another.
func TestKeysAreIndependent(t *testing.T) {
	err := check(t, op{0, put("x", "1"), Output{}, 1, 2}, op{1, get("y"), found("1"), 3, 4})
	if !errors.Is(err, ErrNotLinearizable) {
		t.Errorf("a read of y saw x's write: error %v, want ErrNotLinearizable", err)
	}
}

// An illegal history comes with Porcupine's visualization, named in the
// error, which describes each operation.
func TestIllegalHistoryIsVisualized(t *testing.T) {
	dir := t.TempDir()
	c := &clock{}
	r := NewRecorder(func() int64 { c.now++; return c.now })
	r.Return(r.Call(0, put("x", "1")), Output{})
	r.Return(r.Call(1, get("x")), Output{})
	err := r.Check(time.Second, dir)
	if !errors.Is(err, ErrNotLinearizable) {
		t.Fatalf("error %v, want ErrNotLinearizable", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || !strings.Contains(err.Error(), entries[0].Name()) {
		t.Fatalf("error %q names no file of %v", err, entries)
	}
	html, err := os.ReadFile(dir + "/" + entries[0].Name())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`put(\"x\", \"1\") → ok`, `get(\"x\") → (none)`} {
		if !strings.Contains(string(html), want) {
			t.Errorf("the visualization lacks %s", want)
		}
	}
}
