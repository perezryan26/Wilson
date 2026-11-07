package storage

import (
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

// TestCrashAtEveryWriteBoundary saves storageSteps on a FaultFS that
// crashes during its nth mutating operation, for every n the steps reach
// and 30 seeds each, so a crash strikes mid-record, before and after each
// fsync, and before a new segment's directory entry is synced. Recovery
// must return the state after every Save that returned, plus a prefix of
// the records of the Save that crashed: never less, since an fsynced
// record is never lost. Crashes during recovery itself must not change
// that, and the recovered WAL must keep taking writes.
func TestCrashAtEveryWriteBoundary(t *testing.T) {
	for n := 1; ; n++ {
		crashed := false
		for seed := range uint64(30) {
			crashed = crashAt(t, n, seed) || crashed
		}
		if !crashed {
			t.Logf("checked %d crash points", n-1)
			return
		}
	}
}

// crashAt saves storageSteps until the crash during mutating operation n,
// recovers, and checks the result. It reports whether the crash came
// before the steps were done.
func crashAt(t *testing.T, n int, seed uint64) bool {
	t.Helper()
	fsys := NewFaultFS(rand.New(rand.NewPCG(seed, uint64(n))))
	s := openFileStorage(t, fsys, "wal")
	steps := storageSteps()
	fsys.CrashAfter(n)
	saved, err := saveUntilError(s, steps)
	switch {
	case err == nil:
		fsys.Crash() // everything was synced, so this crash must lose nothing
	case !errors.Is(err, ErrCrashed):
		t.Fatalf("crash point %d, seed %d: Save: %v", n, seed, err)
	}
	crashDuringRecovery(t, fsys)
	s = openFileStorage(t, fsys, "wal")
	got := load(t, s)
	allowed := allowedStates(steps, saved)
	if !slices.ContainsFunc(allowed, func(st raft.RestoredState) bool { return sameState(st, got) }) {
		t.Fatalf("crash point %d, seed %d, after %d Saves returned: recovered %+v, want one of %+v", n, seed, saved, got, allowed)
	}
	next := raft.Ready{HardState: hardState(9, 0), Entries: []raft.Entry{ent(raft.Index(len(got.Entries))+1, 9)}}
	if err := s.Save(next); err != nil {
		t.Fatalf("crash point %d, seed %d: Save after recovery: %v", n, seed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	want := raft.RestoredState{HardState: *next.HardState, Entries: append(got.Entries, next.Entries...)}
	if again := load(t, openFileStorage(t, fsys, "wal")); !sameState(again, want) {
		t.Fatalf("crash point %d, seed %d: after recovery and one more Save, reopened to %+v, want %+v", n, seed, again, want)
	}
	return err != nil
}

// saveUntilError saves steps in order until a Save fails, and returns how
// many succeeded and the error.
func saveUntilError(s *FileStorage, steps []raft.Ready) (int, error) {
	for i, rd := range steps {
		if err := s.Save(rd); err != nil {
			return i, err
		}
	}
	return len(steps), nil
}

// crashDuringRecovery opens file storage on fsys with a crash armed for
// its first mutating operation, then for its second: the truncation of a
// torn record and the fsync after it. Each crash leaves the WAL as a crash
// during recovery would, for the next recovery to handle.
func crashDuringRecovery(t *testing.T, fsys *FaultFS) {
	t.Helper()
	for m := 1; m <= 2; m++ {
		fsys.CrashAfter(m)
		s, err := OpenFileStorage(fsys, "wal", FileOptions{SegmentSize: 64, Logger: slog.New(slog.DiscardHandler)})
		fsys.CrashAfter(0)
		if err == nil {
			_ = s.Close()
			continue
		}
		if !errors.Is(err, ErrCrashed) {
			t.Fatalf("recovery: %v", err)
		}
	}
}

// allowedStates returns every state recovery may return once the Saves of
// steps[:saved] have returned and the Save of the next step, if there is
// one, crashed: the state after steps[:saved], then that state with each
// longer prefix of the crashed Save's records, which file storage writes
// in this order: the hard state, a truncation if the entries overwrite
// stored ones, then each entry.
func allowedStates(steps []raft.Ready, saved int) []raft.RestoredState {
	var mem Memory
	for _, rd := range steps[:saved] {
		_ = mem.Save(rd)
	}
	st, _ := mem.Load()
	states := []raft.RestoredState{st}
	if saved == len(steps) {
		return states
	}
	rd := steps[saved]
	if rd.HardState != nil {
		st.HardState = *rd.HardState
		states = append(states, st)
	}
	if len(rd.Entries) > 0 && rd.Entries[0].Index <= raft.Index(len(st.Entries)) {
		st.Entries = slices.Clone(st.Entries[:rd.Entries[0].Index-1])
		states = append(states, st)
	}
	for _, e := range rd.Entries {
		st.Entries = append(slices.Clone(st.Entries), e)
		states = append(states, st)
	}
	return states
}

// After a write fails, every later Save fails with the same error, even
// one with nothing to write and one the file system could now take: what
// the disk holds is unknown, and the runtime must stop.
func TestFileStorageFailsStopAfterWriteError(t *testing.T) {
	fsys := newFaultFS(1)
	s := openFileStorage(t, fsys, "wal")
	fsys.CrashAfter(1)
	if err := s.Save(raft.Ready{HardState: hardState(1, 1)}); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save during a crash: error %v, want ErrCrashed", err)
	}
	for _, rd := range []raft.Ready{{HardState: hardState(2, 2)}, {}} {
		if err := s.Save(rd); !errors.Is(err, ErrCrashed) {
			t.Errorf("Save of %+v after a failed write: error %v, want ErrCrashed", rd, err)
		}
	}
}
