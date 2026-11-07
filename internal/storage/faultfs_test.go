package storage

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
)

func newFaultFS(seed uint64) *FaultFS { return NewFaultFS(rand.New(rand.NewPCG(seed, 0))) }

// createSynced creates path on fsys, makes its creation durable, and
// writes and syncs data to it.
func createSynced(t *testing.T, fsys *FaultFS, path string, data []byte) File {
	t.Helper()
	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.SyncDir("d"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	return f
}

// readFault returns the contents of path on fsys, or nil if it does not
// exist.
func readFault(t *testing.T, fsys *FaultFS, path string) []byte {
	t.Helper()
	f, err := fsys.OpenFile(path, os.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// A crash keeps every synced byte and a prefix of the unsynced ones: over
// enough seeds, every length from none to all, the shorter ones cutting a
// write short as a torn record would. A file opened before the crash is
// dead after it.
func TestFaultFSCrashKeepsSyncedBytesAndAPrefix(t *testing.T) {
	kept := map[int]bool{}
	for seed := range uint64(300) {
		fsys := newFaultFS(seed)
		f := createSynced(t, fsys, "d/a", []byte("synced "))
		for _, w := range []string{"one", "two"} {
			if _, err := f.Write([]byte(w)); err != nil {
				t.Fatal(err)
			}
		}
		fsys.Crash()
		got := readFault(t, fsys, "d/a")
		n := len(got) - len("synced ")
		if n < 0 || !bytes.Equal(got, []byte("synced onetwo")[:len(got)]) {
			t.Fatalf("seed %d: after the crash the file holds %q", seed, got)
		}
		kept[n] = true
		if _, err := f.Write([]byte("x")); !errors.Is(err, ErrCrashed) {
			t.Fatalf("seed %d: writing to a file opened before the crash: error %v, want ErrCrashed", seed, err)
		}
	}
	for n := range len("onetwo") + 1 {
		if !kept[n] {
			t.Errorf("no crash kept exactly %d unsynced bytes", n)
		}
	}
}

// A file created since its directory's last SyncDir may vanish in a crash,
// contents and all, even if they were synced. Once SyncDir returns, it
// survives every crash.
func TestFaultFSUnsyncedCreationMayVanish(t *testing.T) {
	outcomes := map[bool]int{}
	for seed := range uint64(100) {
		fsys := newFaultFS(seed)
		f, err := fsys.OpenFile("d/a", os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		fsys.Crash()
		got := readFault(t, fsys, "d/a")
		if got != nil && string(got) != "x" {
			t.Fatalf("seed %d: the file holds %q", seed, got)
		}
		outcomes[got != nil]++

		createSynced(t, fsys, "d/b", []byte("y"))
		fsys.Crash()
		if got := readFault(t, fsys, "d/b"); string(got) != "y" {
			t.Fatalf("seed %d: a file created and synced along with its directory holds %q after a crash", seed, got)
		}
	}
	if outcomes[true] == 0 || outcomes[false] == 0 {
		t.Errorf("the unsynced file survived %d of 100 crashes; want some of each", outcomes[true])
	}
}

// CrashAfter(n) crashes the file system during exactly the nth mutating
// operation from then, which returns ErrCrashed; reads and listings do not
// count. Files opened before are dead, but the file system goes on, and a
// truncation the crash struck during either happened or did not.
func TestFaultFSCrashAfterCountsMutations(t *testing.T) {
	fsys := newFaultFS(1)
	fsys.CrashAfter(5)
	var f File
	steps := []func() error{
		func() (err error) {
			f, err = fsys.OpenFile("d/a", os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
			return err
		},
		func() error { return fsys.SyncDir("d") },
		func() error { _, err := f.Write([]byte("hello")); return err },
		func() error { _, err := fsys.ReadDir("d"); readFault(t, fsys, "d/a"); return err },
		func() error { return f.Sync() },
		func() error { return f.Truncate(2) },
	}
	for i, step := range steps {
		err := step()
		wantCrash := i == len(steps)-1
		if wantCrash && !errors.Is(err, ErrCrashed) || !wantCrash && err != nil {
			t.Fatalf("step %d: error %v", i, err)
		}
	}
	if _, err := f.Write([]byte("x")); !errors.Is(err, ErrCrashed) {
		t.Errorf("writing to a file opened before the crash: error %v, want ErrCrashed", err)
	}
	if got := readFault(t, fsys, "d/a"); !slices.Contains([]string{"hello", "he"}, string(got)) {
		t.Errorf("after a crash during a truncation the file holds %q, want %q or %q", got, "hello", "he")
	}
}
