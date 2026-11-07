package storage

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// OSFS carries out each write file storage relies on, in a real directory:
// an exclusive create, appends, fsyncs, and a truncation followed by an
// append at the new end. The directory then lists the file, and it reads
// back as written.
func TestOSFSWritesAndReadsBack(t *testing.T) {
	var fsys FS = OSFS{}
	dir := t.TempDir()
	name := filepath.Join(dir, "a")
	f, err := fsys.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []func() error{
		func() error { _, err := f.Write([]byte("hello, world")); return err },
		f.Sync,
		func() error { return f.Truncate(5) },
		func() error { _, err := f.Write([]byte("!")); return err },
		f.Sync,
		f.Close,
		func() error { return fsys.SyncDir(dir) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	if names, err := fsys.ReadDir(dir); err != nil || !slices.Equal(names, []string{"a"}) {
		t.Errorf("ReadDir = %q, %v; want [a]", names, err)
	}
	r, err := fsys.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if got, err := io.ReadAll(r); err != nil || string(got) != "hello!" {
		t.Errorf("read back %q, %v; want %q", got, err, "hello!")
	}
}

// OSFS refuses to create a file that exists exclusively, and to open one
// that does not exist, returning a nil File rather than a File holding a
// nil *os.File.
func TestOSFSOpenErrors(t *testing.T) {
	var fsys FS = OSFS{}
	dir := t.TempDir()
	name := filepath.Join(dir, "a")
	if err := os.WriteFile(name, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600); !errors.Is(err, fs.ErrExist) {
		t.Errorf("creating an existing file exclusively: error %v, want fs.ErrExist", err)
	}
	if missing, err := fsys.OpenFile(filepath.Join(dir, "missing"), os.O_RDONLY, 0); missing != nil || !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("opening a missing file gave %v, %v; want nil and fs.ErrNotExist", missing, err)
	}
}
