package storage

import (
	"errors"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
)

// ErrCrashed is the error a FaultFS returns from the operation during
// which it crashes, and from every later call on a file opened before the
// crash.
var ErrCrashed = errors.New("storage: simulated crash")

// FaultFS is a file system in memory that crashes on demand, for crash
// tests and the simulator. A crash keeps what was synced and a random
// part of the rest, as a power failure would: a prefix of each file's
// writes and truncations since its last Sync, the last write possibly cut
// short mid-record, and a prefix of the files created since their
// directory's last SyncDir. All its randomness comes from the source
// NewFaultFS is given, so a seed replays a crash exactly. It supports
// what file storage does: creating files, appending to them, truncating,
// reading them whole, listing, and syncing. A FaultFS is not safe for
// concurrent use.
type FaultFS struct {
	rand    *rand.Rand
	files   map[string]*faultFile // every file, by path
	created []string              // files created since their directory's last SyncDir, oldest first
	crashIn int                   // mutating operations left until a crash; 0 for none
	boot    int                   // crashes so far; files opened in an earlier boot are dead
}

// faultFile is one file: what is durable, and the writes and truncations
// since its last Sync, which a crash may lose.
type faultFile struct {
	synced  []byte   // durable contents
	data    []byte   // contents as reads see them: synced with pending applied
	pending []fileOp // in order
}

// fileOp is an append of data or, if truncate is set, a truncation to
// size bytes.
type fileOp struct {
	data     []byte
	truncate bool
	size     int
}

// NewFaultFS returns an empty FaultFS whose crashes draw from r.
func NewFaultFS(r *rand.Rand) *FaultFS {
	return &FaultFS{rand: r, files: map[string]*faultFile{}}
}

// CrashAfter makes the file system crash during the nth mutating
// operation from now: a write, truncation, file creation, fsync, or
// directory fsync. That operation returns ErrCrashed, and how much of it
// and of everything unsynced before it lasts is left to chance, as in a
// real crash. n = 0 cancels a pending crash.
func (f *FaultFS) CrashAfter(n int) { f.crashIn = n }

// Crash crashes the file system now: it keeps what was synced and a
// random prefix of the rest, and files opened before it become unusable.
func (f *FaultFS) Crash() {
	keep := f.rand.IntN(len(f.created) + 1)
	for _, path := range f.created[keep:] {
		delete(f.files, path)
	}
	f.created = nil
	for _, path := range slices.Sorted(maps.Keys(f.files)) {
		f.files[path].crash(f.rand)
	}
	f.crashIn = 0
	f.boot++
}

// mutated counts one mutating operation and, if CrashAfter chose it,
// crashes the file system and returns ErrCrashed. Operations that change
// something record the change before they call it, so that the crash may
// keep it or lose it; fsyncs call it before they take effect.
func (f *FaultFS) mutated() error {
	if f.crashIn == 0 {
		return nil
	}
	f.crashIn--
	if f.crashIn > 0 {
		return nil
	}
	f.Crash()
	return ErrCrashed
}

// OpenFile opens a file. Every write appends, so a file opened for
// writing needs os.O_APPEND, and os.O_TRUNC is not supported.
func (f *FaultFS) OpenFile(name string, flag int, _ fs.FileMode) (File, error) {
	writable := flag&(os.O_WRONLY|os.O_RDWR) != 0
	if flag&os.O_TRUNC != 0 || writable && flag&os.O_APPEND == 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.ErrUnsupported}
	}
	file, err := f.open(filepath.Clean(name), flag)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return &faultHandle{fs: f, file: file, boot: f.boot}, nil
}

// open returns the file at path, creating it if flag says to.
func (f *FaultFS) open(path string, flag int) (*faultFile, error) {
	file, ok := f.files[path]
	switch {
	case ok && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		return nil, fs.ErrExist
	case ok:
		return file, nil
	case flag&os.O_CREATE == 0:
		return nil, fs.ErrNotExist
	}
	file = &faultFile{}
	f.files[path] = file
	f.created = append(f.created, path)
	return file, f.mutated()
}

// ReadDir returns the names of the files in dir, sorted.
func (f *FaultFS) ReadDir(dir string) ([]string, error) {
	var names []string
	for path := range f.files {
		if filepath.Dir(path) == filepath.Clean(dir) {
			names = append(names, filepath.Base(path))
		}
	}
	slices.Sort(names)
	return names, nil
}

// SyncDir makes the creation of every file in dir durable.
func (f *FaultFS) SyncDir(dir string) error {
	if err := f.mutated(); err != nil {
		return err
	}
	f.created = slices.DeleteFunc(f.created, func(path string) bool {
		return filepath.Dir(path) == filepath.Clean(dir)
	})
	return nil
}

// crash keeps the file's synced contents and a random prefix of its
// pending operations, the last of which, if a write, may be cut short.
func (ff *faultFile) crash(r *rand.Rand) {
	if len(ff.pending) == 0 {
		return
	}
	content := slices.Clone(ff.synced)
	n := r.IntN(len(ff.pending) + 1)
	for _, op := range ff.pending[:n] {
		content = op.apply(content)
	}
	if n < len(ff.pending) && !ff.pending[n].truncate {
		torn := ff.pending[n].data
		content = append(content, torn[:r.IntN(len(torn)+1)]...)
	}
	ff.synced, ff.data, ff.pending = content, slices.Clone(content), nil
}

func (op fileOp) apply(b []byte) []byte {
	if op.truncate {
		return b[:op.size]
	}
	return append(b, op.data...)
}

// faultHandle is a file opened on a FaultFS.
type faultHandle struct {
	fs   *FaultFS
	file *faultFile
	boot int // the file system's boot when the file was opened
	off  int // where the next Read starts
}

// alive returns ErrCrashed if the file system has crashed since the file
// was opened: a crash ends the process that had it open.
func (h *faultHandle) alive() error {
	if h.boot != h.fs.boot {
		return ErrCrashed
	}
	return nil
}

func (h *faultHandle) Read(p []byte) (int, error) {
	if err := h.alive(); err != nil {
		return 0, err
	}
	if h.off >= len(h.file.data) {
		return 0, io.EOF
	}
	n := copy(p, h.file.data[h.off:])
	h.off += n
	return n, nil
}

func (h *faultHandle) Write(p []byte) (int, error) {
	if err := h.alive(); err != nil {
		return 0, err
	}
	h.file.data = append(h.file.data, p...)
	h.file.pending = append(h.file.pending, fileOp{data: slices.Clone(p)})
	if err := h.fs.mutated(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Truncate shrinks the file; a FaultFS never grows one this way.
func (h *faultHandle) Truncate(size int64) error {
	if err := h.alive(); err != nil {
		return err
	}
	if size < 0 || size > int64(len(h.file.data)) {
		return errors.ErrUnsupported
	}
	h.file.data = h.file.data[:size]
	h.file.pending = append(h.file.pending, fileOp{truncate: true, size: int(size)})
	return h.fs.mutated()
}

func (h *faultHandle) Sync() error {
	if err := h.alive(); err != nil {
		return err
	}
	if err := h.fs.mutated(); err != nil {
		return err
	}
	h.file.synced, h.file.pending = slices.Clone(h.file.data), nil
	return nil
}

func (h *faultHandle) Close() error { return h.alive() }
