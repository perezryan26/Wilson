package storage

import (
	"io"
	"io/fs"
	"os"
)

// FS is the part of a file system that file storage uses. File storage
// goes through it rather than package os, so that tests and the simulator
// can substitute a file system that crashes at a chosen moment and loses
// whatever was not yet synced.
type FS interface {
	// OpenFile opens a file with the flags and permissions of os.OpenFile.
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	// ReadDir returns the names of the entries in dir, sorted.
	ReadDir(dir string) ([]string, error)
	// SyncDir makes dir's entries durable. A file created in dir survives a
	// crash only once SyncDir has returned, even if its contents were
	// synced.
	SyncDir(dir string) error
}

// File is an open file. *os.File implements it.
type File interface {
	io.Reader
	io.Writer
	io.Closer
	// Sync makes the file's contents durable: what was written survives a
	// crash only once Sync has returned.
	Sync() error
	// Truncate changes the file's size.
	Truncate(size int64) error
}

// OSFS is the operating system's file system.
type OSFS struct{}

// OpenFile calls os.OpenFile.
func (OSFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		// A nil *os.File inside a File would not compare equal to nil.
		return nil, err
	}
	return f, nil
}

// ReadDir returns the names of the entries in dir, sorted.
func (OSFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names, nil
}

// SyncDir opens dir and fsyncs it.
func (OSFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
