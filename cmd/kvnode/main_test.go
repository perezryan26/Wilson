package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/perezryan26/wilson/raft"
)

func TestParseFlags(t *testing.T) {
	peers := "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"
	o, err := parseFlags([]string{"-id", "2", "-peers", peers, "-data", "d"})
	if err != nil {
		t.Fatal(err)
	}
	if o.id != 2 || len(o.peers) != 3 || o.peers[3] != "127.0.0.1:8003" || o.listen != "127.0.0.1:8002" || o.compactEvery != 1000 {
		t.Errorf("options = %+v, want server 2 of three listening at its own address", o)
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"no ID", []string{"-peers", peers, "-data", "d"}, "-id 0"},
		{"ID not a peer", []string{"-id", "4", "-peers", peers, "-data", "d"}, "-id 4"},
		{"no peers", []string{"-id", "1", "-data", "d"}, "-peers entry"},
		{"peer ID 0", []string{"-id", "1", "-peers", "0=a:1,1=b:1", "-data", "d"}, "-peers entry"},
		{"peer without an address", []string{"-id", "1", "-peers", "1=", "-data", "d"}, "-peers entry"},
		{"peer twice", []string{"-id", "1", "-peers", "1=a:1,1=b:1", "-data", "d"}, "twice"},
		{"no data directory", []string{"-id", "1", "-peers", peers}, "-data"},
		{"unknown preset", []string{"-id", "1", "-peers", peers, "-data", "d", "-preset", "fast"}, "-preset"},
		{"negative compaction threshold", []string{"-id", "1", "-peers", peers, "-data", "d", "-compact-every", "-1"}, "-compact-every"},
		{"extra argument", []string{"-id", "1", "-peers", peers, "-data", "d", "now"}, "unexpected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseFlags(tt.args); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want one about %s", err, tt.want)
			}
		})
	}
}

// kvnode creates its data directory, because file storage needs one that
// exists, and accepts one that already exists; a missing parent is the
// operator's to create.
func TestCreateDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "n1")
	for range 2 {
		if err := createDataDir(dir + "/"); err != nil {
			t.Fatal(err)
		}
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Fatalf("%s is not a directory: %v", dir, err)
		}
	}
	if err := createDataDir(filepath.Join(t.TempDir(), "missing", "n1")); err == nil {
		t.Error("created a data directory whose parent is missing")
	}
}

// The state machine kvnode replicates until the key-value store exists
// restores exactly what it snapshotted.
func TestCommandLogRestoresSnapshot(t *testing.T) {
	var l commandLog
	l.Apply(raft.Entry{Index: 1, Data: []byte("a")})
	l.Apply(raft.Entry{Index: 2, Data: []byte("b")})
	data, err := l.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var restored commandLog
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	if got := restored.list(); strings.Join(got, ",") != "a,b" {
		t.Errorf("restored %v, want [a b]", got)
	}
	if err := restored.Restore([]byte("not json")); err == nil {
		t.Error("restored a snapshot that is not JSON")
	}
}
