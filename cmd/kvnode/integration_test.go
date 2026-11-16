package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/perezryan26/wilson/client"
	"github.com/perezryan26/wilson/raft"
)

// kvnodeBinary is the kvnode the integration tests run, built once per
// test process, with the race detector when the tests run with it.
var kvnodeBinary struct {
	once sync.Once
	dir  string
	path string
	err  error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if kvnodeBinary.dir != "" {
		_ = os.RemoveAll(kvnodeBinary.dir)
	}
	os.Exit(code)
}

// buildKvnode returns the path of the kvnode binary, building it first if
// no test has.
func buildKvnode(t *testing.T) string {
	t.Helper()
	kvnodeBinary.once.Do(func() {
		kvnodeBinary.dir, kvnodeBinary.err = os.MkdirTemp("", "kvnode-test-")
		if kvnodeBinary.err != nil {
			return
		}
		kvnodeBinary.path = filepath.Join(kvnodeBinary.dir, "kvnode")
		args := []string{"build", "-o", kvnodeBinary.path}
		if raceEnabled() {
			args = append(args, "-race")
		}
		if out, err := exec.Command("go", append(args, ".")...).CombinedOutput(); err != nil {
			kvnodeBinary.err = fmt.Errorf("go build: %w\n%s", err, out)
		}
	})
	if kvnodeBinary.err != nil {
		t.Fatal(kvnodeBinary.err)
	}
	return kvnodeBinary.path
}

// raceEnabled reports whether this test binary was built with -race.
func raceEnabled() bool {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "-race" {
				return s.Value == "true"
			}
		}
	}
	return false
}

// testCluster is a cluster of kvnode processes on 127.0.0.1, each with
// its data directory and log file in the test's temporary directory. The
// test's cleanup kills every process still running, before the temporary
// directory is removed, whether the test passed or failed.
type testCluster struct {
	t       *testing.T
	bin     string
	dir     string
	addrs   map[raft.NodeID]string
	peers   string                    // the -peers flag
	procs   map[raft.NodeID]*exec.Cmd // the running processes
	client  *client.Client
	written map[string]string // every acknowledged write
	// deadline comes well before go test's -timeout, which panics without
	// running cleanups and would leave the servers running: every wait
	// gives up by then, so the test fails and its cleanup kills them.
	deadline time.Time
}

// startCluster starts servers 1 to size, each compacting its log every
// four entries.
func startCluster(t *testing.T, size int) *testCluster {
	c := &testCluster{
		t:       t,
		bin:     buildKvnode(t),
		dir:     t.TempDir(),
		addrs:   map[raft.NodeID]string{},
		procs:   map[raft.NodeID]*exec.Cmd{},
		written: map[string]string{},
	}
	if d, ok := t.Deadline(); ok {
		c.deadline = d.Add(-30 * time.Second)
	}
	t.Cleanup(c.cleanup)
	var peers []string
	for id := range raft.NodeID(size) {
		c.addrs[id+1] = freeAddr(t, c.addrs)
		peers = append(peers, fmt.Sprintf("%d=%s", id+1, c.addrs[id+1]))
	}
	c.peers = strings.Join(peers, ",")
	var servers []string
	for id := range raft.NodeID(size) {
		servers = append(servers, c.addrs[id+1])
		c.start(id + 1)
	}
	c.client = client.New(servers, client.Options{})
	return c
}

// freeAddr returns an address on 127.0.0.1 that nothing listens on and
// that used does not hold. Its port is below the operating systems'
// ranges for outgoing connections, so no client socket takes it before
// the server that is meant to.
func freeAddr(t *testing.T, used map[raft.NodeID]string) string {
	t.Helper()
	for range 100 {
		addr := fmt.Sprintf("127.0.0.1:%d", 20000+rand.IntN(10000))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			continue
		}
		_ = ln.Close()
		taken := false
		for _, a := range used {
			taken = taken || a == addr
		}
		if !taken {
			return addr
		}
	}
	t.Fatal("found no free port")
	return ""
}

// start starts server id, appending its output to its log file.
func (c *testCluster) start(id raft.NodeID) {
	c.t.Helper()
	log, err := os.OpenFile(c.logPath(id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(c.bin, "-id", fmt.Sprint(id), "-peers", c.peers,
		"-data", filepath.Join(c.dir, fmt.Sprintf("n%d", id)), "-compact-every", "4")
	cmd.Stdout, cmd.Stderr = log, log
	err = cmd.Start()
	// The server has its own copy of the file now.
	_ = log.Close()
	if err != nil {
		c.t.Fatal(err)
	}
	c.procs[id] = cmd
}

func (c *testCluster) logPath(id raft.NodeID) string {
	return filepath.Join(c.dir, fmt.Sprintf("n%d.log", id))
}

// kill kills server id with SIGKILL, as kill -9 would.
func (c *testCluster) kill(id raft.NodeID) {
	c.t.Helper()
	cmd := c.procs[id]
	delete(c.procs, id)
	if err := cmd.Process.Kill(); err != nil {
		c.t.Fatal(err)
	}
	_ = cmd.Wait()
}

// stopAll stops every server with SIGTERM and checks that each shuts down
// cleanly, which with -race also means it saw no data race.
func (c *testCluster) stopAll() {
	c.t.Helper()
	for _, cmd := range c.procs {
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	for id, cmd := range c.procs {
		exited := make(chan error, 1)
		go func() { exited <- cmd.Wait() }()
		select {
		case err := <-exited:
			if err != nil {
				c.t.Errorf("server %d: %v after SIGTERM", id, err)
			}
		case <-time.After(time.Until(c.within(20 * time.Second))):
			_ = cmd.Process.Kill()
			c.t.Errorf("server %d still ran 20 s after SIGTERM, until killed: %v", id, <-exited)
		}
		delete(c.procs, id)
	}
}

// cleanup kills the servers a failed test left running, and fails the
// test if a server reported a data race. If the test failed, it logs
// every server's output.
func (c *testCluster) cleanup() {
	for id := range c.procs {
		c.kill(id)
	}
	if c.client != nil {
		c.client.Close()
	}
	for id := range raft.NodeID(len(c.addrs)) {
		out, err := os.ReadFile(c.logPath(id + 1))
		if err != nil {
			c.t.Errorf("server %d's log: %v", id+1, err)
			continue
		}
		if strings.Contains(string(out), "DATA RACE") {
			c.t.Errorf("server %d reported a data race", id+1)
		}
		if c.t.Failed() {
			c.t.Logf("server %d's log:\n%s", id+1, out)
		}
	}
}

// status asks server id for its status.
func (c *testCluster) status(id raft.NodeID) (client.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return c.client.Status(ctx, c.addrs[id])
}

// waitFor fails the test unless done reports true within twenty seconds,
// or by the test's deadline, asking every ten milliseconds.
func (c *testCluster) waitFor(what string, done func() bool) {
	c.t.Helper()
	for deadline := c.within(20 * time.Second); !done(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// within returns the time d from now, or the test's deadline if that
// comes first.
func (c *testCluster) within(d time.Duration) time.Time {
	t := time.Now().Add(d)
	if !c.deadline.IsZero() && c.deadline.Before(t) {
		return c.deadline
	}
	return t
}

// leader waits until every running server follows one running leader in
// one term, and returns the leader, the term, and its status.
func (c *testCluster) leader() (raft.NodeID, raft.Term, client.Status) {
	c.t.Helper()
	var lead raft.NodeID
	var term raft.Term
	var st client.Status
	c.waitFor("an agreed leader", func() bool {
		lead, term = raft.None, 0
		for id := range c.procs {
			s, err := c.status(id)
			if err != nil || s.Leader == 0 || lead != raft.None && (raft.NodeID(s.Leader) != lead || raft.Term(s.Term) != term) {
				return false
			}
			lead, term = raft.NodeID(s.Leader), raft.Term(s.Term)
			if id == lead {
				st = s
			}
		}
		return c.procs[lead] != nil && st.Role == raft.Leader.String()
	})
	return lead, term, st
}

// put writes key=value through the client, which finds the leader and
// retries, and records it as acknowledged.
func (c *testCluster) put(key, value string) {
	c.t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), c.within(20*time.Second))
	defer cancel()
	if err := c.client.Put(ctx, key, []byte(value)); err != nil {
		c.t.Fatalf("Put(%s): %v", key, err)
	}
	c.written[key] = value
}

// checkWritten reads every acknowledged write back through the client.
func (c *testCluster) checkWritten() {
	c.t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), c.within(20*time.Second))
	defer cancel()
	for key, want := range c.written {
		got, found, err := c.client.Get(ctx, key)
		if err != nil || !found || string(got) != want {
			c.t.Errorf("Get(%s) = %q, %t, %v, want %q", key, got, found, err, want)
		}
	}
}

// A cluster of three kvnode processes elects a leader and takes writes
// through the client library. Killed with SIGKILL, the leader is replaced
// by one of the other two, which go on taking writes. Restarted from its
// disk, the old leader catches up through the new leader's snapshot, sent
// over HTTP, since every server compacts its log every four entries.
// Every acknowledged write reads back, and again after the whole cluster
// shuts down cleanly on SIGTERM and restarts from its disks, when each
// new leader's no-op lets it serve them at once.
func TestIntegrationKillLeader(t *testing.T) {
	c := startCluster(t, 3)
	for i := range 6 {
		c.put(fmt.Sprintf("before-%d", i), fmt.Sprint(i))
	}
	old, oldTerm, _ := c.leader()
	c.kill(old)
	if lead, term, _ := c.leader(); lead == old || term <= oldTerm {
		t.Fatalf("server %d leads term %d after server %d, leader of term %d, was killed", lead, term, old, oldTerm)
	}
	for i := range 6 {
		c.put(fmt.Sprintf("after-%d", i), fmt.Sprint(i))
	}
	c.start(old)
	_, _, lead := c.leader()
	c.waitFor(fmt.Sprintf("server %d to apply index %d", old, lead.CommitIndex), func() bool {
		st, err := c.status(old)
		return err == nil && st.LastApplied >= lead.CommitIndex
	})
	if !c.logged(old, "installed the leader's snapshot") {
		t.Errorf("server %d logged no snapshot install, with its node, term, and role", old)
	}
	c.checkWritten()
	c.stopAll()
	for id := range raft.NodeID(3) {
		c.start(id + 1)
	}
	c.checkWritten()
	c.stopAll()
}

// logged reports whether server id logged a line holding msg, with its
// node, term, and role.
func (c *testCluster) logged(id raft.NodeID, msg string) bool {
	c.t.Helper()
	log, err := os.ReadFile(c.logPath(id))
	if err != nil {
		c.t.Fatal(err)
	}
	for _, line := range strings.Split(string(log), "\n") {
		if strings.Contains(line, msg) && strings.Contains(line, fmt.Sprintf("node=%d ", id)) &&
			strings.Contains(line, "term=") && strings.Contains(line, "role=") {
			return true
		}
	}
	return false
}
