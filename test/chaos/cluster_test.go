package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/perezryan26/wilson/client"
	"github.com/perezryan26/wilson/raft"
)

// size is the number of servers.
const size = 5

// cluster is size kvnode processes on 127.0.0.1 with a proxy on every
// directed link: server i sends to server j through proxy i→j, because
// i's -peers names that proxy as j's address, so cutting the proxy drops
// what i sends j and nothing else. Clients reach each server at its own
// address. A leader hint names the proxy the hinting server sends
// through, so a client that follows one may cross a link that is cut.
//
// Odd-numbered servers serve reads through ReadIndex and even-numbered
// ones through the log, and each compacts at its own threshold, so both
// read paths and snapshot transfers run under every fault.
type cluster struct {
	t       *testing.T
	bin     string
	dir     string
	addrs   map[raft.NodeID]string // where each server listens
	proxies map[link]*proxy
	procs   map[raft.NodeID]*exec.Cmd // the running servers
	status  *client.Client            // asks for servers' status
}

// link is the direction from one server to another.
type link struct{ from, to raft.NodeID }

func ids() []raft.NodeID {
	var all []raft.NodeID
	for id := range raft.NodeID(size) {
		all = append(all, id+1)
	}
	return all
}

// startCluster starts every server and proxy, keeping data and logs in
// dir. The test's cleanup kills the servers still running and stops the
// proxies.
func startCluster(t *testing.T, dir string) *cluster {
	c := &cluster{
		t:       t,
		bin:     buildKvnode(t, dir),
		dir:     dir,
		addrs:   map[raft.NodeID]string{},
		proxies: map[link]*proxy{},
		procs:   map[raft.NodeID]*exec.Cmd{},
		status:  client.New(nil, client.Options{RequestTimeout: time.Second}),
	}
	t.Cleanup(c.cleanup)
	for _, id := range ids() {
		c.addrs[id] = freeAddr(t, c.addrs)
	}
	for _, from := range ids() {
		for _, to := range ids() {
			if from == to {
				continue
			}
			p, err := newProxy(c.addrs[to])
			if err != nil {
				t.Fatal(err)
			}
			c.proxies[link{from, to}] = p
		}
	}
	for _, id := range ids() {
		c.start(id)
	}
	return c
}

// buildKvnode builds kvnode into dir, with the race detector when this
// test runs with it, and returns its path.
func buildKvnode(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "kvnode")
	args := []string{"build", "-o", bin}
	if raceEnabled() {
		args = append(args, "-race")
	}
	args = append(args, "github.com/perezryan26/wilson/cmd/kvnode")
	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
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

// freeAddr returns an address on 127.0.0.1 that nothing listens on and
// that used does not hold. Its port is below the operating systems'
// ranges for outgoing connections, so no client socket takes it while
// its server is down.
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

// servers returns every server's address, for clients.
func (c *cluster) servers() []string {
	var addrs []string
	for _, id := range ids() {
		addrs = append(addrs, c.addrs[id])
	}
	return addrs
}

// start starts server id from its data directory, appending its output
// to its log file. It listens on its own address in its -peers.
func (c *cluster) start(id raft.NodeID) {
	c.t.Helper()
	peers := []string{fmt.Sprintf("%d=%s", id, c.addrs[id])}
	for _, to := range ids() {
		if to != id {
			peers = append(peers, fmt.Sprintf("%d=%s", to, c.proxies[link{id, to}].addr()))
		}
	}
	reads := "index"
	if id%2 == 0 {
		reads = "log"
	}
	log, err := os.OpenFile(c.logPath(id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(c.bin, "-id", fmt.Sprint(id), "-peers", strings.Join(peers, ","),
		"-data", filepath.Join(c.dir, fmt.Sprintf("n%d", id)),
		"-compact-every", fmt.Sprint(10<<(id-1)), "-reads", reads)
	cmd.Stdout, cmd.Stderr = log, log
	err = cmd.Start()
	// The server has its own copy of the file now.
	_ = log.Close()
	if err != nil {
		c.t.Fatal(err)
	}
	c.procs[id] = cmd
}

func (c *cluster) logPath(id raft.NodeID) string {
	return filepath.Join(c.dir, fmt.Sprintf("n%d.log", id))
}

// kill kills server id with SIGKILL, as kill -9 would. It fails the
// test if the server had already exited on its own, as no fault the test
// injects should stop one: not a panic, and not a refusal to recover.
func (c *cluster) kill(id raft.NodeID) {
	cmd := c.procs[id]
	delete(c.procs, id)
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		c.t.Errorf("server %d exited on its own (%v); see %s", id, cmd.ProcessState, c.logPath(id))
	}
}

// down returns the servers not running, in order.
func (c *cluster) down() []raft.NodeID {
	var ds []raft.NodeID
	for _, id := range ids() {
		if c.procs[id] == nil {
			ds = append(ds, id)
		}
	}
	return ds
}

// running returns the servers running, in order.
func (c *cluster) running() []raft.NodeID {
	var rs []raft.NodeID
	for _, id := range ids() {
		if c.procs[id] != nil {
			rs = append(rs, id)
		}
	}
	return rs
}

// leader returns the running server that says it leads the highest term,
// or raft.None, and the highest term any running server reports.
func (c *cluster) leader() (raft.NodeID, raft.Term) {
	lead, leadTerm, top := raft.None, raft.Term(0), raft.Term(0)
	for _, id := range c.running() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		st, err := c.status.Status(ctx, c.addrs[id])
		cancel()
		if err != nil {
			continue
		}
		term := raft.Term(st.Term)
		top = max(top, term)
		if st.Role == raft.Leader.String() && term >= leadTerm {
			lead, leadTerm = id, term
		}
	}
	return lead, top
}

// healAll heals every link.
func (c *cluster) healAll() {
	for _, p := range c.proxies {
		p.setCut(false)
	}
}

// partition heals every link, then cuts every link between side and the
// other servers, both ways.
func (c *cluster) partition(side []raft.NodeID) {
	c.healAll()
	for l, p := range c.proxies {
		if slices.Contains(side, l.from) != slices.Contains(side, l.to) {
			p.setCut(true)
		}
	}
}

// restartAll kills every running server with SIGKILL, then starts them
// all from their disks.
func (c *cluster) restartAll() {
	for _, id := range c.running() {
		c.kill(id)
	}
	for _, id := range ids() {
		c.start(id)
	}
}

// stopAll stops every server with SIGTERM and checks that each shuts down
// cleanly, which with -race also means it saw no data race.
func (c *cluster) stopAll() {
	c.t.Helper()
	for _, id := range c.running() {
		_ = c.procs[id].Process.Signal(syscall.SIGTERM)
	}
	for _, id := range c.running() {
		cmd := c.procs[id]
		exited := make(chan error, 1)
		go func() { exited <- cmd.Wait() }()
		select {
		case err := <-exited:
			if err != nil {
				c.t.Errorf("server %d: %v after SIGTERM", id, err)
			}
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
			c.t.Errorf("server %d still ran 20 s after SIGTERM, until killed: %v", id, <-exited)
		}
		delete(c.procs, id)
	}
}

// cleanup kills the servers still running, stops the proxies, and fails
// the test if a server reported a data race, in any of its lives.
func (c *cluster) cleanup() {
	for _, id := range c.running() {
		c.kill(id)
	}
	for _, p := range c.proxies {
		p.close()
	}
	c.status.Close()
	for _, id := range ids() {
		out, err := os.ReadFile(c.logPath(id))
		if err != nil {
			c.t.Errorf("server %d's log: %v", id, err)
			continue
		}
		if strings.Contains(string(out), "DATA RACE") {
			c.t.Errorf("server %d reported a data race; see %s", id, c.logPath(id))
		}
	}
}
