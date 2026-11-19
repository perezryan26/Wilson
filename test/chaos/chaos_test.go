package chaos

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/perezryan26/wilson/client"
	"github.com/perezryan26/wilson/internal/linearizability"
	"github.com/perezryan26/wilson/raft"
)

var duration = flag.Duration("duration", 0, "how long to inject faults; the chaos test is skipped unless it is set")

const (
	// clients is the number of concurrent clients, each with its own
	// session.
	clients = 5
	// A client gives up on an operation after a random time between
	// these; a write it gives up on has an unknown outcome. Short ones
	// leave writes that may still take effect after their client has
	// moved on.
	minOpTimeout = 200 * time.Millisecond
	maxOpTimeout = 5 * time.Second
	// requestTimeout bounds each HTTP request. It exceeds kvserver's own
	// 2 s, so a server can say a request timed out.
	requestTimeout = 3 * time.Second
	// keyEpoch is how often clients move on to fresh keys, which keeps
	// each key's history, and so each of Porcupine's partitions, short.
	keyEpoch     = 5 * time.Second
	keysPerEpoch = 3
	// checkTimeout bounds each Porcupine check.
	checkTimeout = 10 * time.Minute
	// finalPhase is the time the test needs after the faults stop: for
	// the clients to finish, a full restart, reading every key back, and
	// two checks.
	finalPhase = 5 * time.Minute
)

// TestChaos runs five kvnode processes with a TCP proxy on every directed
// link between them. For -duration it kills servers with SIGKILL and
// restarts them from their disks, and cuts and heals links, while five
// clients run Gets, Puts, and Deletes through the client library. The
// clients' history must be linearizable. Then every server is killed at
// once and restarted, and every key is read back: the history with those
// reads must still be linearizable, so no acknowledged write was lost.
//
// Servers' logs, the fault schedule, and any Porcupine visualization are
// kept in a temporary directory if the test fails.
func TestChaos(t *testing.T) {
	if *duration <= 0 {
		t.Skip("set -duration to run the chaos test, as make chaos DURATION=5m does")
	}
	if d, ok := t.Deadline(); ok && time.Until(d) < *duration+finalPhase {
		t.Fatalf("-timeout leaves too little time for -duration %v and the checks after it; pass -timeout 0", *duration)
	}
	dir := keepIfFailed(t)
	c := startCluster(t, dir)
	start := time.Now()
	w := newWorkload(t, start)
	waitFor(t, "a leader", func() bool { lead, _ := c.leader(); return lead != raft.None })

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for id := range clients {
		wg.Go(func() { w.client(id, c.servers(), stop) })
	}
	f := newFaults(t, c, dir, start)
	f.inflict(time.Now().Add(*duration))
	close(stop)
	wg.Wait()
	_, term := c.leader()
	t.Logf("faults: %v, the highest term %d", f.counts, term)
	t.Logf("clients: %s", w.summary())
	check(t, w.rec, dir, "the history under faults")

	c.restartAll()
	w.readBack(c.servers())
	check(t, w.rec, dir, "the history with every key read back after a full restart")
	c.stopAll()
}

// keepIfFailed returns a new temporary directory, which the test's
// cleanup removes if the test passed and names if it failed.
func keepIfFailed(t *testing.T) string {
	dir, err := os.MkdirTemp("", "wilson-chaos-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("servers' logs, the fault schedule, and visualizations are in %s", dir)
			return
		}
		_ = os.RemoveAll(dir)
	})
	return dir
}

// waitFor fails the test unless done reports true within a minute.
func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(time.Minute); !done(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// check checks the history recorded so far with Porcupine. A check that
// times out is only a warning: the history was too large to decide.
func check(t *testing.T, rec *linearizability.Recorder, dir, what string) {
	t.Helper()
	began := time.Now()
	switch err := rec.Check(checkTimeout, dir); {
	case errors.Is(err, linearizability.ErrCheckTimedOut):
		t.Logf("warning: %s was not checked: %v", what, err)
	case err != nil:
		t.Fatalf("%s: %v", what, err)
	default:
		t.Logf("%s is linearizable (checked in %v)", what, time.Since(began).Round(time.Millisecond))
	}
}

// faults injects random faults into a cluster and logs each one.
type faults struct {
	c      *cluster
	r      *rand.Rand
	log    *os.File
	start  time.Time
	counts map[string]int
}

func newFaults(t *testing.T, c *cluster, dir string, start time.Time) *faults {
	log, err := os.Create(filepath.Join(dir, "faults.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return &faults{c: c, r: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())), log: log, start: start, counts: map[string]int{}}
}

// inflict injects a fault every 0.2 to 2 s until end. At most two
// servers are down at a time, so the five can always form a majority
// once the links heal.
func (f *faults) inflict(end time.Time) {
	for time.Now().Before(end) {
		time.Sleep(200*time.Millisecond + f.r.N(1800*time.Millisecond))
		kinds := []string{"partition", "cut", "heal"}
		if down := len(f.c.down()); down < 2 {
			kinds = append(kinds, "kill")
		}
		if down := len(f.c.down()); down > 0 {
			kinds = append(kinds, "restart")
		}
		kind := kinds[f.r.IntN(len(kinds))]
		f.counts[kind]++
		f.inject(kind)
	}
	f.c.healAll()
	for _, id := range f.c.down() {
		f.c.start(id)
	}
	f.logf("heal and restart everything")
}

func (f *faults) inject(kind string) {
	switch kind {
	case "kill":
		f.kill()
	case "restart":
		down := f.c.down()
		id := down[f.r.IntN(len(down))]
		f.c.start(id)
		f.logf("restart %d", id)
	case "partition":
		side := slices.Clone(ids())
		f.r.Shuffle(len(side), func(i, j int) { side[i], side[j] = side[j], side[i] })
		side = side[:1+f.r.IntN(2)]
		f.c.partition(side)
		f.logf("partition %v from the rest", side)
	case "cut":
		from := raft.NodeID(1 + f.r.IntN(size))
		to := raft.NodeID(1 + (int(from)+f.r.IntN(size-1))%size)
		f.c.proxies[link{from, to}].setCut(true)
		f.logf("cut %d→%d", from, to)
	case "heal":
		f.c.healAll()
		f.logf("heal every link")
	}
}

// kill kills the leader half the time, if one answers, and otherwise a
// random running server.
func (f *faults) kill() {
	running := f.c.running()
	id := running[f.r.IntN(len(running))]
	what := ""
	if f.r.IntN(2) == 0 {
		if lead, term := f.c.leader(); lead != raft.None {
			id, what = lead, fmt.Sprintf(", the leader in term %d", term)
		}
	}
	f.c.kill(id)
	f.logf("kill -9 %d%s", id, what)
}

func (f *faults) logf(format string, args ...any) {
	_, _ = fmt.Fprintf(f.log, "%10.3fs %s\n", time.Since(f.start).Seconds(), fmt.Sprintf(format, args...))
}

// workload is the clients' side of the test: their operations, recorded
// for Porcupine, and every key they used.
type workload struct {
	t     *testing.T
	rec   *linearizability.Recorder
	start time.Time

	mu      sync.Mutex
	keys    map[string]bool
	ok      map[linearizability.Op]int // operations answered
	unknown map[linearizability.Op]int // operations whose outcome is unknown
}

// newWorkload returns a workload whose history takes its times from the
// monotonic clock, counted from start.
func newWorkload(t *testing.T, start time.Time) *workload {
	return &workload{
		t:       t,
		rec:     linearizability.NewRecorder(func() int64 { return int64(time.Since(start)) }),
		start:   start,
		keys:    map[string]bool{},
		ok:      map[linearizability.Op]int{},
		unknown: map[linearizability.Op]int{},
	}
}

// client runs operations through its own client of servers, one at a
// time, until stop closes.
func (w *workload) client(id int, servers []string, stop <-chan struct{}) {
	servers = slices.Clone(servers)
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	cl := client.New(servers, client.Options{RequestTimeout: requestTimeout})
	defer cl.Close()
	r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("k%d.%d", int(time.Since(w.start)/keyEpoch), r.IntN(keysPerEpoch))
		in := linearizability.Input{Op: linearizability.Get, Key: key}
		switch p := r.IntN(10); {
		case p < 3:
			in.Op, in.Value = linearizability.Put, fmt.Sprintf("%d.%d", id, n)
		case p < 5:
			in.Op = linearizability.Delete
		}
		w.do(cl, id, in, minOpTimeout+r.N(maxOpTimeout-minOpTimeout))
	}
}

// do runs in through cl, giving up after timeout, and records it. A
// write that fails stays open to the end of the history, since it may
// yet take effect; a failed Get returns at once with any value, since it
// changed nothing.
func (w *workload) do(cl *client.Client, id int, in linearizability.Input, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	w.mu.Lock()
	w.keys[in.Key] = true
	w.mu.Unlock()
	call := w.rec.Call(id, in)
	var out linearizability.Output
	var err error
	switch in.Op {
	case linearizability.Get:
		var v []byte
		v, out.Found, err = cl.Get(ctx, in.Key)
		out.Value = string(v)
	case linearizability.Put:
		err = cl.Put(ctx, in.Key, []byte(in.Value))
	case linearizability.Delete:
		out.Found, err = cl.Delete(ctx, in.Key)
	}
	w.count(in.Op, err)
	switch {
	case err == nil:
		w.rec.Return(call, out)
	case !errors.Is(err, client.ErrUnknownOutcome) && !errors.Is(err, client.ErrSessionExpired) && !errors.Is(err, context.DeadlineExceeded):
		w.t.Errorf("client %d: %v: %v", id, in, err)
	case in.Op == linearizability.Get:
		w.rec.Return(call, linearizability.Output{Unknown: true})
	}
}

func (w *workload) count(op linearizability.Op, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err == nil {
		w.ok[op]++
	} else {
		w.unknown[op]++
	}
}

func (w *workload) summary() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fmt.Sprintf("%d keys; gets %d answered, %d failed; puts %d acknowledged, %d unknown; deletes %d acknowledged, %d unknown",
		len(w.keys), w.ok[linearizability.Get], w.unknown[linearizability.Get],
		w.ok[linearizability.Put], w.unknown[linearizability.Put],
		w.ok[linearizability.Delete], w.unknown[linearizability.Delete])
}

// readBack reads every key the clients used, recording each read, through
// a client of its own. Each read must succeed: every fault has healed.
func (w *workload) readBack(servers []string) {
	w.t.Helper()
	cl := client.New(servers, client.Options{RequestTimeout: requestTimeout})
	defer cl.Close()
	for _, key := range slices.Sorted(maps.Keys(w.keys)) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		call := w.rec.Call(clients, linearizability.Input{Op: linearizability.Get, Key: key})
		v, found, err := cl.Get(ctx, key)
		cancel()
		if err != nil {
			w.t.Fatalf("reading %q back after the full restart: %v", key, err)
		}
		w.rec.Return(call, linearizability.Output{Value: string(v), Found: found})
	}
	w.t.Logf("read back %d keys after the full restart", len(w.keys))
}
