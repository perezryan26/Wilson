package client_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/perezryan26/wilson/client"
	"github.com/perezryan26/wilson/internal/kvtest"
	"github.com/perezryan26/wilson/internal/leaktest"
	"github.com/perezryan26/wilson/raft"
)

// newClient returns a client of c, whose cleanup closes its connections.
func newClient(t *testing.T, c *kvtest.Cluster) *client.Client {
	cl := client.New(c.Addrs(), client.Options{})
	t.Cleanup(cl.Close)
	return cl
}

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func wantValue(t *testing.T, cl *client.Client, key, want string) {
	t.Helper()
	value, found, err := cl.Get(ctx(t), key)
	switch {
	case err != nil:
		t.Fatalf("Get(%q): %v", key, err)
	case want == "" && found:
		t.Fatalf("Get(%q) = %q, want no such key", key, value)
	case want != "" && string(value) != want:
		t.Fatalf("Get(%q) = %q, %t, want %q", key, value, found, want)
	}
}

// Put, Get, and Delete do what they say, in both read modes, for keys
// with slashes, spaces, and percent signs, and a Delete says whether the
// key existed.
func TestPutGetDelete(t *testing.T) {
	for _, log := range []bool{false, true} {
		t.Run(fmt.Sprintf("ReadThroughLog=%t", log), func(t *testing.T) {
			leaktest.Check(t)
			cl := newClient(t, kvtest.Start(t, kvtest.Options{Size: 3, ReadThroughLog: log}))
			for _, key := range []string{"x", "a/b", "with space", "100%", "?#"} {
				if err := cl.Put(ctx(t), key, []byte("v-"+key)); err != nil {
					t.Fatalf("Put(%q): %v", key, err)
				}
				wantValue(t, cl, key, "v-"+key)
			}
			for _, want := range []bool{true, false} {
				if existed, err := cl.Delete(ctx(t), "x"); err != nil || existed != want {
					t.Fatalf("Delete = %t, %v, want %t", existed, err, want)
				}
			}
			wantValue(t, cl, "x", "")
		})
	}
}

// A client finds the leader past a stopped server and through a
// follower's hint, even one that knows only that follower's address.
func TestFindsLeader(t *testing.T) {
	leaktest.Check(t)
	c := kvtest.Start(t, kvtest.Options{Size: 3})
	lead := c.Leader()
	stopped, follower := lead%3+1, (lead+1)%3+1
	c.Stop(stopped)
	for _, servers := range [][]string{
		{c.Addr(stopped), c.Addr(follower), c.Addr(lead)},
		{c.Addr(follower)},
	} {
		cl := client.New(servers, client.Options{})
		t.Cleanup(cl.Close)
		if err := cl.Put(ctx(t), "x", []byte("1")); err != nil {
			t.Fatalf("a client of %v: %v", servers, err)
		}
		wantValue(t, cl, "x", "1")
	}
}

// A write under way when its leader is cut off is sent again, with the
// same number, until the rest elect a new leader, which applies it. When
// the old leader rejoins and catches up, the copy it took has been
// discarded, never applied: a later write to the key stands.
func TestWriteSurvivesLeaderChange(t *testing.T) {
	leaktest.Check(t)
	c := kvtest.Start(t, kvtest.Options{Size: 3, Timeout: 200 * time.Millisecond})
	a, b := newClient(t, c), newClient(t, c)
	if err := a.Put(ctx(t), "x", []byte("1")); err != nil {
		t.Fatal(err)
	}
	wantValue(t, a, "x", "1")
	old := c.Leader()
	c.Cut(old, true)
	if existed, err := a.Delete(ctx(t), "x"); err != nil || !existed {
		t.Fatalf("Delete across the leader change = %t, %v, want true", existed, err)
	}
	if lead := c.Leader(); lead == old {
		t.Fatalf("server %d still leads", lead)
	}
	c.Cut(old, false)
	if err := b.Put(ctx(t), "x", []byte("2")); err != nil {
		t.Fatal(err)
	}
	lead := c.Leader()
	for deadline := time.Now().Add(10 * time.Second); c.Node(old).Status().LastApplied < c.Node(lead).Status().CommitIndex; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the old leader did not catch up within ten seconds")
		}
	}
	wantValue(t, b, "x", "2")
}

// A client whose session the servers evicted gets ErrSessionExpired for
// the write that found it gone, which does not run, and starts a new
// session for its next write.
func TestSessionExpires(t *testing.T) {
	leaktest.Check(t)
	c := kvtest.Start(t, kvtest.Options{Size: 3, MaxSessions: 1})
	a, b := newClient(t, c), newClient(t, c)
	if err := a.Put(ctx(t), "x", []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx(t), "y", []byte("b1")); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx(t), "x", []byte("a2")); !errors.Is(err, client.ErrSessionExpired) {
		t.Fatalf("Put from an evicted session: error %v, want ErrSessionExpired", err)
	}
	wantValue(t, a, "x", "a1")
	if err := a.Put(ctx(t), "x", []byte("a3")); err != nil {
		t.Fatalf("Put in a new session: %v", err)
	}
	wantValue(t, a, "x", "a3")
}

// When no majority can be reached, a write gives up once its context
// ends, saying that its outcome is unknown, and a read with the context's
// error.
func TestNoMajority(t *testing.T) {
	leaktest.Check(t)
	c := kvtest.Start(t, kvtest.Options{Size: 3, Timeout: 100 * time.Millisecond})
	cl := newClient(t, c)
	if err := cl.Put(ctx(t), "x", []byte("1")); err != nil {
		t.Fatal(err)
	}
	for id := raft.NodeID(1); id <= 3; id++ {
		c.Cut(id, true)
	}
	short, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cl.Put(short, "x", []byte("2")); !errors.Is(err, client.ErrUnknownOutcome) {
		t.Errorf("Put without a majority: error %v, want ErrUnknownOutcome", err)
	}
	if _, _, err := cl.Get(short, "x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Get without a majority: error %v, want the context's", err)
	}
}

// One client used from many goroutines at once: reads run in parallel,
// writes one at a time, and every write lands.
func TestConcurrentUse(t *testing.T) {
	leaktest.Check(t)
	cl := newClient(t, kvtest.Start(t, kvtest.Options{Size: 3}))
	var wg sync.WaitGroup
	errs := make(chan error, 8*10)
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 10 {
				key := fmt.Sprintf("g%d", g)
				if err := cl.Put(ctx(t), key, []byte(fmt.Sprint(i))); err != nil {
					errs <- err
					continue
				}
				if v, _, err := cl.Get(ctx(t), key); err != nil || string(v) != fmt.Sprint(i) {
					errs <- fmt.Errorf("Get(%s) = %q, %v after writing %d", key, v, err, i)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Status reports a server's raft status.
func TestStatus(t *testing.T) {
	leaktest.Check(t)
	c := kvtest.Start(t, kvtest.Options{Size: 3})
	lead := c.Leader()
	cl := newClient(t, c)
	st, err := cl.Status(ctx(t), c.Addr(lead))
	if err != nil {
		t.Fatal(err)
	}
	if st.ID != uint64(lead) || st.Role != "leader" || st.LeaderAddress != c.Addr(lead) {
		t.Errorf("the leader's status = %+v", st)
	}
}
