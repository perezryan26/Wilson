package transport

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/leaktest"
	"github.com/perezryan26/wilson/raft"
)

// everyRPC returns one message of each RPC from server from to server to.
func everyRPC(from, to raft.NodeID) []raft.Message {
	return []raft.Message{
		{From: from, To: to, RequestVote: &raft.RequestVoteRequest{Term: 2, CandidateID: from, LastLogIndex: 7, LastLogTerm: 1}},
		{From: from, To: to, RequestVoteResponse: &raft.RequestVoteResponse{Term: 2, VoteGranted: true}},
		{From: from, To: to, AppendEntries: &raft.AppendEntriesRequest{
			Term: 2, LeaderID: from, PrevLogIndex: 7, PrevLogTerm: 1,
			Entries: []raft.Entry{{Index: 8, Term: 2, Data: []byte("x=1")}, {Index: 9, Term: 2}}, LeaderCommit: 7,
		}},
		{From: from, To: to, AppendEntriesResponse: &raft.AppendEntriesResponse{Term: 2, PrevLogIndex: 7, ConflictTerm: 1, ConflictIndex: 4}},
		{From: from, To: to, InstallSnapshot: &raft.InstallSnapshotRequest{
			Term: 2, LeaderID: from, LastIncludedIndex: 7, LastIncludedTerm: 1, Offset: 3, Data: []byte{0, 1, 255}, Done: true,
		}},
		{From: from, To: to, InstallSnapshotResponse: &raft.InstallSnapshotResponse{Term: 2, LastIncludedIndex: 7, Offset: 6}},
	}
}

// encode returns m as JSON, the form it travels in. Two messages are the
// same exactly when their encodings are.
func encode(t *testing.T, m raft.Message) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// serve starts an HTTP server for server 2, a peer of server 1, that
// handles each request with h, and returns its address.
func serve(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// receiver returns the handler server 2's transport serves, with each
// message it accepts going to the returned channel.
func receiver(t *testing.T) (http.Handler, <-chan raft.Message) {
	t.Helper()
	got := make(chan raft.Message, 100)
	tr := NewHTTP(2, map[raft.NodeID]string{1: "127.0.0.1:1"}, HTTPOptions{})
	t.Cleanup(tr.Close)
	return tr.Handler(func(m raft.Message) { got <- m }), got
}

// sender returns server 1's transport, sending to server 2 at addr.
func sender(t *testing.T, addr string, opts HTTPOptions) *HTTP {
	t.Helper()
	tr := NewHTTP(1, map[raft.NodeID]string{2: addr}, opts)
	t.Cleanup(tr.Close)
	return tr
}

// receive returns the next message from got, failing the test if none
// arrives within five seconds.
func receive(t *testing.T, got <-chan raft.Message) raft.Message {
	t.Helper()
	select {
	case m := <-got:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no message arrived within five seconds")
		return raft.Message{}
	}
}

// Every RPC arrives intact and in the order it was sent.
func TestSendDeliversEveryRPC(t *testing.T) {
	leaktest.Check(t)
	h, got := receiver(t)
	tr := sender(t, serve(t, h), HTTPOptions{})
	sent := everyRPC(1, 2)
	for _, m := range sent {
		tr.Send(m)
	}
	for _, want := range sent {
		if m := receive(t, got); encode(t, m) != encode(t, want) {
			t.Errorf("received %s, want %s", encode(t, m), encode(t, want))
		}
	}
}

// A peer that never answers holds up no one: Send returns at once and
// drops what its queue cannot hold, because Raft tolerates lost
// messages and a blocked Send would stall the runtime.
func TestSendNeverBlocks(t *testing.T) {
	leaktest.Check(t)
	release := make(chan struct{})
	addr := serve(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(func() { close(release) })
	tr := sender(t, addr, HTTPOptions{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 4 * queueSize {
			tr.Send(everyRPC(1, 2)[0])
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Send blocked behind a peer that does not answer")
	}
}

// A request that gets no answer times out, so the sender goes on to the
// next message, and the peer is reported unreachable once and reached
// again once.
func TestRequestTimesOut(t *testing.T) {
	leaktest.Check(t)
	h, got := receiver(t)
	release := make(chan struct{})
	var requests atomic.Int32
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			<-release
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { close(release) })
	var logs syncBuffer
	tr := sender(t, addr, HTTPOptions{Timeout: 100 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	sent := everyRPC(1, 2)
	tr.Send(sent[0])
	tr.Send(sent[1])
	if m := receive(t, got); encode(t, m) != encode(t, sent[1]) {
		t.Errorf("received %s, want the second message", encode(t, m))
	}
	waitFor(t, "the peer to be reported reached again", func() bool {
		return strings.Contains(logs.String(), "reached peer again")
	})
	if n := strings.Count(logs.String(), "cannot reach peer"); n != 1 {
		t.Errorf("reported the peer unreachable %d times, want once:\n%s", n, logs.String())
	}
}

// A peer that is down costs one log line, however many messages fail,
// and messages flow again once it is back at the same address.
func TestUnreachablePeerIsReportedOnce(t *testing.T) {
	leaktest.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	var logs syncBuffer
	tr := sender(t, addr, HTTPOptions{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	for range 50 {
		tr.Send(everyRPC(1, 2)[0])
	}
	waitFor(t, "every queued message to fail", func() bool {
		return len(tr.peers[2].queue) == 0 && strings.Contains(logs.String(), "cannot reach peer")
	})
	h, got := receiver(t)
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen again at %s: %v", addr, err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	tr.Send(everyRPC(1, 2)[1])
	receive(t, got)
	waitFor(t, "the peer to be reported reached again", func() bool {
		return strings.Contains(logs.String(), "reached peer again")
	})
	if n := strings.Count(logs.String(), "cannot reach peer"); n != 1 {
		t.Errorf("reported the peer unreachable %d times, want once:\n%s", n, logs.String())
	}
}

// The handler takes only a POST of one well-formed message, under the
// size limit, from a peer to this server, and answers 204 once it has
// handed the message over.
func TestHandlerChecksEveryMessage(t *testing.T) {
	leaktest.Check(t)
	h, got := receiver(t)
	valid := encode(t, everyRPC(1, 2)[0])
	tests := []struct {
		name, method, body string
		status             int
	}{
		{"a message", http.MethodPost, valid, http.StatusNoContent},
		{"not a POST", http.MethodGet, "", http.StatusMethodNotAllowed},
		{"over the size limit", http.MethodPost, valid + strings.Repeat(" ", MaxMessageSize), http.StatusRequestEntityTooLarge},
		{"not JSON", http.MethodPost, "vote for me", http.StatusBadRequest},
		{"trailing data", http.MethodPost, valid + "{}", http.StatusBadRequest},
		{"no RPC", http.MethodPost, `{"From":1,"To":2}`, http.StatusBadRequest},
		{"two RPCs", http.MethodPost, `{"From":1,"To":2,"RequestVote":{},"RequestVoteResponse":{}}`, http.StatusBadRequest},
		{"for another server", http.MethodPost, encode(t, everyRPC(1, 3)[0]), http.StatusBadRequest},
		{"from a server that is not a peer", http.MethodPost, encode(t, everyRPC(3, 2)[0]), http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(tt.method, MessagePath, strings.NewReader(tt.body)))
			if w.Code != tt.status {
				t.Errorf("status = %d, want %d: %s", w.Code, tt.status, w.Body)
			}
			delivered := len(got) > 0
			if delivered {
				<-got
			}
			if delivered != (tt.status == http.StatusNoContent) {
				t.Errorf("delivered = %t with status %d", delivered, w.Code)
			}
		})
	}
}

// Every message a core configured by raft.DefaultConfig can send fits in
// MaxMessageSize once encoded: the most entries the cap lets one
// AppendEntries carry, the largest command Propose accepts, and a full
// snapshot chunk. Indexes and terms sit near the top of uint64, so they
// encode as long as they can.
func TestEveryMessageFitsMaxMessageSize(t *testing.T) {
	cfg, _ := raft.DefaultConfig()
	base := raft.Index(math.MaxUint64 - 1<<22)
	entries := make([]raft.Entry, cfg.MaxAppendSize/16)
	for i := range entries {
		entries[i] = raft.Entry{Index: base + raft.Index(i+1), Term: math.MaxUint64 - 1, Data: []byte("x")}
	}
	last := base + raft.Index(len(entries))

	n := leader(t, cfg, entries)
	n.Step(raft.Message{From: 2, To: 1, AppendEntriesResponse: &raft.AppendEntriesResponse{
		Term: math.MaxUint64, PrevLogIndex: last, ConflictIndex: base + 1,
	}})
	m := onlyMessage(t, n.Ready())
	if m.AppendEntries == nil || len(m.AppendEntries.Entries) < 2 || len(m.AppendEntries.Entries) == len(entries) {
		t.Fatalf("sent %s, want AppendEntries with as many entries as the cap allows", describe(m))
	}
	wantFits(t, "the largest batch of entries", m)

	n = leader(t, cfg, nil)
	size := cfg.MaxAppendSize
	for ; size > 0; size-- {
		if _, _, err := n.Propose(make([]byte, size)); err == nil {
			break
		}
	}
	wantFits(t, "the largest command", onlyMessage(t, n.Ready()))

	n = leader(t, cfg, nil)
	n.Step(raft.Message{From: 2, To: 1, AppendEntriesResponse: &raft.AppendEntriesResponse{
		Term: math.MaxUint64, PrevLogIndex: base, ConflictIndex: 1,
	}})
	m = onlyMessage(t, n.Ready())
	if m.InstallSnapshot == nil || len(m.InstallSnapshot.Data) != cfg.SnapshotChunkSize {
		t.Fatalf("sent %s, want InstallSnapshot with a full chunk", describe(m))
	}
	wantFits(t, "a full snapshot chunk", m)
}

// leader returns server 1 of a cluster of two, configured by cfg, as the
// leader of the highest term but one a uint64 holds, restarted from a
// snapshot two chunks long through index 2^64 - 2^22 - 1 and entries
// after it, with its Ready drained.
func leader(t *testing.T, cfg raft.Config, entries []raft.Entry) *raft.Node {
	t.Helper()
	cfg.ID, cfg.Peers, cfg.Rand = 1, []raft.NodeID{1, 2}, rand.New(rand.NewPCG(1, 1))
	n := raft.NewNode(cfg, raft.RestoredState{
		HardState: raft.HardState{CurrentTerm: math.MaxUint64 - 1},
		Snapshot: &raft.Snapshot{
			LastIncludedIndex: math.MaxUint64 - 1<<22,
			LastIncludedTerm:  math.MaxUint64 - 1,
			Data:              make([]byte, 2*cfg.SnapshotChunkSize),
		},
		Entries: entries,
	})
	for tick := 0; n.Status().Role != raft.Candidate; tick++ {
		if tick == 2*cfg.ElectionTick {
			t.Fatal("no election within the longest timeout")
		}
		n.Tick()
	}
	n.Step(raft.Message{From: 2, To: 1, RequestVoteResponse: &raft.RequestVoteResponse{Term: math.MaxUint64, VoteGranted: true}})
	if st := n.Status(); st.Role != raft.Leader {
		t.Fatalf("status = %+v, want the leader", st)
	}
	n.Ready()
	return n
}

// onlyMessage returns the one message in rd.
func onlyMessage(t *testing.T, rd raft.Ready) raft.Message {
	t.Helper()
	if len(rd.Messages) != 1 {
		t.Fatalf("Ready has %d messages, want one", len(rd.Messages))
	}
	return rd.Messages[0]
}

// describe names m's RPC and its size, for failure messages.
func describe(m raft.Message) string {
	b, _ := json.Marshal(m)
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}

// wantFits checks that m, once encoded, is no larger than MaxMessageSize.
func wantFits(t *testing.T, what string, m raft.Message) {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > MaxMessageSize {
		t.Errorf("%s encodes in %d bytes, over MaxMessageSize, %d", what, len(b), MaxMessageSize)
	}
	t.Logf("%s encodes in %d bytes", what, len(b))
}

// The message decoder reads bytes from the network. It must never panic,
// and a message it accepts must encode to JSON that decodes to the same
// message.
func FuzzMessageDecode(f *testing.F) {
	for _, m := range everyRPC(1, 2) {
		b, err := json.Marshal(m)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
		f.Add(b[:len(b)/2])
	}
	f.Add([]byte(`{"From":1,"To":2,"RequestVote":{},"AppendEntries":{}}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := decodeMessage(b)
		if err != nil {
			return
		}
		enc, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("message decoded from %q does not encode: %v", b, err)
		}
		again, err := decodeMessage(enc)
		if err != nil {
			t.Fatalf("%s, encoded from a decoded message, does not decode: %v", enc, err)
		}
		if enc2, _ := json.Marshal(again); !bytes.Equal(enc, enc2) {
			t.Fatalf("message decoded from %q encodes as %s, then as %s", b, enc, enc2)
		}
	})
}

// waitFor fails the test unless done reports true within five seconds,
// asking every few milliseconds.
func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !done(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// syncBuffer is a bytes.Buffer that a sender goroutine can log to while
// the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
