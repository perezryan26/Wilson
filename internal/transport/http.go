package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/perezryan26/wilson/raft"
)

// MessagePath is the path at which a server accepts raft messages.
const MessagePath = "/raft/v1/message"

// MaxMessageSize is the largest request body, in bytes, that a server
// accepts at MessagePath. raft.DefaultConfig caps the entries and the
// snapshot chunk in one message at 1 MiB, and JSON's base64 makes them a
// third larger, so every message that config lets the core send fits
// with room to spare.
const MaxMessageSize = 4 << 20

// queueSize is how many messages can wait for one peer. Send drops a
// message for a peer whose queue is full.
const queueSize = 256

// HTTP is a Transport that sends each message as JSON in a POST to the
// receiving server's MessagePath (docs/adr/0002-http-json-transport.md).
// Each peer has one sender goroutine fed by a bounded queue, so messages
// to a peer leave in the order they were sent, and a slow or dead peer
// costs dropped messages, never a blocked Send. Close stops the senders.
type HTTP struct {
	id     raft.NodeID
	peers  map[raft.NodeID]*peer
	client *http.Client
	logger *slog.Logger

	ctx    context.Context // cancelled by Close, which ends every request
	cancel context.CancelFunc
	wg     sync.WaitGroup // the sender goroutines
}

// peer is a server that messages are sent to.
type peer struct {
	id    raft.NodeID
	url   string
	queue chan raft.Message
	// down records that the last send failed, so that a peer that stays
	// unreachable is reported once rather than at every heartbeat. Only
	// the peer's sender goroutine touches it.
	down bool
}

// HTTPOptions tune NewHTTP. The zero value is ready to use.
type HTTPOptions struct {
	// Timeout bounds each request, so a peer that never answers holds up
	// its sender for at most that long. Zero means one second.
	Timeout time.Duration
	// Logger receives a line when a peer becomes unreachable and when it
	// is reached again. Nil means slog.Default().
	Logger *slog.Logger
}

// NewHTTP returns a transport for server id that sends to peers, which
// maps every other server's ID to its address as host:port, and starts
// one sender goroutine per peer. Close stops them.
func NewHTTP(id raft.NodeID, peers map[raft.NodeID]string, opts HTTPOptions) *HTTP {
	if opts.Timeout <= 0 {
		opts.Timeout = time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	// A connection pool of its own, so that Close closes only this
	// transport's connections, and no proxy: messages go straight to
	// the peer.
	pool := http.DefaultTransport.(*http.Transport).Clone()
	pool.Proxy = nil
	t := &HTTP{
		id:     id,
		peers:  map[raft.NodeID]*peer{},
		client: &http.Client{Transport: pool, Timeout: opts.Timeout},
		logger: opts.Logger,
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	for pid, addr := range peers {
		p := &peer{id: pid, url: "http://" + addr + MessagePath, queue: make(chan raft.Message, queueSize)}
		t.peers[pid] = p
		t.wg.Add(1)
		go t.run(p)
	}
	return t
}

// Send queues m for its receiver's sender goroutine and returns at once.
// It drops m if that queue is full, as a congested network would, rather
// than hold up the runtime behind a slow peer.
func (t *HTTP) Send(m raft.Message) {
	p := t.peers[m.To]
	if p == nil {
		t.logger.Error("transport: no address for the receiver", "to", m.To)
		return
	}
	select {
	case p.queue <- m:
	default:
	}
}

// Close stops the sender goroutines, abandoning the requests under way
// and the messages still queued, and closes idle connections. Nothing
// sent afterwards leaves.
func (t *HTTP) Close() {
	t.cancel()
	t.wg.Wait()
	t.client.CloseIdleConnections()
}

// run sends p's queued messages one at a time until Close.
func (t *HTTP) run(p *peer) {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case m := <-p.queue:
			t.deliver(p, m)
		}
	}
}

// deliver posts m to p, and logs when p becomes unreachable and when it
// is reached again. A message that fails is not retried: Raft resends
// what it still needs.
func (t *HTTP) deliver(p *peer, m raft.Message) {
	err := t.post(p.url, m)
	switch {
	case t.ctx.Err() != nil:
		// Close cut the request short; the peer is not at fault.
	case err != nil && !p.down:
		p.down = true
		t.logger.Warn("transport: cannot reach peer", "peer", p.id, "err", err)
	case err == nil && p.down:
		p.down = false
		t.logger.Info("transport: reached peer again", "peer", p.id)
	}
}

// post sends m to url, and returns an error unless the receiver answers
// 204 No Content, which means it took the message.
func (t *HTTP) post(url string, m raft.Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(t.ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	// Reading the body to its end lets the connection carry the next
	// request.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("%s answered %s", url, resp.Status)
	}
	return nil
}

// Handler returns the handler for POST requests to MessagePath. It
// decodes each message, checks that it carries one RPC and comes from a
// peer to this server, hands it to deliver, and answers 204 No Content.
// A body over MaxMessageSize gets 413 and any other bad request 400.
// deliver must not block: a runtime that cannot take a message drops it,
// as the network might have.
func (t *HTTP) Handler(deliver func(raft.Message)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+MessagePath, func(w http.ResponseWriter, r *http.Request) {
		m, status, err := t.read(w, r)
		if err != nil {
			http.Error(w, err.Error(), status)
			return
		}
		deliver(m)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// read reads, decodes, and checks the message in r's body. On failure it
// also returns the status to answer with.
func (t *HTTP) read(w http.ResponseWriter, r *http.Request) (raft.Message, int, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxMessageSize))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return raft.Message{}, http.StatusRequestEntityTooLarge, err
	}
	if err != nil {
		return raft.Message{}, http.StatusBadRequest, err
	}
	m, err := decodeMessage(body)
	if err == nil {
		err = t.checkAddress(m)
	}
	return m, http.StatusBadRequest, err
}

// decodeMessage decodes a message from JSON and checks that it carries
// exactly one RPC, as every message the core sends does.
func decodeMessage(data []byte) (raft.Message, error) {
	var m raft.Message
	if err := json.Unmarshal(data, &m); err != nil {
		return raft.Message{}, fmt.Errorf("transport: decode a message: %w", err)
	}
	rpcs := 0
	for _, set := range []bool{
		m.RequestVote != nil, m.RequestVoteResponse != nil,
		m.AppendEntries != nil, m.AppendEntriesResponse != nil,
		m.InstallSnapshot != nil, m.InstallSnapshotResponse != nil,
	} {
		if set {
			rpcs++
		}
	}
	if rpcs != 1 {
		return raft.Message{}, fmt.Errorf("transport: a message carries %d RPCs, not one", rpcs)
	}
	return m, nil
}

// checkAddress refuses a message that is not for this server or that
// comes from a server that is not its peer: either means that two servers
// disagree about the cluster's members or addresses.
func (t *HTTP) checkAddress(m raft.Message) error {
	if m.To != t.id {
		return fmt.Errorf("transport: a message for server %d reached server %d", m.To, t.id)
	}
	if t.peers[m.From] == nil {
		return fmt.Errorf("transport: server %d sent a message but is not a peer of server %d", m.From, t.id)
	}
	return nil
}
