package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var (
	// ErrUnknownOutcome is the error a write returns when no server
	// answered it in time. The write may or may not have been applied,
	// and it will not be applied twice.
	ErrUnknownOutcome = errors.New("client: outcome unknown")
	// ErrSessionExpired is the error a write returns when the servers no
	// longer hold the client's session, because more sessions were
	// registered since it was last used. Whether the write was applied
	// is unknown, as with ErrUnknownOutcome. The client registers a new
	// session for its next write.
	ErrSessionExpired = errors.New("client: session expired")
)

// Options tune New. The zero value is ready to use.
type Options struct {
	// RequestTimeout bounds each HTTP request to a server. It should
	// exceed the server's own timeout, so that the server can say that a
	// request timed out. Zero means five seconds.
	RequestTimeout time.Duration
}

// Client is a client of the key-value store. It sends each request to the
// server it believes leads, follows a server's hint to the leader, and
// tries the next server, after a backoff with jitter, when one cannot
// answer. It registers a session before its first write and numbers its
// writes, sending one write at a time and retrying it with the same
// number, so the servers apply it once (§8). Reads need no session and
// may run in parallel. A Client is safe for concurrent use.
type Client struct {
	servers []string
	http    *http.Client

	mu     sync.Mutex
	leader string // the server believed to lead, or the next to try

	writing sync.Mutex // held for each write, which uses the session below
	session uint64     // 0 until registered
	seq     uint64     // the number of the last write sent
}

// New returns a client of the cluster whose servers listen at servers,
// each given as host:port.
func New(servers []string, opts Options) *Client {
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 5 * time.Second
	}
	pool := http.DefaultTransport.(*http.Transport).Clone()
	pool.Proxy = nil
	c := &Client{servers: servers, http: &http.Client{Transport: pool, Timeout: opts.RequestTimeout}}
	if len(servers) > 0 {
		c.leader = servers[rand.IntN(len(servers))]
	}
	return c
}

// Close closes the client's idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// Get returns key's value and true, or false if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	a, err := c.do(ctx, http.MethodGet, keyPath(key), nil)
	switch {
	case err != nil:
		return nil, false, err
	case a.status == http.StatusOK:
		return a.body, true, nil
	case a.status == http.StatusNotFound:
		return nil, false, nil
	}
	return nil, false, a.err()
}

// Put sets key's value.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	a, err := c.write(ctx, http.MethodPut, key, value)
	if err == nil && a.status != http.StatusNoContent {
		err = a.err()
	}
	return err
}

// Delete removes key, and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	a, err := c.write(ctx, http.MethodDelete, key, nil)
	switch {
	case err != nil:
		return false, err
	case a.status == http.StatusNoContent:
		return true, nil
	case a.status == http.StatusNotFound:
		return false, nil
	}
	return false, a.err()
}

// write sends a Put or Delete of key in the client's session, registering
// one first if needed, and returns the answer.
func (c *Client) write(ctx context.Context, method, key string, value []byte) (answer, error) {
	c.writing.Lock()
	defer c.writing.Unlock()
	if c.session == 0 {
		if err := c.register(ctx); err != nil {
			return answer{}, err
		}
	}
	c.seq++
	path := fmt.Sprintf("%s?session=%d&seq=%d", keyPath(key), c.session, c.seq)
	a, err := c.do(ctx, method, path, value)
	switch {
	case err != nil:
		return answer{}, fmt.Errorf("%w: %w", ErrUnknownOutcome, err)
	case a.status == http.StatusGone:
		c.session = 0
		return answer{}, ErrSessionExpired
	}
	return a, nil
}

// register registers a new session. A retried registration may leave a
// session that nobody uses, which the servers evict in time.
func (c *Client) register(ctx context.Context) error {
	a, err := c.do(ctx, http.MethodPost, "/v1/sessions", nil)
	if err != nil {
		return err
	}
	var r struct{ Session uint64 }
	if a.status != http.StatusCreated {
		return a.err()
	}
	if err := json.Unmarshal(a.body, &r); err != nil || r.Session == 0 {
		return fmt.Errorf("client: a session's answer %q: %w", a.body, err)
	}
	c.session, c.seq = r.Session, 0
	return nil
}

func keyPath(key string) string { return "/v1/kv/" + url.PathEscape(key) }

// answer is a server's answer to a request.
type answer struct {
	status int
	body   []byte
}

// err returns the error that an answer no caller expected stands for.
func (a answer) err() error {
	var e struct{ Error string }
	if json.Unmarshal(a.body, &e) != nil || e.Error == "" {
		e.Error = string(a.body)
	}
	return fmt.Errorf("client: %d %s: %s", a.status, http.StatusText(a.status), e.Error)
}

// do sends a request to the leader and returns the first answer that is
// not a redirect, a timeout, or a stopped server, trying again until it
// gets one or ctx ends. The next attempt goes to the leader a server that
// does not lead names, or else to the next server, after a backoff.
func (c *Client) do(ctx context.Context, method, path string, body []byte) (answer, error) {
	if len(c.servers) == 0 {
		return answer{}, errors.New("client: no servers")
	}
	for attempt := 0; ; attempt++ {
		server := c.target()
		a, err := c.send(ctx, server, method, path, body)
		if err == nil && !retryable(a.status) {
			c.setLeader(server)
			return a, nil
		}
		if err != nil || a.status != http.StatusMisdirectedRequest || !c.followHint(server, a) {
			c.next(server)
		}
		if err := c.backoff(ctx, attempt); err != nil {
			return answer{}, err
		}
	}
}

// retryable reports whether an answer with status asks for the request to
// be sent again: to the leader, or once the server can answer.
func retryable(status int) bool {
	return status == http.StatusMisdirectedRequest || status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

// send sends one request to server.
func (c *Client) send(ctx context.Context, server, method, path string, body []byte) (answer, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+server+path, bytes.NewReader(body))
	if err != nil {
		return answer{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return answer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return answer{}, err
	}
	return answer{status: resp.StatusCode, body: b}, nil
}

// followHint makes the leader that a's redirect from server names the next
// server to try, and reports whether it named one other than server.
func (c *Client) followHint(server string, a answer) bool {
	var hint struct{ Leader string }
	if json.Unmarshal(a.body, &hint) != nil || hint.Leader == "" || hint.Leader == server {
		return false
	}
	c.setLeader(hint.Leader)
	return true
}

// backoff waits before the next attempt: a random time up to 10 ms,
// doubling with each attempt to at most a second, so that clients that
// failed together do not retry together. It returns ctx's error if ctx
// ends first.
func (c *Client) backoff(ctx context.Context, attempt int) error {
	ceiling := min(time.Second, 10*time.Millisecond<<min(attempt, 7))
	t := time.NewTimer(rand.N(ceiling) + time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *Client) setLeader(server string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = server
}

// next makes the server after failed, in the list, the next to try,
// unless another request has already moved on from failed.
func (c *Client) next(failed string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != failed {
		return
	}
	for i, s := range c.servers {
		if s == failed {
			c.leader = c.servers[(i+1)%len(c.servers)]
			return
		}
	}
	c.leader = c.servers[0]
}

// Status is a server's raft status, as GET /v1/status reports it.
type Status struct {
	ID            uint64 `json:"id"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	Leader        uint64 `json:"leader"`
	LeaderAddress string `json:"leaderAddress"`
	CommitIndex   uint64 `json:"commitIndex"`
	LastApplied   uint64 `json:"lastApplied"`
}

// Status asks server, given as host:port, for its status, once.
func (c *Client) Status(ctx context.Context, server string) (Status, error) {
	a, err := c.send(ctx, server, http.MethodGet, "/v1/status", nil)
	if err != nil {
		return Status{}, err
	}
	if a.status != http.StatusOK {
		return Status{}, a.err()
	}
	var st Status
	if err := json.Unmarshal(a.body, &st); err != nil {
		return Status{}, fmt.Errorf("client: a status of %q: %w", a.body, err)
	}
	return st, nil
}
