package chaos

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/leaktest"
)

// proxy forwards TCP connections from an address of its own to a target
// until it is cut. A cut drops every connection the proxy carries and
// holds new ones without forwarding a byte, as a partitioned network
// would, so a sender waits for its timeout instead of being refused.
// Healing the proxy releases the connections it held.
type proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	cut   bool
	gen   int                   // counts cuts and heals
	conns map[net.Conn]struct{} // every connection open through the proxy, either side
	wg    sync.WaitGroup        // serve and the goroutine of every connection
}

// newProxy starts a proxy to target on a free port of 127.0.0.1.
func newProxy(target string) (*proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.serve()
	return p, nil
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

// setCut cuts or heals the proxy, closing every connection open through
// it if that changes its state.
func (p *proxy) setCut(cut bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cut == cut {
		return
	}
	p.cut = cut
	p.gen++
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

// close stops the proxy, closes every connection open through it, and
// waits for its goroutines.
func (p *proxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *proxy) serve() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.handle(c)
	}
}

// handle forwards down to the target, or holds it while the proxy is
// cut, until either side closes or the proxy is cut or healed.
func (p *proxy) handle(down net.Conn) {
	defer p.wg.Done()
	gen, cut := p.track(down, -1)
	defer p.untrack(down)
	if cut {
		_, _ = io.Copy(io.Discard, down)
		return
	}
	up, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		return // the server is down: the sender sees its connection close
	}
	defer p.untrack(up)
	if _, ok := p.track(up, gen); !ok {
		return // cut or healed while dialing
	}
	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(up, down)
		_, _ = up.Close(), down.Close()
		close(copied)
	}()
	_, _ = io.Copy(down, up)
	_, _ = up.Close(), down.Close()
	<-copied
}

// track records c as open through the proxy, unless the proxy has been
// cut or healed since generation gen (-1 for any). It returns the current
// generation and whether the proxy is cut, or ok false after closing c.
func (p *proxy) track(c net.Conn, gen int) (current int, cutOrOK bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen >= 0 && gen != p.gen {
		_ = c.Close()
		return p.gen, false
	}
	p.conns[c] = struct{}{}
	if gen >= 0 {
		return p.gen, true
	}
	return p.gen, p.cut
}

func (p *proxy) untrack(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = c.Close()
	delete(p.conns, c)
}

// A proxy forwards requests until it is cut. Cut, it drops the
// connection a client keeps alive and holds new ones until the client
// times out, rather than refusing them. Healed, it forwards again, and
// closed, it leaves no goroutine running.
func TestProxyCutsAndHeals(t *testing.T) {
	leaktest.Check(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "hello")
	}))
	t.Cleanup(srv.Close)
	p, err := newProxy(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	cl := &http.Client{Timeout: 300 * time.Millisecond}
	t.Cleanup(cl.CloseIdleConnections)
	get := func() error {
		resp, err := cl.Get("http://" + p.addr())
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err == nil && string(body) != "hello" {
			err = fmt.Errorf("body %q", body)
		}
		return err
	}

	if err := get(); err != nil {
		t.Fatalf("through the proxy: %v", err)
	}
	p.setCut(true)
	var netErr net.Error
	if err := get(); !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("through the cut proxy: got %v, want a timeout", err)
	}
	p.setCut(false)
	if err := get(); err != nil {
		t.Fatalf("through the healed proxy: %v", err)
	}
}
