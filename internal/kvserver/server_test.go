package kvserver_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/kvserver"
	"github.com/perezryan26/wilson/internal/kvtest"
	"github.com/perezryan26/wilson/internal/leaktest"
	"github.com/perezryan26/wilson/raft"
)

// api makes requests to a test cluster's servers.
type api struct {
	t      *testing.T
	c      *kvtest.Cluster
	client *http.Client
}

func newAPI(t *testing.T, opts kvtest.Options) *api {
	a := &api{t: t, c: kvtest.Start(t, opts), client: &http.Client{Timeout: 10 * time.Second}}
	t.Cleanup(a.client.CloseIdleConnections)
	return a
}

// do sends a request with body to server id and returns the status and
// the answer's body.
func (a *api) do(id raft.NodeID, method, path, body string) (int, string) {
	a.t.Helper()
	req, err := http.NewRequest(method, "http://"+a.c.Addr(id)+path, strings.NewReader(body))
	if err != nil {
		a.t.Fatal(err)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		a.t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// want sends a request and fails the test unless the answer has status,
// and, if body is not empty, contains body.
func (a *api) want(id raft.NodeID, method, path, body string, status int, contains string) string {
	a.t.Helper()
	got, answer := a.do(id, method, path, body)
	if got != status || !strings.Contains(answer, contains) {
		a.t.Fatalf("%s %s: %d %q, want %d and %q", method, path, got, answer, status, contains)
	}
	return answer
}

// register registers a session at server id and returns its ID.
func (a *api) register(id raft.NodeID) uint64 {
	a.t.Helper()
	var r struct{ Session uint64 }
	if err := json.Unmarshal([]byte(a.want(id, "POST", kvserver.SessionsPath, "", http.StatusCreated, "")), &r); err != nil || r.Session == 0 {
		a.t.Fatalf("a session's answer decodes as %+v, %v", r, err)
	}
	return r.Session
}

func key(k string, session, seq uint64) string {
	return fmt.Sprintf("%s%s?session=%d&seq=%d", kvserver.KeysPath, k, session, seq)
}

// The API's round trip, in both read modes: a session, a Put, Gets, and
// Deletes of a key that exists and of one that does not. Keys may hold
// slashes.
func TestPutGetDelete(t *testing.T) {
	for _, log := range []bool{false, true} {
		t.Run(fmt.Sprintf("ReadThroughLog=%t", log), func(t *testing.T) {
			leaktest.Check(t)
			a := newAPI(t, kvtest.Options{Size: 3, ReadThroughLog: log})
			lead := a.c.Leader()
			s := a.register(lead)
			a.want(lead, "PUT", key("a/b", s, 1), "1", http.StatusNoContent, "")
			if got := a.want(lead, "GET", kvserver.KeysPath+"a/b", "", http.StatusOK, ""); got != "1" {
				t.Fatalf("GET a/b = %q, want 1", got)
			}
			a.want(lead, "DELETE", key("a/b", s, 2), "", http.StatusNoContent, "")
			a.want(lead, "DELETE", key("a/b", s, 3), "", http.StatusNotFound, "no key")
			a.want(lead, "GET", kvserver.KeysPath+"a/b", "", http.StatusNotFound, "no key")
		})
	}
}

// §8: a retried write is answered from its session's record, not run
// again: a later write to the same key by another session stands, and a
// retried Delete answers what it found the first time.
func TestRetriedWriteIsAnsweredFromRecord(t *testing.T) {
	leaktest.Check(t)
	a := newAPI(t, kvtest.Options{Size: 3})
	lead := a.c.Leader()
	s1, s2 := a.register(lead), a.register(lead)
	a.want(lead, "PUT", key("x", s1, 1), "1", http.StatusNoContent, "")
	a.want(lead, "DELETE", key("x", s1, 2), "", http.StatusNoContent, "")
	a.want(lead, "PUT", key("x", s2, 1), "2", http.StatusNoContent, "")
	a.want(lead, "DELETE", key("x", s1, 2), "", http.StatusNoContent, "")
	a.want(lead, "GET", kvserver.KeysPath+"x", "", http.StatusOK, "2")
}

// A write from a session the store does not hold gets 410, and one
// numbered below its session's last 409; neither runs.
func TestRefusedWrites(t *testing.T) {
	leaktest.Check(t)
	a := newAPI(t, kvtest.Options{Size: 3})
	lead := a.c.Leader()
	a.want(lead, "PUT", key("x", 999, 1), "1", http.StatusGone, "session expired")
	s := a.register(lead)
	a.want(lead, "PUT", key("x", s, 2), "2", http.StatusNoContent, "")
	a.want(lead, "PUT", key("x", s, 1), "1", http.StatusConflict, "older")
	a.want(lead, "GET", kvserver.KeysPath+"x", "", http.StatusOK, "2")
}

// A server that does not lead takes no request, and names the leader's
// address, so the client can go there.
func TestFollowerNamesLeader(t *testing.T) {
	leaktest.Check(t)
	a := newAPI(t, kvtest.Options{Size: 3})
	lead := a.c.Leader()
	follower := lead%3 + 1
	s := a.register(lead)
	for _, r := range []struct{ method, path string }{
		{"POST", kvserver.SessionsPath},
		{"GET", kvserver.KeysPath + "x"},
		{"PUT", key("x", s, 1)},
		{"DELETE", key("x", s, 1)},
	} {
		var e struct{ Error, Leader string }
		answer := a.want(follower, r.method, r.path, "1", http.StatusMisdirectedRequest, "not the leader")
		if err := json.Unmarshal([]byte(answer), &e); err != nil || e.Leader != a.c.Addr(lead) {
			t.Errorf("%s %s at a follower named leader %q, want %q", r.method, r.path, e.Leader, a.c.Addr(lead))
		}
	}
}

// A leader cut off from the others cannot commit or confirm anything:
// in both read modes, its requests time out with 504, and for a write the
// answer says that the outcome is unknown.
func TestCutOffLeaderAnswersUnknownOutcome(t *testing.T) {
	for _, log := range []bool{false, true} {
		t.Run(fmt.Sprintf("ReadThroughLog=%t", log), func(t *testing.T) {
			leaktest.Check(t)
			a := newAPI(t, kvtest.Options{Size: 3, ReadThroughLog: log, Timeout: 300 * time.Millisecond})
			lead := a.c.Leader()
			s := a.register(lead)
			a.c.Cut(lead, true)
			a.want(lead, "PUT", key("x", s, 1), "1", http.StatusGatewayTimeout, "outcome is unknown")
			a.want(lead, "GET", kvserver.KeysPath+"x", "", http.StatusGatewayTimeout, "")
		})
	}
}

// Requests that make no sense are refused before they reach the log: a
// write without a session or a number from 1, an empty key, or a value
// larger than an AppendEntries can carry.
func TestBadRequests(t *testing.T) {
	leaktest.Check(t)
	a := newAPI(t, kvtest.Options{Size: 1})
	lead := a.c.Leader()
	for _, path := range []string{
		kvserver.KeysPath + "x",
		kvserver.KeysPath + "x?session=1",
		kvserver.KeysPath + "x?seq=1",
		key("x", 1, 0),
		kvserver.KeysPath + "x?session=one&seq=1",
		key("", 1, 1),
	} {
		a.want(lead, "PUT", path, "1", http.StatusBadRequest, "")
	}
	a.want(lead, "GET", kvserver.KeysPath, "", http.StatusBadRequest, "no key")
	cfg, _ := raft.DefaultConfig()
	a.want(lead, "PUT", key("x", 1, 1), strings.Repeat("v", cfg.MaxAppendSize+1), http.StatusRequestEntityTooLarge, "")
	a.want(lead, "PUT", key("x", 1, 1), strings.Repeat("v", cfg.MaxAppendSize-10), http.StatusRequestEntityTooLarge, "too large")
}

// Every server reports its role, term, and leader, with the leader's
// address.
func TestStatus(t *testing.T) {
	leaktest.Check(t)
	a := newAPI(t, kvtest.Options{Size: 3})
	lead := a.c.Leader()
	for id := raft.NodeID(1); id <= 3; id++ {
		var st kvserver.Status
		if err := json.Unmarshal([]byte(a.want(id, "GET", kvserver.StatusPath, "", http.StatusOK, "")), &st); err != nil {
			t.Fatal(err)
		}
		role := raft.Follower.String()
		if id == lead {
			role = raft.Leader.String()
		}
		if st.ID != id || st.Role != role || st.Leader != lead || st.LeaderAddress != a.c.Addr(lead) || st.Term == 0 {
			t.Errorf("server %d's status = %+v, want a %s of leader %d at %s", id, st, role, lead, a.c.Addr(lead))
		}
	}
}
