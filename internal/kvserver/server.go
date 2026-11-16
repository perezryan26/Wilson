package kvserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/perezryan26/wilson/internal/kv"
	"github.com/perezryan26/wilson/node"
	"github.com/perezryan26/wilson/raft"
)

// Paths and parameters of the API.
const (
	// SessionsPath takes a POST that registers a session.
	SessionsPath = "/v1/sessions"
	// KeysPath, followed by a key, takes GET, PUT, and DELETE.
	KeysPath = "/v1/kv/"
	// StatusPath takes a GET of the server's raft status.
	StatusPath = "/v1/status"
)

// Options configure a Server.
type Options struct {
	// ID is this server's raft ID.
	ID raft.NodeID
	// Peers maps every server's ID to the address its API is served at,
	// for the leader hint that a server that does not lead answers with.
	Peers map[raft.NodeID]string
	// ReadThroughLog makes the server serve a Get as a command through
	// the log instead of through ReadIndex (§8).
	ReadThroughLog bool
	// Timeout bounds how long a request waits for its command to apply,
	// or its read to be confirmed. Zero means two seconds.
	Timeout time.Duration
	// MaxValue is the largest value, in bytes, that a PUT may carry; a
	// larger body is refused unread. Zero means no limit.
	MaxValue int
}

// Server serves the key-value API over HTTP for one server of the
// cluster (docs/adr/0008-read-path.md):
//
//	POST   /v1/sessions                        register a session: 201 {"session": id}
//	GET    /v1/kv/{key}                        the value: 200, or 404 if the key does not exist
//	PUT    /v1/kv/{key}?session=S&seq=N        set the value to the body: 204
//	DELETE /v1/kv/{key}?session=S&seq=N        204 if the key existed, 404 if not
//	GET    /v1/status                          {"id", "role", "term", "leader", "leaderAddress", "commitIndex", "lastApplied"}
//
// A server that does not lead answers 421 with the leader's address, if
// it knows it. A write from a session the store does not hold gets 410,
// and one numbered below its session's last 409. A request that cannot
// finish in time gets 504: for a write, whether it was applied is then
// unknown, and the client must send it again with the same number. A
// stopped node gets 503, and errors carry a JSON body {"error", "leader"}.
type Server struct {
	node *node.Node
	sm   *StateMachine
	opts Options
}

// New returns a server for n, whose state machine is sm.
func New(n *node.Node, sm *StateMachine, opts Options) *Server {
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Second
	}
	return &Server{node: n, sm: sm, opts: opts}
}

// Handler returns the handler for the API's paths.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+SessionsPath, s.register)
	mux.HandleFunc("GET "+KeysPath+"{key...}", s.get)
	mux.HandleFunc("PUT "+KeysPath+"{key...}", s.put)
	mux.HandleFunc("DELETE "+KeysPath+"{key...}", s.delete)
	mux.HandleFunc("GET "+StatusPath, s.status)
	return mux
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	result, ok := s.propose(w, r, kv.Command{Op: kv.OpRegister})
	if ok {
		writeJSON(w, http.StatusCreated, map[string]uint64{"session": result.Session})
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, errors.New("kvserver: no key"))
		return
	}
	var value []byte
	var found bool
	if s.opts.ReadThroughLog {
		result, ok := s.propose(w, r, kv.Command{Op: kv.OpGet, Key: key})
		if !ok {
			return
		}
		value, found = result.Value, result.Found
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), s.opts.Timeout)
		defer cancel()
		if err := s.node.ReadIndex(ctx); err != nil {
			s.fail(w, err, "the read timed out")
			return
		}
		value, found = s.sm.store.Get(key)
	}
	if !found {
		writeError(w, http.StatusNotFound, fmt.Errorf("kvserver: no key %q", key))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(value)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	cmd, ok := writeCommand(w, r, kv.OpPut)
	if !ok {
		return
	}
	body := r.Body
	if s.opts.MaxValue > 0 {
		body = http.MaxBytesReader(w, r.Body, int64(s.opts.MaxValue))
	}
	value, err := io.ReadAll(body)
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("kvserver: a value over %d bytes", s.opts.MaxValue))
		return
	case err != nil:
		writeError(w, http.StatusBadRequest, fmt.Errorf("kvserver: read the value: %w", err))
		return
	}
	cmd.Value = value
	if result, ok := s.propose(w, r, cmd); ok && answered(w, result) {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	cmd, ok := writeCommand(w, r, kv.OpDelete)
	if !ok {
		return
	}
	result, ok := s.propose(w, r, cmd)
	switch {
	case !ok || !answered(w, result):
	case result.Found:
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusNotFound, fmt.Errorf("kvserver: no key %q", cmd.Key))
	}
}

// writeCommand returns the write r asks for, or answers 400 and returns
// false if r has no key, or no session and number from 1.
func writeCommand(w http.ResponseWriter, r *http.Request, op kv.Op) (kv.Command, bool) {
	cmd := kv.Command{Op: op, Key: r.PathValue("key")}
	var errSession, errSeq error
	cmd.Session, errSession = strconv.ParseUint(r.URL.Query().Get("session"), 10, 64)
	cmd.Seq, errSeq = strconv.ParseUint(r.URL.Query().Get("seq"), 10, 64)
	switch {
	case cmd.Key == "":
		writeError(w, http.StatusBadRequest, errors.New("kvserver: no key"))
	case errSession != nil || errSeq != nil || cmd.Seq == 0:
		writeError(w, http.StatusBadRequest, errors.New("kvserver: a write needs ?session=S&seq=N, numbered from 1"))
	default:
		return cmd, true
	}
	return kv.Command{}, false
}

// answered answers 410 or 409 if result refused a write, and reports
// whether the write was carried out or answered from its session's
// record.
func answered(w http.ResponseWriter, result kv.Result) bool {
	switch {
	case errors.Is(result.Err, kv.ErrSessionExpired):
		writeError(w, http.StatusGone, result.Err)
	case result.Err != nil:
		writeError(w, http.StatusConflict, result.Err)
	default:
		return true
	}
	return false
}

// propose proposes cmd and waits until it applies on this server, then
// returns its result. Otherwise it answers the request with the reason
// and returns false.
func (s *Server) propose(w http.ResponseWriter, r *http.Request, cmd kv.Command) (kv.Result, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.Timeout)
	defer cancel()
	cmd.Request = rand.Uint64() | 1
	results, stop := s.sm.wait(cmd.Request)
	defer stop()
	if _, _, err := s.node.Propose(ctx, cmd.Encode()); err != nil {
		s.fail(w, err, "the outcome is unknown: the command may or may not have been applied")
		return kv.Result{}, false
	}
	select {
	case result := <-results:
		return result, true
	case <-ctx.Done():
		s.fail(w, ctx.Err(), "the outcome is unknown: the command may or may not have been applied")
		return kv.Result{}, false
	}
}

// fail answers a request that err stopped: 421 with the leader's address
// if this server does not lead, 413 for a command too large, 503 if the
// node has stopped, and otherwise 504 with timedOut as the reason.
func (s *Server) fail(w http.ResponseWriter, err error, timedOut string) {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		writeJSON(w, http.StatusMisdirectedRequest, errorBody{Error: err.Error(), Leader: s.leaderAddress()})
	case errors.Is(err, raft.ErrProposalTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err)
	case errors.Is(err, node.ErrStopped):
		writeError(w, http.StatusServiceUnavailable, err)
	default:
		writeError(w, http.StatusGatewayTimeout, fmt.Errorf("kvserver: %s: %w", timedOut, err))
	}
}

// leaderAddress returns the address of the leader this server knows of,
// or "" if it knows of none.
func (s *Server) leaderAddress() string { return s.opts.Peers[s.node.Status().Leader] }

// Status is the JSON that GET /v1/status answers with.
type Status struct {
	ID            raft.NodeID `json:"id"`
	Role          string      `json:"role"`
	Term          raft.Term   `json:"term"`
	Leader        raft.NodeID `json:"leader"`
	LeaderAddress string      `json:"leaderAddress"`
	CommitIndex   raft.Index  `json:"commitIndex"`
	LastApplied   raft.Index  `json:"lastApplied"`
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	st := s.node.Status()
	writeJSON(w, http.StatusOK, Status{
		ID:            s.opts.ID,
		Role:          st.Role.String(),
		Term:          st.Term,
		Leader:        st.Leader,
		LeaderAddress: s.opts.Peers[st.Leader],
		CommitIndex:   st.CommitIndex,
		LastApplied:   st.LastApplied,
	})
}

// errorBody is the JSON body of an error answer.
type errorBody struct {
	Error  string `json:"error"`
	Leader string `json:"leader,omitempty"` // with 421: the leader's address, if known
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorBody{Error: err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
