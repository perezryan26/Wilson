package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"sync"

	"github.com/perezryan26/wilson/node"
	"github.com/perezryan26/wilson/raft"
)

// commandLog is the state machine kvnode replicates until the key-value
// store exists: the list of every command applied, in log order. Its
// length counts them, and the list lets a test check that every server
// applied the same commands at the same indexes.
type commandLog struct {
	mu       sync.Mutex
	commands []string
}

func (l *commandLog) Apply(e raft.Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commands = append(l.commands, string(e.Data))
}

func (l *commandLog) Snapshot() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.Marshal(l.commands)
}

func (l *commandLog) Restore(data []byte) error {
	var commands []string
	if err := json.Unmarshal(data, &commands); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commands = commands
	return nil
}

func (l *commandLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.commands)
}

// debugAPI serves two endpoints for driving a cluster by hand or from a
// test until the key-value API exists: one proposes a command, and one
// shows the node's status and the commands it applied.
type debugAPI struct {
	node       *node.Node
	sm         *commandLog
	maxCommand int // the body size beyond which a proposal is refused unread
}

// state is the JSON that GET /debug/state answers with.
type state struct {
	Role        string      `json:"role"`
	Term        raft.Term   `json:"term"`
	Leader      raft.NodeID `json:"leader"`
	CommitIndex raft.Index  `json:"commitIndex"`
	LastApplied raft.Index  `json:"lastApplied"`
	Commands    []string    `json:"commands"`
}

func (d debugAPI) register(mux *http.ServeMux) {
	mux.HandleFunc("POST /debug/propose", d.propose)
	mux.HandleFunc("GET /debug/state", d.state)
}

// propose proposes the request's body as a command. It answers 200 with
// the new entry's index and term once the entry is in this server's log
// on disk, which does not mean it is committed; 421 with the leader this
// server knows of, if it is not the leader; and 413 for a command too
// large to replicate.
func (d debugAPI) propose(w http.ResponseWriter, r *http.Request) {
	cmd, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(d.maxCommand)))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": err.Error()})
		return
	}
	index, term, err := d.node.Propose(r.Context(), cmd)
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		writeJSON(w, http.StatusMisdirectedRequest, map[string]any{"error": err.Error(), "leader": d.node.Status().Leader})
	case errors.Is(err, raft.ErrProposalTooLarge):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": err.Error()})
	case err != nil:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"index": index, "term": term})
	}
}

// state answers with the node's status and every command it applied.
func (d debugAPI) state(w http.ResponseWriter, _ *http.Request) {
	st := d.node.Status()
	writeJSON(w, http.StatusOK, state{
		Role:        st.Role.String(),
		Term:        st.Term,
		Leader:      st.Leader,
		CommitIndex: st.CommitIndex,
		LastApplied: st.LastApplied,
		Commands:    d.sm.list(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
