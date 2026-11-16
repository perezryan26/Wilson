package kvserver

import (
	"sync"

	"github.com/perezryan26/wilson/internal/kv"
	"github.com/perezryan26/wilson/raft"
)

// StateMachine is the key-value store as a node replicates it. It applies
// each committed command to the store and hands the result to the
// request on this server that proposed the command, if one waits for it.
// It skips entries that hold no command, such as the election no-op.
type StateMachine struct {
	store *kv.Store

	mu      sync.Mutex
	waiting map[uint64]chan kv.Result // by request ID
}

// NewStateMachine returns a state machine that applies commands to store.
func NewStateMachine(store *kv.Store) *StateMachine {
	return &StateMachine{store: store, waiting: map[uint64]chan kv.Result{}}
}

// Apply applies e's command and answers the request that waits for it.
func (m *StateMachine) Apply(e raft.Entry) {
	if e.Type != raft.EntryNormal {
		return
	}
	r := m.store.Apply(uint64(e.Index), e.Data)
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.waiting[r.Request]; ok && r.Request != 0 {
		delete(m.waiting, r.Request)
		ch <- r
	}
}

// Snapshot returns the store's state.
func (m *StateMachine) Snapshot() ([]byte, error) { return m.store.Snapshot(), nil }

// Restore replaces the store's state with a snapshot's.
func (m *StateMachine) Restore(data []byte) error { return m.store.Restore(data) }

// wait returns a channel that receives the result of the command with
// request ID id once it applies, and a function that stops waiting, which
// the caller must call. A request waits before it proposes its command,
// which may apply before the proposal returns.
func (m *StateMachine) wait(id uint64) (<-chan kv.Result, func()) {
	ch := make(chan kv.Result, 1)
	m.mu.Lock()
	m.waiting[id] = ch
	m.mu.Unlock()
	return ch, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.waiting, id)
	}
}
