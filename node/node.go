package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/perezryan26/wilson/internal/storage"
	"github.com/perezryan26/wilson/internal/transport"
	"github.com/perezryan26/wilson/raft"
)

// ErrStopped is the error Propose returns once the node has stopped,
// whether Stop stopped it or a storage error did.
var ErrStopped = errors.New("node: stopped")

// inboxSize is how many inbound messages can wait for the node's
// goroutine. Step drops a message that arrives when the inbox is full.
const inboxSize = 1024

// StateMachine is the service a node replicates. The node calls it from
// its one goroutine, so its methods need no locking against each other,
// only against the service's own readers.
type StateMachine interface {
	// Apply applies a committed entry's command. Entries arrive in index
	// order, each once per boot.
	Apply(e raft.Entry)
	// Snapshot returns the state machine's state as of the last entry it
	// applied, for log compaction (§7).
	Snapshot() ([]byte, error)
	// Restore replaces the state machine's state with data, which
	// Snapshot returned on this server or on the leader.
	Restore(data []byte) error
}

// Config configures a node.
type Config struct {
	// Raft configures the core.
	Raft raft.Config
	// Tick is how much wall-clock time one raft tick takes, such as the
	// interval raft.DefaultConfig returns.
	Tick time.Duration
	// CompactEvery makes the node snapshot its state machine and compact
	// the log once it has applied that many entries past its latest
	// snapshot (§7). Zero means never.
	CompactEvery int
	// Logger receives a line when the node's role, term, or leader
	// changes, when it installs a snapshot from the leader, and when it
	// stops for a storage error. Nil means slog.Default().
	Logger *slog.Logger
}

// Node runs a raft core inside a real process. One goroutine owns the
// core: it turns time into ticks and feeds the core ticks, inbound
// messages, and proposals, and after each it handles the core's Ready in
// the order ADR-0001 requires: save and fsync through storage, send
// through the transport, then restore and apply on the state machine.
// The other methods are safe to call from any goroutine. Stop ends the
// goroutine, and so does a storage error, for good: once a write or fsync
// fails, what the disk holds is unknown.
type Node struct {
	core         *raft.Node
	storage      storage.Storage
	transport    transport.Transport
	sm           StateMachine
	tick         time.Duration
	compactEvery int
	logger       *slog.Logger

	applied  raft.Index // the last index the state machine holds
	snapshot raft.Index // the last index the latest snapshot includes

	inbox     chan raft.Message
	proposals chan proposal
	stop      chan struct{} // closed by Stop
	stopOnce  sync.Once
	done      chan struct{} // closed when the goroutine has ended
	err       error         // why the goroutine ended, if not Stop; read only after done closes

	status atomic.Pointer[raft.Status]
}

// proposal is a command on its way to the core, and where the result goes.
type proposal struct {
	cmd    []byte
	result chan<- proposed
}

// proposed is the result of a proposal.
type proposed struct {
	index raft.Index
	term  raft.Term
	err   error
}

// Start restores the state machine and the core from what st holds and
// starts the node's goroutine. The node sends through tr and applies to
// sm, which must be empty: Start restores it from the latest snapshot.
func Start(cfg Config, st storage.Storage, tr transport.Transport, sm StateMachine) (*Node, error) {
	if cfg.Tick <= 0 {
		return nil, fmt.Errorf("node: tick interval %v is not positive", cfg.Tick)
	}
	restored, err := st.Load()
	if err != nil {
		return nil, fmt.Errorf("node: load: %w", err)
	}
	n := &Node{
		storage:      st,
		transport:    tr,
		sm:           sm,
		tick:         cfg.Tick,
		compactEvery: cfg.CompactEvery,
		logger:       cfg.Logger,
		inbox:        make(chan raft.Message, inboxSize),
		proposals:    make(chan proposal),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	if n.logger == nil {
		n.logger = slog.Default()
	}
	if s := restored.Snapshot; s != nil {
		if err := sm.Restore(s.Data); err != nil {
			return nil, fmt.Errorf("node: restore the snapshot through index %d: %w", s.LastIncludedIndex, err)
		}
		n.applied, n.snapshot = s.LastIncludedIndex, s.LastIncludedIndex
	}
	n.core = raft.NewNode(cfg.Raft, restored)
	status := n.core.Status()
	n.status.Store(&status)
	go n.run()
	return n, nil
}

// Step hands an inbound message to the node and returns at once. It
// drops the message if the node is stopped or its inbox is full, as the
// network might have: Raft resends what it still needs.
func (n *Node) Step(m raft.Message) {
	select {
	case n.inbox <- m:
	default:
	}
}

// Propose hands cmd to the core and returns the index and term of the
// entry it became, once that entry is durable in this server's log. The
// entry is not yet committed: a later leader may still overwrite it,
// which the service learns when another entry is applied at that index.
// Propose returns raft.ErrNotLeader on a server that is not the leader,
// raft.ErrProposalTooLarge for a command too large to replicate,
// ErrStopped once the node has stopped, and ctx's error if ctx ends
// first, in which case the entry may or may not exist.
func (n *Node) Propose(ctx context.Context, cmd []byte) (raft.Index, raft.Term, error) {
	result := make(chan proposed, 1)
	select {
	case n.proposals <- proposal{cmd: cmd, result: result}:
	case <-n.done:
		return 0, 0, ErrStopped
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	select {
	case r := <-result:
		return r.index, r.term, r.err
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

// Status returns the core's role, term, leader, commitIndex, and
// lastApplied as of the last input the node handled.
func (n *Node) Status() raft.Status { return *n.status.Load() }

// Done returns a channel that is closed once the node has stopped, by
// Stop or by a storage error.
func (n *Node) Done() <-chan struct{} { return n.done }

// Err returns the error that stopped the node, or nil if it is running
// or Stop stopped it.
func (n *Node) Err() error {
	select {
	case <-n.done:
		return n.err
	default:
		return nil
	}
}

// Stop stops the node's goroutine, once it has finished handling the
// input under way, and waits for it to end. It returns the error that
// stopped the node earlier, if one did. Stop may be called more than
// once.
func (n *Node) Stop() error {
	n.stopOnce.Do(func() { close(n.stop) })
	<-n.done
	return n.err
}

// run is the node's goroutine. It handles one input at a time until Stop
// or a storage error.
func (n *Node) run() {
	defer close(n.done)
	ticker := time.NewTicker(n.tick)
	defer ticker.Stop()
	for n.err == nil {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.core.Tick()
			n.err = n.handleReady()
		case m := <-n.inbox:
			n.core.Step(m)
			n.err = n.handleReady()
		case p := <-n.proposals:
			n.err = n.propose(p)
		}
		n.publish()
	}
	n.logger.Error("node: stopped", "err", n.err)
}

// propose hands p's command to the core, and answers p once the new entry
// is durable, so that a caller never learns of an entry this server could
// lose in a crash.
func (n *Node) propose(p proposal) error {
	index, term, err := n.core.Propose(p.cmd)
	if err != nil {
		p.result <- proposed{err: err}
		return nil
	}
	if err := n.handleReady(); err != nil {
		p.result <- proposed{err: ErrStopped}
		return err
	}
	p.result <- proposed{index: index, term: term}
	return nil
}

// handleReady carries out the core's Ready in the order ADR-0001 requires.
// Saving comes first: a vote or an acknowledgment must not leave before
// the state it reflects is durable, nor an entry be applied before it is
// on this server's disk. A failed save stops the node with nothing sent.
func (n *Node) handleReady() error {
	rd := n.core.Ready()
	if err := n.storage.Save(rd); err != nil {
		return fmt.Errorf("node: save: %w", err)
	}
	for _, m := range rd.Messages {
		n.transport.Send(m)
	}
	if err := n.apply(rd); err != nil {
		return err
	}
	return n.maybeCompact()
}

// apply restores the state machine from rd's snapshot when that is past
// what it applied, which happens only for a snapshot the leader sent
// (Figure 13, step 8), then applies rd's committed entries.
func (n *Node) apply(rd raft.Ready) error {
	if s := rd.Snapshot; s != nil {
		if s.LastIncludedIndex > n.applied {
			if err := n.sm.Restore(s.Data); err != nil {
				return fmt.Errorf("node: restore the snapshot through index %d: %w", s.LastIncludedIndex, err)
			}
			n.applied = s.LastIncludedIndex
			n.logger.Info("node: installed the leader's snapshot", "lastIncludedIndex", uint64(s.LastIncludedIndex))
		}
		n.snapshot = s.LastIncludedIndex
	}
	for _, e := range rd.CommittedEntries {
		n.sm.Apply(e)
		n.applied = e.Index
	}
	return nil
}

// maybeCompact snapshots the state machine and compacts the log through
// the last applied entry, once compactEvery entries have been applied
// past the latest snapshot, then saves the snapshot.
func (n *Node) maybeCompact() error {
	if n.compactEvery == 0 || n.applied < n.snapshot+raft.Index(n.compactEvery) {
		return nil
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		return fmt.Errorf("node: snapshot the state machine: %w", err)
	}
	if err := n.core.Compact(n.applied, data); err != nil {
		return fmt.Errorf("node: %w", err)
	}
	n.logger.Debug("node: compacted the log", "lastIncludedIndex", uint64(n.applied))
	return n.handleReady()
}

// publish makes the core's status visible to Status, and logs a change of
// role, term, or leader.
func (n *Node) publish() {
	st := n.core.Status()
	was := n.status.Swap(&st)
	if st.Role != was.Role || st.Term != was.Term || st.Leader != was.Leader {
		n.logger.Info("node: role, term, or leader changed", "leader", st.Leader)
	}
}
