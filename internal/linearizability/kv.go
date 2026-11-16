package linearizability

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

var (
	// ErrNotLinearizable is the error Check returns for a history that no
	// order of its operations explains.
	ErrNotLinearizable = errors.New("linearizability: history is not linearizable")
	// ErrCheckTimedOut is the error Check returns when Porcupine has not
	// decided by the timeout: the history is too large to check, not
	// wrong.
	ErrCheckTimedOut = errors.New("linearizability: check timed out")
)

// Op is what a client asked of the store.
type Op uint8

// The operations a client can ask for.
const (
	Get Op = iota + 1
	Put
	Delete
)

// Input is one request a client made.
type Input struct {
	Op    Op
	Key   string
	Value string // Put
}

// Output is what a client was told. Found is, for a Get, whether the key
// exists, and for a Delete, whether it existed. Unknown marks an
// operation whose outcome the client never learned, which the model
// accepts whatever it was.
type Output struct {
	Value   string // Get
	Found   bool
	Unknown bool
}

// state is one key's state in the model: its value, if it exists.
type state struct {
	value  string
	exists bool
}

// model returns the key-value store as Porcupine checks it: one key per
// partition, since operations on different keys never constrain each
// other, which makes checks far faster.
func model() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByKey,
		Init:      func() any { return state{} },
		Step:      step,
		DescribeOperation: func(input, output any) string {
			return describe(input.(Input), output.(Output))
		},
		DescribeState: func(s any) string {
			if st := s.(state); st.exists {
				return fmt.Sprintf("%q", st.value)
			}
			return "(none)"
		},
	}
}

// step reports whether a key in state s could answer in with out, and
// returns the key's next state.
func step(s, input, output any) (bool, any) {
	st, in, out := s.(state), input.(Input), output.(Output)
	switch in.Op {
	case Get:
		return out.Unknown || out.Found == st.exists && (!st.exists || out.Value == st.value), st
	case Put:
		return true, state{value: in.Value, exists: true}
	default:
		return out.Unknown || out.Found == st.exists, state{}
	}
}

func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := map[string][]porcupine.Operation{}
	for _, op := range history {
		key := op.Input.(Input).Key
		byKey[key] = append(byKey[key], op)
	}
	var parts [][]porcupine.Operation
	for _, key := range slices.Sorted(maps.Keys(byKey)) {
		parts = append(parts, byKey[key])
	}
	return parts
}

func describe(in Input, out Output) string {
	answer := "?"
	switch {
	case out.Unknown:
	case in.Op == Get && out.Found:
		answer = fmt.Sprintf("%q", out.Value)
	case in.Op == Get:
		answer = "(none)"
	case in.Op == Delete:
		answer = fmt.Sprintf("existed=%t", out.Found)
	default:
		answer = "ok"
	}
	switch in.Op {
	case Get:
		return fmt.Sprintf("get(%q) → %s", in.Key, answer)
	case Put:
		return fmt.Sprintf("put(%q, %q) → %s", in.Key, in.Value, answer)
	}
	return fmt.Sprintf("delete(%q) → %s", in.Key, answer)
}

// Recorder records a history of client operations for Check. It takes
// every call and return time from one clock under one lock, so that times
// are ordered across goroutines as they happened; Porcupine warns that
// timestamps taken without such ordering can show violations that never
// happened. A Recorder is safe for concurrent use.
type Recorder struct {
	mu  sync.Mutex
	now func() int64
	ops []porcupine.Operation
	end []bool // whether each operation has returned
}

// NewRecorder returns a recorder that reads times from now, which must
// never go backwards.
func NewRecorder(now func() int64) *Recorder {
	return &Recorder{now: now}
}

// Call records that client asked for in, and returns the operation's
// number for Return. An operation that never returns, because the client
// gave up or never learned its outcome, stays open until the end of the
// history, so it may take effect at any time after its call, or not at
// all.
func (r *Recorder) Call(client int, in Input) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Call: r.now()})
	r.end = append(r.end, false)
	return len(r.ops) - 1
}

// Return records that operation op returned out.
func (r *Recorder) Return(op int, out Output) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops[op].Output, r.ops[op].Return = out, r.now()
	r.end[op] = true
}

// Check checks the history recorded so far against the model, for up to
// timeout. It returns nil if the history is linearizable, and
// ErrCheckTimedOut if Porcupine has not decided by then. Otherwise it
// writes Porcupine's visualization of the history to a new file in dir,
// or in the system's temporary directory if dir is empty, and returns an
// error wrapping ErrNotLinearizable that names the file.
func (r *Recorder) Check(timeout time.Duration, dir string) error {
	history := r.history()
	result, info := porcupine.CheckOperationsVerbose(model(), history, timeout)
	switch result {
	case porcupine.Ok:
		return nil
	case porcupine.Unknown:
		return fmt.Errorf("%w after %v with %d operations", ErrCheckTimedOut, timeout, len(history))
	}
	f, err := os.CreateTemp(dir, "porcupine-*.html")
	if err == nil {
		err = errors.Join(porcupine.Visualize(model(), info, f), f.Close())
	}
	if err != nil {
		return fmt.Errorf("%w; writing the visualization failed: %w", ErrNotLinearizable, err)
	}
	return fmt.Errorf("%w; see %s", ErrNotLinearizable, f.Name())
}

// history returns the operations recorded, those still open returning
// just after everything else with an unknown output.
func (r *Recorder) history() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	last := int64(0)
	for i, op := range r.ops {
		last = max(last, op.Call)
		if r.end[i] {
			last = max(last, op.Return)
		}
	}
	history := slices.Clone(r.ops)
	for i := range history {
		if !r.end[i] {
			history[i].Output, history[i].Return = Output{Unknown: true}, last+1
		}
	}
	return history
}
