package kv

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// DefaultMaxSessions is how many client sessions kvnode's store keeps.
// Every server of a cluster must keep the same number, or their stores
// would evict different sessions and then apply the same write
// differently.
const DefaultMaxSessions = 10000

var (
	// ErrSessionExpired is the result of a write from a session the store
	// does not hold: one it evicted, or one that never existed. The write
	// did not run now, and whether it ran before is unknown. The client
	// must start a new session.
	ErrSessionExpired = errors.New("kv: session expired")
	// ErrStaleRequest is the result of a write numbered below its
	// session's latest. Its own result is gone, and only a client that
	// sent requests out of order, or a copy of a request delayed past the
	// next one, can ask for it.
	ErrStaleRequest = errors.New("kv: request older than its session's latest")
	// ErrMalformedCommand is the result of a log entry that does not
	// decode as a command. It changes nothing.
	ErrMalformedCommand = errors.New("kv: malformed command")
)

// Result is what applying a command produced.
type Result struct {
	Request uint64 // the command's Request
	Err     error  // ErrSessionExpired, ErrStaleRequest, ErrMalformedCommand, or nil
	Session uint64 // Register: the new session's ID
	Value   []byte // Get: the key's value
	Found   bool   // Get: the key exists; Delete: the key existed
}

// session is what the store keeps for one client session (§8): the last
// write it applied, that write's response, and how recently the session
// was active.
type session struct {
	seq       uint64 // the number of the last write applied
	found     bool   // the last write's response: for a Delete, whether the key existed
	lastIndex uint64 // the log index of the session's last command
}

// Store is the replicated key-value state machine: the keys and their
// values, and the client sessions that make each write apply once. Every
// server applies the same commands in log order, and Apply depends on
// nothing else, so every server's store holds the same state at each
// index. A Store is safe for concurrent use.
type Store struct {
	mu          sync.Mutex
	maxSessions int
	data        map[string][]byte
	sessions    map[uint64]*session
}

// NewStore returns an empty store that keeps at most maxSessions client
// sessions, which must be at least one.
func NewStore(maxSessions int) *Store {
	if maxSessions < 1 {
		panic(fmt.Sprintf("kv: a store must keep at least one session, not %d", maxSessions))
	}
	return &Store{maxSessions: maxSessions, data: map[string][]byte{}, sessions: map[uint64]*session{}}
}

// Apply applies the command in data, the log entry at index, and returns
// its result. A Register makes index the new session's ID, and every
// command of a session marks it active as of index.
func (s *Store) Apply(index uint64, data []byte) Result {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return Result{Err: ErrMalformedCommand}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := Result{Request: cmd.Request}
	switch cmd.Op {
	case OpRegister:
		r.Session = s.register(index)
	case OpGet:
		r.Value, r.Found = s.data[cmd.Key]
	default:
		r.Found, r.Err = s.write(index, cmd)
	}
	return r
}

// register starts a session whose ID is index. If the store already
// holds maxSessions sessions, it first evicts the one whose last command
// is oldest. Every server applies the same commands, so every server
// evicts the same session; no two sessions share a last index, because
// each index holds one command.
func (s *Store) register(index uint64) uint64 {
	if len(s.sessions) >= s.maxSessions {
		// ponytail: a scan of every session per Register once the table is
		// full; keep sessions in a heap by lastIndex if that ever shows.
		ids := slices.Collect(maps.Keys(s.sessions))
		delete(s.sessions, slices.MinFunc(ids, func(a, b uint64) int {
			return cmp.Compare(s.sessions[a].lastIndex, s.sessions[b].lastIndex)
		}))
	}
	s.sessions[index] = &session{lastIndex: index}
	return index
}

// write applies a Put or Delete, the log entry at index, unless its
// session has applied it already, and returns its response: for a
// Delete, whether the key existed.
func (s *Store) write(index uint64, cmd Command) (bool, error) {
	ses := s.sessions[cmd.Session]
	switch {
	case ses == nil:
		return false, ErrSessionExpired
	case cmd.Seq < ses.seq:
		return false, ErrStaleRequest
	}
	ses.lastIndex = index
	// §8: a write the session has applied already is answered from its
	// record, so a retried write runs once.
	if cmd.Seq == ses.seq {
		return ses.found, nil
	}
	ses.seq, ses.found = cmd.Seq, false
	if cmd.Op == OpPut {
		s.data[cmd.Key] = cmd.Value
		return false, nil
	}
	_, ses.found = s.data[cmd.Key]
	delete(s.data, cmd.Key)
	return ses.found, nil
}

// Get returns the value of key, and whether it exists, for a read that
// does not go through the log. The caller must not modify the value.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

// A snapshot of the store is laid out as
//
//	[version u8][pair count u32]{[key length u32][key][value length u32][value]}
//	[session count u32]{[id u64][seq u64][last index u64][found u8]}
//
// in little-endian byte order, with pairs in increasing order of key and
// sessions of ID, so stores holding the same state encode it the same way
// (docs/adr/0007-client-sessions.md).
const stateVersion = 1

// Snapshot returns the store's state: every key and value and every
// session, which a snapshot must hold (§7, §8).
func (s *Store) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	le := binary.LittleEndian
	b := []byte{stateVersion}
	b = le.AppendUint32(b, uint32(len(s.data)))
	for _, key := range slices.Sorted(maps.Keys(s.data)) {
		b = le.AppendUint32(b, uint32(len(key)))
		b = append(b, key...)
		b = le.AppendUint32(b, uint32(len(s.data[key])))
		b = append(b, s.data[key]...)
	}
	b = le.AppendUint32(b, uint32(len(s.sessions)))
	for _, id := range slices.Sorted(maps.Keys(s.sessions)) {
		ses := s.sessions[id]
		b = le.AppendUint64(b, id)
		b = le.AppendUint64(b, ses.seq)
		b = le.AppendUint64(b, ses.lastIndex)
		b = append(b, boolByte(ses.found))
	}
	return b
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// Restore replaces the store's state with a snapshot's. It refuses a
// snapshot that does not decode, changing nothing.
func (s *Store) Restore(snapshot []byte) error {
	data, sessions, err := decodeState(snapshot)
	if err != nil {
		return fmt.Errorf("kv: restore: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.sessions = data, sessions
	return nil
}

// decodeState decodes a snapshot of the store. It refuses any bytes that
// Snapshot would not have written, so a state decodes from exactly one
// byte string.
func decodeState(b []byte) (map[string][]byte, map[uint64]*session, error) {
	r := reader{b: b}
	if v := r.u8(); r.err == nil && v != stateVersion {
		return nil, nil, fmt.Errorf("state version %d, want %d", v, stateVersion)
	}
	data, keys := readPairs(&r)
	sessions, ids := readSessions(&r)
	switch {
	case r.err != nil:
		return nil, nil, r.err
	case len(r.b) > 0:
		return nil, nil, fmt.Errorf("%d bytes after the last session", len(r.b))
	case !increasing(keys) || !increasing(ids):
		return nil, nil, errors.New("keys or sessions out of order")
	}
	return data, sessions, nil
}

// readPairs reads the keys and values of a snapshot, and returns them with
// the keys in the order they came.
func readPairs(r *reader) (map[string][]byte, []string) {
	data := map[string][]byte{}
	var keys []string
	for n := r.u32(); n > 0 && r.err == nil; n-- {
		key := string(r.next(r.u32()))
		value := bytes.Clone(r.next(r.u32()))
		data[key], keys = value, append(keys, key)
	}
	return data, keys
}

// readSessions reads the sessions of a snapshot, and returns them with
// their IDs in the order they came.
func readSessions(r *reader) (map[uint64]*session, []uint64) {
	sessions := map[uint64]*session{}
	var ids []uint64
	for n := r.u32(); n > 0 && r.err == nil; n-- {
		id, seq, lastIndex, found := r.u64(), r.u64(), r.u64(), r.u8()
		if found > 1 {
			r.fail("a found byte of %d", found)
		}
		sessions[id], ids = &session{seq: seq, lastIndex: lastIndex, found: found == 1}, append(ids, id)
	}
	return sessions, ids
}

// increasing reports whether s is in strictly increasing order.
func increasing[T cmp.Ordered](s []T) bool {
	for i := 1; i < len(s); i++ {
		if s[i-1] >= s[i] {
			return false
		}
	}
	return true
}

// reader reads a snapshot's fields in order. A read past the end returns
// zero values and records an error, which every later read keeps.
type reader struct {
	b   []byte
	err error
}

func (r *reader) next(n uint32) []byte {
	if r.err != nil {
		return nil
	}
	if uint64(n) > uint64(len(r.b)) {
		r.fail("a %d-byte field where %d bytes remain", n, len(r.b))
		return nil
	}
	field := r.b[:n]
	r.b = r.b[n:]
	return field
}

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

func (r *reader) u8() byte {
	if b := r.next(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.next(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (r *reader) u64() uint64 {
	if b := r.next(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}
