package raft

// Term numbers an election and the leadership that follows it. Terms act
// as a logical clock: every message carries one, and a server that sees a
// later term than its own adopts it (§5.1).
type Term uint64

// Index is the position of an entry in the log. The log is 1-indexed as in
// the paper, so index 0 means "before the first entry".
type Index uint64

// NodeID identifies a server. Real IDs start at 1, because 0 is None.
type NodeID uint64

// None is the NodeID that names no server, as in a votedFor that is null.
const None NodeID = 0

// EntryType says what a log entry holds.
type EntryType uint8

const (
	// EntryNormal is the type of an entry that holds a command for the
	// state machine.
	EntryNormal EntryType = 0
	// EntryNoOp is the type of the entry a new leader appends at the start
	// of its term when Config.CommitNoOpOnElection is set. It holds no
	// command, and the state machine skips it.
	EntryNoOp EntryType = 1
)

// Entry is one log entry: a command for the state machine and the term in
// which the leader received it (Figure 2).
type Entry struct {
	Index Index
	Term  Term
	Type  EntryType // what Data holds
	Data  []byte
}

// Snapshot is the state machine's state as of a log index, which replaces
// every log entry up to and including that index (§7, Figure 12). It
// covers only committed entries.
type Snapshot struct {
	LastIncludedIndex Index // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  Term  // term of LastIncludedIndex

	// Data is the state machine's state, as the service encoded it. No
	// one modifies it once the snapshot exists, so it is shared rather
	// than copied.
	Data []byte
}

// HardState is the state Figure 2 requires a server to keep on stable
// storage, apart from the log, before it responds to any RPC.
type HardState struct {
	CurrentTerm Term
	VotedFor    NodeID
}

// Message is one RPC request or response travelling between two servers.
// Exactly one of the payload fields is set. A response is a message of its
// own rather than a return value, so a reply can leave only after the
// state it reflects is durable (see Ready).
type Message struct {
	From, To NodeID

	RequestVote             *RequestVoteRequest
	RequestVoteResponse     *RequestVoteResponse
	AppendEntries           *AppendEntriesRequest
	AppendEntriesResponse   *AppendEntriesResponse
	InstallSnapshot         *InstallSnapshotRequest
	InstallSnapshotResponse *InstallSnapshotResponse
}

// term returns the term m carries: the sender's currentTerm, which every
// request and response includes.
func (m Message) term() Term {
	switch {
	case m.RequestVote != nil:
		return m.RequestVote.Term
	case m.RequestVoteResponse != nil:
		return m.RequestVoteResponse.Term
	case m.AppendEntries != nil:
		return m.AppendEntries.Term
	case m.AppendEntriesResponse != nil:
		return m.AppendEntriesResponse.Term
	case m.InstallSnapshot != nil:
		return m.InstallSnapshot.Term
	case m.InstallSnapshotResponse != nil:
		return m.InstallSnapshotResponse.Term
	}
	return 0
}

// RequestVoteRequest holds the arguments of the RequestVote RPC, which
// candidates send to gather votes (§5.2, Figure 2).
type RequestVoteRequest struct {
	Term         Term   // candidate's term
	CandidateID  NodeID // candidate requesting the vote
	LastLogIndex Index  // index of the candidate's last log entry (§5.4)
	LastLogTerm  Term   // term of the candidate's last log entry (§5.4)
}

// RequestVoteResponse holds the results of the RequestVote RPC. The
// envelope's From says who voted.
type RequestVoteResponse struct {
	Term        Term // currentTerm, for the candidate to update itself
	VoteGranted bool // true means the candidate received the vote
}

// AppendEntriesRequest holds the arguments of the AppendEntries RPC, which
// a leader sends to replicate log entries (§5.3) and, with no entries, as a
// heartbeat (§5.2).
type AppendEntriesRequest struct {
	Term         Term    // leader's term
	LeaderID     NodeID  // so the follower can redirect clients
	PrevLogIndex Index   // index of the log entry immediately preceding new ones
	PrevLogTerm  Term    // term of the PrevLogIndex entry
	Entries      []Entry // log entries to store; empty for a heartbeat
	LeaderCommit Index   // leader's commitIndex

	// ReadRound is the leader's latest round of leadership checks for
	// reads when it sent the request, which the follower's reply echoes:
	// a read is served once a majority has answered a request sent after
	// the read arrived (§8; docs/adr/0008-read-path.md).
	ReadRound uint64
}

// AppendEntriesResponse holds the results of the AppendEntries RPC.
type AppendEntriesResponse struct {
	Term    Term // currentTerm, for the leader to update itself
	Success bool // true if the follower held an entry matching PrevLogIndex and PrevLogTerm

	// MatchIndex is, on success, the index of the last entry the request
	// carried, PrevLogIndex + len(Entries). The reply names what it
	// acknowledges because replies can arrive late, twice, or out of order,
	// so the leader cannot assume a reply answers what it last sent.
	MatchIndex Index

	// PrevLogIndex is, on a rejection for a log inconsistency, the
	// PrevLogIndex of the request refused: the probe of the follower's
	// log that the rejection answers. The leader acts only on a rejection
	// of its latest probe, because one answering an earlier probe could
	// back nextIndex up past entries the follower holds.
	PrevLogIndex Index

	// ConflictTerm and ConflictIndex say, on a rejection for a log
	// inconsistency, where the logs part, so the leader can skip a whole
	// conflicting term per round trip (§5.3;
	// docs/adr/0004-fast-log-backtracking.md). ConflictTerm is the term of
	// the follower's entry at PrevLogIndex, or 0 if it has none, and
	// ConflictIndex is the first index of that term in its log, or its last
	// index + 1.
	ConflictTerm  Term
	ConflictIndex Index

	// ReadRound echoes the request's ReadRound, on success or a rejection
	// for a log inconsistency.
	ReadRound uint64
}

// InstallSnapshotRequest holds the arguments of the InstallSnapshot RPC,
// which a leader sends to a follower that needs entries the leader has
// already replaced with a snapshot (§7, Figure 13).
type InstallSnapshotRequest struct {
	Term              Term   // leader's term
	LeaderID          NodeID // so the follower can redirect clients
	LastIncludedIndex Index  // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  Term   // term of LastIncludedIndex
	Offset            int    // byte offset where the chunk is positioned in the snapshot
	Data              []byte // raw bytes of the snapshot chunk, starting at Offset
	Done              bool   // true if this is the last chunk
}

// InstallSnapshotResponse holds the results of the InstallSnapshot RPC.
// Beyond Figure 13's term, it says what it acknowledges, because replies
// can arrive late, twice, or out of order.
type InstallSnapshotResponse struct {
	Term Term // currentTerm, for the leader to update itself

	// LastIncludedIndex names the snapshot the reply answers.
	LastIncludedIndex Index
	// Offset, until Done, is how many of the snapshot's bytes the
	// follower holds: where the next chunk must begin.
	Offset int
	// Done is true once the follower's log matches the leader's through
	// LastIncludedIndex, because it installed the snapshot or already had
	// every entry it covers.
	Done bool
}

// Ready holds the effects a node produced since the previous call to
// Node.Ready. The runtime carries them out in field order: it persists and
// fsyncs HardState, Snapshot, and Entries, then sends Messages, then
// restores the state machine from Snapshot, when it must, and applies
// CommittedEntries. A granted vote or an acknowledged entry thus never
// leaves before it is durable, and no entry is applied before it is on
// this server's disk (docs/adr/0001-deterministic-core.md).
type Ready struct {
	// HardState is the hard state to persist, or nil if it is unchanged.
	HardState *HardState
	// Snapshot is a snapshot to persist, or nil: one Compact took, or one
	// the leader sent with InstallSnapshot. It and Entries, which then
	// hold every log entry after it, replace the whole stored log. If it
	// is past the last entry the state machine applied, which is always
	// so for one from the leader and never for one from Compact, the
	// runtime restores the state machine from it before applying
	// CommittedEntries (Figure 13, step 8).
	Snapshot *Snapshot
	// Entries are log entries to persist, in index order. When
	// Entries[0].Index is at or below the last stored index, storage first
	// discards every stored entry from that index on, because the node is
	// overwriting a conflicting suffix (Figure 2, AppendEntries step 3).
	Entries []Entry
	// Messages are the messages to send once HardState and Entries are
	// durable.
	Messages []Message
	// CommittedEntries are the entries committed since the last Ready, to
	// apply to the state machine in index order (Figure 2, all servers: if
	// commitIndex > lastApplied, apply log[lastApplied]). Each entry comes
	// out exactly once per boot.
	CommittedEntries []Entry
	// ReadStates are the reads that ReadIndex asked about and the leader
	// has since confirmed. Each may be served from the state machine once
	// it has applied the read's Index, which applying this Ready's
	// CommittedEntries always reaches.
	ReadStates []ReadState
}
