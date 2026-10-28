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

// Entry is one log entry: a command for the state machine and the term in
// which the leader received it (Figure 2).
type Entry struct {
	Index Index
	Term  Term
	Data  []byte
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

	RequestVote           *RequestVoteRequest
	RequestVoteResponse   *RequestVoteResponse
	AppendEntries         *AppendEntriesRequest
	AppendEntriesResponse *AppendEntriesResponse
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
// a leader sends as a heartbeat to keep its authority (§5.2, Figure 2).
type AppendEntriesRequest struct {
	Term     Term   // leader's term
	LeaderID NodeID // so the follower can redirect clients
}

// AppendEntriesResponse holds the results of the AppendEntries RPC.
type AppendEntriesResponse struct {
	Term    Term // currentTerm, for the leader to update itself
	Success bool // false when the request's term was stale
}

// Ready holds the effects a node produced since the previous call to
// Node.Ready. The runtime carries them out in field order: it persists and
// fsyncs HardState before it sends Messages, so a granted vote never
// leaves before the vote is durable (docs/adr/0001-deterministic-core.md).
type Ready struct {
	// HardState is the hard state to persist, or nil if it is unchanged.
	HardState *HardState
	// Messages are the messages to send once HardState is durable.
	Messages []Message
}
