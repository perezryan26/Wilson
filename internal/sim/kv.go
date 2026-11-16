package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/perezryan26/wilson/internal/kv"
	"github.com/perezryan26/wilson/internal/linearizability"
	"github.com/perezryan26/wilson/raft"
)

// clientTimeout is how long a client waits for an answer before it sends
// its request again, to another server.
const clientTimeout = 300 * time.Millisecond

// Reply is a server's answer to a client's request, as the key-value
// service would send it.
type Reply struct {
	// NotLeader says the server could not take the request because it is
	// not the leader. Leader names the leader it knows of, or None.
	NotLeader bool
	Leader    raft.NodeID
	// Result is the request's result, once the command applied or, for a
	// read through ReadIndex, once the leader confirmed it.
	Result kv.Result
}

// request is a client's request at a server, and where the answer goes.
type request struct {
	cmd    kv.Command
	answer func(Reply)
}

// Call is a request a scripted test made with Request. Done turns true,
// and Reply holds the answer, once the server answers.
type Call struct {
	Done  bool
	Reply Reply
}

// Request hands cmd to server id at once, as a client would, and returns
// the call, which the server answers as the key-value service does: a
// write or a Register once its command applies on that server, and a Get
// once it applies, or, if the cluster reads through ReadIndex, once the
// leader confirms the read. A server that is down, or that crashes first,
// never answers. The server picks cmd's Request.
func (c *Cluster) Request(id raft.NodeID, cmd kv.Command) *Call {
	call := &Call{}
	c.serve(c.node(id), request{cmd: cmd, answer: func(r Reply) { call.Done, call.Reply = true, r }})
	c.observe()
	return call
}

// serve takes a client's request at server n, if it is up, as the
// key-value service does.
func (c *Cluster) serve(n *node, req request) {
	if n.core == nil {
		return
	}
	c.requests++
	id := c.requests
	if req.cmd.Op == kv.OpGet && c.readIndex {
		c.serveRead(n, id, req)
		return
	}
	req.cmd.Request = id
	index, term, err := n.core.Propose(req.cmd.Encode())
	if err != nil {
		c.tracef("%s %s: %v", name(n.id), commandName(req.cmd.Encode()), err)
		req.answer(Reply{NotLeader: true, Leader: n.core.Status().Leader})
		return
	}
	c.tracef("%s propose %s: index=%d term=%d", name(n.id), commandName(req.cmd.Encode()), index, term)
	n.waiting[id] = req
	c.handleReady(n)
}

// serveRead asks server n to confirm a read through ReadIndex (§8).
func (c *Cluster) serveRead(n *node, id uint64, req request) {
	if err := n.core.ReadIndex(id); err != nil {
		req.answer(Reply{NotLeader: true, Leader: n.core.Status().Leader})
		return
	}
	c.tracef("%s read %q: id=%d", name(n.id), req.cmd.Key, id)
	n.reads[id] = req
	c.handleReady(n)
}

// answerCommand answers the request, if n holds it, whose command
// produced r when n applied it: a write, a Register, or a Get through the
// log.
func (c *Cluster) answerCommand(n *node, r kv.Result) {
	if req, ok := n.waiting[r.Request]; ok && r.Request != 0 {
		delete(n.waiting, r.Request)
		req.answer(Reply{Result: r})
	}
}

// answerReads answers the reads n's core confirmed in its last Ready,
// whose committed entries n has applied by now, from n's store. A leader
// that stopped leading dropped the reads it held, which then never come.
func (c *Cluster) answerReads(n *node, confirmed []raft.ReadState) {
	for _, rs := range confirmed {
		if rs.Index > n.applied {
			c.fail(fmt.Errorf("server %d confirmed read %d at index %d with only %d applied", n.id, rs.ID, rs.Index, n.applied))
			return
		}
		req, ok := n.reads[rs.ID]
		if !ok {
			continue
		}
		delete(n.reads, rs.ID)
		value, found := n.store.Get(req.cmd.Key)
		c.tracef("%s confirm read id=%d index=%d", name(n.id), rs.ID, rs.Index)
		req.answer(Reply{Result: kv.Result{Value: value, Found: found}})
	}
	if n.core.Status().Role != raft.Leader {
		clear(n.reads)
	}
}

// commandName prints a key-value command as the trace shows it, or its
// bytes if it is not one.
func commandName(data []byte) string {
	cmd, err := kv.DecodeCommand(data)
	if err != nil {
		return fmt.Sprintf("%q", data)
	}
	switch cmd.Op {
	case kv.OpRegister:
		return "register"
	case kv.OpGet:
		return fmt.Sprintf("get(%q)", cmd.Key)
	case kv.OpPut:
		return fmt.Sprintf("put(%q, %q) session=%d seq=%d", cmd.Key, cmd.Value, cmd.Session, cmd.Seq)
	}
	return fmt.Sprintf("delete(%q) session=%d seq=%d", cmd.Key, cmd.Session, cmd.Seq)
}

// client is a simulated client of the key-value service. It registers a
// session, then makes one request at a time on two keys. It sends half
// its requests to the server that last answered it and half to a random
// server, as a client that lost track of the leader would, so that old
// leaders cut off in a minority get requests too. It sends a request
// again, unchanged, to another server when it hears nothing in time,
// follows a server's hint when that server does not lead, and starts a
// new session when its session has expired. Every Get, Put, and Delete
// goes into the run's history for Porcupine.
type client struct {
	id      int         // c1, c2, and so on; also its Porcupine client ID
	session uint64      // its session, or 0 before it registers
	seq     uint64      // the number of its last write
	server  raft.NodeID // where it sends its next attempt
	done    int         // requests answered

	// The request under way, if waiting: its command, its number among
	// the client's requests, its attempts so far, and its number in the
	// history, or -1 for a Register.
	waiting bool
	cmd     kv.Command
	number  int
	attempt int
	op      int
	in      linearizability.Input
}

// startClients starts n clients, each at a random moment of the first
// 200 ms. With idle set, the clients send nothing for up to two seconds
// whenever a leader of a later term appears.
func (c *Cluster) startClients(n int, idle bool) {
	c.idleClients = idle
	for i := range n {
		cl := &client{id: i + 1, server: c.randomServer()}
		c.clients = append(c.clients, cl)
		c.schedule(c.now+c.randDuration(0, 200*time.Millisecond), func() { c.nextRequest(cl) })
	}
}

func (c *Cluster) randomServer() raft.NodeID { return raft.NodeID(1 + c.rng.IntN(len(c.nodes))) }

// nextRequest begins cl's next request: a Register if it has no session,
// and otherwise a Get, Put, or Delete of one of two keys, few so that
// operations overlap, with a value no other Put uses.
func (c *Cluster) nextRequest(cl *client) {
	cl.waiting, cl.number, cl.attempt, cl.op = true, cl.number+1, 0, -1
	if c.rng.IntN(2) == 0 {
		cl.server = c.randomServer()
	}
	if cl.session == 0 {
		cl.cmd = kv.Command{Op: kv.OpRegister}
		c.sendRequest(cl)
		return
	}
	key := [...]string{"x", "y"}[c.rng.IntN(2)]
	switch r := c.rng.IntN(10); {
	case r < 4:
		cl.cmd, cl.in = kv.Command{Op: kv.OpGet, Key: key}, linearizability.Input{Op: linearizability.Get, Key: key}
	case r < 8:
		cl.seq++
		value := fmt.Sprintf("c%d.%d", cl.id, cl.seq)
		cl.cmd = kv.Command{Op: kv.OpPut, Session: cl.session, Seq: cl.seq, Key: key, Value: []byte(value)}
		cl.in = linearizability.Input{Op: linearizability.Put, Key: key, Value: value}
	default:
		cl.seq++
		cl.cmd = kv.Command{Op: kv.OpDelete, Session: cl.session, Seq: cl.seq, Key: key}
		cl.in = linearizability.Input{Op: linearizability.Delete, Key: key}
	}
	cl.op = c.history.Call(cl.id, cl.in)
	c.sendRequest(cl)
}

// sendRequest sends the request under way to cl's server, over the
// network, and sends it again to another server if no answer comes in
// time.
func (c *Cluster) sendRequest(cl *client) {
	if wait := c.clientPause(); wait > 0 {
		c.schedule(c.now+wait, func() { c.sendRequest(cl) })
		return
	}
	cl.attempt++
	number, attempt, to := cl.number, cl.attempt, c.node(cl.server)
	c.tracef("c%d → %s %s", cl.id, name(to.id), commandName(cl.cmd.Encode()))
	answer := func(r Reply) { c.carry(func() { c.receive(cl, number, attempt, to.id, r) }) }
	c.carry(func() { c.serve(to, request{cmd: cl.cmd, answer: answer}) })
	c.schedule(c.now+clientTimeout, func() {
		if cl.waiting && cl.number == number && cl.attempt == attempt {
			c.tracef("c%d timed out", cl.id)
			cl.server = c.otherServer(cl.server)
			c.sendRequest(cl)
		}
	})
}

// clientPause returns how long clients must still wait before they send
// anything, when they idle after each election.
func (c *Cluster) clientPause() time.Duration {
	if !c.idleClients {
		return 0
	}
	if term := c.highestLeaderTerm(); term > c.seenTerm {
		c.seenTerm = term
		c.pauseUntil = c.now + c.randDuration(0, 2*time.Second)
	}
	return max(0, c.pauseUntil-c.now)
}

// highestLeaderTerm returns the highest term a running server leads, or 0.
func (c *Cluster) highestLeaderTerm() raft.Term {
	var term raft.Term
	for _, id := range c.ids() {
		if st, up := c.Status(id); up && st.Role == raft.Leader {
			term = max(term, st.Term)
		}
	}
	return term
}

// otherServer returns a random server other than id, or id in a cluster
// of one.
func (c *Cluster) otherServer(id raft.NodeID) raft.NodeID {
	if len(c.nodes) == 1 {
		return id
	}
	other := raft.NodeID(1 + c.rng.IntN(len(c.nodes)-1))
	if other >= id {
		other++
	}
	return other
}

// receive handles server from's reply r to attempt of cl's request number.
// A reply to an earlier request is late and changes nothing; so is a
// redirect that a later attempt has overtaken. Any other answer, to this
// attempt or an earlier one, answers the request.
func (c *Cluster) receive(cl *client, number, attempt int, from raft.NodeID, r Reply) {
	if !cl.waiting || cl.number != number {
		return
	}
	if r.NotLeader {
		if attempt == cl.attempt {
			c.tracef("%s → c%d not the leader; leader=%s", name(from), cl.id, name(r.Leader))
			cl.server = r.Leader
			if r.Leader == raft.None || r.Leader == from {
				cl.server = c.otherServer(from)
			}
			c.schedule(c.now+c.randDuration(10*time.Millisecond, 50*time.Millisecond), func() { c.sendRequest(cl) })
		}
		return
	}
	c.tracef("%s → c%d %s", name(from), cl.id, resultName(cl.cmd, r.Result))
	cl.waiting, cl.server = false, from
	if err := c.finish(cl, r.Result); err != nil {
		c.fail(fmt.Errorf("client %d: %w", cl.id, err))
		return
	}
	cl.done++
	c.schedule(c.now+c.randDuration(0, 100*time.Millisecond), func() { c.nextRequest(cl) })
}

// finish records the outcome of cl's request. A write whose session has
// expired stays open in the history, since whether it ran is unknown,
// and the client starts a new session.
func (c *Cluster) finish(cl *client, r kv.Result) error {
	switch {
	case errors.Is(r.Err, kv.ErrSessionExpired):
		cl.session = 0
		return nil
	case r.Err != nil:
		return fmt.Errorf("%s: %w", commandName(cl.cmd.Encode()), r.Err)
	case cl.cmd.Op == kv.OpRegister:
		cl.session, cl.seq = r.Session, 0
		return nil
	}
	c.history.Return(cl.op, linearizability.Output{Value: string(r.Value), Found: r.Found})
	return nil
}

// resultName prints the result of cmd as the trace shows it.
func resultName(cmd kv.Command, r kv.Result) string {
	switch {
	case r.Err != nil:
		return r.Err.Error()
	case cmd.Op == kv.OpRegister:
		return fmt.Sprintf("session=%d", r.Session)
	case cmd.Op == kv.OpPut:
		return "ok"
	case r.Found && cmd.Op == kv.OpGet:
		return fmt.Sprintf("%q", r.Value)
	}
	return fmt.Sprintf("found=%t", r.Found)
}

// carry delivers a message between a client and a server: like a raft
// message it may be lost, duplicated, or delayed, but no partition cuts
// clients off from any server.
func (c *Cluster) carry(deliver func()) {
	if c.rng.IntN(100) < c.net.LossPercent {
		return
	}
	copies := 1
	if c.rng.IntN(100) < c.net.DupPercent {
		copies = 2
	}
	for range copies {
		c.schedule(c.now+c.randDuration(c.net.MinDelay, c.net.MaxDelay), deliver)
	}
}

// checkClients requires every client to finish a request, within
// livenessTime, after the faults have healed.
func (c *Cluster) checkClients() error {
	done := make([]int, len(c.clients))
	for i, cl := range c.clients {
		done[i] = cl.done
	}
	return c.RunUntil(livenessTime, "every client to finish a request", func() bool {
		for i, cl := range c.clients {
			if cl.done == done[i] {
				return false
			}
		}
		return true
	})
}
