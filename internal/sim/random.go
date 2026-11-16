package sim

import (
	"fmt"
	"slices"
	"time"

	"github.com/perezryan26/wilson/internal/kv"
	"github.com/perezryan26/wilson/raft"
)

const (
	// faultTime is how long a randomized run injects faults.
	faultTime = 10 * time.Second
	// livenessTime bounds how long a healed cluster may take to agree on a
	// leader: about ten of the longest election timeouts.
	livenessTime = 10 * time.Second
	// checkTime bounds how long Porcupine may take to check a seed's
	// history. A seed's few hundred operations on three keys take
	// milliseconds.
	checkTime = 10 * time.Second
)

// RunRandomized runs one seed of the randomized schedule. A cluster of one
// to five servers, sized by the seed, spends ten seconds of virtual time
// under faults the seed chooses: partitions, one-way cuts, crashes,
// restarts, and networks that lose, duplicate, and delay messages. All the
// while, five clients of the key-value service send requests, retrying
// and following redirects, in half the seeds pausing after each new
// leader. The servers compact their logs
// as often as every entry, or never, all alike or in half the seeds each
// on its own schedule, and send snapshots whole or in small chunks and
// entries all at once or three at a time, as the seed chooses. In half
// the seeds each new leader commits a no-op, in half reads go through
// ReadIndex instead of the log, and in some the stores keep only five
// sessions. Then every fault heals: within ten seconds the servers must
// agree on a leader, within ten more apply a command proposed to it, and
// within ten more every client must finish a request. Last, Porcupine
// checks the clients' history. RunRandomized returns the cluster, whose
// trace tells what happened, and the first violation, liveness failure,
// or history that is not linearizable. traceMessages adds every message
// to the trace.
func RunRandomized(seed uint64, traceMessages bool) (*Cluster, error) {
	thresholds := []int{0, 1, 2, 4, 8}
	c := New(Options{
		Size:              1 + int(seed%5),
		Seed:              seed,
		TraceMessages:     traceMessages,
		CompactEvery:      thresholds[seed/5%5],
		SnapshotChunkSize: []int{0, 16, 128}[seed/25%3],
		// Three entries' worth, so leaders send logs in small batches.
		MaxAppendSize: []int{0, 210}[seed/300%2],
		// Without the no-op, a new leader commits entries of earlier terms
		// only with a command of its own, which Figure 8's hazard needs.
		CommitNoOpOnElection: seed/2%2 == 0,
		ReadIndex:            seed%2 == 1,
		// As many sessions as clients, so that a repeated Register evicts
		// a session still in use.
		MaxSessions: []int{5, 1000}[seed/600%2],
	})
	if seed/150%2 == 1 {
		// Servers that compact on schedules of their own leave a follower
		// with a snapshot past entries its leader still sends it.
		for _, n := range c.nodes {
			n.compactEvery = thresholds[c.rng.IntN(len(thresholds))]
			c.tracef("%s compactEvery=%d", name(n.id), n.compactEvery)
		}
	}
	c.startClients(5, seed/75%2 == 1)
	for c.now < faultTime {
		if err := c.RunFor(c.randDuration(50*time.Millisecond, time.Second)); err != nil {
			return c, err
		}
		c.randomFault()
	}
	c.healAll()
	if err := c.checkLiveness(); err != nil {
		return c, err
	}
	if err := c.checkClients(); err != nil {
		return c, err
	}
	return c, c.history.Check(checkTime, "")
}

// checkLiveness requires the healed cluster to make progress: the servers
// agree on a leader, and a command proposed to it is applied everywhere,
// along with every entry before it in the leader's log.
func (c *Cluster) checkLiveness() error {
	var lead raft.NodeID
	err := c.RunUntil(livenessTime, "an agreed leader after healing", func() bool {
		lead, _ = c.AgreedLeader()
		return lead != raft.None
	})
	if err != nil {
		return err
	}
	c.requests++
	index, _, err := c.Propose(lead, kv.Command{Op: kv.OpGet, Request: c.requests, Key: "x"}.Encode())
	if err != nil {
		return err
	}
	return c.RunUntil(livenessTime, fmt.Sprintf("every server to apply entry %d", index), func() bool {
		for _, id := range c.ids() {
			if st, up := c.Status(id); !up || st.LastApplied < index {
				return false
			}
		}
		return true
	})
}

// randomFault injects one fault: a partition or a heal, a one-way cut, a
// crash or a restart, or new network conditions.
func (c *Cluster) randomFault() {
	ids := c.ids()
	id, other := ids[c.rng.IntN(len(ids))], ids[c.rng.IntN(len(ids))]
	switch c.rng.IntN(10) {
	case 0, 1:
		c.Partition(c.randomGroups()...)
	case 2, 3:
		c.Heal()
	case 4:
		if id != other {
			c.Cut(id, other)
		}
	case 5:
		c.randomCrash(id)
	case 6, 7:
		c.Restart(id)
	default:
		c.SetNetwork(c.randomNetwork())
	}
}

// randomCrash crashes a server: half the time the current leader, if one
// is up, since leaders that crash and come back are how Figure 8's hazard
// arises, and otherwise id. Half the crashes happen at once, between
// events; the rest strike during one of the server's next six disk
// operations and lose whatever it had not synced.
func (c *Cluster) randomCrash(id raft.NodeID) {
	if lead := c.currentLeader(); lead != raft.None && c.rng.IntN(2) == 0 {
		id = lead
	}
	if c.rng.IntN(2) == 0 {
		c.Crash(id)
		return
	}
	c.CrashDuringDiskOp(id, 1+c.rng.IntN(6))
}

// currentLeader returns the running server that leads the highest term,
// or None if no running server believes it leads.
func (c *Cluster) currentLeader() raft.NodeID {
	lead, term := raft.None, raft.Term(0)
	for _, id := range c.ids() {
		if st, up := c.Status(id); up && st.Role == raft.Leader && st.Term > term {
			lead, term = id, st.Term
		}
	}
	return lead
}

// randomGroups splits the servers into two or three groups at random,
// leaving out any group that ends up empty.
func (c *Cluster) randomGroups() [][]raft.NodeID {
	groups := make([][]raft.NodeID, 2+c.rng.IntN(2))
	for _, id := range c.ids() {
		g := c.rng.IntN(len(groups))
		groups[g] = append(groups[g], id)
	}
	return slices.DeleteFunc(groups, func(g []raft.NodeID) bool { return len(g) == 0 })
}

// randomNetwork returns network conditions from perfect to severe: up to
// half of all messages lost, a fifth duplicated, and delays of up to
// 400 ms, longer than the shortest election timeout.
func (c *Cluster) randomNetwork() Network {
	loss := []int{0, 0, 5, 20, 50}
	dup := []int{0, 0, 5, 20}
	maxDelay := []time.Duration{10 * time.Millisecond, 50 * time.Millisecond, 150 * time.Millisecond, 400 * time.Millisecond}
	return Network{
		LossPercent: loss[c.rng.IntN(len(loss))],
		DupPercent:  dup[c.rng.IntN(len(dup))],
		MinDelay:    time.Millisecond,
		MaxDelay:    maxDelay[c.rng.IntN(len(maxDelay))],
	}
}

// healAll ends every fault: it heals all links, restores the default
// network, cancels crashes still to come, and restarts every crashed
// server.
func (c *Cluster) healAll() {
	c.Heal()
	c.SetNetwork(DefaultNetwork())
	for _, id := range c.ids() {
		c.CrashDuringDiskOp(id, 0)
		c.Restart(id)
	}
}
