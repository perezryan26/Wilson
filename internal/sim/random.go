package sim

import (
	"fmt"
	"slices"
	"time"

	"github.com/perezryan26/wilson/raft"
)

const (
	// faultTime is how long a randomized run injects faults.
	faultTime = 10 * time.Second
	// livenessTime bounds how long a healed cluster may take to agree on a
	// leader: about ten of the longest election timeouts.
	livenessTime = 10 * time.Second
)

// RunRandomized runs one seed of the randomized schedule. A cluster of one
// to five servers, sized by the seed, spends ten seconds of virtual time
// under faults the seed chooses: partitions, one-way cuts, crashes,
// restarts, and networks that lose, duplicate, and delay messages. All the
// while, a client proposes commands to whichever servers believe they
// lead. Then every fault heals, and within ten seconds the servers must
// agree on a leader, and within ten more apply a command proposed to it.
// RunRandomized returns the cluster, whose trace tells what happened, and
// the first violation or liveness failure. traceMessages adds every
// message to the trace.
func RunRandomized(seed uint64, traceMessages bool) (*Cluster, error) {
	c := New(Options{Size: 1 + int(seed%5), Seed: seed, TraceMessages: traceMessages})
	c.runClient()
	for c.now < faultTime {
		if err := c.RunFor(c.randDuration(50*time.Millisecond, time.Second)); err != nil {
			return c, err
		}
		c.randomFault()
	}
	c.healAll()
	return c, c.checkLiveness()
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
	index, _, err := c.Propose(lead, c.command())
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

// runClient proposes a new command every 10 to 200 ms, for the rest of the
// run, to a server that believes it leads, if any does. A stale leader
// accepts commands that a later leader overwrites, which is the point.
func (c *Cluster) runClient() {
	c.schedule(c.now+c.randDuration(10*time.Millisecond, 200*time.Millisecond), func() {
		var leaders []raft.NodeID
		for _, id := range c.ids() {
			if st, up := c.Status(id); up && st.Role == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) > 0 {
			// A server that believes it leads accepts the command.
			_, _, _ = c.Propose(leaders[c.rng.IntN(len(leaders))], c.command())
		}
		c.runClient()
	})
}

// command returns a command no other proposal in the run uses, so that two
// entries with the same command are the same entry.
func (c *Cluster) command() []byte {
	c.commands++
	return []byte(fmt.Sprintf("v%d", c.commands))
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

// randomCrash crashes server id at once, between events, or during one of
// its next six disk operations, losing whatever it had not synced.
func (c *Cluster) randomCrash(id raft.NodeID) {
	if c.rng.IntN(2) == 0 {
		c.Crash(id)
		return
	}
	c.CrashDuringDiskOp(id, 1+c.rng.IntN(6))
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
