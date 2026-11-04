package sim

import (
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
// restarts, and networks that lose, duplicate, and delay messages. Then
// every fault heals, and the servers must agree on a leader within ten
// seconds. RunRandomized returns the cluster, whose trace tells what
// happened, and the first violation or liveness failure. traceMessages
// adds every message to the trace.
func RunRandomized(seed uint64, traceMessages bool) (*Cluster, error) {
	c := New(Options{Size: 1 + int(seed%5), Seed: seed, TraceMessages: traceMessages})
	for c.now < faultTime {
		if err := c.RunFor(c.randDuration(50*time.Millisecond, time.Second)); err != nil {
			return c, err
		}
		c.randomFault()
	}
	c.healAll()
	err := c.RunUntil(livenessTime, "an agreed leader after healing", func() bool {
		lead, _ := c.AgreedLeader()
		return lead != raft.None
	})
	return c, err
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
		c.Crash(id)
	case 6, 7:
		c.Restart(id)
	default:
		c.SetNetwork(c.randomNetwork())
	}
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
// network, and restarts every crashed server.
func (c *Cluster) healAll() {
	c.Heal()
	c.SetNetwork(DefaultNetwork())
	for _, id := range c.ids() {
		c.Restart(id)
	}
}
