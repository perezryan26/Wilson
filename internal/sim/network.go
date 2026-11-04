package sim

import (
	"time"

	"github.com/perezryan26/wilson/raft"
)

// Network describes how messages travel between servers.
type Network struct {
	LossPercent int           // chance in 100 that a message is lost
	DupPercent  int           // chance in 100 that a message arrives twice
	MinDelay    time.Duration // each arrival takes a delay drawn uniformly
	MaxDelay    time.Duration // from [MinDelay, MaxDelay]
}

// DefaultNetwork loses and duplicates nothing and delivers each message
// within 1 to 10 ms, so messages still overtake one another.
func DefaultNetwork() Network {
	return Network{MinDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond}
}

// SetNetwork changes how the messages sent from now on travel.
func (c *Cluster) SetNetwork(n Network) { c.net = n }

// link is the one-way path from one server to another.
type link struct{ from, to raft.NodeID }

// Partition splits the cluster into groups: a server can then reach only
// the servers in its own group, and a server named in no group can reach
// no one. It replaces any earlier partition or cut. A message already in
// flight across a new boundary is lost when it arrives.
func (c *Cluster) Partition(groups ...[]raft.NodeID) {
	group := map[raft.NodeID]int{}
	for i, g := range groups {
		for _, id := range g {
			group[id] = i + 1
		}
	}
	c.blocked = map[link]bool{}
	for _, a := range c.ids() {
		for _, b := range c.ids() {
			if a != b && (group[a] == 0 || group[a] != group[b]) {
				c.blocked[link{a, b}] = true
			}
		}
	}
}

// Cut blocks messages from one server to another but not the reverse, as a
// one-way network failure does.
func (c *Cluster) Cut(from, to raft.NodeID) { c.blocked[link{from, to}] = true }

// Heal restores every link that a partition or a cut blocked.
func (c *Cluster) Heal() { c.blocked = map[link]bool{} }

// send puts m in flight: it may be lost or duplicated, and each copy
// arrives after its own random delay.
func (c *Cluster) send(m raft.Message) {
	if c.rng.IntN(100) < c.net.LossPercent {
		return
	}
	copies := 1
	if c.rng.IntN(100) < c.net.DupPercent {
		copies = 2
	}
	for range copies {
		at := c.now + c.randDuration(c.net.MinDelay, c.net.MaxDelay)
		c.schedule(at, func() { c.deliver(m) })
	}
}

// deliver hands m to its receiver, unless the receiver is down or the link
// is blocked, and carries out the receiver's Ready.
func (c *Cluster) deliver(m raft.Message) {
	n := c.node(m.To)
	if n.core == nil || c.blocked[link{m.From, m.To}] {
		return
	}
	n.core.Step(m)
	c.handleReady(n)
}
