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

// deliver hands m to its receiver, unless the receiver is down, and carries
// out the receiver's Ready.
func (c *Cluster) deliver(m raft.Message) {
	n := c.node(m.To)
	if n.core == nil {
		return
	}
	n.core.Step(m)
	c.handleReady(n)
}
