package sim

import (
	"time"

	"github.com/perezryan26/wilson/raft"
)

// network decides how each message travels between servers.
type network struct {
	minDelay, maxDelay time.Duration
}

// send puts m in flight. It arrives after a delay drawn at random, so
// messages overtake one another as they would on a real network.
func (c *Cluster) send(m raft.Message) {
	at := c.now + c.randDuration(c.net.minDelay, c.net.maxDelay)
	c.schedule(at, func() { c.deliver(m) })
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
