package sim

import (
	"time"

	"github.com/perezryan26/wilson/raft"
)

const (
	// runTime is how long a randomized run lasts before its liveness check.
	runTime = 10 * time.Second
	// livenessTime bounds how long the cluster may then take to agree on a
	// leader: about ten of the longest election timeouts.
	livenessTime = 10 * time.Second
)

// RunRandomized runs one seed of the randomized schedule. A cluster of one
// to five servers, sized by the seed, runs for ten seconds of virtual time
// and must then agree on a leader within ten more. It returns the cluster,
// whose trace tells what happened, and the first violation or liveness
// failure. traceMessages adds every message to the trace.
func RunRandomized(seed uint64, traceMessages bool) (*Cluster, error) {
	c := New(Options{Size: 1 + int(seed%5), Seed: seed, TraceMessages: traceMessages})
	if err := c.RunFor(runTime); err != nil {
		return c, err
	}
	err := c.RunUntil(livenessTime, "an agreed leader", func() bool {
		lead, _ := c.AgreedLeader()
		return lead != raft.None
	})
	return c, err
}
