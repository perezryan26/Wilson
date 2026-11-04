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
// and must then agree on a leader within ten more. RunRandomized returns
// the first violation or liveness failure.
func RunRandomized(seed uint64) error {
	c := New(Options{Size: 1 + int(seed%5), Seed: seed})
	if err := c.RunFor(runTime); err != nil {
		return err
	}
	return c.RunUntil(livenessTime, "an agreed leader", func() bool {
		lead, _ := c.AgreedLeader()
		return lead != raft.None
	})
}
