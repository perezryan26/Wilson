package sim_test

import (
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/sim"
	"github.com/perezryan26/wilson/raft"
)

var seeds = flag.Uint64("seeds", 20, "how many seeds TestRandomized runs")

// TestRandomized runs seeds 0 to N-1 of the randomized schedule in
// parallel, where -seeds=N sets N. Each seed checks the invariants after
// every event, then checks liveness.
func TestRandomized(t *testing.T) {
	for seed := range *seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			if err := sim.RunRandomized(seed); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
		})
	}
}

// Lost, duplicated, and slow messages delay an election but never break
// safety: under 30% loss, 20% duplication, and delays of up to 100 ms, a
// cluster of five still agrees on a leader, and the invariants hold.
func TestElectsLeaderDespiteLossAndDuplication(t *testing.T) {
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 5, Seed: seed})
		c.SetNetwork(sim.Network{LossPercent: 30, DupPercent: 20, MinDelay: time.Millisecond, MaxDelay: 100 * time.Millisecond})
		err := c.RunUntil(30*time.Second, "an agreed leader", func() bool {
			lead, _ := c.AgreedLeader()
			return lead != raft.None
		})
		if err == nil {
			err = c.RunFor(30 * time.Second)
		}
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}
