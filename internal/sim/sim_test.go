package sim_test

import (
	"flag"
	"fmt"
	"testing"

	"github.com/perezryan26/wilson/internal/sim"
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
