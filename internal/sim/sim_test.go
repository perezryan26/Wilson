package sim_test

import (
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/sim"
	"github.com/perezryan26/wilson/raft"
)

var (
	seeds  = flag.Uint64("seeds", 20, "how many seeds TestRandomized runs")
	replay = flag.Int64("seed", -1, "run only this seed of TestRandomized and log its trace, every message included")
)

// TestRandomized runs seeds 0 to N-1 of the randomized schedule in
// parallel, where -seeds=N sets N. Each seed checks the invariants after
// every event, then checks liveness. A failure prints the seed's trace;
// -seed=N replays seed N alone with every message traced.
func TestRandomized(t *testing.T) {
	if *replay >= 0 {
		c, err := sim.RunRandomized(uint64(*replay), true)
		t.Logf("seed %d trace:\n%s", *replay, c.Trace())
		if err != nil {
			t.Fatalf("seed %d: %v", *replay, err)
		}
		return
	}
	for seed := range *seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			c, err := sim.RunRandomized(seed, false)
			if err != nil {
				t.Fatalf("seed %d: %v\n%s\nReplay it with every message traced: go test ./internal/sim -run TestRandomized -seed=%d -v",
					seed, err, c.Trace(), seed)
			}
		})
	}
}

// The simulator is deterministic: a seed run twice traces the same bytes,
// every message included, and another seed traces differently.
func TestSameSeedSameTrace(t *testing.T) {
	trace := func(seed uint64) string {
		c, err := sim.RunRandomized(seed, true)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		return c.Trace()
	}
	first, second := trace(9), trace(9)
	if first != second {
		t.Fatalf("seed 9 traced differently the second time, from line %d", firstDifference(first, second))
	}
	if !strings.Contains(first, "RequestVote ") || !strings.Contains(first, "AppendEntries ") {
		t.Fatalf("seed 9's trace has no messages:\n%s", first)
	}
	if first == trace(14) {
		t.Error("seeds 9 and 14 traced the same bytes")
	}
}

// firstDifference returns the number of the first line where a and b
// differ.
func firstDifference(a, b string) int {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := range min(len(al), len(bl)) {
		if al[i] != bl[i] {
			return i + 1
		}
	}
	return min(len(al), len(bl)) + 1
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

// awaitLeader runs c until the running servers among ids, or all servers
// if there are none, agree on a leader, and returns it and its term.
func awaitLeader(t *testing.T, c *sim.Cluster, ids ...raft.NodeID) (raft.NodeID, raft.Term) {
	t.Helper()
	var lead raft.NodeID
	var term raft.Term
	err := c.RunUntil(10*time.Second, "an agreed leader", func() bool {
		lead, term = c.AgreedLeader(ids...)
		return lead != raft.None
	})
	if err != nil {
		t.Fatal(err)
	}
	return lead, term
}

// §5.2: a leader cut off from the rest of a cluster of five keeps believing
// it leads, but the majority elects a new leader in a later term. When the
// partition heals, the old leader learns the new term and steps down.
func TestIsolatedLeaderIsReplaced(t *testing.T) {
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 5, Seed: seed})
		old, oldTerm := awaitLeader(t, c)
		var rest []raft.NodeID
		for id := raft.NodeID(1); id <= 5; id++ {
			if id != old {
				rest = append(rest, id)
			}
		}
		c.Partition([]raft.NodeID{old}, rest)
		lead, term := awaitLeader(t, c, rest...)
		if lead == old || term <= oldTerm {
			t.Fatalf("seed %d: isolating leader %d of term %d gave leader %d of term %d", seed, old, oldTerm, lead, term)
		}
		if st, _ := c.Status(old); st.Role != raft.Leader || st.Term != oldTerm {
			t.Fatalf("seed %d: isolated server's status = %+v, want still leading term %d", seed, st, oldTerm)
		}
		c.Heal()
		err := c.RunUntil(10*time.Second, "the old leader to step down", func() bool {
			st, _ := c.Status(old)
			return st.Term >= term && st.Role == raft.Follower
		})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		awaitLeader(t, c)
	}
}

// §5.2: two servers cut off from the other three never elect a leader,
// however high their terms climb, while the three do.
func TestMinorityPartitionNeverElects(t *testing.T) {
	minority, majority := []raft.NodeID{1, 2}, []raft.NodeID{3, 4, 5}
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 5, Seed: seed})
		c.Partition(minority, majority)
		if err := c.RunFor(30 * time.Second); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		for term, lead := range c.Leaders() {
			if lead == 1 || lead == 2 {
				t.Fatalf("seed %d: server %d of the minority led term %d", seed, lead, term)
			}
		}
		if lead, _ := c.AgreedLeader(majority...); lead == raft.None {
			t.Errorf("seed %d: the majority has no agreed leader", seed)
		}
		for _, id := range minority {
			if st, _ := c.Status(id); st.Term < 10 {
				t.Errorf("seed %d: server %d reached only term %d in 30 s", seed, id, st.Term)
			}
		}
	}
}

// §5.2: when the leader crashes, the others elect a new one in a later
// term. The old leader restarts in the term its storage holds and joins
// the new leader.
func TestCrashedLeaderIsReplaced(t *testing.T) {
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 3, Seed: seed})
		old, oldTerm := awaitLeader(t, c)
		c.Crash(old)
		lead, term := awaitLeader(t, c)
		if lead == old || term <= oldTerm {
			t.Fatalf("seed %d: crashing leader %d of term %d gave leader %d of term %d", seed, old, oldTerm, lead, term)
		}
		c.Restart(old)
		if st, _ := c.Status(old); st.Term != oldTerm {
			t.Fatalf("seed %d: restarted server is in term %d, want its saved term %d", seed, st.Term, oldTerm)
		}
		awaitLeader(t, c)
	}
}
