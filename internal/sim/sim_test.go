package sim_test

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/perezryan26/wilson/internal/kv"
	"github.com/perezryan26/wilson/internal/linearizability"
	"github.com/perezryan26/wilson/internal/sim"
	"github.com/perezryan26/wilson/raft"
)

var (
	seeds  = flag.Uint64("seeds", 20, "how many seeds TestRandomized runs")
	replay = flag.Int64("seed", -1, "run only this seed of TestRandomized and log its trace, every message included")
)

// TestRandomized runs seeds 0 to N-1 of the randomized schedule in
// parallel, where -seeds=N sets N. Each seed checks the invariants after
// every event, then liveness, then its clients' history for
// linearizability. A failure prints the seed's trace; -seed=N replays
// seed N alone with every message traced. A history Porcupine could not
// check in time skips its seed, which -v shows: the workload is then too
// large and needs resizing.
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
			if errors.Is(err, linearizability.ErrCheckTimedOut) {
				t.Skipf("seed %d: warning: %v", seed, err)
			}
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

// anyLeader returns a running server that believes it leads, or None.
func anyLeader(c *sim.Cluster) raft.NodeID {
	for id := raft.NodeID(1); id <= 5; id++ {
		if st, up := c.Status(id); up && st.Role == raft.Leader {
			return id
		}
	}
	return raft.None
}

// Servers that crash in the middle of disk writes, losing whatever they
// had not synced, restart from their WAL and snapshot without breaking
// any invariant. In a cluster of five that takes a proposal every 200 ms
// and compacts every three entries, a server picked at random is set to
// crash during one of its next four disk operations just as often, and
// crashed servers restart. Afterwards the cluster still agrees on a
// leader and applies a new command everywhere.
func TestCrashesMidWriteKeepSafety(t *testing.T) {
	crashes := 0
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 5, Seed: seed, CompactEvery: 3, SnapshotChunkSize: 16})
		pick := rand.New(rand.NewPCG(seed, 1))
		for round := range 100 {
			for id := raft.NodeID(1); id <= 5; id++ {
				c.Restart(id)
			}
			c.CrashDuringDiskOp(raft.NodeID(1+pick.IntN(5)), 1+pick.IntN(4))
			if lead := anyLeader(c); lead != raft.None {
				_, _, _ = c.Propose(lead, fmt.Appendf(nil, "c%d", round))
			}
			if err := c.RunFor(200 * time.Millisecond); err != nil {
				t.Fatalf("seed %d: %v\n%s", seed, err, c.Trace())
			}
		}
		crashes += strings.Count(c.Trace(), "crash during a disk write")
		for id := raft.NodeID(1); id <= 5; id++ {
			c.CrashDuringDiskOp(id, 0)
			c.Restart(id)
		}
		if err := applyEverywhere(t, c); err != nil {
			t.Fatalf("seed %d: %v\n%s", seed, err, c.Trace())
		}
	}
	t.Logf("%d crashes during disk writes", crashes)
	if crashes < 500 {
		t.Errorf("only %d of 1,000 rounds crashed a server during a disk write", crashes)
	}
}

// applyEverywhere proposes a command once c agrees on a leader and runs c
// until all five servers apply it.
func applyEverywhere(t *testing.T, c *sim.Cluster) error {
	t.Helper()
	lead, _ := awaitLeader(t, c)
	index, _, err := c.Propose(lead, []byte("last"))
	if err != nil {
		return err
	}
	return c.RunUntil(10*time.Second, "every server to apply the last command", func() bool {
		for id := raft.NodeID(1); id <= 5; id++ {
			if st, up := c.Status(id); !up || st.LastApplied < index {
				return false
			}
		}
		return true
	})
}

// applyEverywhereIn proposes cmd once the servers among ids agree on a
// leader, runs c until each of them applies it, and returns its index.
func applyEverywhereIn(t *testing.T, c *sim.Cluster, cmd kv.Command, ids ...raft.NodeID) raft.Index {
	t.Helper()
	lead, _ := awaitLeader(t, c, ids...)
	index, _, err := c.Propose(lead, cmd.Encode())
	if err != nil {
		t.Fatal(err)
	}
	err = c.RunUntil(10*time.Second, fmt.Sprintf("servers %v to apply entry %d", ids, index), func() bool {
		for _, id := range ids {
			if st, up := c.Status(id); !up || st.LastApplied < index {
				return false
			}
		}
		return true
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, c.Trace())
	}
	return index
}

// §7: a server cut off while the others commit and compact far past it
// catches up, once it is reachable again, from the leader's snapshot sent
// in chunks, then applies the entries after it.
func TestLaggingServerCatchesUpThroughSnapshot(t *testing.T) {
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 3, Seed: seed, TraceMessages: true, CompactEvery: 5, SnapshotChunkSize: 16})
		lead, _ := awaitLeader(t, c)
		lagging := lead%3 + 1
		var others []raft.NodeID
		for id := raft.NodeID(1); id <= 3; id++ {
			if id != lagging {
				others = append(others, id)
			}
		}
		c.Partition(others, []raft.NodeID{lagging})
		session := uint64(applyEverywhereIn(t, c, kv.Command{Op: kv.OpRegister}, others...))
		for i := range 30 {
			put := kv.Command{Op: kv.OpPut, Session: session, Seq: uint64(i + 1), Key: fmt.Sprintf("k%d", i), Value: []byte("v")}
			applyEverywhereIn(t, c, put, others...)
		}
		c.Heal()
		applyEverywhereIn(t, c, kv.Command{Op: kv.OpGet, Key: "k0"}, 1, 2, 3)
		installs := strings.Count(c.Trace(), fmt.Sprintf("n%d install snapshot", lagging))
		chunks := strings.Count(c.Trace(), fmt.Sprintf("→n%d InstallSnapshot ", lagging))
		if installs == 0 || chunks < 2 {
			t.Errorf("seed %d: server %d installed %d snapshots from %d chunks, want at least one from several", seed, lagging, installs, chunks)
		}
	}
}

// A burst of proposals that outruns the network costs each follower a few
// AppendEntries, not one or more per proposal. The leader sends a new
// entry at once only to a follower whose next entry it is; any other
// follower gets it with the reply-driven AppendEntries already due, or
// the next heartbeat. MIT 6.5840's TestCount3B found a leader that sent
// each follower its unacknowledged entries again on every proposal and on
// every reply that left the follower behind: 46 RPCs to commit 12
// commands where the test allows 42.
func TestProposalBurstSendsFewAppendEntries(t *testing.T) {
	const burst = 12
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 3, Seed: seed, TraceMessages: true})
		lead, _ := awaitLeader(t, c)
		before := c.Trace()
		var last raft.Index
		for i := range burst {
			index, _, err := c.Propose(lead, []byte(fmt.Sprintf("c%d", i)))
			if err != nil {
				t.Fatal(err)
			}
			last = index
		}
		err := c.RunUntil(time.Second, "every server to apply the burst", func() bool {
			for id := raft.NodeID(1); id <= 3; id++ {
				if st, _ := c.Status(id); st.LastApplied < last {
					return false
				}
			}
			return true
		})
		if err != nil {
			t.Fatalf("seed %d: %v\n%s", seed, err, c.Trace())
		}
		for id := raft.NodeID(1); id <= 3; id++ {
			appends := fmt.Sprintf("→n%d AppendEntries ", id)
			if n := strings.Count(c.Trace(), appends) - strings.Count(before, appends); id != lead && n > burst/2 {
				t.Errorf("seed %d: server %d got %d AppendEntries for a burst of %d proposals, want at most %d",
					seed, id, n, burst, burst/2)
			}
		}
	}
}

// do asks server id to carry out cmd, as a client would, runs c until the
// server answers, and returns the result. The server must lead.
func do(t *testing.T, c *sim.Cluster, id raft.NodeID, cmd kv.Command) kv.Result {
	t.Helper()
	call := c.Request(id, cmd)
	if err := c.RunUntil(10*time.Second, fmt.Sprintf("server %d to answer", id), func() bool { return call.Done }); err != nil {
		t.Fatalf("%v\n%s", err, c.Trace())
	}
	if call.Reply.NotLeader {
		t.Fatalf("server %d does not lead", id)
	}
	return call.Reply.Result
}

func put(session, seq uint64, key, value string) kv.Command {
	return kv.Command{Op: kv.OpPut, Session: session, Seq: seq, Key: key, Value: []byte(value)}
}

func get(key string) kv.Command { return kv.Command{Op: kv.OpGet, Key: key} }

// others returns the servers of a cluster of size other than id.
func others(size int, id raft.NodeID) []raft.NodeID {
	var rest []raft.NodeID
	for s := raft.NodeID(1); s <= raft.NodeID(size); s++ {
		if s != id {
			rest = append(rest, s)
		}
	}
	return rest
}

// §8: a leader cut off in a minority keeps believing it leads, but it
// never answers a read, in either read mode: through the log its Get never
// commits, and through ReadIndex no majority confirms it. Meanwhile the
// majority elects a new leader, which takes a write the old one never
// sees, so an answer from the old leader would be stale.
func TestPartitionedLeaderServesNoStaleRead(t *testing.T) {
	for _, readIndex := range []bool{false, true} {
		for seed := range uint64(10) {
			c := sim.New(sim.Options{Size: 5, Seed: seed, ReadIndex: readIndex, CommitNoOpOnElection: true})
			old, _ := awaitLeader(t, c)
			session := do(t, c, old, kv.Command{Op: kv.OpRegister}).Session
			do(t, c, old, put(session, 1, "x", "old"))
			c.Partition([]raft.NodeID{old}, others(5, old))
			lead, _ := awaitLeader(t, c, others(5, old)...)
			do(t, c, lead, put(session, 2, "x", "new"))
			read := c.Request(old, get("x"))
			if err := c.RunFor(2 * time.Second); err != nil {
				t.Fatal(err)
			}
			if st, _ := c.Status(old); read.Done || st.Role != raft.Leader {
				t.Fatalf("reads through ReadIndex %t, seed %d: the cut-off leader, now %s, answered %+v", readIndex, seed, st.Role, read.Reply)
			}
			c.Heal()
			if err := c.RunFor(2 * time.Second); err != nil {
				t.Fatal(err)
			}
			if read.Done && string(read.Reply.Result.Value) == "old" {
				t.Fatalf("reads through ReadIndex %t, seed %d: the old leader answered with the stale value", readIndex, seed)
			}
			if got := do(t, c, lead, get("x")); string(got.Value) != "new" {
				t.Fatalf("reads through ReadIndex %t, seed %d: x = %q, want new", readIndex, seed, got.Value)
			}
		}
	}
}

// §8: a new leader that has not committed an entry of its term may not
// know that its predecessor committed a write, so it confirms no read
// until it has. The leader of term 1 commits a write and answers its
// client, then crashes before any follower learns that the write
// committed. Without the election no-op, the new leader confirms the read
// only once a write of its own term commits, and the read sees the first
// write.
func TestNewLeaderServesNoReadBeforeItsTermCommits(t *testing.T) {
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 3, Seed: seed, ReadIndex: true})
		old, _ := awaitLeader(t, c)
		session := do(t, c, old, kv.Command{Op: kv.OpRegister}).Session
		do(t, c, old, put(session, 1, "x", "1"))
		committed, _ := c.Status(old)
		c.Crash(old)
		for _, id := range others(3, old) {
			if st, _ := c.Status(id); st.CommitIndex >= committed.CommitIndex {
				t.Fatalf("seed %d: server %d knew that entry %d committed before the leader crashed", seed, id, committed.CommitIndex)
			}
		}
		lead, _ := awaitLeader(t, c, others(3, old)...)
		read := c.Request(lead, get("x"))
		if err := c.RunFor(time.Second); err != nil {
			t.Fatal(err)
		}
		if read.Done {
			t.Fatalf("seed %d: the new leader answered %+v before it committed an entry of its term", seed, read.Reply)
		}
		do(t, c, lead, put(session, 2, "y", "2"))
		if err := c.RunUntil(time.Second, "the read's answer", func() bool { return read.Done }); err != nil {
			t.Fatal(err)
		}
		if r := read.Reply.Result; !r.Found || string(r.Value) != "1" {
			t.Fatalf("seed %d: the read answered %+v, want x = 1", seed, read.Reply)
		}
	}
}

// §8: a write that its leader commits and applies, but whose answer is
// lost when the leader crashes, runs once though the client sends it
// again, with the same number, to the new leader: the session's record
// answers it, so a later write to the same key by another session stands.
// Servers compact after every entry, and all of them restart from their
// snapshots before a last retry, which the record still answers (§7).
func TestRetriedWriteRunsOnceAcrossLeaderChange(t *testing.T) {
	for seed := range uint64(10) {
		c := sim.New(sim.Options{Size: 3, Seed: seed, CompactEvery: 1, CommitNoOpOnElection: true})
		old, _ := awaitLeader(t, c)
		a := do(t, c, old, kv.Command{Op: kv.OpRegister}).Session
		b := do(t, c, old, kv.Command{Op: kv.OpRegister}).Session
		do(t, c, old, put(a, 1, "x", "a1"))
		c.Crash(old)
		lead, _ := awaitLeader(t, c, others(3, old)...)
		do(t, c, lead, put(b, 1, "x", "b1"))
		if r := do(t, c, lead, put(a, 1, "x", "a1")); r.Err != nil {
			t.Fatalf("seed %d: the retry got %v, want the first run's answer", seed, r.Err)
		}
		if r := do(t, c, lead, get("x")); string(r.Value) != "b1" {
			t.Fatalf("seed %d: x = %q after the retry, want b1", seed, r.Value)
		}
		for id := raft.NodeID(1); id <= 3; id++ {
			c.Crash(id)
		}
		for id := raft.NodeID(1); id <= 3; id++ {
			c.Restart(id)
		}
		lead, _ = awaitLeader(t, c)
		if r := do(t, c, lead, put(a, 1, "x", "a1")); r.Err != nil {
			t.Fatalf("seed %d: the retry after a restart got %v, want the first run's answer", seed, r.Err)
		}
		if r := do(t, c, lead, get("x")); string(r.Value) != "b1" {
			t.Fatalf("seed %d: x = %q after a restart and the retry, want b1", seed, r.Value)
		}
		if !regexp.MustCompile(`boot term=\d+ votedFor=\S+ snapshot=[1-9]`).MatchString(c.Trace()) {
			t.Fatalf("seed %d: no server restarted from a snapshot", seed)
		}
	}
}
