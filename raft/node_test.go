package raft

import (
	"math/rand/v2"
	"testing"
)

// testConfig returns the default timing for server id of a cluster of
// servers 1 to size, with a random source seeded by seed and the ID.
func testConfig(id NodeID, size int, seed uint64) Config {
	cfg, _ := DefaultConfig()
	cfg.ID = id
	for p := range NodeID(size) {
		cfg.Peers = append(cfg.Peers, p+1)
	}
	cfg.Rand = rand.New(rand.NewPCG(seed, uint64(id)))
	return cfg
}

func newTestNode(id NodeID, size int, seed uint64) *Node {
	return NewNode(testConfig(id, size, seed), RestoredState{})
}

// tickUntilElection ticks n until it starts an election and returns how
// many ticks that took. It fails the test if no election starts within the
// longest possible timeout.
func tickUntilElection(t *testing.T, n *Node) int {
	t.Helper()
	term := n.Status().Term
	for ticks := 1; ticks <= 2*n.electionTick; ticks++ {
		n.Tick()
		if n.Status().Term > term {
			return ticks
		}
	}
	t.Fatalf("no election within %d ticks", 2*n.electionTick)
	return 0
}

func wantHardState(t *testing.T, rd Ready, want HardState) {
	t.Helper()
	if rd.HardState == nil || *rd.HardState != want {
		t.Errorf("Ready hard state = %v, want %+v", rd.HardState, want)
	}
}

func TestNewNodeRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"ID not a peer", func(c *Config) { c.ID = 4 }},
		{"None as a peer", func(c *Config) { c.Peers = []NodeID{0, 1, 2} }},
		{"duplicate peer", func(c *Config) { c.Peers = []NodeID{1, 2, 2} }},
		{"no heartbeat interval", func(c *Config) { c.HeartbeatTick = 0 }},
		{"heartbeat as long as the election timeout", func(c *Config) { c.HeartbeatTick = c.ElectionTick }},
		{"nil Rand", func(c *Config) { c.Rand = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(1, 3, 1)
			tt.edit(&cfg)
			defer func() {
				if recover() == nil {
					t.Error("NewNode accepted the config")
				}
			}()
			NewNode(cfg, RestoredState{})
		})
	}
}

// A restarted server resumes as a follower in the term and with the vote it
// persisted, and does not ask the runtime to persist them again.
func TestNewNodeRestoresHardState(t *testing.T) {
	hs := HardState{CurrentTerm: 5, VotedFor: 2}
	n := NewNode(testConfig(1, 3, 1), RestoredState{HardState: hs})
	if st := n.Status(); st != (Status{Role: Follower, Term: 5}) {
		t.Errorf("status = %+v, want a follower in term 5", st)
	}
	if rd := n.Ready(); rd.HardState != nil {
		t.Errorf("Ready hard state = %+v, want nil: the restored state is already durable", *rd.HardState)
	}
}
