package sim

import "time"

// event is something that happens at one instant of virtual time.
type event struct {
	at  time.Duration
	key uint64 // drawn at random, so simultaneous events run in a seeded order
	seq uint64 // insertion order, which makes the order total
	run func()
}

// queue is a container/heap of events, earliest first.
type queue []*event

func (q queue) Len() int { return len(q) }

func (q queue) Less(i, j int) bool {
	a, b := q[i], q[j]
	if a.at != b.at {
		return a.at < b.at
	}
	if a.key != b.key {
		return a.key < b.key
	}
	return a.seq < b.seq
}

func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *queue) Push(x any) { *q = append(*q, x.(*event)) }

func (q *queue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}
