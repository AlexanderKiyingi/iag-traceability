// Package metrics holds lightweight in-process counters for the Kafka ingest
// path. They are not Prometheus metrics (the service has no metrics exporter
// yet) but make consumer health observable via the admin monitoring summary —
// previously the ingest path was entirely uninstrumented.
package metrics

import "sync/atomic"

// Counters tracks Kafka consumer outcomes. The zero value is usable; all
// methods are safe for concurrent use.
type Counters struct {
	consumed     atomic.Int64
	projected    atomic.Int64
	failed       atomic.Int64
	deadLettered atomic.Int64
	deduped      atomic.Int64
}

func New() *Counters { return &Counters{} }

// The nil check must come before touching any field — taking the address of a
// field on a nil receiver would panic before the guard could run.
func (c *Counters) IncConsumed() {
	if c != nil {
		c.consumed.Add(1)
	}
}

func (c *Counters) IncProjected() {
	if c != nil {
		c.projected.Add(1)
	}
}

func (c *Counters) IncFailed() {
	if c != nil {
		c.failed.Add(1)
	}
}

func (c *Counters) IncDeadLettered() {
	if c != nil {
		c.deadLettered.Add(1)
	}
}

func (c *Counters) IncDeduped() {
	if c != nil {
		c.deduped.Add(1)
	}
}

// Snapshot is a point-in-time read of the counters, JSON-serialisable for the
// monitoring endpoint.
type Snapshot struct {
	Consumed     int64 `json:"consumed"`
	Projected    int64 `json:"projected"`
	Failed       int64 `json:"failed"`
	DeadLettered int64 `json:"dead_lettered"`
	Deduped      int64 `json:"deduped"`
}

func (c *Counters) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	return Snapshot{
		Consumed:     c.consumed.Load(),
		Projected:    c.projected.Load(),
		Failed:       c.failed.Load(),
		DeadLettered: c.deadLettered.Load(),
		Deduped:      c.deduped.Load(),
	}
}
