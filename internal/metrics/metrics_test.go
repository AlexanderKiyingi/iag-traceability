package metrics

import (
	"sync"
	"testing"
)

func TestCountersSnapshot(t *testing.T) {
	c := New()
	c.IncConsumed()
	c.IncConsumed()
	c.IncProjected()
	c.IncFailed()
	c.IncDeadLettered()
	c.IncDeduped()

	got := c.Snapshot()
	want := Snapshot{Consumed: 2, Projected: 1, Failed: 1, DeadLettered: 1, Deduped: 1}
	if got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestCountersConcurrent(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.IncConsumed()
		}()
	}
	wg.Wait()
	if got := c.Snapshot().Consumed; got != 100 {
		t.Fatalf("consumed = %d, want 100", got)
	}
}

// A nil *Counters must be safe to use — the consumer falls back to a fresh one,
// but defensive call sites may pass nil.
func TestNilCountersSafe(t *testing.T) {
	var c *Counters
	c.IncConsumed() // must not panic
	if got := c.Snapshot(); got != (Snapshot{}) {
		t.Fatalf("nil snapshot = %+v, want zero", got)
	}
}
