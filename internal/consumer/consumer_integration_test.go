package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"iag-traceability/backend/internal/metrics"
	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/testdb"
)

func msgFor(t *testing.T, id, eventType string, data map[string]any) kafka.Message {
	t.Helper()
	env := map[string]any{"id": id, "type": eventType, "data": data}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafka.Message{Topic: "iag.supply-chain", Value: raw}
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestHandleMessage_Integration exercises the real consume→project→dedupe and
// dead-letter paths against Postgres: idempotent redelivery, and unmapped-event
// dead-lettering (with its own dedupe so it isn't re-processed).
func TestHandleMessage_Integration(t *testing.T) {
	pool := testdb.Pool(t)
	st := store.New(pool)
	m := metrics.New()
	c := New(Config{}, st, nil, m, nil)
	ctx := context.Background()

	// 1. A mapped event projects one trace_events row + one dedupe row.
	pub := msgFor(t, "evt-1", "scm.lot.qr_published", map[string]any{
		"lot_business_id": "LOT-1", "public_token": "tok", "public_url": "http://x/tok",
	})
	if err := c.handleMessage(ctx, pub); err != nil {
		t.Fatalf("handle qr_published: %v", err)
	}
	if got := count(t, pool, `SELECT COUNT(*)::int FROM trace_events WHERE entity_business_id='LOT-1'`); got != 1 {
		t.Fatalf("trace_events after publish = %d, want 1", got)
	}
	if got := count(t, pool, `SELECT COUNT(*)::int FROM kafka_dedupe WHERE event_id='evt-1'`); got != 1 {
		t.Fatalf("dedupe rows = %d, want 1", got)
	}

	// 2. Redelivery of the same event is deduped — no second row.
	if err := c.handleMessage(ctx, pub); err != nil {
		t.Fatalf("handle redelivery: %v", err)
	}
	if got := count(t, pool, `SELECT COUNT(*)::int FROM trace_events WHERE entity_business_id='LOT-1'`); got != 1 {
		t.Fatalf("trace_events after redelivery = %d, want 1 (idempotent)", got)
	}

	// 3. An unmapped event type is dead-lettered (not projected) and deduped.
	bad := msgFor(t, "evt-2", "totally.unknown.type", map[string]any{"x": "y"})
	if err := c.handleMessage(ctx, bad); err != nil {
		t.Fatalf("handle unmapped: %v", err)
	}
	if got := count(t, pool, `SELECT COUNT(*)::int FROM kafka_dead_letter WHERE event_id='evt-2'`); got != 1 {
		t.Fatalf("dead_letter rows = %d, want 1", got)
	}

	// 4. Redelivery of the unmapped event does NOT create a duplicate dead-letter.
	if err := c.handleMessage(ctx, bad); err != nil {
		t.Fatalf("handle unmapped redelivery: %v", err)
	}
	if got := count(t, pool, `SELECT COUNT(*)::int FROM kafka_dead_letter WHERE event_id='evt-2'`); got != 1 {
		t.Fatalf("dead_letter rows after redelivery = %d, want 1", got)
	}

	// Counters reflect the outcomes: 2 consumed (evt-1, evt-2), 1 projected,
	// 1 dead-lettered, 2 deduped (the two redeliveries).
	snap := m.Snapshot()
	if snap.Consumed != 2 || snap.Projected != 1 || snap.DeadLettered != 1 || snap.Deduped != 2 {
		t.Fatalf("counters = %+v, want consumed=2 projected=1 dead=1 deduped=2", snap)
	}
}
