package main

import (
	"context"
	"testing"

	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/testdb"
)

// TestCutoverRunbook_Integration walks the README's CHAIN_ENABLED cutover:
// history arrives unchained (sync with the chain off), the backfill chains it
// from genesis in occurred_at order, then runtime appends chain on top — and
// the whole chain verifies.
func TestCutoverRunbook_Integration(t *testing.T) {
	pool := testdb.Pool(t)
	ctx := context.Background()

	// (a) CHAIN_ENABLED=false: events (as the SCM sync appends them) land unchained.
	off := store.New(pool)
	for _, id := range []string{"BAT-1", "BAT-2", "BAT-1"} {
		if _, err := off.AppendEvent(ctx, store.AppendEventInput{
			EventType: "CHERRY_RECEIVED", EntityType: "batch", EntityBusinessID: id,
			SourceService: "iag-supply-chain-migration",
		}); err != nil {
			t.Fatal(err)
		}
	}
	var blocks int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM custody_chain`).Scan(&blocks)
	if blocks != 0 {
		t.Fatalf("chain off but %d blocks", blocks)
	}

	// (b) backfill: dry run reports, real run chains, re-run is a no-op.
	if n, err := backfill(ctx, pool, true); err != nil || n != 3 {
		t.Fatalf("dry-run: %d %v", n, err)
	}
	if n, err := backfill(ctx, pool, false); err != nil || n != 3 {
		t.Fatalf("backfill: %d %v", n, err)
	}
	if n, _ := backfill(ctx, pool, true); n != 0 {
		t.Fatalf("after backfill dry-run = %d, want 0", n)
	}

	// (c) CHAIN_ENABLED=true: new events chain at runtime on top.
	on := store.New(pool).SetChainEnabled(true)
	if _, err := on.AppendEvent(ctx, store.AppendEventInput{EventType: "STAGE_CHANGED", EntityType: "batch", EntityBusinessID: "BAT-2"}); err != nil {
		t.Fatal(err)
	}
	res, err := on.VerifyChain(ctx)
	if err != nil || !res.IsValid || res.CheckedCount != 4 {
		t.Fatalf("VerifyChain: %+v %v", res, err)
	}
}
