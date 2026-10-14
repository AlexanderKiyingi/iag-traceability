package scmsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/testdb"
)

// scmFixture recreates the slice of SCM's schema (public.*) the sync reads —
// the columns of IAG_SCM_backend migrations 000002/000003/000007/000010 that
// matter here.
const scmFixture = `
DROP TABLE IF EXISTS public.lot_qr_codes, public.trace_events, public.farms,
	public.suppliers, public.batches, public.export_lots CASCADE;
CREATE TABLE public.export_lots (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id TEXT NOT NULL UNIQUE);
CREATE TABLE public.batches (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id TEXT NOT NULL UNIQUE);
CREATE TABLE public.suppliers (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id TEXT NOT NULL UNIQUE,
	supplier_type TEXT NOT NULL DEFAULT 'vendor');
CREATE TABLE public.farms (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id TEXT NOT NULL UNIQUE);
CREATE TABLE public.trace_events (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	occurred_at TIMESTAMPTZ NOT NULL,
	event_type TEXT NOT NULL,
	entity_type TEXT NOT NULL,
	entity_id UUID NOT NULL,
	related_ids JSONB NOT NULL DEFAULT '{}',
	actor_type TEXT NOT NULL DEFAULT 'user',
	actor_id TEXT NOT NULL DEFAULT '',
	measurements JSONB NOT NULL DEFAULT '{}',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE public.lot_qr_codes (
	lot_id UUID PRIMARY KEY REFERENCES public.export_lots(id) ON DELETE CASCADE,
	public_token TEXT NOT NULL UNIQUE,
	version INT NOT NULL DEFAULT 1,
	published_at TIMESTAMPTZ,
	revoked_at TIMESTAMPTZ,
	scan_count BIGINT NOT NULL DEFAULT 0
);
INSERT INTO public.export_lots (id, business_id) VALUES ('00000000-0000-0000-0000-00000000a001', 'LOT-S1');
INSERT INTO public.batches (id, business_id) VALUES
	('00000000-0000-0000-0000-00000000b001', 'BAT-S1'),
	('00000000-0000-0000-0000-00000000b002', 'BAT-S2');
INSERT INTO public.suppliers (id, business_id, supplier_type) VALUES ('00000000-0000-0000-0000-00000000c001', 'FMR-S1', 'farmer');
INSERT INTO public.lot_qr_codes (lot_id, public_token, version, published_at, scan_count)
	VALUES ('00000000-0000-0000-0000-00000000a001', 'scm-token-1', 2, '2026-09-01T00:00:00Z', 5);
`

type fixture struct {
	pool *pgxpool.Pool
	st   *store.Store
}

func setup(t *testing.T, chain bool) fixture {
	t.Helper()
	pool := testdb.Pool(t)
	if _, err := pool.Exec(context.Background(), scmFixture); err != nil {
		t.Fatalf("scm fixture: %v", err)
	}
	return fixture{pool: pool, st: store.New(pool).SetChainEnabled(chain)}
}

// scmEvent inserts one SCM event; created_at defaults to now().
func (f fixture) scmEvent(t *testing.T, id, occurred, eventType, entityType, entityID string, related, meas string) {
	t.Helper()
	if related == "" {
		related = "{}"
	}
	if meas == "" {
		meas = "{}"
	}
	if _, err := f.pool.Exec(context.Background(), `
		INSERT INTO public.trace_events (id, occurred_at, event_type, entity_type, entity_id, related_ids, measurements)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)`,
		id, occurred, eventType, entityType, entityID, related, meas); err != nil {
		t.Fatalf("scm event %s: %v", id, err)
	}
}

func (f fixture) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (f fixture) seedBasic(t *testing.T) {
	f.scmEvent(t, "10000000-0000-0000-0000-000000000001", "2026-07-12T08:00:00Z", "FARMER_REGISTERED", "farmer",
		"00000000-0000-0000-0000-00000000c001", "", `{"district":"Kapchorwa"}`)
	f.scmEvent(t, "10000000-0000-0000-0000-000000000002", "2026-07-20T08:00:00Z", "CHERRY_RECEIVED", "batch",
		"00000000-0000-0000-0000-00000000b001", `{"lot_business_id":"LOT-S1"}`, `{"weight_kg":410.5}`)
	f.scmEvent(t, "10000000-0000-0000-0000-000000000003", "2026-07-21T08:00:00Z", "CHERRY_RECEIVED", "batch",
		"00000000-0000-0000-0000-00000000b002", "", `{"weight_kg":90}`)
	// No business id resolvable: skipped, never copied.
	f.scmEvent(t, "10000000-0000-0000-0000-000000000004", "2026-07-22T08:00:00Z", "CHERRY_RECEIVED", "batch",
		"00000000-0000-0000-0000-0000000000ff", "", "")
}

func TestFirstTickCopiesAllSecondNothing_Integration(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()
	f.seedBasic(t)
	scmBefore := f.count(t, `SELECT COUNT(*)::int FROM public.trace_events`)

	s := New(f.pool, f.st)
	res, err := s.Tick(ctx)
	if err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if !res.Full || res.Copied != 3 || res.Skipped != 1 || res.QRCopied != 1 {
		t.Fatalf("tick 1: %s", res)
	}
	// Mapping: business id, source_service, idempotency key, payload = measurements.
	var biz, src, key string
	var weight float64
	if err := f.pool.QueryRow(ctx, `
		SELECT entity_business_id, source_service, idempotency_key, (payload->>'weight_kg')::float8
		FROM traceability.trace_events WHERE idempotency_key = 'scm-migrate:10000000-0000-0000-0000-000000000002'`,
	).Scan(&biz, &src, &key, &weight); err != nil {
		t.Fatal(err)
	}
	if biz != "BAT-S1" || src != SourceService || weight != 410.5 {
		t.Fatalf("mapped row: %s %s %v", biz, src, weight)
	}
	if n := f.count(t, `SELECT COUNT(*)::int FROM traceability.trace_events WHERE entity_business_id = 'FMR-S1' AND entity_type = 'farmer'`); n != 1 {
		t.Fatalf("farmer row = %d", n)
	}
	// Copied in occurred_at order (created_at order would be the same here,
	// so check the ledger reads forwards).
	// The sync reads public.* but never writes there.
	if n := f.count(t, `SELECT COUNT(*)::int FROM public.trace_events`); n != scmBefore {
		t.Fatalf("sync wrote to SCM's table: %d -> %d", scmBefore, n)
	}
	// QR copied exactly as migrate-scm does.
	var tok string
	var ver, scans int
	if err := f.pool.QueryRow(ctx, `SELECT public_token, version, scan_count FROM traceability.lot_qr_codes WHERE lot_business_id='LOT-S1'`).Scan(&tok, &ver, &scans); err != nil {
		t.Fatal(err)
	}
	if tok != "scm-token-1" || ver != 2 || scans != 5 {
		t.Fatalf("qr: %s %d %d", tok, ver, scans)
	}
	// Story projection refreshed as for a posted event: CHERRY_RECEIVED
	// referencing LOT-S1 rebuilds that lot's story.
	if n := f.count(t, `SELECT COUNT(*)::int FROM traceability.lot_story_projections WHERE lot_business_id='LOT-S1'`); n != 1 {
		t.Fatalf("lot story projection = %d, want 1", n)
	}
	// Mark stored.
	if n := f.count(t, `SELECT COUNT(*)::int FROM scm_sync_state WHERE last_created_at IS NOT NULL AND last_error = ''`); n != 1 {
		t.Fatal("mark not stored")
	}

	res, err = s.Tick(ctx)
	if err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if res.Full || res.Copied != 0 || res.QRCopied != 0 {
		t.Fatalf("tick 2 should copy nothing: %s", res)
	}
	if n := f.count(t, `SELECT COUNT(*)::int FROM traceability.trace_events`); n != 3 {
		t.Fatalf("trace_events = %d, want 3", n)
	}
}

func TestLateOlderEventIsPickedUp_Integration(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()
	f.seedBasic(t)
	s := New(f.pool, f.st)
	if _, err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// Recorded now, back-dated before everything already copied.
	f.scmEvent(t, "10000000-0000-0000-0000-000000000009", "2026-06-01T08:00:00Z", "CHERRY_RECEIVED", "batch",
		"00000000-0000-0000-0000-00000000b002", "", `{"weight_kg":12}`)
	res, err := s.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Full || res.Copied != 1 {
		t.Fatalf("incremental tick should copy the late event: %s", res)
	}

	// A row inserted far behind the mark (created_at in the past, beyond the
	// window) is missed incrementally and caught by the periodic full rescan.
	if _, err := f.pool.Exec(ctx, `
		INSERT INTO public.trace_events (id, occurred_at, event_type, entity_type, entity_id, created_at)
		VALUES ('10000000-0000-0000-0000-000000000010', '2026-05-01T00:00:00Z', 'CHERRY_RECEIVED', 'batch',
		        '00000000-0000-0000-0000-00000000b001', now() - interval '2 days')`); err != nil {
		t.Fatal(err)
	}
	res, _ = s.Tick(ctx)
	if res.Copied != 0 {
		t.Fatalf("beyond the window should wait for the full rescan: %s", res)
	}
	s.FullEvery = 3 // ticks so far: 3, so the next one is full
	res, err = s.Tick(ctx)
	if err != nil || !res.Full || res.Copied != 1 {
		t.Fatalf("full rescan: %s %v", res, err)
	}
}

func TestIdempotentWithPriorOneShot_Integration(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()
	f.seedBasic(t)
	// What the old cmd/migrate-scm wrote for event 1.
	if _, err := f.pool.Exec(ctx, `
		INSERT INTO traceability.trace_events (occurred_at, event_type, entity_type, entity_business_id, source_service, idempotency_key)
		VALUES ('2026-07-12T08:00:00Z', 'FARMER_REGISTERED', 'farmer', 'FMR-S1', 'iag-supply-chain-migration',
		        'scm-migrate:10000000-0000-0000-0000-000000000001')`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		INSERT INTO traceability.lot_qr_codes (lot_business_id, public_token, version) VALUES ('LOT-S1', 'already-here', 1)`); err != nil {
		t.Fatal(err)
	}
	res, err := New(f.pool, f.st).Run(ctx, true) // the one-shot command's call
	if err != nil {
		t.Fatal(err)
	}
	if res.Copied != 2 || res.Existing != 1 || res.QRCopied != 0 {
		t.Fatalf("one-shot over prior copy: %s", res)
	}
	if n := f.count(t, `SELECT COUNT(*)::int FROM traceability.trace_events WHERE idempotency_key LIKE 'scm-migrate:%'`); n != 3 {
		t.Fatalf("rows = %d, want 3 (no duplicates)", n)
	}
	var tok string
	_ = f.pool.QueryRow(ctx, `SELECT public_token FROM traceability.lot_qr_codes WHERE lot_business_id='LOT-S1'`).Scan(&tok)
	if tok != "already-here" {
		t.Fatalf("existing QR overwritten: %s", tok)
	}
	// And the loop after the one-shot finds nothing to do.
	res, _ = New(f.pool, f.st).Tick(ctx)
	if res.Copied != 0 {
		t.Fatalf("loop after one-shot: %s", res)
	}
}

func TestChainEnabledChainsBatchEvents_Integration(t *testing.T) {
	f := setup(t, true)
	ctx := context.Background()
	// A runtime batch append already on the chain: the sync builds on it.
	if _, err := f.st.AppendEvent(ctx, store.AppendEventInput{EventType: "STAGE_CHANGED", EntityType: "batch", EntityBusinessID: "BAT-RT"}); err != nil {
		t.Fatal(err)
	}
	f.seedBasic(t)
	s := New(f.pool, f.st)
	res, err := s.Tick(ctx)
	if err != nil || res.Copied != 3 {
		t.Fatalf("tick: %s %v", res, err)
	}
	// Two batch events chained (the farmer event is not a batch event).
	if n := f.count(t, `SELECT COUNT(*)::int FROM traceability.custody_chain`); n != 3 {
		t.Fatalf("custody_chain = %d, want 3 (1 runtime + 2 synced)", n)
	}
	// A late, back-dated batch event appends at the head, not in the middle.
	f.scmEvent(t, "10000000-0000-0000-0000-000000000009", "2026-06-01T08:00:00Z", "CHERRY_RECEIVED", "batch",
		"00000000-0000-0000-0000-00000000b002", "", "")
	if res, err := s.Tick(ctx); err != nil || res.Copied != 1 {
		t.Fatalf("late tick: %s %v", res, err)
	}
	vr, err := f.st.VerifyChain(ctx)
	if err != nil || !vr.IsValid || vr.CheckedCount != 4 {
		t.Fatalf("VerifyChain: %+v %v", vr, err)
	}
	// Synced blocks are in occurred_at order within the tick.
	rows, _ := f.pool.Query(ctx, `
		SELECT cc.occurred_at FROM traceability.custody_chain cc
		JOIN traceability.trace_events te ON te.id = cc.trace_event_id
		WHERE te.source_service = $1 ORDER BY cc.seq_index`, SourceService)
	var times []time.Time
	for rows.Next() {
		var at time.Time
		_ = rows.Scan(&at)
		times = append(times, at)
	}
	rows.Close()
	if len(times) != 3 || !times[0].Before(times[1]) {
		t.Fatalf("first tick's blocks out of occurred_at order: %v", times)
	}
}

func TestSyncErrorDoesNotCrash_Integration(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()
	f.seedBasic(t)
	if _, err := f.pool.Exec(ctx, `ALTER TABLE public.trace_events RENAME TO trace_events_gone`); err != nil {
		t.Fatal(err)
	}
	s := New(f.pool, f.st)
	if _, err := s.Tick(ctx); err == nil {
		t.Fatal("expected an error with SCM's table missing")
	}
	var lastErr string
	_ = f.pool.QueryRow(ctx, `SELECT last_error FROM scm_sync_state`).Scan(&lastErr)
	if lastErr == "" {
		t.Fatal("last_error not recorded")
	}
	s.safeTick(ctx) // logs, does not panic

	// A panicking hook is recovered by the loop too.
	if _, err := f.pool.Exec(ctx, `ALTER TABLE public.trace_events_gone RENAME TO trace_events`); err != nil {
		t.Fatal(err)
	}
	s.AfterAppend = func(context.Context, *store.Store, store.TraceEvent) { panic("boom") }
	s.safeTick(ctx)

	// Recovery: the next tick copies everything that is left.
	s.AfterAppend = func(context.Context, *store.Store, store.TraceEvent) {}
	res, err := s.Run(ctx, true)
	if err != nil {
		t.Fatalf("recovery tick: %v", err)
	}
	if n := f.count(t, `SELECT COUNT(*)::int FROM traceability.trace_events`); n != 3 {
		t.Fatalf("after recovery = %d (%s)", n, res)
	}
}

func TestOneReplicaAtATime_Integration(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()
	conn, err := f.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, syncLockKey); err != nil {
		t.Fatal(err)
	}
	if _, err := New(f.pool, f.st).Tick(ctx); !errors.Is(err, ErrLocked) {
		t.Fatalf("held lock: %v, want ErrLocked", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, syncLockKey); err != nil {
		t.Fatal(err)
	}
	if _, err := New(f.pool, f.st).Tick(ctx); err != nil {
		t.Fatalf("after unlock: %v", err)
	}
}
