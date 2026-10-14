// Package scmsync copies iag-supply-chain's custody events (public.trace_events)
// and published QR codes (public.lot_qr_codes) into this service.
//
// It replaces the Kafka path for SCM events, which does not run: SCM's outbox
// is never relayed and service-to-service auth is refused. SCM and this service
// share one Postgres, so the sync reads SCM's tables directly — always
// schema-qualified (public.*), never through search_path, which on this
// service's own DSN resolves to the traceability schema first.
//
// Used by the one-shot cmd/migrate-scm (Full: true) and by the background loop
// in cmd/server. Both share the mapping, source_service and idempotency key, so
// rows copied by one are never duplicated by the other.
//
// Every event goes through store.AppendEvent — the same append path as
// POST /api/v1/events — so with CHAIN_ENABLED on, batch events are hash-chained
// under the custody-chain advisory lock exactly like runtime appends, and then
// story.AfterAppend refreshes the lot story as it does for a posted event.
package scmsync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/story"
)

const (
	// SourceService is stamped on every copied event (unchanged from the
	// original one-shot migration).
	SourceService = "iag-supply-chain-migration"
	// IdempotencyPrefix + SCM event id is each copied event's idempotency key.
	IdempotencyPrefix = "scm-migrate:"
	// stateKey names this sync's row in scm_sync_state.
	stateKey = "scm.trace_events"
	// syncLockKey is the session advisory lock that keeps one replica (or the
	// one-shot command) syncing at a time. Distinct from the migrator's and the
	// custody chain's keys.
	syncLockKey int64 = 0x5C3_5A7C
)

// ErrLocked is returned when another process holds the sync lock.
var ErrLocked = errors.New("scm sync already running elsewhere")

// Syncer copies SCM rows into the traceability store.
type Syncer struct {
	// Source reads SCM's public.* tables: this service's own pool in
	// same-database mode, or a pool on SCM_DATABASE_URL.
	Source *pgxpool.Pool
	// Store is the destination (and holds the sync lock and state).
	Store *store.Store
	// PageSize rows are read per query; MaxPerTick caps one incremental tick.
	PageSize   int
	MaxPerTick int
	// Window is how far behind the mark each incremental tick re-reads, for
	// SCM transactions that commit out of created_at order.
	Window time.Duration
	// FullEvery forces a full rescan every N incremental ticks, the safety
	// net for anything inserted behind the mark by more than Window.
	FullEvery int
	// AfterAppend runs for each newly copied event. Defaults to
	// story.AfterAppend.
	AfterAppend func(ctx context.Context, st *store.Store, ev store.TraceEvent)

	ticks int
}

// New returns a Syncer with the default tuning.
func New(source *pgxpool.Pool, st *store.Store) *Syncer {
	return &Syncer{Source: source, Store: st, PageSize: 500, MaxPerTick: 5000, Window: 10 * time.Minute, FullEvery: 60}
}

// Result counts one run.
type Result struct {
	Full     bool // read every SCM row rather than from the mark
	Scanned  int  // SCM rows read
	Copied   int  // new trace_events rows appended
	Existing int  // already copied (idempotency key present)
	Skipped  int  // no business id could be resolved
	QRCopied int  // new lot_qr_codes rows
}

func (r Result) String() string {
	return fmt.Sprintf("full=%v scanned=%d copied=%d existing=%d skipped=%d qr_copied=%d",
		r.Full, r.Scanned, r.Copied, r.Existing, r.Skipped, r.QRCopied)
}

// Tick runs one pass for the background loop: full on the first tick after
// start and every FullEvery ticks (and whenever no mark is stored yet),
// incremental otherwise.
func (s *Syncer) Tick(ctx context.Context) (Result, error) {
	// The first tick after start and every FullEvery-th after it read
	// everything; the rest read from the mark.
	full := s.ticks == 0 || (s.FullEvery > 0 && s.ticks%s.FullEvery == 0)
	s.ticks++
	return s.Run(ctx, full)
}

// Run syncs once. full=true reads every SCM row (the one-shot migration);
// otherwise it reads from the stored mark minus Window. Returns ErrLocked
// when another process is syncing.
func (s *Syncer) Run(ctx context.Context, full bool) (Result, error) {
	res := Result{Full: full}
	pool := s.Store.Pool()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return res, err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, syncLockKey).Scan(&got); err != nil {
		return res, fmt.Errorf("sync lock: %w", err)
	}
	if !got {
		return res, ErrLocked
	}
	defer func() {
		// Unlock on a fresh context: ctx may be the one that was cancelled.
		uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, syncLockKey)
	}()

	runErr := s.run(ctx, full, &res)
	s.saveRun(res, runErr)
	return res, runErr
}

type mark struct {
	createdAt time.Time
	id        string
}

func (m mark) after(o mark) bool {
	return m.createdAt.After(o.createdAt) || (m.createdAt.Equal(o.createdAt) && m.id > o.id)
}

func (s *Syncer) run(ctx context.Context, full bool, res *Result) error {
	cur, hasMark, err := s.loadMark(ctx)
	if err != nil {
		return err
	}
	if !hasMark {
		full = true
		res.Full = true
	}
	high := cur
	var from *mark
	if !full {
		f := mark{createdAt: cur.createdAt.Add(-s.window())}
		from = &f
	}
	pageSize := s.PageSize
	if pageSize <= 0 {
		pageSize = 500
	}
	for {
		page, err := s.readPage(ctx, from, pageSize)
		if err != nil {
			return err
		}
		res.Scanned += len(page)
		if err := s.copyPage(ctx, page, res); err != nil {
			return err
		}
		for _, r := range page {
			if m := (mark{r.createdAt, r.scmID}); !hasMark || m.after(high) {
				high, hasMark = m, true
			}
		}
		// Persist progress per page: a failure later in the tick resumes
		// from here, and anything re-read is dropped by its key.
		if len(page) > 0 {
			if err := s.saveMark(ctx, high); err != nil {
				return err
			}
		}
		if len(page) < pageSize || (!full && s.MaxPerTick > 0 && res.Scanned >= s.MaxPerTick) {
			break
		}
		last := page[len(page)-1]
		from = &mark{createdAt: last.createdAt, id: last.scmID}
	}
	n, err := s.copyQRCodes(ctx)
	res.QRCopied = n
	return err
}

func (s *Syncer) window() time.Duration {
	if s.Window <= 0 {
		return 10 * time.Minute
	}
	return s.Window
}

type scmEvent struct {
	scmID      string
	createdAt  time.Time
	occurredAt time.Time
	eventType  string
	entityType string
	businessID *string
	entityID   *string
	related    map[string]any
	payload    map[string]any
}

// readPage reads SCM events after from in (created_at, id) order. The business
// id resolution is the original one-shot migration's, schema-qualified.
func (s *Syncer) readPage(ctx context.Context, from *mark, limit int) ([]scmEvent, error) {
	q := `
		SELECT te.id::text, te.created_at, te.occurred_at, te.event_type, te.entity_type,
			COALESCE(
				CASE te.entity_type
					WHEN 'batch' THEN (SELECT business_id FROM public.batches WHERE id = te.entity_id)
					WHEN 'lot' THEN (SELECT business_id FROM public.export_lots WHERE id = te.entity_id)
					WHEN 'farmer' THEN (SELECT business_id FROM public.suppliers WHERE id = te.entity_id AND supplier_type = 'farmer')
					WHEN 'farm' THEN (SELECT business_id FROM public.farms WHERE id = te.entity_id)
					WHEN 'party' THEN (SELECT business_id FROM public.suppliers WHERE id = te.entity_id)
				END,
				te.related_ids->>'batch_business_id',
				te.related_ids->>'lot_business_id',
				te.related_ids->>'farmer_business_id',
				te.related_ids->>'farm_business_id',
				te.related_ids->>'party_business_id'
			),
			te.entity_id::text, te.related_ids, te.measurements
		FROM public.trace_events te`
	args := []any{}
	if from != nil {
		q += ` WHERE (te.created_at, te.id::text) > ($1, $2)`
		args = append(args, from.createdAt, from.id)
	}
	q += fmt.Sprintf(` ORDER BY te.created_at, te.id::text LIMIT %d`, limit)
	rows, err := s.Source.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("read public.trace_events: %w", err)
	}
	defer rows.Close()
	var out []scmEvent
	for rows.Next() {
		var e scmEvent
		if err := rows.Scan(&e.scmID, &e.createdAt, &e.occurredAt, &e.eventType, &e.entityType,
			&e.businessID, &e.entityID, &e.related, &e.payload); err != nil {
			return nil, fmt.Errorf("scan public.trace_events: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// copyPage appends the page's not-yet-copied events in (occurred_at, id) order.
func (s *Syncer) copyPage(ctx context.Context, page []scmEvent, res *Result) error {
	if len(page) == 0 {
		return nil
	}
	keys := make([]string, len(page))
	for i, e := range page {
		keys[i] = IdempotencyPrefix + e.scmID
	}
	have := map[string]bool{}
	rows, err := s.Store.Pool().Query(ctx,
		`SELECT idempotency_key FROM trace_events WHERE idempotency_key = ANY($1::text[])`, keys)
	if err != nil {
		return fmt.Errorf("existing keys: %w", err)
	}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return err
		}
		have[k] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	todo := make([]scmEvent, 0, len(page))
	for _, e := range page {
		if have[IdempotencyPrefix+e.scmID] {
			res.Existing++
			continue
		}
		if e.businessID == nil || *e.businessID == "" {
			res.Skipped++
			continue
		}
		todo = append(todo, e)
	}
	sort.SliceStable(todo, func(i, j int) bool {
		if !todo[i].occurredAt.Equal(todo[j].occurredAt) {
			return todo[i].occurredAt.Before(todo[j].occurredAt)
		}
		return todo[i].scmID < todo[j].scmID
	})

	after := s.AfterAppend
	if after == nil {
		after = func(ctx context.Context, st *store.Store, ev store.TraceEvent) {
			story.AfterAppend(ctx, st, ev.EventType, ev.EntityType, ev.EntityBusinessID, ev.RelatedIDs)
		}
	}
	for _, e := range todo {
		key := IdempotencyPrefix + e.scmID
		var entityID *uuid.UUID
		if e.entityID != nil {
			if id, err := uuid.Parse(*e.entityID); err == nil {
				entityID = &id
			}
		}
		ev, err := s.Store.AppendEvent(ctx, store.AppendEventInput{
			OccurredAt:       e.occurredAt,
			EventType:        e.eventType,
			EntityType:       e.entityType,
			EntityBusinessID: *e.businessID,
			EntityID:         entityID,
			RelatedIDs:       e.related,
			SourceService:    SourceService,
			Payload:          e.payload,
			IdempotencyKey:   &key,
		})
		if err != nil {
			return fmt.Errorf("append scm event %s: %w", e.scmID, err)
		}
		if !ev.Inserted {
			res.Existing++
			continue
		}
		res.Copied++
		after(ctx, s.Store, ev)
	}
	return nil
}

// copyQRCodes copies SCM's published QR codes that this service does not have.
// Existing rows are never touched (a lot's token here is authoritative once
// present). revoked_at is carried so a QR SCM revoked does not come back live.
func (s *Syncer) copyQRCodes(ctx context.Context) (int, error) {
	rows, err := s.Source.Query(ctx, `
		SELECT el.business_id, q.public_token, q.version, q.published_at, q.revoked_at, q.scan_count
		FROM public.lot_qr_codes q JOIN public.export_lots el ON el.id = q.lot_id`)
	if err != nil {
		return 0, fmt.Errorf("read public.lot_qr_codes: %w", err)
	}
	type qr struct {
		lot, token         string
		version            int
		published, revoked *time.Time
		scans              int64
	}
	var all []qr
	for rows.Next() {
		var r qr
		if err := rows.Scan(&r.lot, &r.token, &r.version, &r.published, &r.revoked, &r.scans); err != nil {
			rows.Close()
			return 0, err
		}
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	n := 0
	for _, r := range all {
		// Bare ON CONFLICT DO NOTHING: also absorbs a public_token collision,
		// which would otherwise fail every tick forever.
		tag, err := s.Store.Pool().Exec(ctx, `
			INSERT INTO lot_qr_codes (lot_business_id, public_token, version, published_at, revoked_at, scan_count)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
			r.lot, r.token, r.version, r.published, r.revoked, r.scans)
		if err != nil {
			return n, fmt.Errorf("insert qr %s: %w", r.lot, err)
		}
		n += int(tag.RowsAffected())
	}
	return n, nil
}

func (s *Syncer) loadMark(ctx context.Context) (mark, bool, error) {
	var m mark
	var at *time.Time
	err := s.Store.Pool().QueryRow(ctx,
		`SELECT last_created_at, last_scm_id FROM scm_sync_state WHERE source = $1`, stateKey,
	).Scan(&at, &m.id)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && at == nil) {
		return mark{}, false, nil
	}
	if err != nil {
		return m, false, fmt.Errorf("load sync mark: %w", err)
	}
	m.createdAt = *at
	return m, true, nil
}

func (s *Syncer) saveMark(ctx context.Context, m mark) error {
	_, err := s.Store.Pool().Exec(ctx, `
		INSERT INTO scm_sync_state (source, last_created_at, last_scm_id, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (source) DO UPDATE SET
			last_created_at = EXCLUDED.last_created_at,
			last_scm_id = EXCLUDED.last_scm_id,
			updated_at = now()`, stateKey, m.createdAt, m.id)
	return err
}

func (s *Syncer) saveRun(res Result, runErr error) {
	msg := ""
	if runErr != nil {
		msg = runErr.Error()
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.Store.Pool().Exec(sctx, `
		INSERT INTO scm_sync_state (source, last_run_at, last_copied, last_error, updated_at)
		VALUES ($1, now(), $2, $3, now())
		ON CONFLICT (source) DO UPDATE SET
			last_run_at = now(), last_copied = EXCLUDED.last_copied,
			last_error = EXCLUDED.last_error, updated_at = now()`,
		stateKey, res.Copied, msg)
}
