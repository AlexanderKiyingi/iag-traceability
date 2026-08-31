package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	pool         *pgxpool.Pool
	chainEnabled bool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// SetChainEnabled toggles tamper-evident custody-chain appends for batch events.
func (s *Store) SetChainEnabled(b bool) *Store {
	s.chainEnabled = b
	return s
}

// ChainEnabled reports whether custody-chain appends are active.
func (s *Store) ChainEnabled() bool { return s.chainEnabled }

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

type TraceEvent struct {
	ID               uuid.UUID      `json:"id"`
	OccurredAt       time.Time      `json:"occurred_at"`
	EventType        string         `json:"event_type"`
	EntityType       string         `json:"entity_type"`
	EntityBusinessID string         `json:"entity_business_id"`
	EntityID         *uuid.UUID     `json:"entity_id,omitempty"`
	RelatedIDs       map[string]any `json:"related_ids,omitempty"`
	ActorUserID      *uuid.UUID     `json:"actor_user_id,omitempty"`
	SourceService    string         `json:"source_service"`
	Payload          map[string]any `json:"payload,omitempty"`
	IdempotencyKey   *string        `json:"idempotency_key,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

type AppendEventInput struct {
	OccurredAt       time.Time
	EventType        string
	EntityType       string
	EntityBusinessID string
	EntityID         *uuid.UUID
	RelatedIDs       map[string]any
	ActorUserID      *uuid.UUID
	SourceService    string
	Payload          map[string]any
	IdempotencyKey   *string
}

func (s *Store) AppendEvent(ctx context.Context, in AppendEventInput) (TraceEvent, error) {
	if in.OccurredAt.IsZero() {
		in.OccurredAt = time.Now().UTC()
	}
	if in.SourceService == "" {
		in.SourceService = "iag-traceability"
	}
	if in.RelatedIDs == nil {
		in.RelatedIDs = map[string]any{}
	}
	if in.Payload == nil {
		in.Payload = map[string]any{}
	}

	// Batch events are appended to the tamper-evident custody chain (when enabled)
	// atomically with the event insert; everything else uses the plain path.
	if s.chainEnabled && strings.EqualFold(in.EntityType, "batch") {
		return s.appendEventChained(ctx, in)
	}

	var ev TraceEvent
	err := s.pool.QueryRow(ctx, `
		INSERT INTO trace_events (
			occurred_at, event_type, entity_type, entity_business_id, entity_id,
			related_ids, actor_user_id, source_service, payload, idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id, occurred_at, event_type, entity_type, entity_business_id, entity_id,
			source_service, created_at`,
		in.OccurredAt, in.EventType, in.EntityType, in.EntityBusinessID, in.EntityID,
		in.RelatedIDs, in.ActorUserID, in.SourceService, in.Payload, in.IdempotencyKey,
	).Scan(
		&ev.ID, &ev.OccurredAt, &ev.EventType, &ev.EntityType, &ev.EntityBusinessID, &ev.EntityID,
		&ev.SourceService, &ev.CreatedAt,
	)
	ev.RelatedIDs = in.RelatedIDs
	ev.Payload = in.Payload
	ev.IdempotencyKey = in.IdempotencyKey
	return ev, err
}

func (s *Store) ListEventsForEntity(ctx context.Context, entityType, businessID string, limit int) ([]TraceEvent, error) {
	return s.listEvents(ctx, entityType, businessID, limit)
}

// ListRecentEvents returns the newest events across every entity.
//
// The per-entity list above answers "what happened to this batch", which is the
// right question for a drill-down and the wrong one for a ledger page: a ledger
// has no entity in hand, and both this service and iag-supply-chain previously
// required one. That left the event ledger with no endpoint it could call at
// all.
//
// Ordered newest-first, unlike the per-entity list — a chain of custody is read
// forwards, a ledger is read backwards.
func (s *Store) ListRecentEvents(ctx context.Context, limit int) ([]TraceEvent, error) {
	return s.listEvents(ctx, "", "", limit)
}

func (s *Store) listEvents(ctx context.Context, entityType, businessID string, limit int) ([]TraceEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `
		SELECT id, occurred_at, event_type, entity_type, entity_business_id, entity_id,
			source_service, related_ids, payload, created_at
		FROM trace_events`
	args := []any{}
	scoped := entityType != "" && businessID != ""
	if scoped {
		q += ` WHERE entity_type = $1 AND entity_business_id = $2`
		args = append(args, entityType, businessID)
		q += ` ORDER BY occurred_at ASC LIMIT $3`
	} else {
		q += ` ORDER BY occurred_at DESC LIMIT $1`
	}
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TraceEvent
	for rows.Next() {
		var ev TraceEvent
		var relatedIDs, payload map[string]any
		if err := rows.Scan(
			&ev.ID, &ev.OccurredAt, &ev.EventType, &ev.EntityType, &ev.EntityBusinessID, &ev.EntityID,
			&ev.SourceService, &relatedIDs, &payload, &ev.CreatedAt,
		); err != nil {
			return nil, err
		}
		ev.RelatedIDs = relatedIDs
		ev.Payload = payload
		out = append(out, ev)
	}
	return out, rows.Err()
}

type LotQR struct {
	LotBusinessID string
	PublicToken   string
	Version       int
	PublishedAt   *time.Time
	RevokedAt     *time.Time
	ScanCount     int64
}

func upsertLotQR(ctx context.Context, q execer, lotBusinessID, token string) error {
	_, err := q.Exec(ctx, `
		INSERT INTO lot_qr_codes (lot_business_id, public_token, version, published_at)
		VALUES ($1, $2, 1, now())
		ON CONFLICT (lot_business_id) DO UPDATE SET
			public_token = EXCLUDED.public_token,
			version = lot_qr_codes.version + 1,
			published_at = now(),
			revoked_at = NULL,
			updated_at = now()`,
		lotBusinessID, token)
	return err
}

func (s *Store) UpsertLotQR(ctx context.Context, lotBusinessID, token string) error {
	return upsertLotQR(ctx, s.pool, lotBusinessID, token)
}

// UpsertLotQRTx upserts the QR token and enqueues its outbox event in ONE
// transaction, so the published QR and the scm.lot.qr_published event commit
// atomically (transactional outbox).
func (s *Store) UpsertLotQRTx(ctx context.Context, lotBusinessID, token string, outbox OutboxRow) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := upsertLotQR(ctx, tx, lotBusinessID, token); err != nil {
		return err
	}
	if err := enqueueOutbox(ctx, tx, outbox); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) GetLotQRByToken(ctx context.Context, token string) (LotQR, error) {
	var qr LotQR
	err := s.pool.QueryRow(ctx, `
		SELECT lot_business_id, public_token, version, published_at, revoked_at, scan_count
		FROM lot_qr_codes
		WHERE public_token = $1 AND revoked_at IS NULL`, token).Scan(
		&qr.LotBusinessID, &qr.PublicToken, &qr.Version, &qr.PublishedAt, &qr.RevokedAt, &qr.ScanCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return qr, ErrNotFound
	}
	return qr, err
}

func (s *Store) IncrementScanCount(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE lot_qr_codes SET scan_count = scan_count + 1, updated_at = now()
		WHERE public_token = $1`, token)
	return err
}

func (s *Store) GetStoryProjection(ctx context.Context, lotBusinessID string) (map[string]any, error) {
	var payload map[string]any
	err := s.pool.QueryRow(ctx, `
		SELECT payload FROM lot_story_projections WHERE lot_business_id = $1`, lotBusinessID).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return payload, err
}

func (s *Store) UpsertStoryProjection(ctx context.Context, lotBusinessID string, payload map[string]any) error {
	if payload == nil {
		payload = map[string]any{}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO lot_story_projections (lot_business_id, payload, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (lot_business_id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()`,
		lotBusinessID, payload)
	return err
}

func (s *Store) UpsertEntityProjection(ctx context.Context, entityType, businessID string, payload map[string]any) error {
	if payload == nil {
		payload = map[string]any{}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO entity_projections (entity_type, entity_business_id, payload, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (entity_type, entity_business_id) DO UPDATE SET
			payload = EXCLUDED.payload,
			updated_at = now()`,
		entityType, businessID, payload)
	return err
}

func (s *Store) GetEntityProjection(ctx context.Context, entityType, businessID string) (map[string]any, error) {
	var payload map[string]any
	err := s.pool.QueryRow(ctx, `
		SELECT payload FROM entity_projections
		WHERE entity_type = $1 AND entity_business_id = $2`, entityType, businessID).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return payload, err
}

func revokeLotQR(ctx context.Context, q execer, lotBusinessID string) error {
	tag, err := q.Exec(ctx, `
		UPDATE lot_qr_codes SET revoked_at = now(), updated_at = now()
		WHERE lot_business_id = $1 AND revoked_at IS NULL`, lotBusinessID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RevokeLotQR(ctx context.Context, lotBusinessID string) error {
	return revokeLotQR(ctx, s.pool, lotBusinessID)
}

// RevokeLotQRTx revokes the QR and enqueues its outbox event in ONE
// transaction. Returns ErrNotFound (rolling back) when no active QR exists, so
// no spurious qr_revoked event is emitted.
func (s *Store) RevokeLotQRTx(ctx context.Context, lotBusinessID string, outbox OutboxRow) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := revokeLotQR(ctx, tx, lotBusinessID); err != nil {
		return err
	}
	if err := enqueueOutbox(ctx, tx, outbox); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) GetLotQRByLotID(ctx context.Context, lotBusinessID string) (LotQR, error) {
	var qr LotQR
	err := s.pool.QueryRow(ctx, `
		SELECT lot_business_id, public_token, version, published_at, revoked_at, scan_count
		FROM lot_qr_codes
		WHERE lot_business_id = $1 AND revoked_at IS NULL`, lotBusinessID).Scan(
		&qr.LotBusinessID, &qr.PublicToken, &qr.Version, &qr.PublishedAt, &qr.RevokedAt, &qr.ScanCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return qr, ErrNotFound
	}
	return qr, err
}

// LotHasCOA reports whether a Certificate of Analysis has been issued for the
// lot. A CoA event is normally projected onto the "lot" entity, but it may
// instead arrive referencing the lot via related_ids (e.g. issued against a
// batch). The local compliance gate uses this so it doesn't wrongly block a
// publish just because the CoA wasn't projected onto the lot entity directly.
func (s *Store) LotHasCOA(ctx context.Context, lotBusinessID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM trace_events
			WHERE event_type IN ('COA_ISSUED', 'qc.coa.issued')
			  AND (
			    (entity_type = 'lot' AND entity_business_id = $1)
			    OR related_ids->>'lot_business_id' = $1
			  )
		)`, lotBusinessID).Scan(&exists)
	return exists, err
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
