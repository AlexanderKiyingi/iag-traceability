package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

type TraceEvent struct {
	ID               uuid.UUID
	OccurredAt       time.Time
	EventType        string
	EntityType       string
	EntityBusinessID string
	EntityID         *uuid.UUID
	RelatedIDs       map[string]any
	ActorUserID      *uuid.UUID
	ActorPartyID     *uuid.UUID
	SourceService    string
	Payload          map[string]any
	IdempotencyKey   *string
	CreatedAt        time.Time
}

type AppendEventInput struct {
	OccurredAt       time.Time
	EventType        string
	EntityType       string
	EntityBusinessID string
	EntityID         *uuid.UUID
	RelatedIDs       map[string]any
	ActorUserID      *uuid.UUID
	ActorPartyID     *uuid.UUID
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

	var ev TraceEvent
	err := s.pool.QueryRow(ctx, `
		INSERT INTO trace_events (
			occurred_at, event_type, entity_type, entity_business_id, entity_id,
			related_ids, actor_user_id, actor_party_id, source_service, payload, idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id, occurred_at, event_type, entity_type, entity_business_id, entity_id,
			source_service, created_at`,
		in.OccurredAt, in.EventType, in.EntityType, in.EntityBusinessID, in.EntityID,
		in.RelatedIDs, in.ActorUserID, in.ActorPartyID, in.SourceService, in.Payload, in.IdempotencyKey,
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
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, occurred_at, event_type, entity_type, entity_business_id, entity_id,
			source_service, related_ids, payload, created_at
		FROM trace_events
		WHERE entity_type = $1 AND entity_business_id = $2
		ORDER BY occurred_at ASC
		LIMIT $3`, entityType, businessID, limit)
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

func (s *Store) UpsertLotQR(ctx context.Context, lotBusinessID, token string) error {
	_, err := s.pool.Exec(ctx, `
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

func (s *Store) RevokeLotQR(ctx context.Context, lotBusinessID string) error {
	tag, err := s.pool.Exec(ctx, `
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

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
