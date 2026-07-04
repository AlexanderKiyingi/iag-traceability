package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

// execer is satisfied by both *pgxpool.Pool and pgx.Tx, so a query can run
// either standalone or inside a caller-owned transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// OutboxRow is a durable, not-yet-published domain event. Payload is the
// serialized CloudEvents envelope; EventID is that envelope's stable id so the
// relay can re-publish (and downstream can dedupe) idempotently.
type OutboxRow struct {
	ID      int64
	EventID string
	Topic   string
	Key     string
	Subject string
	Payload []byte
}

func enqueueOutbox(ctx context.Context, q execer, row OutboxRow) error {
	_, err := q.Exec(ctx, `
		INSERT INTO event_outbox (event_id, topic, partition_key, subject, payload)
		VALUES ($1, $2, $3, $4, $5)`,
		row.EventID, row.Topic, row.Key, row.Subject, row.Payload)
	return err
}

// EnqueueOutbox writes an outbox row outside any caller transaction. Prefer the
// *Tx store methods when the event must commit atomically with a state change.
func (s *Store) EnqueueOutbox(ctx context.Context, row OutboxRow) error {
	return enqueueOutbox(ctx, s.pool, row)
}

// FetchUnpublishedOutbox returns the oldest undelivered outbox rows for the
// relay to publish.
func (s *Store) FetchUnpublishedOutbox(ctx context.Context, limit int) ([]OutboxRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, topic, partition_key, subject, payload
		FROM event_outbox
		WHERE published_at IS NULL
		ORDER BY id ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.ID, &r.EventID, &r.Topic, &r.Key, &r.Subject, &r.Payload); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkOutboxPublished marks a row delivered so the relay skips it thereafter.
func (s *Store) MarkOutboxPublished(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE event_outbox SET published_at = now() WHERE id = $1`, id)
	return err
}

// MarkOutboxFailed records a delivery attempt failure for observability; the
// row stays unpublished and is retried on the next relay tick.
func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, errMsg string) error {
	if len(errMsg) > 500 {
		errMsg = errMsg[:500]
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE event_outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
		id, errMsg)
	return err
}
