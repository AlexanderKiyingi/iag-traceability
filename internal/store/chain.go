package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"iag-traceability/backend/internal/hashchain"
)

// custodyChainLockKey is the transaction-scoped advisory-lock key that serializes
// custody-chain appends so seq_index / prev_hash stay consistent under concurrent
// ingestion. Any fixed constant works as long as every appender uses the same one.
const custodyChainLockKey int64 = 0x1A6_C7A19 // "trace chain"

// ChainBlock is one hash-linked custody event, shaped to match the TraceAG
// portal's Block model so a BFF can consume it directly.
type ChainBlock struct {
	Index          int64          `json:"index"`
	Timestamp      time.Time      `json:"timestamp"`
	BatchID        string         `json:"batchId"`
	Action         string         `json:"action"`
	Actor          string         `json:"actor"`
	QualityMetrics map[string]any `json:"qualityMetrics"`
	PreviousHash   string         `json:"previousHash"`
	Hash           string         `json:"hash"`
	Nonce          int            `json:"nonce"`
}

// ChainVerifyResult is the outcome of walking and revalidating the chain.
type ChainVerifyResult struct {
	IsValid      bool     `json:"isValid"`
	Anomalies    []string `json:"anomalies"`
	CheckedCount int      `json:"checkedCount"`
}

// appendEventChained inserts the trace event and, only when it is a genuinely new
// row (not an idempotent replay), appends a hash-linked custody_chain block in the
// same transaction. A transaction-scoped advisory lock serializes the seq_index /
// prev_hash read-modify-write against concurrent appenders.
func (s *Store) appendEventChained(ctx context.Context, in AppendEventInput) (TraceEvent, error) {
	var ev TraceEvent
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ev, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, custodyChainLockKey); err != nil {
		return ev, fmt.Errorf("custody chain lock: %w", err)
	}

	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO trace_events (
			occurred_at, event_type, entity_type, entity_business_id, entity_id,
			related_ids, actor_user_id, source_service, payload, idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id, occurred_at, event_type, entity_type, entity_business_id, entity_id,
			source_service, created_at, (xmax = 0) AS inserted`,
		in.OccurredAt, in.EventType, in.EntityType, in.EntityBusinessID, in.EntityID,
		in.RelatedIDs, in.ActorUserID, in.SourceService, in.Payload, in.IdempotencyKey,
	).Scan(
		&ev.ID, &ev.OccurredAt, &ev.EventType, &ev.EntityType, &ev.EntityBusinessID, &ev.EntityID,
		&ev.SourceService, &ev.CreatedAt, &inserted,
	)
	if err != nil {
		return ev, err
	}
	ev.RelatedIDs = in.RelatedIDs
	ev.Payload = in.Payload
	ev.IdempotencyKey = in.IdempotencyKey
	ev.Inserted = inserted

	if inserted {
		var lastSeq int64
		prevHash := hashchain.GenesisHash
		err = tx.QueryRow(ctx,
			`SELECT seq_index, event_hash FROM custody_chain ORDER BY seq_index DESC LIMIT 1`).
			Scan(&lastSeq, &prevHash)
		if err != nil && err != pgx.ErrNoRows {
			return ev, err
		}
		if err == pgx.ErrNoRows {
			lastSeq = 0
			prevHash = hashchain.GenesisHash
		}
		seq := lastSeq + 1
		actor := ""
		if in.ActorUserID != nil {
			actor = in.ActorUserID.String()
		}
		hash := hashchain.EventHash(seq, ev.OccurredAt.UTC().UnixMilli(), ev.EntityBusinessID, ev.EventType, actor, in.Payload, prevHash)
		if _, err := tx.Exec(ctx, `
			INSERT INTO custody_chain (
				seq_index, trace_event_id, occurred_at, batch_business_id, action,
				actor, quality_metrics, prev_hash, event_hash
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			seq, ev.ID, ev.OccurredAt, ev.EntityBusinessID, ev.EventType,
			actor, in.Payload, prevHash, hash,
		); err != nil {
			return ev, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ev, err
	}
	return ev, nil
}

// ListChain returns custody-chain blocks in sequence order. When batchID is
// non-empty it is scoped to that batch; otherwise it returns the global chain.
func (s *Store) ListChain(ctx context.Context, batchID string, limit int) ([]ChainBlock, error) {
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	var rows pgx.Rows
	var err error
	if batchID != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT seq_index, occurred_at, batch_business_id, action, actor, quality_metrics, prev_hash, event_hash
			FROM custody_chain WHERE batch_business_id = $1 ORDER BY seq_index ASC LIMIT $2`, batchID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT seq_index, occurred_at, batch_business_id, action, actor, quality_metrics, prev_hash, event_hash
			FROM custody_chain ORDER BY seq_index ASC LIMIT $1`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ChainBlock{}
	for rows.Next() {
		var b ChainBlock
		if err := rows.Scan(&b.Index, &b.Timestamp, &b.BatchID, &b.Action, &b.Actor, &b.QualityMetrics, &b.PreviousHash, &b.Hash); err != nil {
			return nil, err
		}
		if b.QualityMetrics == nil {
			b.QualityMetrics = map[string]any{}
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// VerifyChain walks the whole chain, recomputing each block hash and checking
// linkage to the previous block. It reports every anomaly rather than stopping at
// the first, so an operator sees the full extent of any tampering.
func (s *Store) VerifyChain(ctx context.Context) (ChainVerifyResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq_index, occurred_at, batch_business_id, action, actor, quality_metrics, prev_hash, event_hash
		FROM custody_chain ORDER BY seq_index ASC`)
	if err != nil {
		return ChainVerifyResult{}, err
	}
	defer rows.Close()

	res := ChainVerifyResult{IsValid: true, Anomalies: []string{}}
	expectedPrev := hashchain.GenesisHash
	for rows.Next() {
		var (
			seq        int64
			occurred   time.Time
			batchID    string
			action     string
			actor      string
			metrics    map[string]any
			storedPrev string
			storedHash string
		)
		if err := rows.Scan(&seq, &occurred, &batchID, &action, &actor, &metrics, &storedPrev, &storedHash); err != nil {
			return ChainVerifyResult{}, err
		}
		if storedPrev != expectedPrev {
			res.Anomalies = append(res.Anomalies, fmt.Sprintf("block %d: prev_hash breaks linkage", seq))
		}
		want := hashchain.EventHash(seq, occurred.UTC().UnixMilli(), batchID, action, actor, metrics, storedPrev)
		if want != storedHash {
			res.Anomalies = append(res.Anomalies, fmt.Sprintf("block %d: event_hash does not match content", seq))
		}
		expectedPrev = storedHash
		res.CheckedCount++
	}
	if err := rows.Err(); err != nil {
		return ChainVerifyResult{}, err
	}
	res.IsValid = len(res.Anomalies) == 0
	return res, nil
}
