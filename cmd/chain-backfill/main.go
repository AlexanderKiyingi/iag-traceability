// Command chain-backfill retro-populates custody_chain from the flat
// trace_events ledger.
//
// The runtime append path (store.appendEventChained) only hash-links batch
// events that arrive *after* CHAIN_ENABLED is switched on. When the chain is
// activated on a deployment that already has history, every pre-existing batch
// event is unchained. This one-shot job walks those events in deterministic
// chronological order and appends a custody_chain block for each, building on
// whatever head already exists — so it is safe to run before or after the
// runtime chain has started, and safe to re-run (blocks already present are
// skipped via the unique index on trace_event_id).
//
// The hash computation MUST stay byte-for-byte identical to
// store.appendEventChained, otherwise VerifyChain will flag every backfilled
// block. Both call hashchain.EventHash with the same field order.
//
// Usage: DATABASE_URL=... go run ./cmd/chain-backfill [--dry-run]
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"iag-traceability/backend/internal/db"
	"iag-traceability/backend/internal/hashchain"
)

// custodyChainLockKey mirrors store.custodyChainLockKey so the backfill
// serializes against any concurrent runtime appends on the same chain.
const custodyChainLockKey int64 = 0x1A6_C7A19

func main() {
	dryRun := flag.Bool("dry-run", false, "report how many events would be chained without writing")
	flag.Parse()

	_ = godotenv.Load()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx := context.Background()
	// db.NewPool pins search_path to traceability. A bare pgxpool on a DSN
	// without one resolves trace_events to SCM's public.trace_events in the
	// shared database.
	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	n, err := backfill(ctx, pool, *dryRun)
	if err != nil {
		log.Fatalf("backfill: %v", err)
	}
	if *dryRun {
		log.Printf("[chain-backfill] dry-run: %d batch event(s) would be chained", n)
		return
	}
	log.Printf("[chain-backfill] appended %d block(s) to custody_chain", n)
}

// backfill runs the whole retro-population inside a single transaction under the
// custody-chain advisory lock. Doing it in one transaction means either the full
// backfill lands or none of it does — the chain is never left half-linked.
func backfill(ctx context.Context, pool *pgxpool.Pool, dryRun bool) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, custodyChainLockKey); err != nil {
		return 0, err
	}

	// Current chain head. When empty, start from genesis — identical to the
	// runtime path's cold-start behaviour.
	var lastSeq int64
	prevHash := hashchain.GenesisHash
	err = tx.QueryRow(ctx,
		`SELECT seq_index, event_hash FROM custody_chain ORDER BY seq_index DESC LIMIT 1`).
		Scan(&lastSeq, &prevHash)
	if err != nil && err != pgx.ErrNoRows {
		return 0, err
	}
	if err == pgx.ErrNoRows {
		lastSeq, prevHash = 0, hashchain.GenesisHash
	}

	// Batch events with no custody_chain block yet, in deterministic
	// chronological order so the backfilled chain is reproducible.
	rows, err := tx.Query(ctx, `
		SELECT te.id::text, te.occurred_at, te.event_type, te.entity_business_id,
		       te.actor_user_id::text, te.payload
		FROM trace_events te
		LEFT JOIN custody_chain cc ON cc.trace_event_id = te.id
		WHERE te.entity_type ILIKE 'batch' AND cc.trace_event_id IS NULL
		ORDER BY te.occurred_at ASC, te.created_at ASC, te.id ASC`)
	if err != nil {
		return 0, err
	}

	type pending struct {
		id         string
		occurredAt time.Time
		eventType  string
		businessID string
		actor      string
		payload    map[string]any
	}
	var batch []pending
	for rows.Next() {
		var (
			id         string
			occurredAt time.Time
			eventType  string
			businessID string
			actor      *string
			payload    map[string]any
		)
		if err := rows.Scan(&id, &occurredAt, &eventType, &businessID, &actor, &payload); err != nil {
			rows.Close()
			return 0, err
		}
		if payload == nil {
			payload = map[string]any{}
		}
		a := ""
		if actor != nil {
			a = *actor
		}
		batch = append(batch, pending{id, occurredAt, eventType, businessID, a, payload})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if dryRun {
		return len(batch), nil
	}

	seq := lastSeq
	for _, p := range batch {
		seq++
		hash := hashchain.EventHash(seq, p.occurredAt.UTC().UnixMilli(), p.businessID, p.eventType, p.actor, p.payload, prevHash)
		if _, err := tx.Exec(ctx, `
			INSERT INTO custody_chain (
				seq_index, trace_event_id, occurred_at, batch_business_id, action,
				actor, quality_metrics, prev_hash, event_hash
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			seq, p.id, p.occurredAt, p.businessID, p.eventType,
			p.actor, p.payload, prevHash, hash,
		); err != nil {
			return 0, err
		}
		prevHash = hash
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}
