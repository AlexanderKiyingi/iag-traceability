package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// One-time migration: copy public.trace_events and lot_qr_codes into traceability schema.
// Usage: SCM_DATABASE_URL=... DATABASE_URL=... go run ./cmd/migrate-scm
func main() {
	_ = godotenv.Load()
	scmURL := os.Getenv("SCM_DATABASE_URL")
	traceURL := os.Getenv("DATABASE_URL")
	if scmURL == "" || traceURL == "" {
		log.Fatal("SCM_DATABASE_URL and DATABASE_URL are required")
	}
	ctx := context.Background()
	scmPool, err := pgxpool.New(ctx, scmURL)
	if err != nil {
		log.Fatal(err)
	}
	defer scmPool.Close()
	tracePool, err := pgxpool.New(ctx, traceURL)
	if err != nil {
		log.Fatal(err)
	}
	defer tracePool.Close()

	rows, err := scmPool.Query(ctx, `
		SELECT te.occurred_at, te.event_type, te.entity_type,
			COALESCE(te.related_ids->>'batch_business_id', te.related_ids->>'lot_business_id', te.related_ids->>'farmer_business_id', te.entity_type || ':' || te.entity_id::text),
			te.entity_id, te.related_ids, te.actor_id, te.measurements
		FROM trace_events te ORDER BY te.occurred_at`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var n int
	for rows.Next() {
		var occurred time.Time
		var eventType, entityType, entityBiz, actorID string
		var entityID *string
		var rel, meas []byte
		if err := rows.Scan(&occurred, &eventType, &entityType, &entityBiz, &entityID, &rel, &actorID, &meas); err != nil {
			log.Fatal(err)
		}
		payload := map[string]any{}
		_ = json.Unmarshal(meas, &payload)
		related := map[string]any{}
		_ = json.Unmarshal(rel, &related)
		_, err := tracePool.Exec(ctx, `
			INSERT INTO trace_events (occurred_at, event_type, entity_type, entity_business_id, entity_id, related_ids, source_service, payload)
			VALUES ($1,$2,$3,$4,$5::uuid,$6,'iag-supply-chain-migration',$7)
			ON CONFLICT DO NOTHING`,
			occurred, eventType, entityType, entityBiz, entityID, related, payload)
		if err == nil {
			n++
		}
	}
	log.Printf("migrated %d trace_events", n)

	qrRows, err := scmPool.Query(ctx, `
		SELECT el.business_id, q.public_token, q.version, q.published_at, q.scan_count
		FROM lot_qr_codes q JOIN export_lots el ON el.id = q.lot_id`)
	if err != nil {
		log.Fatal(err)
	}
	defer qrRows.Close()
	var qn int
	for qrRows.Next() {
		var lotBiz, token string
		var version int
		var published *time.Time
		var scans int64
		if err := qrRows.Scan(&lotBiz, &token, &version, &published, &scans); err != nil {
			log.Fatal(err)
		}
		_, err := tracePool.Exec(ctx, `
			INSERT INTO lot_qr_codes (lot_business_id, public_token, version, published_at, scan_count)
			VALUES ($1,$2,$3,$4,$5) ON CONFLICT (lot_business_id) DO NOTHING`,
			lotBiz, token, version, published, scans)
		if err == nil {
			qn++
		}
	}
	fmt.Printf("migrated %d lot_qr_codes\n", qn)
}
