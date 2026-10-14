// Command migrate-scm copies iag-supply-chain's public.trace_events and
// public.lot_qr_codes into the traceability schema, once.
//
// It is the one-shot form of the background SCM sync (internal/scmsync, run
// by cmd/server when SCM_SYNC_ENABLED=true): same mapping, same source_service,
// same idempotency key (scm-migrate:<scm id>), so re-running it — or running it
// alongside the loop — never duplicates a row. Events go through the normal
// append path, so they are hash-chained when CHAIN_ENABLED=true.
//
// Usage:
//
//	DATABASE_URL=... go run ./cmd/migrate-scm                  # SCM in the same database
//	DATABASE_URL=... SCM_DATABASE_URL=... go run ./cmd/migrate-scm
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"iag-traceability/backend/internal/db"
	"iag-traceability/backend/internal/scmsync"
	"iag-traceability/backend/internal/store"
)

func main() {
	_ = godotenv.Load()
	traceURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	scmURL := strings.TrimSpace(os.Getenv("SCM_DATABASE_URL"))
	if traceURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	ctx := context.Background()
	// db.NewPool pins search_path to traceability, so the destination writes
	// can never land in SCM's same-named public tables.
	tracePool, err := db.NewPool(ctx, traceURL)
	if err != nil {
		log.Fatal(err)
	}
	defer tracePool.Close()

	source := tracePool
	if scmURL != "" {
		scmPool, err := pgxpool.New(ctx, scmURL)
		if err != nil {
			log.Fatal(err)
		}
		defer scmPool.Close()
		source = scmPool
	}

	st := store.New(tracePool).SetChainEnabled(os.Getenv("CHAIN_ENABLED") == "true")
	res, err := scmsync.New(source, st).Run(ctx, true)
	if errors.Is(err, scmsync.ErrLocked) {
		log.Fatal("the SCM sync is running in a server replica; it already does this — or retry once it finishes")
	}
	if err != nil {
		log.Fatalf("migrate-scm: %v (%s)", err, res)
	}
	log.Printf("migrated %d trace_events (%d already present, %d skipped without business id); migrated %d lot_qr_codes",
		res.Copied, res.Existing, res.Skipped, res.QRCopied)
}
