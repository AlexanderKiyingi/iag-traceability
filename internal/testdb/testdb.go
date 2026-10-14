// Package testdb provides a real-Postgres harness for integration tests.
//
// Run with:
//
//	TEST_DATABASE_URL=postgres://user:pass@localhost:5432/iag_platform?sslmode=disable \
//	  go test ./... -run Integration -v
//
// Tests skip automatically when TEST_DATABASE_URL is unset, so the default
// `go test ./...` (and CI without a database) stays green.
//
// Packages share the database and truncate it, so run them serially (-p 1)
// against a database nothing else uses.
package testdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"iag-traceability/backend/internal/db"
	"iag-traceability/backend/internal/migrate"
)

// Pool connects via TEST_DATABASE_URL (skipping if unset), applies migrations,
// truncates the trace tables, and registers cleanup.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create the schema once, on one connection, before the pool opens. The
	// pool's AfterConnect runs CREATE SCHEMA IF NOT EXISTS on every new
	// connection, and concurrent ones race on a virgin database
	// (duplicate key on pg_namespace_nspname_index).
	if conn, err := pgx.Connect(ctx, url); err == nil {
		_, _ = conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+db.Schema)
		_ = conn.Close(ctx)
	}
	pool, err := db.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := migrate.Up(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	Truncate(t, pool)
	t.Cleanup(pool.Close)
	return pool
}

// Truncate clears the trace tables between tests.
func Truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tables := []string{
		"trace_events",
		"kafka_dedupe",
		"kafka_dead_letter",
		"lot_qr_codes",
		"lot_story_projections",
		"entity_projections",
		"custody_chain",
		"lot_story_overrides",
		"lot_story_override_audit",
	}
	for _, tbl := range tables {
		if _, err := pool.Exec(ctx, "TRUNCATE TABLE "+tbl+" RESTART IDENTITY CASCADE"); err != nil {
			t.Fatalf("truncate %s: %v", tbl, err)
		}
	}
}
