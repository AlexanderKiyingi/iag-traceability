package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"iag-traceability/backend/migrations"
)

const migrateAdvisoryLockKey1 int32 = 881234501
const migrateAdvisoryLockKey2 int32 = 401100337

const migrationTable = `
CREATE TABLE IF NOT EXISTS traceability.schema_migrations (
	version TEXT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

func Up(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, migrateAdvisoryLockKey1, migrateAdvisoryLockKey2); err != nil {
		return fmt.Errorf("migrate advisory lock: %w", err)
	}
	if _, err := tx.Exec(ctx, migrationTable); err != nil {
		return fmt.Errorf("migration table: %w", err)
	}

	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		version := strings.TrimSuffix(name, ".sql")
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM traceability.schema_migrations WHERE version = $1)`,
			version,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if exists {
			continue
		}

		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if err := execSQL(ctx, tx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO traceability.schema_migrations (version) VALUES ($1)`,
			version,
		); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate commit: %w", err)
	}
	committed = true
	return nil
}

func execSQL(ctx context.Context, tx pgx.Tx, sql string) error {
	sql = strings.TrimSpace(strings.ReplaceAll(sql, "\r\n", "\n"))
	if sql == "" {
		return nil
	}
	for _, chunk := range strings.Split(sql, ";\n\n") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}
		if _, err := tx.Exec(ctx, chunk); err != nil {
			snippet := chunk
			if len(snippet) > 400 {
				snippet = snippet[:400] + "…"
			}
			return fmt.Errorf("exec migration chunk: %w\n--\n%s", err, snippet)
		}
	}
	return nil
}
