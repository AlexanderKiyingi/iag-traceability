package auditlog

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) LogAPIRequest(ctx context.Context, method, path string, statusCode int, userName string, durationMs int, clientIP string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO traceability_api_audit (method, path, status_code, user_name, duration_ms, client_ip)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		method, path, statusCode, userName, durationMs, clientIP)
	return err
}

func (s *Store) ListAPIAuditLogs(ctx context.Context, limit int) ([]map[string]any, int, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM traceability_api_audit`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT method, path, status_code, user_name, duration_ms, logged_at
		FROM traceability_api_audit ORDER BY logged_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var method, path, user string
		var status, dur int
		var at time.Time
		if err := rows.Scan(&method, &path, &status, &user, &dur, &at); err != nil {
			return nil, 0, err
		}
		out = append(out, map[string]any{
			"method":      method,
			"path":        path,
			"status":      status,
			"user":        user,
			"duration_ms": dur,
			"logged_at":   at,
		})
	}
	return out, total, rows.Err()
}
