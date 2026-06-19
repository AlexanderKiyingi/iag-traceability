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

func (s *Store) APIMonitoringSummary(ctx context.Context) (map[string]any, error) {
	var total24h, errors24h int
	var avgMs float64
	err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*)::int,
			COUNT(*) FILTER (WHERE status_code >= 400)::int,
			COALESCE(AVG(duration_ms), 0)
		FROM traceability_api_audit
		WHERE logged_at >= NOW() - INTERVAL '24 hours'
	`).Scan(&total24h, &errors24h, &avgMs)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"requests_24h":    total24h,
		"errors_24h":      errors24h,
		"avg_duration_ms": avgMs,
	}, nil
}

func (s *Store) APIMonitoringActivity(ctx context.Context, limit int) ([]map[string]any, error) {
	items, _, err := s.ListAPIAuditLogs(ctx, limit)
	return items, err
}

func (s *Store) MonitoringSummary(ctx context.Context, kafkaEnabled bool) (map[string]any, error) {
	summary, err := s.APIMonitoringSummary(ctx)
	if err != nil {
		return nil, err
	}
	var events, activeQR int
	_ = s.pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM trace_events`).Scan(&events)
	_ = s.pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM lot_qr_codes WHERE revoked_at IS NULL`).Scan(&activeQR)
	summary["trace_events"] = events
	summary["active_qr_codes"] = activeQR
	summary["kafka_consumer_enabled"] = kafkaEnabled
	summary["database"] = true

	// Ingest-health signals: total public scans, deduped events seen, and
	// dead-lettered (unmapped) events — the latter surfaces upstream contract
	// drift that was previously written to a table nothing read.
	var totalScans int64
	_ = s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(scan_count), 0)::bigint FROM lot_qr_codes`).Scan(&totalScans)
	summary["total_qr_scans"] = totalScans

	var dedupeCount, deadLetterCount int
	_ = s.pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM kafka_dedupe`).Scan(&dedupeCount)
	_ = s.pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM kafka_dead_letter`).Scan(&deadLetterCount)
	summary["kafka_events_seen"] = dedupeCount
	summary["kafka_dead_letters"] = deadLetterCount

	return summary, nil
}

// RecentDeadLetters returns the most recent dead-lettered (unmapped) events so
// operators can see exactly which upstream event types are being dropped.
func (s *Store) RecentDeadLetters(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, topic, event_type, reason, received_at
		FROM kafka_dead_letter ORDER BY received_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var eventID, topic, eventType, reason string
		var at time.Time
		if err := rows.Scan(&eventID, &topic, &eventType, &reason, &at); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"event_id":    eventID,
			"topic":       topic,
			"event_type":  eventType,
			"reason":      reason,
			"received_at": at,
		})
	}
	return out, rows.Err()
}
