package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// QR register statuses.
const (
	QRStatusLive        = "Live"
	QRStatusRevoked     = "Revoked"
	QRStatusUnpublished = "Unpublished"
)

// StoryOverride is the hand-edited part of a lot's public story. A nil field
// means "not overridden": the composed value stands.
type StoryOverride struct {
	StoryTitle      *string    `json:"story_title"`
	StoryBody       *string    `json:"story_body"`
	UpdatedBy       string     `json:"updated_by"`
	UpdatedByUserID *uuid.UUID `json:"updated_by_user_id,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// QRRegisterRow is one row of the QR register: the lot's QR joined to its
// story projection and override. PublicURL is filled by the handler, which
// owns PUBLIC_TRACE_BASE_URL.
type QRRegisterRow struct {
	LotBusinessID string         `json:"lot_business_id"`
	Token         string         `json:"token"`
	Version       int            `json:"version"`
	PublicURL     string         `json:"public_url"`
	PublishedAt   *time.Time     `json:"published_at"`
	RevokedAt     *time.Time     `json:"revoked_at"`
	ScanCount     int64          `json:"scan_count"`
	Status        string         `json:"status"`
	StoryTitle    *string        `json:"story_title"`
	StoryOverride *StoryOverride `json:"story_override"`
	FarmerNames   []string       `json:"farmer_names"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// QRStatus maps the QR row's timestamps to the register status.
func QRStatus(publishedAt, revokedAt *time.Time) string {
	switch {
	case revokedAt != nil:
		return QRStatusRevoked
	case publishedAt != nil:
		return QRStatusLive
	default:
		return QRStatusUnpublished
	}
}

// QRRegisterFilter narrows ListQRRegister.
type QRRegisterFilter struct {
	// Status is "live" or "revoked" (case-insensitive); empty means all.
	Status string
	// Q matches lot business id, story title/body override, the composed
	// title, and farmer names, case-insensitively.
	Q     string
	Limit int
}

const qrRegisterSelect = `
	SELECT q.lot_business_id, q.public_token, q.version, q.published_at, q.revoked_at,
		q.scan_count, q.created_at, GREATEST(q.updated_at, COALESCE(o.updated_at, q.updated_at)),
		p.payload,
		o.lot_business_id IS NOT NULL, o.story_title, o.story_body,
		COALESCE(o.updated_by, ''), o.updated_by_user_id, COALESCE(o.updated_at, q.updated_at)
	FROM lot_qr_codes q
	LEFT JOIN lot_story_projections p ON p.lot_business_id = q.lot_business_id
	LEFT JOIN lot_story_overrides o ON o.lot_business_id = q.lot_business_id`

// ListQRRegister lists published lot QR codes, newest-touched first.
func (s *Store) ListQRRegister(ctx context.Context, f QRRegisterFilter) ([]QRRegisterRow, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var where []string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	switch strings.ToLower(strings.TrimSpace(f.Status)) {
	case "":
	case "live":
		where = append(where, "q.revoked_at IS NULL AND q.published_at IS NOT NULL")
	case "revoked":
		where = append(where, "q.revoked_at IS NOT NULL")
	default:
		return nil, ErrInvalidFilter
	}
	if q := strings.TrimSpace(f.Q); q != "" {
		like := arg("%" + escapeLike(q) + "%")
		where = append(where, `(
			q.lot_business_id ILIKE `+like+`
			OR o.story_title ILIKE `+like+`
			OR o.story_body ILIKE `+like+`
			OR p.payload->>'title' ILIKE `+like+`
			OR EXISTS (
				SELECT 1 FROM jsonb_array_elements(
					CASE WHEN jsonb_typeof(p.payload->'farmers') = 'array'
					     THEN p.payload->'farmers' ELSE '[]'::jsonb END) f
				WHERE f->>'name' ILIKE `+like+`
				   OR f->>'full_name' ILIKE `+like+`
				   OR f->>'display_name' ILIKE `+like+`
			))`)
	}
	sql := qrRegisterSelect
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	sql += " ORDER BY 8 DESC, q.lot_business_id LIMIT " + arg(limit)

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []QRRegisterRow{}
	for rows.Next() {
		r, err := scanQRRegisterRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetQRRegisterRow returns the register row for one lot (live or revoked).
func (s *Store) GetQRRegisterRow(ctx context.Context, lotBusinessID string) (QRRegisterRow, error) {
	row := s.pool.QueryRow(ctx, qrRegisterSelect+` WHERE q.lot_business_id = $1`, lotBusinessID)
	r, err := scanQRRegisterRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func scanQRRegisterRow(row pgx.Row) (QRRegisterRow, error) {
	var (
		r           QRRegisterRow
		payload     map[string]any
		hasOverride bool
		ov          StoryOverride
	)
	err := row.Scan(
		&r.LotBusinessID, &r.Token, &r.Version, &r.PublishedAt, &r.RevokedAt,
		&r.ScanCount, &r.CreatedAt, &r.UpdatedAt,
		&payload,
		&hasOverride, &ov.StoryTitle, &ov.StoryBody, &ov.UpdatedBy, &ov.UpdatedByUserID, &ov.UpdatedAt,
	)
	if err != nil {
		return r, err
	}
	r.Status = QRStatus(r.PublishedAt, r.RevokedAt)
	if hasOverride {
		r.StoryOverride = &ov
	}
	r.StoryTitle = registerTitle(r.StoryOverride, payload)
	r.FarmerNames = FarmerNames(payload)
	return r, nil
}

// registerTitle is the title the public sees: the override when set, else a
// title the composed story carries (an SCM preview may supply one), else nil.
func registerTitle(ov *StoryOverride, payload map[string]any) *string {
	if ov != nil && ov.StoryTitle != nil {
		t := *ov.StoryTitle
		return &t
	}
	if t, ok := payload["title"].(string); ok && strings.TrimSpace(t) != "" {
		return &t
	}
	return nil
}

// FarmerNames extracts display names from a composed story's farmers array.
// The story carries whatever the party projection held, so the name key
// varies; entries without a recognisable name are skipped.
func FarmerNames(payload map[string]any) []string {
	names := []string{}
	var entries []map[string]any
	switch arr := payload["farmers"].(type) {
	case []any:
		for _, e := range arr {
			if m, ok := e.(map[string]any); ok {
				entries = append(entries, m)
			}
		}
	case []map[string]any:
		entries = arr
	}
	seen := map[string]bool{}
	for _, e := range entries {
		for _, k := range []string{"name", "full_name", "display_name"} {
			if n, ok := e[k].(string); ok && strings.TrimSpace(n) != "" {
				n = strings.TrimSpace(n)
				if !seen[n] {
					seen[n] = true
					names = append(names, n)
				}
				break
			}
		}
	}
	return names
}

func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// ErrInvalidFilter is returned for an unrecognised filter value.
var ErrInvalidFilter = errors.New("invalid filter")

// GetStoryOverride returns the lot's story override, or nil when none is set.
func (s *Store) GetStoryOverride(ctx context.Context, lotBusinessID string) (*StoryOverride, error) {
	var ov StoryOverride
	err := s.pool.QueryRow(ctx, `
		SELECT story_title, story_body, updated_by, updated_by_user_id, updated_at
		FROM lot_story_overrides WHERE lot_business_id = $1`, lotBusinessID).Scan(
		&ov.StoryTitle, &ov.StoryBody, &ov.UpdatedBy, &ov.UpdatedByUserID, &ov.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ov, nil
}

// StoryOverridePatch is a partial edit: a nil field is left as it is, an
// empty string clears that field's override.
type StoryOverridePatch struct {
	StoryTitle      *string
	StoryBody       *string
	UpdatedBy       string
	UpdatedByUserID *uuid.UUID
}

// SetStoryOverride applies patch to the lot's story override, writes an audit
// row, and mirrors the result into the stored story projection — all in one
// transaction. It never touches lot_qr_codes: the token and version (and so
// every printed label) stay exactly as they were.
//
// Returns ErrNotFound when the lot has no QR row.
func (s *Store) SetStoryOverride(ctx context.Context, lotBusinessID string, patch StoryOverridePatch) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM lot_qr_codes WHERE lot_business_id = $1)`, lotBusinessID,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}

	var prevTitle, prevBody *string
	err = tx.QueryRow(ctx, `
		SELECT story_title, story_body FROM lot_story_overrides
		WHERE lot_business_id = $1 FOR UPDATE`, lotBusinessID).Scan(&prevTitle, &prevBody)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	title, body := prevTitle, prevBody
	if patch.StoryTitle != nil {
		title = nonEmpty(*patch.StoryTitle)
	}
	if patch.StoryBody != nil {
		body = nonEmpty(*patch.StoryBody)
	}

	if title == nil && body == nil {
		if _, err := tx.Exec(ctx, `DELETE FROM lot_story_overrides WHERE lot_business_id = $1`, lotBusinessID); err != nil {
			return err
		}
	} else if _, err := tx.Exec(ctx, `
		INSERT INTO lot_story_overrides
			(lot_business_id, story_title, story_body, updated_by, updated_by_user_id, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (lot_business_id) DO UPDATE SET
			story_title = EXCLUDED.story_title,
			story_body = EXCLUDED.story_body,
			updated_by = EXCLUDED.updated_by,
			updated_by_user_id = EXCLUDED.updated_by_user_id,
			updated_at = now()`,
		lotBusinessID, title, body, patch.UpdatedBy, patch.UpdatedByUserID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO lot_story_override_audit
			(lot_business_id, previous_title, previous_body, story_title, story_body, updated_by, updated_by_user_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		lotBusinessID, prevTitle, prevBody, title, body, patch.UpdatedBy, patch.UpdatedByUserID); err != nil {
		return err
	}

	// Keep the stored projection (the public fallback when a live compose
	// fails) consistent with the override, without recomposing it.
	overlay := map[string]any{}
	if title != nil {
		overlay["story_title"] = *title
	}
	if body != nil {
		overlay["story_body"] = *body
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lot_story_projections
		SET payload = (payload - 'story_title' - 'story_body') || $2::jsonb
		WHERE lot_business_id = $1`, lotBusinessID, overlay); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// StoryOverrideAudit is one recorded edit of a lot's public story.
type StoryOverrideAudit struct {
	PreviousTitle   *string    `json:"previous_title"`
	PreviousBody    *string    `json:"previous_body"`
	StoryTitle      *string    `json:"story_title"`
	StoryBody       *string    `json:"story_body"`
	UpdatedBy       string     `json:"updated_by"`
	UpdatedByUserID *uuid.UUID `json:"updated_by_user_id,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

// ListStoryOverrideAudit returns a lot's story edits, newest first.
func (s *Store) ListStoryOverrideAudit(ctx context.Context, lotBusinessID string, limit int) ([]StoryOverrideAudit, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT previous_title, previous_body, story_title, story_body, updated_by, updated_by_user_id, created_at
		FROM lot_story_override_audit WHERE lot_business_id = $1
		ORDER BY created_at DESC, id DESC LIMIT $2`, lotBusinessID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StoryOverrideAudit{}
	for rows.Next() {
		var a StoryOverrideAudit
		if err := rows.Scan(&a.PreviousTitle, &a.PreviousBody, &a.StoryTitle, &a.StoryBody,
			&a.UpdatedBy, &a.UpdatedByUserID, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func nonEmpty(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}
