-- Editable public story for a published lot QR (QR register).
--
-- Additive and boot-safe: two new tables, nothing existing is altered, so it
-- is safe under AUTO_MIGRATE on a live database.
--
-- Why a separate table and not columns/keys on lot_story_projections:
-- lot_story_projections.payload is rewritten wholesale by every composer
-- rebuild (publish, story_updated, manual events, the consumer). Anything kept
-- inside the payload is lost on the next rebuild, and a row there only exists
-- once a compose has run. Keyed by the lot business id on its own, the
-- override outlives any rebuild and is applied on top of whatever the composer
-- produced (override fields win, the rest of the composed story stays).
--
-- Editing a story never touches lot_qr_codes: re-publishing rotates the token
-- and breaks every printed label.

CREATE TABLE IF NOT EXISTS lot_story_overrides (
  lot_business_id     TEXT PRIMARY KEY,
  story_title         TEXT,
  story_body          TEXT,
  updated_by          TEXT NOT NULL DEFAULT '',
  updated_by_user_id  UUID,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per edit: who changed the public story, from what, to what.
CREATE TABLE IF NOT EXISTS lot_story_override_audit (
  id                  BIGSERIAL PRIMARY KEY,
  lot_business_id     TEXT NOT NULL,
  previous_title      TEXT,
  previous_body       TEXT,
  story_title         TEXT,
  story_body          TEXT,
  updated_by          TEXT NOT NULL DEFAULT '',
  updated_by_user_id  UUID,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lot_story_override_audit_lot
  ON lot_story_override_audit (lot_business_id, created_at DESC);

-- The QR register lists newest-first.
CREATE INDEX IF NOT EXISTS idx_lot_qr_codes_updated
  ON lot_qr_codes (updated_at DESC);
