-- 010: high-water mark for the SCM custody sync (internal/scmsync).
--
-- Additive and boot-safe: one new table, nothing existing is altered.
--
-- The mark is SCM's (created_at, id) — insertion order — not occurred_at.
-- SCM back-dates events (an intake recorded today for cherries received last
-- week), so an occurred_at mark would step over every late arrival. Each tick
-- re-reads a short trailing window behind the mark (commit-order skew between
-- concurrent SCM transactions) and drops rows already copied by their
-- idempotency key, `scm-migrate:<scm id>`.

CREATE TABLE IF NOT EXISTS scm_sync_state (
  source            TEXT PRIMARY KEY,
  last_created_at   TIMESTAMPTZ,
  last_scm_id       TEXT NOT NULL DEFAULT '',
  last_run_at       TIMESTAMPTZ,
  last_copied       INT NOT NULL DEFAULT 0,
  last_error        TEXT NOT NULL DEFAULT '',
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
