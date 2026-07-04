-- Transactional outbox for domain events emitted by iag-traceability.
-- Previously QR publish/revoke/story-updated events were a fire-and-forget
-- dual-write to Kafka from the request path: if the broker was briefly
-- unavailable the event was lost while the state change committed. The outbox
-- makes emission durable — the event row is written in the SAME transaction as
-- the state change, and a background relay drains rows to Kafka with retries.

CREATE TABLE IF NOT EXISTS event_outbox (
  id            BIGSERIAL PRIMARY KEY,
  event_id      TEXT NOT NULL DEFAULT '',   -- CloudEvents envelope id (stable for dedupe)
  topic         TEXT NOT NULL,
  partition_key TEXT NOT NULL DEFAULT '',
  subject       TEXT NOT NULL,              -- event type, e.g. scm.lot.qr_published
  payload       JSONB NOT NULL,             -- the serialized CloudEvents envelope
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ,
  attempts      INT NOT NULL DEFAULT 0,
  last_error    TEXT NOT NULL DEFAULT ''
);

-- The relay scans only unpublished rows, oldest first — keep that lookup cheap
-- and ordered without walking published history.
CREATE INDEX IF NOT EXISTS event_outbox_unpublished_idx
  ON event_outbox (id) WHERE published_at IS NULL;
