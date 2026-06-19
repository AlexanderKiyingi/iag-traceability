-- Dead-letter table for consumed Kafka events that could not be mapped to a
-- known trace event type. Previously such events were silently dropped
-- (consumer projectEvent returned nil), hiding upstream contract drift.

CREATE TABLE IF NOT EXISTS kafka_dead_letter (
  id          BIGSERIAL PRIMARY KEY,
  event_id    TEXT NOT NULL DEFAULT '',
  topic       TEXT NOT NULL DEFAULT '',
  event_type  TEXT NOT NULL DEFAULT '',
  reason      TEXT NOT NULL DEFAULT '',
  payload     JSONB NOT NULL DEFAULT '{}',
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS kafka_dead_letter_received_at_idx
  ON kafka_dead_letter (received_at DESC);

-- Partial unique index lets the consumer dedupe dead-letter writes
-- (ON CONFLICT) so a redelivery before the kafka_dedupe row commits cannot
-- create duplicate rows. Partial on non-empty event_id so malformed events
-- with no derivable id are not collapsed into a single row.
CREATE UNIQUE INDEX IF NOT EXISTS kafka_dead_letter_event_id_uidx
  ON kafka_dead_letter (event_id) WHERE event_id <> '';
