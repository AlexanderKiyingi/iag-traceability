-- iag-traceability schema (Phase 2 extract)
-- No FKs to iag-supply-chain tables; cross-service refs use business IDs.

CREATE TABLE IF NOT EXISTS trace_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  occurred_at TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_business_id TEXT NOT NULL,
  entity_id UUID,
  related_ids JSONB NOT NULL DEFAULT '{}',
  actor_user_id UUID,
  actor_party_id UUID,
  source_service TEXT NOT NULL DEFAULT 'iag-traceability',
  payload JSONB NOT NULL DEFAULT '{}',
  idempotency_key TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trace_events_idempotency
  ON trace_events (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trace_events_entity
  ON trace_events (entity_type, entity_business_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_trace_events_type_time
  ON trace_events (event_type, occurred_at DESC);

CREATE TABLE IF NOT EXISTS lot_qr_codes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lot_business_id TEXT NOT NULL UNIQUE,
  public_token TEXT NOT NULL UNIQUE,
  version INT NOT NULL DEFAULT 1,
  published_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  scan_count BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lot_story_projections (
  lot_business_id TEXT PRIMARY KEY,
  payload JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kafka_dedupe (
  event_id TEXT PRIMARY KEY,
  topic TEXT NOT NULL DEFAULT '',
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
