-- Denormalized party/farm snapshots updated by Kafka consumers (Phase 3/5).

CREATE TABLE IF NOT EXISTS entity_projections (
  entity_type TEXT NOT NULL,
  entity_business_id TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (entity_type, entity_business_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_projections_type
  ON entity_projections (entity_type, updated_at DESC);
