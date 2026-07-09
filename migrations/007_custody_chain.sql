-- 007: Tamper-evident custody hash-chain.
--
-- trace_events is a flat append-only ledger; custody_chain adds a hash-linked,
-- globally-ordered chain over BATCH events so the chain can be walked and
-- verified (GET /chain/verify). Each row's event_hash = sha256(seq_index |
-- occurred_at_ms | batch_business_id | action | actor | quality_metrics_json |
-- prev_hash); the first row links to the genesis hash (64 zeros). Appends are
-- serialized with a transaction-scoped advisory lock so seq_index / prev_hash
-- stay consistent under concurrent ingestion.
CREATE TABLE IF NOT EXISTS custody_chain (
  seq_index         BIGINT PRIMARY KEY,
  trace_event_id    UUID NOT NULL REFERENCES trace_events(id),
  occurred_at       TIMESTAMPTZ NOT NULL,
  batch_business_id TEXT NOT NULL,
  action            TEXT NOT NULL,
  actor             TEXT NOT NULL DEFAULT '',
  quality_metrics   JSONB NOT NULL DEFAULT '{}',
  prev_hash         TEXT NOT NULL,
  event_hash        TEXT NOT NULL UNIQUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One chain block per trace event (idempotent replays must not double-append).
CREATE UNIQUE INDEX IF NOT EXISTS idx_custody_chain_event
  ON custody_chain (trace_event_id);

CREATE INDEX IF NOT EXISTS idx_custody_chain_batch
  ON custody_chain (batch_business_id, seq_index);
