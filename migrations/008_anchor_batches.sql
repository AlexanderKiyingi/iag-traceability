-- 008: L2 notarization + L3 public anchoring.
--
-- An anchor batch fixes a contiguous run of custody_chain blocks
-- [from_seq, to_seq] by their Merkle root and signs that root with a platform
-- key. Auditors can then verify any single event with a Merkle proof against the
-- signed root, without walking the whole chain or trusting our database.
--
-- The same row optionally carries an L3 public-ledger reference (provider +
-- tx_ref) once the Merkle root has been posted to a public/permissioned chain.
-- L3 is additive: an anchor is a valid L2 notarization the moment it is signed;
-- provider/tx_ref stay NULL until (and unless) a public anchor is published.
CREATE TABLE IF NOT EXISTS anchor_batches (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_seq      BIGINT NOT NULL,
  to_seq        BIGINT NOT NULL,
  event_count   INTEGER NOT NULL,
  merkle_root   TEXT NOT NULL,                 -- hex(sha256 tree root over event_hash leaves)
  algo          TEXT NOT NULL DEFAULT 'none',  -- signing algo: ed25519 | hmac-sha256 | none
  key_id        TEXT NOT NULL DEFAULT '',      -- signer key identifier (for rotation)
  signature     TEXT NOT NULL DEFAULT '',      -- hex signature over merkle_root; empty when algo=none
  period_start  TIMESTAMPTZ NOT NULL,          -- occurred_at of first block in range
  period_end    TIMESTAMPTZ NOT NULL,          -- occurred_at of last block in range
  -- L3 public anchoring (NULL until a root is posted on-chain)
  provider      TEXT NOT NULL DEFAULT 'none',  -- none | polygon | ...
  tx_ref        TEXT,                          -- public ledger tx hash / reference
  anchored_at   TIMESTAMPTZ,                   -- when tx_ref was confirmed
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- One anchor per chain head position: makes "select events after last anchor"
  -- unambiguous and blocks accidental double-notarization of the same range.
  CONSTRAINT anchor_batches_to_seq_uniq UNIQUE (to_seq),
  CONSTRAINT anchor_batches_range_ck CHECK (to_seq >= from_seq AND event_count > 0)
);

-- Newest-anchor lookups (GET /v1/anchors/latest, "events since last anchor").
CREATE INDEX IF NOT EXISTS idx_anchor_batches_to_seq ON anchor_batches (to_seq DESC);
