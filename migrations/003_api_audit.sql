-- HTTP request audit log (platform parity with fleet, procurement, DMS).

CREATE TABLE IF NOT EXISTS traceability_api_audit (
    id           BIGSERIAL PRIMARY KEY,
    logged_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    method       TEXT NOT NULL,
    path         TEXT NOT NULL,
    status_code  INT NOT NULL DEFAULT 0,
    user_name    TEXT NOT NULL DEFAULT '',
    duration_ms  INT NOT NULL DEFAULT 0,
    client_ip    TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS traceability_api_audit_logged_at_idx
  ON traceability_api_audit (logged_at DESC);
