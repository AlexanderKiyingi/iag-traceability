# iag-traceability

> Custody events, coffee story composition, and public QR for the IAG Coffee platform.

| Field | Value |
|-------|-------|
| **Port** | `4011` |
| **Gateway prefix** | `/api/v1/traceability` |
| **Audience** | `iag.traceability` |
| **Postgres schema** | `traceability` |
| **Remote** | [iag-traceability](https://github.com/AlexanderKiyingi/iag-traceability) |

## Role

Central **traceability hub** for IAG Coffee:

- Append-only **`trace_events`** (custody log)
- **Story composer** — farmer + processing + QC narrative for export lots
- **Public QR** — `/public/q/{token}` and PNG generation
- **Kafka consumer** — `iag.supply-chain`, `iag.production`, `iag.quality`

**System of record** for parties, batches, and export lots remains **`iag-supply-chain`** (`IAG_SCM_backend`). This service consumes events and optional SCM read APIs; it does not duplicate SCM operational CRUD.

Plan: [TRACEABILITY_AND_SUPPLIER_PLATFORM.md](../../../docs/planning/TRACEABILITY_AND_SUPPLIER_PLATFORM.md)

## Quick start (monorepo)

```bash
cd services/operations/traceability
cp .env.example .env
go run ./cmd/server
curl http://localhost:4011/ready
```

Via gateway (when `UPSTREAM_TRACEABILITY` is set):

```bash
curl http://localhost:8080/api/v1/traceability/health
```

## Layout

```
cmd/server/           HTTP API + permission registration
cmd/healthcheck/      Container HEALTHCHECK
internal/
  config/             Environment
  db/                   Postgres pool (traceability schema)
  migrate/              SQL migrations
  handlers/             Gin routes
  middleware/           JWT auth, CORS, security headers
  models/               Permissions catalogue
  store/                trace_events, lot_qr_codes
  consumer/             Kafka — scm.*, mes.*, qc.* → trace_events + projections
  scmclient/            Service-to-service SCM snapshot fetch
  story/                Story composer, publish gate, public QR
cmd/migrate-scm/        One-time SCM → traceability data copy
docs/openapi.yaml       OpenAPI spec
```

## Status

**Phase 2–6 implemented** — custody APIs, Kafka consumers, SCM client for story composition, publish gate (CoA), public QR, migration job (`cmd/migrate-scm`), OpenAPI spec. SCM proxies legacy routes with `X-Traceability-Proxy: iag-traceability` during cutover.

## Standalone repo

Clone from [github.com/AlexanderKiyingi/iag-traceability](https://github.com/AlexanderKiyingi/iag-traceability). Run `scripts/sync-platform-go.sh` before standalone Docker builds.
