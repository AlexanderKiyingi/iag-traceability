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

**Next.js frontend:** custody/QR/public routes are documented in the SCM guide — [FRONTEND_INTEGRATION.md](../../IAG_SCM_backend/docs/FRONTEND_INTEGRATION.md) (§7 Traceability).

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

## Registers API (Farmer Traceability frontend)

Full contract in `docs/openapi.yaml`.

### Custody transfers — `POST /api/v1/events` (`traceability.add_trace_event`)

`CUSTODY_TRANSFERRED`, `CUSTODY_IN_TRANSIT` and `CUSTODY_RECEIVED` are accepted for
`entity_type` `lot` or `batch`. The payload must carry `from` and `to` (non-empty
strings) and `quantity_kg` (a number ≥ 0); anything else is a 400. They take the
ordinary append path (and the custody hash-chain for batches when `CHAIN_ENABLED`).
In the public journey they appear only as stage `"Custody"` — from/to never reach it.

```json
{
  "event_type": "CUSTODY_TRANSFERRED",
  "entity_type": "lot",
  "entity_business_id": "LOT-2026-014",
  "occurred_at": "2026-10-14T08:30:00Z",
  "payload": { "from": "Wet mill A", "to": "Warehouse 2", "quantity_kg": 1200.5 }
}
```

### Event ledger — `GET /api/v1/events` (`traceability.view_events`)

No query = newest events across every entity. Optional, AND-combined:
`event_type` (comma list, OR), `entity_type` (alone or with `entity_business_id`),
`from` (inclusive) / `to` (exclusive; a bare `YYYY-MM-DD` includes that whole UTC
day), `limit` (default 200, max 499). Single-entity scope reads oldest-first,
everything else newest-first.

```json
{
  "events": [
    {
      "id": "8d0c…", "occurred_at": "2026-10-14T08:30:00Z",
      "event_type": "CUSTODY_TRANSFERRED", "entity_type": "lot",
      "entity_business_id": "LOT-2026-014", "actor_user_id": "1b2c…",
      "source_service": "iag-traceability",
      "payload": { "from": "Wet mill A", "to": "Warehouse 2", "quantity_kg": 1200.5 },
      "created_at": "2026-10-14T08:30:01Z"
    }
  ],
  "has_more": false,
  "limit": 200
}
```

### QR register — `GET /api/v1/qr-codes`, `GET /api/v1/qr-codes/:lotBusinessId` (`traceability.view_chain`)

Filters: `status` (`live` | `revoked`), `q` (lot id, story title/body, farmer
name), `limit`. List response `{ "qr_codes": [row…], "has_more": false, "limit": 200 }`;
the single read returns the row itself (404 when the lot has no QR).

```json
{
  "lot_business_id": "LOT-2026-014",
  "token": "q2Vx…",
  "version": 1,
  "public_url": "https://trace.example/q/q2Vx…",
  "published_at": "2026-10-10T09:00:00Z",
  "revoked_at": null,
  "scan_count": 37,
  "status": "Live",
  "story_title": "Washed Arabica from Mt Elgon",
  "story_override": {
    "story_title": "Washed Arabica from Mt Elgon",
    "story_body": "Hand picked by 42 smallholders.",
    "updated_by": "Grace Akello",
    "updated_by_user_id": "1b2c…",
    "updated_at": "2026-10-14T08:40:00Z"
  },
  "farmer_names": ["Akello Grace"],
  "created_at": "2026-10-10T09:00:00Z",
  "updated_at": "2026-10-14T08:40:00Z"
}
```

`status` is `Revoked` when `revoked_at` is set, `Live` when published and not
revoked (`Unpublished` is defensive only). `story_override` is `null` when the
story has not been edited.

### Edit the story — `PATCH /api/v1/qr-codes/:lotBusinessId` (`traceability.publish_qr`)

```json
{ "story_title": "Washed Arabica from Mt Elgon", "story_body": "Hand picked by 42 smallholders." }
```

Omitted field = unchanged; `""` clears it; both cleared removes the override.
Limits 200 / 5000 characters. Returns the updated register row. **Never
re-publishes**: token and version are untouched, so printed labels keep working
(re-publishing rotates the token). The override lives in `lot_story_overrides`
(migration 009), separate from `lot_story_projections`, because every composer
rebuild rewrites the projection payload wholesale; the composer and
`/public/q/:token` lay it over the composed story as `story.story_title` /
`story.story_body`. Each edit writes a `lot_story_override_audit` row (who,
previous, new).

### Revoke — `POST /api/v1/lots/:businessId/revoke` (`traceability.publish_qr`)

`{ "lot_business_id": "LOT-2026-014", "revoked": true, "qr_code": { …register row, "status": "Revoked" } }`.
404 when there is no active QR. The token then 404s on `/public/q/:token`.

## Tests

`go test ./...` runs unit tests; DB tests skip unless `TEST_DATABASE_URL` points
at a throwaway Postgres (the real migrator runs on it, tables are truncated):

```bash
docker run -d --name trace-test-pg -e POSTGRES_USER=iag -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=trace -p 127.0.0.1:55491:5432 postgres:16
TEST_DATABASE_URL='postgres://iag:pw@127.0.0.1:55491/trace?sslmode=disable' go test -p 1 ./...
```

`-p 1` matters: the packages share and truncate one database.

## Status

**Phase 2–6 implemented** — custody APIs, Kafka consumers, SCM client for story composition, publish gate (CoA), public QR, migration job (`cmd/migrate-scm`), OpenAPI spec. SCM proxies legacy routes with `X-Traceability-Proxy: iag-traceability` during cutover.

## Standalone repo

Clone from [github.com/AlexanderKiyingi/iag-traceability](https://github.com/AlexanderKiyingi/iag-traceability). Run `scripts/sync-platform-go.sh` before standalone Docker builds.
