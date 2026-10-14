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
  scmsync/              Same-database copy of SCM custody events + QR codes
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

## SCM custody sync (no Kafka, no service auth)

SCM writes its custody events to `public.trace_events` in the same Postgres
this service uses, but nothing relays SCM's outbox and service-to-service auth
is refused, so none of them reach `traceability.trace_events`.
`internal/scmsync` copies them directly:

- Reads `public.trace_events` and `public.lot_qr_codes` (joined to
  `public.export_lots`, `public.batches`, `public.suppliers`, `public.farms`),
  always schema-qualified, so it works on this service's own DSN.
- Same mapping as the original `cmd/migrate-scm`: business id resolved from
  the SCM entity (or `related_ids`), payload = SCM `measurements`,
  `source_service = iag-supply-chain-migration`, idempotency key
  `scm-migrate:<scm event id>`. Rows a previous one-shot run copied are never
  duplicated; events with no resolvable business id are skipped.
- Every event goes through `store.AppendEvent`, the path `POST /api/v1/events`
  uses: with `CHAIN_ENABLED=true` batch events are hash-chained under the
  custody-chain advisory lock, and the lot story is refreshed as for a posted
  event (`story.AfterAppend`).
- Incremental: the high-water mark is SCM's `(created_at, id)` in
  `traceability.scm_sync_state` (migration 010). Insertion order, not
  `occurred_at`, because SCM back-dates events — a late, back-dated event has
  a new `created_at` and is picked up on the next tick. Each tick re-reads 10
  minutes behind the mark (out-of-order commits) and drops already-copied
  rows by key. The first tick after start, and every 60th, is a full rescan
  as a safety net. Within a page (500 rows) events are appended in
  `(occurred_at, id)` order.
- QR codes: SCM rows this service lacks are inserted (`ON CONFLICT DO
  NOTHING`, with `revoked_at` carried); existing rows are never changed.
- One replica syncs at a time (`pg_try_advisory_lock` per tick). Each tick
  logs `[scm-sync] tick ok: full=… scanned=… copied=… existing=… skipped=…
  qr_copied=…`; a failure is logged, written to `scm_sync_state.last_error`
  and retried next tick — it never stops the server.

| Variable | Default | Meaning |
|---|---|---|
| `SCM_SYNC_ENABLED` | `false` | Run the background sync in `cmd/server`. |
| `SCM_SYNC_INTERVAL` | `60s` | Tick period (Go duration). |
| `SCM_SYNC_SAME_DB` | `true` when `SCM_DATABASE_URL` is empty | Read SCM's `public.*` through this service's pool. |
| `SCM_DATABASE_URL` | — | Separate SCM database (used when `SCM_SYNC_SAME_DB=false`). |

The database role needs `SELECT` on SCM's `public.trace_events`,
`public.lot_qr_codes`, `public.export_lots`, `public.batches`,
`public.suppliers` and `public.farms`; without it every tick logs
`permission denied` and copies nothing.

`go run ./cmd/migrate-scm` is the same copy, once (full scan). It takes the same
lock, so it refuses to run while a replica is syncing.

### CHAIN_ENABLED cutover runbook

1. **Deploy with `SCM_SYNC_ENABLED=true` and `CHAIN_ENABLED=false`.** Within a
   tick the log shows `copied=42` (SCM's history: FARMER_REGISTERED +
   CHERRY_RECEIVED; `skipped=` counts any whose business id cannot be
   resolved). Confirm:
   ```sql
   SELECT event_type, count(*) FROM traceability.trace_events
   WHERE source_service = 'iag-supply-chain-migration' GROUP BY 1;
   SELECT last_run_at, last_copied, last_error FROM traceability.scm_sync_state;
   ```
2. **Backfill the chain:** `DATABASE_URL=… go run ./cmd/chain-backfill --dry-run`
   (reports how many batch events would be chained), then the same without
   `--dry-run`. Re-running is safe: chained events are skipped.
3. **Set `CHAIN_ENABLED=true`** and redeploy. Run `--dry-run` once more; it
   should report 0. If events arrived between steps 2 and 3, run it for real
   again. Then `GET /api/v1/chain/verify` should return `isValid: true`.

Why this order: runtime chaining only links events appended *after*
`CHAIN_ENABLED` is on, and the backfill appends every unchained batch event,
oldest first, *after the current chain head*. Copying the history with the
chain off and backfilling first puts that history at the start of the chain
in chronological order. Turning the chain on first would put new runtime
events at the head and the history after them — still a valid chain, but no
longer in time order. Backfill and runtime appends share the custody-chain
advisory lock, so they never interleave a block.

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
