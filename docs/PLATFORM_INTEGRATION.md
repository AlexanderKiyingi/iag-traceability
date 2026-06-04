# Platform integration — iag-traceability

Custody events, story composition, and public QR behind the IAG API gateway.

| Field | Value |
|-------|-------|
| **Port** | `4011` |
| **Audience** | `iag.traceability` |
| **Gateway prefix** | `/api/v1/traceability` |
| **Postgres schema** | `traceability` |
| **Role** | `svc_iag_traceability` |

## Gateway routes

| Gateway path | Upstream |
|--------------|----------|
| `/api/v1/traceability/health` | `:4011/health` |
| `/api/v1/traceability/ready` | `:4011/ready` |
| `/api/v1/traceability/api/v1/*` | `:4011/api/v1/*` |
| `/api/v1/traceability/public/q/*` | `:4011/public/q/*` (public, no JWT) |

Compose: `UPSTREAM_TRACEABILITY=http://traceability:4011` on `api-gateway`.

## Permissions (registered at boot)

- `traceability.view_chain`
- `traceability.add_trace_event`
- `traceability.publish_qr`
- `traceability.view_events`

These mirror SCM codenames so existing groups keep working during Phase 2 cutover.

## Kafka (Phase 2)

Consumes (stub projection):

- `iag.supply-chain`
- `iag.production`
- `iag.quality`

## Local dev

```bash
cd services/operations/traceability
cp .env.example .env
go run ./cmd/server
curl http://localhost:4011/ready
curl http://localhost:8080/api/v1/traceability/health   # via gateway
```

## Relationship to iag-supply-chain

SCM remains system of record for parties, batches, and export lots. Traceability owns `trace_events`, `lot_qr_codes`, and public QR. During migration, SCM may proxy or dual-write until cutover (see planning doc).
