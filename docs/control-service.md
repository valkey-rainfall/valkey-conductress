# Fleet control service

The Phase 2 control service is a durable mailbox and status registry for independent Conductress runners. It runs only on the data host. It does not execute benchmarks and it never opens connections to benchmark hosts.

Runner polling and inbox-to-local-queue import are Phase 4 work. Deploying this service alone does not change any runner.

## Safety invariants

- The service listens on `127.0.0.1` only; an existing TLS reverse proxy is the network entrance.
- Every API route requires a bearer token.
- Runner identity comes from its token, not a request body.
- A claim lease covers transfer into the runner's local queue only.
- Lease expiry can requeue `claimed` work, but never `accepted` work.
- Accepted tasks are never reassigned automatically.
- A runner may own only one accepted task at a time; prefetch/pipelining is intentionally deferred.
- Authentication attempts and API requests are rate-limited by the reverse proxy example.
- Control-plane loss cannot interrupt a benchmark because no execution lease exists.
- SQLite and the append-only audit log survive service restart.

## Install

Use a dedicated source checkout and virtual environment on the data host:

```bash
python3 -m venv /opt/conductress-control/.venv
/opt/conductress-control/.venv/bin/pip install '/opt/conductress-control[control]'
```

The `control` extra is intentionally absent from runner installations. It contains exact-pinned `aiohttp` and `jsonschema` dependencies.

## Private configuration

Create:

```text
/etc/conductress-control/fleet.json
/etc/conductress-control/tokens.json
/var/lib/conductress-control/
/var/log/conductress-control/
```

Suggested ownership and modes:

```text
/etc/conductress-control/fleet.json      root:conductress 0640
/etc/conductress-control/tokens.json     root:conductress 0640
/var/lib/conductress-control             conductress:conductress 0750
/var/log/conductress-control             conductress:conductress 0750
```

Start from `deploy/control-service/fleet.json.example` and `tokens.json.example`. The example hashes are inert placeholders and must be replaced.

Generate a random token, keep the plaintext only at its client, and hash it without exposing it in the process list:

```bash
conductress-control hash-token
```

Store only the printed SHA-256 digest. Give each runner a separate token so it can be revoked independently.

## Run locally

```bash
CONTROL_DB_PATH=/var/lib/conductress-control/control.db \
FLEET_MANIFEST_PATH=/etc/conductress-control/fleet.json \
TOKENS_PATH=/etc/conductress-control/tokens.json \
AUDIT_JSONL_PATH=/var/log/conductress-control/audit.jsonl \
conductress-control serve --port 8390
```

The host is deliberately not configurable: the process always binds `127.0.0.1`. Use `deploy/control-service/conductress-control.service` and the reverse-proxy snippet for persistent deployment.

The production data host uses local XFS storage, which supports SQLite WAL mode. Do not place `control.db` on NFS.

## API

All routes are under `/api/v1/` and all JSON responses include `schema_version: 1`.

Operator routes:

```text
GET    /api/v1/health
POST   /api/v1/tasks
GET    /api/v1/tasks?runner_id=&state=&limit=&offset=
GET    /api/v1/tasks/{task_id}
DELETE /api/v1/tasks/{task_id}
GET    /api/v1/fleet
GET    /api/v1/fleet/{runner_id}
```

Runner routes:

```text
PUT  /api/v1/runners/{runner_id}/status
POST /api/v1/runners/{runner_id}/claim
POST /api/v1/tasks/{task_id}/accept
POST /api/v1/tasks/{task_id}/complete
POST /api/v1/tasks/{task_id}/fail
```

Submit a task envelope:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $CONDUCTRESS_OPERATOR_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-request-1' \
  --data @task-envelope.json \
  https://data.conductress.rainsupreme.net/api/v1/tasks
```

Errors use stable codes:

```json
{
  "schema_version": 1,
  "error": "only queued tasks can be cancelled",
  "code": "TASK_NOT_CANCELLABLE"
}
```

## Task lifecycle

```text
queued -> claimed -> accepted -> completed
                    \-> accepted -> failed
claimed --lease expires before accept--> queued
queued -> cancelled
```

`POST .../claim` is idempotent for a runner while its transfer lease is active: repeated requests return the same task and claim token. The runner persists the task locally, then sends the token to `POST .../accept`. No heartbeat or lease renewal occurs after acceptance.

Task submission supports an `Idempotency-Key` header. Replaying the same key and payload returns the existing task; changing the payload produces `IDEMPOTENCY_CONFLICT`.

## Persistence and backup

SQLite uses WAL mode, foreign keys, explicit `BEGIN IMMEDIATE` claim transactions, and a five-second busy timeout. Back up with SQLite's online backup command rather than copying only the main file while WAL files are active:

```bash
sqlite3 /var/lib/conductress-control/control.db ".backup '/var/lib/conductress-control/control.backup.db'"
```

`audit_log` in SQLite is authoritative. `/var/log/conductress-control/audit.jsonl` is a best-effort append-only mirror for external inspection.

## Phase boundary

This PR intentionally does not include:

- fleet-aware local CLI commands;
- runner mailbox polling or local queue import;
- boundary-only status publication migration;
- daily canary scheduling;
- live data-host deployment;
- multiple same-platform scheduling.
