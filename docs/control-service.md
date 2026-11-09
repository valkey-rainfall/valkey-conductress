# Fleet control service

The control service is a durable mailbox and status registry for independent Conductress runners. It runs only on the data host. It does not execute benchmarks and it never opens connections to benchmark hosts.

Runner polling and inbox-to-local-queue import are handled elsewhere; deploying this service alone changes no runner.

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

Public read-only route:

```text
GET    /api/v1/public/dashboard
```

This route is intentionally unauthenticated for the public status page. It returns only nonterminal mailbox task summaries: runner ID, task ID, state, class, priority, submitted time, type, source, specifier, note, and expected duration. Each runner always includes the authoritative `total_count`, plus `returned_count`, `truncated`, and `expected_duration_complete`; task details remain capped at 50 per runner. It never returns submitter identity, full envelopes, claim tokens, leases, outcomes, credentials, or mutation controls. Disabled runners are omitted. Responses allow cross-origin `GET`/`OPTIONS` reads and permit five seconds of shared caching. The reverse proxy MUST rate-limit this route; the provided `/api/v1/` rate limit applies to it.

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
pending-approval -> queued        (approved)
pending-approval -> cancelled     (rejected)
queued | pending-approval -> cancelled          (caller cancels an unclaimed task)
claimed | accepted -> cancel-requested          (caller cancels an in-flight task)
```

`POST .../claim` is idempotent for a runner while its transfer lease is active: repeated requests return the same task and claim token. The runner persists the task locally, then sends the token to `POST .../accept`. No heartbeat or lease renewal occurs after acceptance.

Task submission supports an `Idempotency-Key` header. Replaying the same key and payload returns the existing task; changing the payload produces `IDEMPOTENCY_CONFLICT`.

## Identity, provenance, and approval

A directory of users and agents (`users.toml`, path from `USERS_PATH`) maps a bearer-token login to a GitHub account, a role (`owner`, `collaborator`, `approver`), a daily runner-minute quota, and allowed sources. There is one account per GitHub user: an agent entry names only its login and its human `sponsor`, and inherits the sponsor's GitHub account, role, quota and sources at load time (setting any of them on the agent is rejected). The agent's own login exists so it holds its own token and each task records which of the two submitted it. Tokens carry a `user` role and a `login`; operator and runner tokens are unchanged.

When provenance verification is enabled (`PROVENANCE_VERIFICATION=true`, with a `GITHUB_TOKEN` for a higher rate limit), a submission's `provenance.sha` is accepted only if it is reachable from an allowlisted repository (the two project repositories plus the submitter's own fork) or is the head of an open pull request against the upstream project; the verified pull-request identity is stored on the envelope. An owner may bypass with `?bypass_provenance=true`. A rejected sha returns `PROVENANCE_REJECTED`.

A submission from a login that is not in the directory, or from a user over its daily quota, is held in `pending-approval` rather than queued. An approver lists held tasks and approves or rejects them by task ID or batch ID.

### Approval, batch, and cancel routes

```text
GET  /api/v1/tasks/mine            (a user's own tasks)
GET  /api/v1/tasks/pending         (approver: tasks awaiting approval)
POST /api/v1/tasks/{selector}/approve   (approver: queue a held task or batch)
POST /api/v1/tasks/{selector}/reject    (approver: reject a held task or batch)
GET  /api/v1/batches/{batch_id}    (tasks sharing a batch id)
DELETE /api/v1/tasks/{selector}    (cancel a task or whole batch)
```

`selector` is a task ID or a batch ID. Cancelling an unclaimed task (`queued` or `pending-approval`) cancels it outright; cancelling an in-flight task (`claimed` or `accepted`) marks it `cancel-requested`, and the runner stops at its next rep/cell boundary. A non-owner may cancel only their own tasks; an owner or operator may cancel anyone's. The `conductress queue cancel|mine|pending|approve|reject` commands and `queue add* --batch` are thin clients over these routes.

### Approval notification hook

When `NOTIFICATION_URL` is set, the control plane POSTs a small JSON notice to that URL each time a submission lands in `pending-approval`. Delivery is best effort and never blocks submission. The payload is:

```json
{
  "schema_version": 1,
  "event": "task.pending_approval",
  "task_id": "2026.09.30_20.00.00.000000",
  "batch_id": "batch-0123456789abcdef",
  "runner_id": "armbench",
  "submitter_login": "ghost",
  "submitted_by": "ghost",
  "reason": "submitter is not a known user"
}
```

### Canary notices and static canary status

After every newly accepted canary observation the service writes two static
files under the published canary directory (`PUBLISHED_CANARY_DIR`; default:
`PUBLISHED_TASKS_DIR`), no auth like the task files:
`canary/<runner_id>.json` (`{"schema_version": 1, "runner": <the public
projection of the entry GET /api/v1/canary/status returns for that runner>}`)
and `canary/fleet.json` (the projection of the whole status document). They
are also written at startup. The dashboard reads them for the runner cards and
the platform banner.

The public files are an ALLOWLIST projection (`public_fields.py`), not a copy
of the API document. The API keeps the recorded environment of every
observation (harness revision, load generator commit, instance id, boot id,
host fingerprint); the files keep only the keys the allowlist names: runner
and profile identity, dates, scores, median, MAD, thresholds, phase and state,
the schedule, the calibration report, and of the environment only package
versions and CPU words (`kernel_release`, `glibc_version`, `cpu_microcode`,
`cpu_model`, `packages`, ...). The change annotation keeps version changes as
`field: old -> new` and turns private fields into the type of change:
`harness revision changed`, `load generator changed`, `reboot`, `host
replaced`. `environment_json` is never published. A key the allowlist has not
classified is dropped, and `unknown_canary_keys` makes the test suite fail on
it, so a new API field is decided before it can be published. The same module
gives the sweep exporter its period labels and environments and drops
`closed_by` from a manifest's generation entries.

The two directories are separate on purpose. A task record repeats the task
envelope and the full result (source, specifier, note, binary paths, the
client TOML, host details), which can name private builds and hosts. A canary
status only carries the public canary profile and the public runner
environment.
A deployment that must not publish task records sets `PUBLISHED_CANARY_DIR`
and leaves `PUBLISHED_TASKS_DIR` unset. The production data host does exactly
that, and as a second guard its nginx server block answers 404 for `/tasks/`
(`location ^~ /tasks/ { return 404; }`), so a task file that reaches the web
root by mistake is still not served.

It also POSTs, best effort and never blocking the ingest, one notice per event
per accepted observation to `CANARY_NOTIFICATION_URL` (default: the approval
webhook `NOTIFICATION_URL`):

- `canary.alarm` when `candidate_signal` is `alarm` (and `warning` when
  `CANARY_NOTIFY_WARNING=true`);
- `canary.environment_change` when the observation carries an
  `env_change_annotation`.

Both carry `schema_version`, `event`, `runner_id`, `profile_id`, `utc_date`,
`task_id`, `score`, `ref_median`, `delta_pct`, `candidate_signal`, `phase` and
`env_change_annotation`. An alarm in the `observation` or `calibrating` phase
is posted with its phase: it is a notice about an uncalibrated series, not a
verdict. `actionable` stays false; nothing acts on a notice automatically. A
replayed outcome posts nothing.

## Results on the data host

At completion a runner pushes the full result record, not a truncated summary. The control plane stores it in the task's SQLite row (authoritative). When configured it also mirrors each completed task to an append-only results JSONL (`RESULTS_JSONL_PATH`) and publishes a static per-task JSON under the published tasks directory (`PUBLISHED_TASKS_DIR/tasks/<task_id>.json`), written atomically. The static files carry no auth (results are public) so the dashboard and agents can fetch them directly, and `GET /api/v1/tasks/{task_id}` and `GET /api/v1/batches/{batch_id}` return the same records over the API.

The record keeps every scalar and aggregate, the peak-memory scalars, and the categorized jemalloc `breakdown`. It drops only the unbounded stack arrays: the jemalloc per-frame `raw_stacks` and the collapsed CPU flamegraph stacks (`cpu_stacks_main`, `cpu_stacks_io`). The full stacks remain on the runner and in the runner-published artifacts.

### Environment provenance (schema 3)

Every result row carries `provenance_schema_version`, `runner_id`, `platform` and an `environment` block built by `runner_identity.get_result_provenance`. Schema 2 added, next to `host_fingerprint`, `instance_id`, `kernel_release`, `machine`, `cpu_model` and `conductress_revision`:

| Field | Source | Absent |
|---|---|---|
| `glibc_version` | `gnu_get_libc_version` of the running process (ctypes), fallback `platform.libc_ver` | `null` on a non-glibc libc |
| `cpu_microcode` | `/proc/cpuinfo` `microcode` of the first processor | `null` on Graviton (not exposed); `null -> null` is never a change |
| `boot_id` | `/proc/sys/kernel/random/boot_id` | `null` off Linux |
| `cachecannon_commit`, `cachecannon_dirty` | `git rev-parse HEAD` and `git status` of the checkout above the task's `cachecannon_binary` (cached per path and mtime); `cachecannon_sha256` instead when the binary is not in a checkout | `null` for tasks without cachecannon |

Schema 3 adds `packages`: the installed `version-release` of `kernel`, `glibc`, `microcode_ctl` and `tuned` (`PACKAGE_NAMES`), read once per runner process with `rpm -q --qf '%{NAME}=%{VERSION}-%{RELEASE}'` (2 s timeout). A package that is not installed is `null`; when several kernels are installed the one whose version-release is the prefix of the running `kernel_release` is recorded. The whole block is `null` on a host without rpm. It exists because the running libc reports `2.34` before and after a glibc update (the 2026-11-06 update moved the package from `2.34-231.amzn2023.0.4` to `.0.5`), so a package release change was invisible. In a change list a package shows as `packages.glibc: old -> new`.

The host fields are cached for the runner process: kernel, boot id, glibc, microcode and installed packages cannot change under a running runner (a reboot ends the process, a package update applies to new processes), so the cached values are what the runner ran with.

A row also carries `server_environment` when the task runner could read the host the primary server ran on (one shell command over the host connection, once per task, before the task starts): `host_fingerprint`, `instance_id`, `kernel_release`, `machine`, `cpu_model`, `glibc_version`, `cpu_microcode`, `boot_id`, `packages` and `same_host` (true when the server's boot id equals the runner's). On the loopback runners the two blocks describe one host; on g4net the server block is the other machine. For the canary annotation the service adds the server block's `kernel_release`, `glibc_version`, `cpu_microcode`, `boot_id` and `host_fingerprint` as `server_*` keys only when `same_host` is false.

The canary's `env_change_annotation` is built by `drift_analyzer.environment_changes`: a key the previous observation did not carry because its provenance schema predates the field is not a change (a deploy that adds fields produces no annotation); a real `null` that gains a value is. A nested block (`packages`) produces one dotted change per member. The sweep exporter uses the same function for its environment period markers (configuration.md, "Environment periods").

## Outcome size

One request body limit applies on the whole path from the runner to the database, and every part of the deployment must agree on it:

| Where | Setting | Default |
|---|---|---|
| Control service (app) | `MAX_BODY_BYTES` env, `ControlConfig.max_body_bytes` | 4 MiB (`DEFAULT_MAX_BODY_BYTES`) |
| nginx in front of it | `client_max_body_size` in `deploy/control-service/nginx-control.conf.example` | `4m`; MUST be >= `MAX_BODY_BYTES` |
| Runner (client) | `CONDUCTRESS_MAX_OUTCOME_BYTES` env, `RunnerMailbox(max_outcome_bytes=...)` | 3 MiB (`outcome_size.DEFAULT_MAX_OUTCOME_BYTES`), below the server default |

`GET /api/v1/health` (operator token) returns the app's deployed value as `limits.max_body_bytes`, so it can be checked against the nginx value. A body over the app limit gets `413 BODY_TOO_LARGE`. When nginx is the lower limit it answers 413 itself with no JSON body, and the runner reports `HTTP_413` instead of `BODY_TOO_LARGE`: that difference says which of the two limits was hit.

A completed task's outcome is the largest body a runner sends. The runner measures it as it will go on the wire (compact JSON, no extra whitespace) and, when it is over `max_outcome_bytes`, trims it before the first send. Trimming removes bulk, optional values, biggest first, until the outcome fits: the result keys `series` (the replica-read per-rep INFO series), `samples` (memory series), `lscpu`, `reader_toml`, `writer_toml`, `toml_config` and `sample_errors`, at any depth of the result. Scores, per-rep aggregates, verdicts, lag statistics, memory scalars and the jemalloc `breakdown` are never dropped. A scalar that shares a bulk name (`bottleneck.samples` is a count) is kept.

The control service never retries the same bytes after a permanent rejection (HTTP 413, 400 or 422). The runner steps down instead, each step sent at once:

1. the full outcome is refused: the runner sends a reduced outcome, with every bulk key dropped;
2. the reduced outcome is refused: the runner reports `state: failed` with an error such as `outcome_rejected: HTTP_413 (HTTP 413), 196565 bytes; full result kept at /home/<user>/conductress/results/output.jsonl`;
3. that failure report is refused too: the runner clears its journal and keeps only a downgrade record.

Transient errors (5xx, unreachable, token or control-plane state problems such as 401/403/404/409) keep the outcome in the journal and retry at the next boundary, as before. The full result row always stays on the runner in `results/output.jsonl` (or `failed_tasks.jsonl`), whatever was sent.

A trimmed or reduced outcome carries an `outcome_delivery` object on the result:

```json
"outcome_delivery": {
  "trimmed": true,
  "reason": "rejected: HTTP_413 (HTTP 413), 183943 bytes",
  "dropped": ["result.data.per_rep_results[3].series (7649 bytes)", "..."],
  "original_bytes": 183943,
  "sent_bytes": 16765,
  "full_result_path": "/home/ec2-user/conductress/results/output.jsonl"
}
```

`reason` starts with `preflight:` when the client budget trimmed it and `rejected:` when the server did. The runner also keeps the last downgrade in its status as `fleet_control.last_outcome_downgrade` (`stage` is `trimmed`, `reduced`, `failed` or `dropped`), shown by `conductress fleet status --json` and as a `Last outcome:` line in `conductress fleet show <runner>`.

## Archiving a runner's canary history

The drift analyzer counts and windows a runner's observations by `runner_id`,
so a series restarts when its history gets another id. When a host's
environment changes so much that later canaries cannot be judged against the
earlier ones (intelbench, 2026-11-06), the operator archives the history:

```
sudo systemctl stop conductress-control.service
conductress-control archive-runner --runner intelbench \
    --archive-id intelbench-pre-2026-11-07 --until 2026-11-06 \
    --note "kernel 6.1.170 -> 6.1.189, glibc, microcode 0x2b000685; canary +12.3%" [--dry-run]
# paste the printed entry into fleet.json
sudo systemctl start conductress-control.service
```

It refuses while the service is active (an ingest could race the rename),
takes a backup with SQLite's online backup first (`<db>.pre-archive-<id>.db`,
never overwritten), and in one transaction renames the runner's
`canary_observations` and `canary_schedule` rows with `utc_date <= until` and
its `canary_calibration_reports` row to the archive id, resets the rolling
statistics of live observations after the cut (they were computed against the
old history; the series now reads as a fresh start, ordinal 1, 2, ...), and
writes one `runner.archive` audit entry. Nothing is deleted; `tasks`,
`runner_status`, the audit log and result blobs keep the live id. The archive
id is a series identity, not a host: it has no token and no status file.

The printed manifest entry is `enabled: false` with `archived: true`,
`archive_of`, `archived_until` and `archive_note`, and no platform aliases, so
the scheduler creates nothing for it, every runner route answers
`RUNNER_ARCHIVED`, and `--platform` keeps resolving to the live runner.
`GET /api/v1/canary/status` lists archives after the live runners with those
fields, and each live runner carries an `archives` list; `conductress canary
status` shows them. The runner-side half of the same decision is
`conductress sweep close-generation` (configuration.md, "Series generations").

Rollback: stop the service, restore the backup (`sqlite3 control.db ".restore
'<backup>'"`), remove the manifest entry, start. Observations ingested in
between are in the tasks' outcomes and can be re-ingested.

## Persistence and backup

SQLite uses WAL mode, foreign keys, explicit `BEGIN IMMEDIATE` claim transactions, and a five-second busy timeout. Back up with SQLite's online backup command rather than copying only the main file while WAL files are active:

```bash
sqlite3 /var/lib/conductress-control/control.db ".backup '/var/lib/conductress-control/control.backup.db'"
```

`audit_log` in SQLite is authoritative. `/var/log/conductress-control/audit.jsonl` is a best-effort append-only mirror for external inspection.

## Out of scope for this service

This service does not include:

- fleet-aware local CLI commands;
- runner mailbox polling or local queue import;
- status publication between jobs;
- daily canary scheduling;
- deployment to the data host;
- multiple same-platform scheduling.
