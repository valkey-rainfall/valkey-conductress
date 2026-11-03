# Runner fleet mailbox

The runner fleet mailbox connects an independent Conductress runner to its central per-runner inbox. Runners push updates between jobs; during a job no data is transferred and no other work is done, so task execution stays local and independent.

## Modes

```text
off     existing local-only behavior; no control-plane contact
shadow  publish boundary health/status, but never claim a task
live    claim, atomically import, accept, execute, and report one remote task
```

Enable explicitly:

```bash
conductress run --sweep --publish ec2-user@data.conductress.rainsupreme.net:/var/www/data \
  --fleet-mode shadow --management-settle 2
```

No runner changes behavior merely because this code is installed.

## Runner credentials

Store each runner's distinct token in an owner-only file:

```bash
install -d -m 0700 ~/.config/conductress
install -m 0600 /dev/stdin ~/.config/conductress/runner.token
```

Overrides:

```text
CONDUCTRESS_RUNNER_TOKEN
CONDUCTRESS_RUNNER_TOKEN_FILE
CONDUCTRESS_CONTROL_URL
CONDUCTRESS_CONTROL_TIMEOUT
CONDUCTRESS_CONTROL_CA_BUNDLE
```

A new short-lived `FleetClient` is created at each boundary, so the runner does not retain the token in a long-lived client object.

## Boundary sequence

For a live runner:

1. Finish the current task and persist its result or failure.
2. Stage a terminal outcome in the durable delivery journal.
3. Remove the completed task from the local queue.
4. Report the outcome; retain it for retry if the control service is unavailable.
5. Prefer any existing local task.
6. If local work is empty, claim at most one remote task.
7. Persist the claim, validate its existing task schema, and atomically import it with fsync plus rename.
8. Acknowledge acceptance. An imported task is never executed before acceptance succeeds.
9. Publish read-only boundary status to the control service and static dashboard status path.
10. Close management requests and wait the configured settle interval.
11. Execute the task without fleet/status management calls.

The control plane never automatically reassigns an accepted task. The runner does not prefetch another remote task.

## Recovery

The owner-only `fleet_delivery.json` journal records one active remote delivery:

```text
claimed -> imported -> accepted -> outcome_pending -> cleared
```

At restart:

- claimed/imported work is atomically restored and acceptance is retried;
- accepted work remains the next task even if newer local files exist;
- an existing result/failure is detected and reported instead of re-executed;
- pending outcomes are replayed idempotently;
- missing accepted work without a result is treated as a blocking recovery error.

### Outcome size and rejected outcomes

While the journal holds an undelivered outcome the runner claims no new remote task, so an outcome the control service can never accept would block the runner for good. The mailbox therefore measures each outcome before the first send and trims it to `max_outcome_bytes` (default 3 MiB, env `CONDUCTRESS_MAX_OUTCOME_BYTES`), and it never retries the same bytes after a permanent rejection (HTTP 413, 400, 422). It steps down instead: full outcome, then a reduced outcome (bulk result keys dropped), then a `state: failed` report whose error starts with `outcome_rejected:` and names the local file that keeps the full result, then (if even that is refused) the journal is cleared. Each step is persisted in the journal entry (`rejections`) before it is sent, so a restart resumes at the same step. Transient errors keep today's retry-at-next-boundary behaviour. The full row is never removed from `results/output.jsonl`. Details and the shape of a downgraded record: `control-service.md`, "Outcome size".

## Status publication

Runners publish status only between jobs and while idle; there is no periodic
status timer. The `status-export` CLI subcommand remains for a manual one-off
export.

Bootstrap disables and removes any periodic status-timer unit it finds
(`retire_status_timer`), so a re-bootstrapped host does not acquire one. A
runner that reports `host` with a `python3` main thread on a random core is the
signature of a periodic status timer running.

The runner service should still carry:

```text
CONDUCTRESS_BOUNDARY_STATUS_ONLY=1
```

The dashboard field `measurement_isolation.status_timer_migration_required` reflects that variable.

Rollback:

1. Set `--fleet-mode off` and restart the runner.
2. Leave any accepted journal entry intact until its outcome is reconciled; do not delete it manually.

## Read-only monitoring fields

Each static host status document gains additive fields:

- fleet mode and control reachability;
- last contact latency and error;
- last poll result;
- accepted task and active journal stage;
- pending outcome count;
- the client outcome budget (`max_outcome_bytes`) and the last outcome downgrade (`last_outcome_downgrade`: task, stage `trimmed`/`reduced`/`failed`/`dropped`, reason, what was dropped, sizes, time);
- imported task count and latest imported task;
- boundary state/task/timestamp;
- boundary publisher status and status-timer migration warning.

These fields only report state. They expose no queue, cancel, or execution controls.

## Staged deployment

1. Deploy the control service and CLI.
2. Configure runner identity and token.
3. Enable `shadow` on one runner first; verify status/authentication over several boundaries.
4. Enable `live`; submit one harmless task and verify claim/import/accept/outcome.
5. Verify data-host access logs show no runner requests between starting and completion boundaries.
6. Confirm bootstrap retired the periodic status timer (`systemctl is-enabled conductress-status.timer` reports not-found) and set `CONDUCTRESS_BOUNDARY_STATUS_ONLY=1`.
7. Observe at least three clean boundaries.
8. Repeat sequentially for each remaining runner.

## Versioned sweep epoch toggle

V2 GET and 80:20 mixed sweeps are additive and disabled by default. Enable them without modifying the source checkout:

```ini
# /etc/systemd/system/conductress.service.d/v2-sweeps.conf
[Service]
Environment=CONDUCTRESS_SWEEP_V2_ENABLED=1
```

Then reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart conductress.service
```

Accepted true values are `1`, `true`, `yes`, and `on`; accepted false values are `0`, `false`, `no`, and `off` (case-insensitive). Any other value fails startup rather than silently selecting an epoch.

Rollback is immediate and does not alter either dataset: remove the drop-in (or set the value to `0`), reload systemd, and restart. V1 coordinators remain active while v2 is enabled; v2 uses separate planner state and epoch-qualified dashboard files.
