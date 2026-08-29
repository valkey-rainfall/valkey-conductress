# Runner fleet mailbox

Step 4 connects an independent Conductress runner to its central per-runner inbox. Management traffic occurs only between tasks; task execution remains local and independent.

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

## Boundary-only status migration

The existing `conductress-status.timer` performs network rsync every 60 seconds and must remain active during initial shadow verification. After boundary status appears reliably on the dashboard:

```bash
sudo systemctl disable --now conductress-status.timer
```

Then set this environment variable in the runner service:

```text
CONDUCTRESS_BOUNDARY_STATUS_ONLY=1
```

The dashboard field `measurement_isolation.status_timer_migration_required` remains true until that explicit deployment step is complete. The code does not disable systemd units automatically.

Rollback:

1. Set `--fleet-mode off` and restart the runner.
2. Re-enable `conductress-status.timer` if boundary-only publication had replaced it.
3. Leave any accepted journal entry intact until its outcome is reconciled; do not delete it manually.

## Read-only monitoring fields

Each static host status document gains additive fields:

- fleet mode and control reachability;
- last contact latency and error;
- last poll result;
- accepted task and active journal stage;
- pending outcome count;
- imported task count and latest imported task;
- boundary state/task/timestamp;
- boundary publisher status and status-timer migration warning.

These fields only report state. They expose no queue, cancel, or execution controls.

## Staged deployment

1. Deploy the control service and CLI.
2. Configure runner identity and token.
3. Enable `shadow` on `armbench`; verify status/authentication over several boundaries.
4. Enable `live`; submit one harmless task and verify claim/import/accept/outcome.
5. Verify data-host access logs show no runner requests between starting and completion boundaries.
6. Disable the periodic status timer and set `CONDUCTRESS_BOUNDARY_STATUS_ONLY=1`.
7. Observe at least three clean boundaries.
8. Repeat sequentially for `g4bench`, `bench`, then `intelbench`.
