# Conductress Fleet Control Plane and Daily Drift Canary

Status: implementation-ready plan

Scope: the current four independent benchmark runners, one runner per platform. Multiple same-platform runners, cross-host equivalence testing, experiment affinity, and load balancing are explicitly deferred.

## 1. Decision summary

Build a small pull-based control plane on `data.conductress.rainsupreme.net` while preserving each benchmark host as an independent Conductress runner with its existing local queue.

- Humans and agents use one fleet-aware Conductress CLI.
- The data host owns the fleet registry, durable remote inboxes, task routing, cached runner status, and canary scheduling.
- Runners expose no inbound service. They contact the control plane only at quiescent task boundaries.
- A claimed remote task is copied atomically into the runner's existing local `benchmark_queue/`; normal local execution then takes over.
- Timed benchmark execution performs no control-plane, status-publish, result-publish, or remote mailbox network activity.
- Each runner executes one pinned daily canary benchmark. Canary history detects long-term environmental drift and validates the existing precision guide's untested thresholds.
- The schema records stable `runner_id` provenance from the beginning, but this phase maps each platform to exactly one runner.

This is a centralized control plane over federated data planes, not a centralized benchmark executor and not a peer-to-peer gossip system.

## 2. Current-state findings

The plan is based on the current implementation at `ab13c88`:

1. `TaskQueue` stores task JSON files in `benchmark_queue/` and the runner executes the lexically first file.
2. `TaskRunner.run()` already has a safe boundary after task completion/failure and before the next `get_next_task()` call.
3. Automated sweep coordinators currently enqueue local work whenever the local queue is empty. NIGHTLY work has absolute priority inside that local scheduler.
4. `DashboardPublisher` publishes sweep results after successful task completion, which is already outside the timed benchmark path.
5. A separate systemd timer runs `conductress status-export --publish ...` every 60 seconds. This invokes rsync even while a task is running and is incompatible with the desired zero-remote-network measurement invariant.
6. Status filenames are platform-derived (`arm.json`, `x86.json`, `intel.json`, and the Graviton 4 platform identifier), not runner-derived.
7. `servers.json` describes Valkey server targets used by one Conductress installation. It is not a fleet registry and must remain separate from fleet configuration.
8. `docs/benchmark-precision-guide.md` already identifies daily canary stability as the highest-priority unvalidated experiment and specifies an initial baseline of at least 10 runs.

## 3. Goals

### 3.1 Human and agent usability

Provide one discoverable interface:

```text
conductress fleet list [--json]
conductress fleet status [--json]
conductress fleet show RUNNER [--json]
conductress queue add ... --runner armbench
conductress queue add ... --platform graviton3
conductress remote-queue list [--runner RUNNER] [--json]
conductress canary status [--runner RUNNER] [--json]
```

Human output is a concise table. `--json` is a stable, versioned machine contract for agents and automation. Help text must explain selection, queue priority, status freshness, and the fact that remote work is imported only between local tasks.

### 3.2 Measurement isolation

During a task's measured interval:

- no control-plane requests;
- no status rsync;
- no dashboard/result rsync;
- no remote inbox polling;
- no lease renewal or heartbeat;
- no fleet discovery;
- no software update or repository polling beyond behavior already proven to happen outside tasks.

Loopback traffic used by the benchmark is expected. The invariant applies to non-loopback management traffic initiated by Conductress.

### 3.3 Operational resilience

- An active benchmark completes if the control plane or network fails.
- Claimed work survives runner or control-plane restart.
- Completed results remain local until publication succeeds.
- Duplicate delivery is harmless and detected by `task_id`.
- The existing direct local queue remains available as a recovery path.
- Control-plane rollout and rollback do not invalidate existing result history.

### 3.4 Drift monitoring

Run one canonical benchmark per runner per UTC day, build a robust historical baseline, and surface warning/alarm states without interrupting ordinary tasks.

## 4. Non-goals for this phase

The following work is deferred until two or more runners share a platform:

- least-loaded selection among same-platform runners;
- experiment-group affinity and sticky retries across equivalent hosts;
- cross-host equivalence studies or correction factors;
- pooling repetitions from multiple physical runners;
- automated failover of an assigned task to another same-platform runner;
- cross-host canary comparison and common-mode diagnosis;
- fleet-wide capacity optimization.

The data model must not prevent these features later. In this phase, `--platform` resolves only when exactly one enabled runner matches; ambiguity is an explicit error rather than an implicit scheduling decision.

## 5. Architecture

### 5.1 Components

#### Control service on the data host

A Conductress control service runs on `data.conductress.rainsupreme.net`, bound to localhost behind the host's authenticated HTTPS reverse proxy.

Responsibilities:

- load a private fleet deployment manifest;
- accept validated task submissions;
- resolve an explicit runner or unique platform;
- persist task and delivery state in SQLite using WAL mode;
- expose durable per-runner inboxes through a claim API;
- store pushed status and task outcomes;
- create due daily canary tasks;
- calculate canary baseline and drift state;
- expose fleet, queue, and canary data to the CLI/dashboard;
- keep an append-only audit record of submission and state transitions.

The service does not execute benchmarks, open connections to runners, or mutate runner-local queues.

#### Local CLI

The normal `conductress` command runs on a laptop, benchdev, or any authorized host. Fleet commands call the control service. Existing local queue commands remain local unless `--runner` or `--platform` is supplied.

#### Runner control client

Each benchmark host receives a small `FleetClient` configured with:

- stable `runner_id`;
- control-plane URL;
- per-runner authentication token;
- request timeout;
- local durable delivery journal.

The client is called synchronously only at runner startup, after task completion/failure, and while idle. It is never invoked from a task runner's `run()` method or from a timed repetition.

#### Existing local task queue

The existing `TaskQueue` remains the execution source of truth. The remote mailbox does not replace it. A claimed task is validated and atomically installed as a normal local task file before execution.

### 5.2 Data-host paths

Recommended deployment paths:

```text
/etc/conductress-control/fleet.json       private deployment manifest
/etc/conductress-control/tokens.json      hashed operator/runner credentials
/var/lib/conductress-control/control.db   SQLite state
/var/log/conductress-control/audit.jsonl  append-only audit events
```

Repository-provided examples and JSON schemas live in the Conductress package. Actual hostnames, tokens, and deployment secrets do not enter the public repository.

### 5.3 Fleet manifest

Minimum runner record:

```json
{
  "runner_id": "armbench",
  "display_name": "Graviton 3",
  "platform": "arm64/c7g.metal/graviton3",
  "platform_aliases": ["graviton3", "arm64"],
  "enabled": true,
  "canary_profile": "throughput-get-v1",
  "status_ttl_seconds": 900
}
```

Do not include SSH keys. The control plane never SSHes to runners.

Future-compatible fields may include a pool or capability list, but no multi-runner scheduling behavior is implemented now.

### 5.4 Control-plane task envelope

Keep fleet metadata outside existing task dataclasses so every task subtype does not need immediate constructor changes:

```json
{
  "schema_version": 1,
  "task_id": "2026.08.29_00.00.00.000000",
  "runner_id": "armbench",
  "task_class": "manual",
  "priority": 100,
  "submitted_at": "2026-08-29T00:00:00Z",
  "submitted_by": "rain",
  "canary_id": null,
  "task": {
    "task_type": "PerfTaskData",
    "source": "valkey-rainfall",
    "specifier": "..."
  }
}
```

`task` is the existing serialized `BaseTaskData`. The envelope is persisted centrally and in a local sidecar/journal for provenance.

### 5.5 Task states

```text
queued -> claimed -> accepted -> running -> completed
                              \-> failed
claimed -> queued             lease expired before acceptance
accepted/running -> unknown   runner disappeared; never auto-reassign in phase 1
```

Rules:

- Claim is an atomic SQLite transaction.
- A short claim lease covers only transfer into the local queue.
- The runner validates and atomically persists the task before acknowledging `accepted`.
- No lease renewal occurs during benchmark execution.
- Once accepted, the control plane never automatically assigns the task elsewhere.
- Runner recovery reconciles accepted task IDs against its local queue, active task directory, delivery journal, and result log.
- Repeated delivery of an already persisted or completed `task_id` returns an idempotent acceptance/completion response.

### 5.6 Runner boundary sequence

At startup and after every task outcome:

1. Finish local cleanup and persist result/failure.
2. Publish pending results and a boundary status update.
3. Reconcile any previously accepted remote task.
4. Claim at most one eligible remote task.
5. Validate the payload using the existing task registry.
6. Write task JSON to a temporary file in `benchmark_queue/`.
7. Flush the file, atomically rename it to its canonical filename, and persist the delivery journal.
8. Acknowledge acceptance to the control plane.
9. If no manual/canary remote task was imported, allow the existing sweep coordinator to enqueue work.
10. Publish `starting` status with the selected task and expected duration.
11. Close all control-plane/publication connections.
12. Wait a configurable management-settle interval.
13. Execute the task through the existing local queue.

While idle, the runner may poll the control plane at a low rate. Once a task is selected, polling stops before the settle interval begins.

### 5.7 Queue policy for the current single-runner-per-platform fleet

At each boundary:

1. Existing explicitly queued local/manual task.
2. Remote manual task.
3. Due daily canary task.
4. Existing NIGHTLY sweep task.
5. Highest-urgency sweep/backfill task.

This fixes the present failure mode where continuously generated sweep work can starve an urgent manual request. Canary is important but does not preempt a human-requested task. It runs at the next available boundary within its daily window.

Priority is represented explicitly rather than inferred from task filename timestamp. The migration must preserve FIFO ordering within each priority class.

## 6. Status and publication behavior

### 6.1 Replace networked 60-second status publication

The current `conductress-status.timer` performs rsync every 60 seconds, including during benchmarks. Replace it as follows:

- During a task, local status files may continue to update; no remote publication occurs.
- Immediately before execution, publish a `starting` snapshot with task ID, start time, expected duration, and runner provenance.
- Immediately after completion/failure, publish the final outcome and refreshed queue state.
- While genuinely idle, publish periodic idle status from the runner's boundary loop.
- Disable and remove the networked systemd status timer during migration.

The dashboard therefore shows the last boundary status during a long task rather than live progress. That is an intentional tradeoff for measurement isolation. It must display status age and expected completion rather than pretending the snapshot is live.

### 6.2 Runner-derived filenames

Add canonical runner-specific status paths:

```text
status/runners/armbench.json
status/runners/g4bench.json
status/runners/bench.json
status/runners/intelbench.json
```

During migration, continue publishing the current platform aliases so the existing dashboard remains functional. Remove aliases only after the dashboard reads runner-specific status.

### 6.3 Result provenance

Every new result record must include or be joinable to:

- `runner_id`;
- canonical platform label;
- control-plane task class;
- canary ID/profile version when applicable;
- Conductress commit;
- kernel release;
- stable instance identity/fingerprint where available;
- benchmark binary/build commit;
- task submission and execution timestamps.

Use a runner context/sidecar merged by `FileProtocol.write_results()` and the latency result writer. Avoid duplicating provenance assembly in every task implementation.

## 7. Daily drift canary

### 7.1 Initial scope

Run exactly one canary benchmark per enabled runner per UTC day. Do not rotate workloads in the first implementation; a stable series is more valuable than broad but sparse coverage.

Define `throughput-get-v1` from the existing precision guide and a short pilot:

- pinned Valkey source commit, never a moving branch;
- pinned build arguments;
- GET workload;
- fixed 512-byte value and 3-million-key dataset;
- pipeline depth 10;
- platform's existing stabilized IO-thread setting;
- fixed clients, threads, warmup, duration, repetitions, and dataset seed;
- no profiling;
- immutable profile version recorded in every result.

The exact pinned commit and final duration/repetition count are selected in the phase-5 pilot, then frozen as `throughput-get-v1`. Changing any parameter creates `v2`; historical series are never silently mixed.

### 7.2 Scheduling

- The control service creates at most one task per `(runner_id, canary_profile, UTC date)` using a database uniqueness constraint.
- Canary becomes due in a configurable UTC window.
- It is imported only at a normal task boundary.
- Manual work has higher priority.
- If it cannot run before the freshness deadline, mark the date `missed`; do not run multiple catch-up canaries.
- Canary does not interrupt or cancel work.

### 7.3 Baseline and drift detection

The first 14 successful daily runs are observation-only. A baseline requires at least 10 accepted samples, matching the precision guide.

Initial analysis:

- rolling 28-sample median;
- median absolute deviation (MAD);
- percent change from the frozen baseline median;
- warning when the platform-specific candidate threshold is breached twice consecutively;
- immediate alarm at twice that threshold;
- no automatic baseline update while warning/alarm is active;
- annotations for reboot, kernel change, Conductress revision change, or instance replacement.

The guide's current unvalidated candidate thresholds are starting hypotheses, not truth:

- AMD: 0.5%;
- Graviton: 2%;
- Intel: 5%.

After the first 28 days, produce a threshold-calibration report using observed within-host daily variation and revise the guide. Alerting remains observation-only until Rain accepts those calibrated thresholds.

### 7.4 Pinned source versus pinned binary

Phase 1 uses a rebuilt pinned source commit because it detects end-to-end drift in hardware, OS, compiler, dependencies, and Conductress. Record build provenance so toolchain changes can be diagnosed.

A second pinned-binary canary is deferred because the requirement is one benchmark per day. Add it only if canary evidence shows a need to distinguish machine drift from toolchain drift.

### 7.5 Canary dashboard

Show, per runner:

- latest score and percent from baseline;
- warning/alarm/insufficient-data state;
- run and missed-day history;
- median and MAD band;
- status annotations;
- profile version and pinned commit;
- direct links to underlying result records.

Never merge canary series across platforms or profile versions.

## 8. Security model

- Serve the API over HTTPS.
- Give each runner a distinct revocable token limited to status push, claim for its own runner ID, acceptance, and outcome reporting.
- Give human/agent clients separate operator tokens for fleet reads and task submission.
- Store only token hashes on the data host.
- Do not accept arbitrary runner IDs from token-authenticated runners; identity comes from the token.
- Validate task envelopes and existing task payloads before persistence and again on the runner.
- Apply body-size limits and reject unknown schema versions/fields where safe.
- Audit actor, request ID, task ID, prior state, new state, and timestamp.
- Do not expose repository credentials, SSH keys, or runner filesystem paths through fleet APIs.

## 9. Implementation phases

### Phase 0: contracts and regression fixtures

Deliverables:

- JSON schemas for fleet manifest, API task envelope, runner status, and task outcome.
- Golden fixtures for every current task subtype.
- Explicit priority constants and state-transition table.
- Tests showing current task JSON round-trips unchanged.
- Documented timed-window network invariant.

Acceptance:

- Existing unit suite remains green.
- Invalid/unknown task payloads fail closed.
- Schemas are versioned from the first commit.

### Phase 1: runner identity and provenance, no remote behavior

Deliverables:

- Stable local `runner_id` configuration.
- Runner/platform fingerprint collector.
- Shared result-provenance injection for normal and latency results.
- Runner-specific status export alongside legacy aliases.
- `conductress runner-info [--json]`.

Acceptance:

- Existing dashboard files are byte-compatible except for additive metadata.
- All result writers include identical provenance keys.
- No new network calls.

### Phase 2: data-host control service

Deliverables:

- Optional control-plane dependency group, pinned to validated versions.
- SQLite schema and migrations.
- Fleet registry loader and validation.
- Task submit/list/show/cancel-before-claim APIs.
- Runner status, claim, accept, complete, and fail APIs.
- Idempotency and lease recovery.
- Audit log.
- systemd unit and reverse-proxy deployment documentation.

Acceptance:

- Concurrent claim test proves one winner.
- Restart tests preserve queued, claimed, and accepted tasks.
- Duplicate submit/accept/complete requests are idempotent.
- An accepted task is never automatically reassigned.
- API tests use a temporary SQLite database and require no live hosts.

### Phase 3: fleet-aware CLI

Deliverables:

- `fleet list/status/show` commands with human and JSON output.
- Remote queue submission/list/cancel commands.
- `--runner` and unique `--platform` selection.
- Clear status age, queue class, canary state, and routing explanation.
- Machine-readable errors and stable exit codes.

Acceptance:

- Agents can discover all four platforms without SSH/config archaeology.
- Ambiguous or unavailable platform selection fails with a useful explanation.
- Existing local queue CLI behavior is unchanged without remote flags.

### Phase 4: runner mailbox integration and measurement isolation

Deliverables:

- Boundary-only `FleetClient`.
- Atomic inbox-to-local-queue import.
- Durable delivery journal and restart reconciliation.
- Explicit queue classes/priority ordering.
- Boundary status/result publication.
- Management-settle interval.
- migration that disables the 60-second networked status timer.

Acceptance:

- Control-plane outage does not interrupt an active task.
- A network failure at every claim/accept step recovers without task loss.
- Duplicate delivery never executes a completed task twice.
- Remote manual work runs before newly generated sweep work at the next boundary.
- Data-host access logs contain no runner requests between published `starting` and completion timestamps.
- No status/result rsync process starts during that interval.
- Local queue submission and offline execution still work.

### Phase 5: daily canary and drift analysis

Deliverables:

- Versioned canary profile configuration.
- Daily task creation with uniqueness and freshness rules.
- Canary result classifier.
- Observation-only baseline/MAD analysis.
- Missed-run handling and environmental annotations.
- Canary CLI status.

Acceptance:

- Exactly zero or one canary is created per runner/day.
- Restarting the control service cannot duplicate a canary.
- Manual tasks preempt a due canary; canary preempts sweep backfill.
- Baseline is not emitted before 10 accepted samples.
- Profile changes start a distinct series.
- The first 14 days generate no actionable alert.

### Phase 6: dashboard integration and calibrated alarms

Deliverables:

- Fleet runner cards backed by runner-specific status.
- Remote inbox depth and status age.
- Canary trend and annotations.
- Warning/alarm presentation.
- 28-day threshold-calibration report and update to `benchmark-precision-guide.md`.

Acceptance:

- Existing performance series remain unchanged.
- Stale status is visibly stale rather than displayed as live.
- Alarm links identify runner, canary profile, result, and relevant environment changes.
- Alert thresholds become active only after explicit acceptance.

### Phase 7: staged fleet deployment

1. Deploy control service with read-only fleet/status APIs.
2. Deploy Phase 1 provenance to all runners.
3. Enable mailbox shadow mode on `armbench`: query/validate but do not import.
4. Run synthetic claim/accept failure tests while no benchmark is active.
5. Enable live mailbox import on `armbench`.
6. Verify one manually submitted task and one canary end to end.
7. Verify the no-management-network interval.
8. Observe for at least three successful task boundaries.
9. Roll out sequentially to `g4bench`, `bench`, and `intelbench`.
10. Disable each host's networked status timer only when its boundary publisher is confirmed.
11. Begin the 14-day canary observation period.

Do not deploy simultaneously to all runners.

## 10. Test strategy

### Unit tests

- schema and manifest validation;
- platform alias resolution;
- priority ordering;
- state-transition legality;
- claim transaction exclusivity;
- lease expiry before acceptance;
- accepted-task no-reassignment rule;
- duplicate task idempotency;
- atomic local import;
- delivery-journal recovery;
- canary uniqueness and freshness;
- median/MAD classification;
- provenance injection into all result formats;
- status alias compatibility.

### Integration tests

- real HTTP service with temporary SQLite database;
- fake runner completing full claim/import/accept/outcome cycle;
- failures before and after each durable write;
- service restart at every task state;
- runner restart with queued, active, completed, and unknown tasks;
- local manual, remote manual, canary, nightly, and sweep ordering;
- control-plane outage during a synthetic benchmark;
- dashboard/status publication only at boundaries.

### Production validation

- compare control-plane access logs against task start/end intervals;
- verify no periodic status rsync remains enabled;
- record physical-interface connection counters before and after a timed task;
- run a null A/A benchmark before and after mailbox enablement;
- ensure result distributions and CV do not move materially;
- exercise rollback while retaining accepted local work.

## 11. Rollback and failure handling

- Feature flags independently control remote submission, remote claim, boundary publication, and canary creation.
- Disabling remote claim leaves local queue/sweeps operational.
- Any accepted task already copied locally remains executable after control-plane disablement.
- Keep legacy platform status aliases until the new dashboard path is proven.
- Preserve the old status unit file for one release, but keep it disabled; rollback may re-enable it only after acknowledging that it restores during-run network activity.
- Database migrations are forward-only with pre-migration backup and a documented export command.
- Control-plane unavailability is non-fatal to active and locally queued work.

## 12. Deferred same-platform-host work

When a second runner is added for a platform, create a follow-up design covering:

- host-equivalence experiment and predeclared equivalence bounds;
- experiment-group affinity;
- sticky retries;
- least-loaded/pool scheduling;
- cross-host canary diagnostics;
- dashboard host overlays;
- whether results may be pooled for any workload class.

Until that work is approved, adding a second enabled runner with the same platform alias causes `--platform` routing to fail and require explicit `--runner`.

## 13. Recommended implementation commits

Keep review units small and deployable:

1. `docs: define fleet control-plane contracts and invariants`
2. `feat: add stable runner identity and result provenance`
3. `feat: add runner-specific status export`
4. `feat: add fleet control service persistence and API`
5. `feat: add fleet discovery and remote queue CLI`
6. `feat: import remote tasks at runner boundaries`
7. `feat: move network publication to task boundaries`
8. `feat: add daily drift canary scheduling and analysis`
9. `feat: expose fleet and canary dashboard data`
10. `docs: publish calibrated canary thresholds after observation`

Each behavior-changing commit must include focused tests. Deployment configuration follows only after the corresponding unit and integration tests pass.

## 14. Definition of done

This phase is complete when:

- humans and agents can discover the four-runner fleet through Conductress;
- a task submitted by runner or platform reaches the intended local queue without inbound runner access;
- remote work cannot be starved indefinitely by generated sweep work;
- no Conductress management traffic occurs during timed benchmark execution;
- status and results publish successfully at boundaries;
- all new results identify their runner and environment;
- each runner produces at most one pinned daily canary result;
- 14 observation days complete without false actionable alarms;
- a 28-day report calibrates drift thresholds;
- existing local queues, sweeps, results, and dashboard series remain operational;
- multiple same-platform scheduling and result comparison remain explicitly deferred rather than accidentally implied.
