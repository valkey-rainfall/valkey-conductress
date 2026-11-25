# Multi-agent queue: identity, accounting, fair share, and sweep share

Status: design note. Rollout steps 1 (server-set identity and token scope) and 2 (own-only cancel rules) are implemented; the other steps are not. It describes the changes in the order they will be built, names the files they touch, and says how each one is tested.

## 1. Purpose

Several agents and people now submit work to the same runners at the same time. The control service already authenticates them and records who submitted a task. It does not yet stop one submitter from using the whole queue, it does not report who used how much runner time, and the sweep only runs when nothing else is waiting. This note sets out these decisions:

1. Every submitter has its own token, and the server decides who submitted a task.
2. Nobody interferes with a running task, except the operator.
3. Per-agent quotas are off by default: the service only logs and tracks usage. Every task is accounted, and the next task is chosen so that recent runner time is shared fairly. A quota is an optional limit that an operator can turn on for one login.
4. The sweep gets a reserved share of a runner while it has real work to do (bisection need), and runs on spare capacity otherwise.
5. Agents reach the queue through an MCP server that wraps the CLI.

Non-goals: new task types, changes to what a measurement does, changes to the published sweep data format, and any change to how a runner executes a task.

## 2. What exists today

The design builds on this code. File names are relative to `src/conductress/`.

| Area | What it does now | Where |
|---|---|---|
| Tokens | `tokens.json` maps a SHA-256 token hash to a role (`operator`, `runner`, `user`). A `user` token names a `login`. | `control/auth.py` |
| Directory | `users.toml` has humans (`github`, `role` owner/collaborator/approver, `quota_runner_minutes_per_day`, `sources`) and agents (a `sponsor`; everything else is inherited). | `control/users.py` |
| Submit | `ControlService.submit_task` validates the envelope, resolves the submitter from the token login (or from the envelope), runs the provenance gate, and stores `submitted_by` and `submitter_login`. `submitted_by` is still the value the client wrote into the envelope (`task_envelope.build_task_envelope` defaults it to the local OS user). | `control/service.py`, `task_envelope.py` |
| Hold | `_approval_decision` puts a task into `pending-approval` for an unknown login or when `_over_quota` says the account passed its daily runner minutes. Canary and sweep classes are never held. | `control/service.py` |
| Cancel | `cancel_task` cancels `queued` and `pending-approval` tasks outright. A `claimed` or `accepted` task becomes `cancel-requested`, and the runner stops at the next rep or cell boundary. A non-owner can cancel only own tasks; an `owner` directory role or an operator token can cancel any task. | `control/service.py`, `control/app.py` (`_is_owner`), `runner_mailbox.py` (`cancel_requested`) |
| Claim | A runner pulls its own tasks. `claim_task` takes the next `queued` task for that runner, ordered by class (`manual`, then `canary`, then `sweep`), then `priority` (higher first), then `submitted_at`. A runner owns at most one `accepted` task. The `priority` value comes from the client (`queue add --priority`, default 100). | `control/service.py`, `cli.py` |
| Runner loop | `TaskRunner._choose_next`: a recovered remote task first, then the local queue, then a remote claim, and only then `_schedule_next`, which asks the sweep coordinators to queue a task. So a sweep task is queued only when no remote task can be claimed. | `task_runner.py` |
| Sweep need | Each series has a `SweepPlanner`. `get_unresolved_segments()` returns the adjacent pairs of points whose relative delta is at least `SweepState.threshold` (default 0.02), that pass a Welch t-test approximation (`Segment.is_significant`), and that still have commits between them. `_check_bisection` measures the midpoint of the largest one. Task priorities are `NIGHTLY`, `LANDMARK`, `BISECTION`, `BACKFILL`, `CONFIRM`. | `sweep/planner.py`, `sweep/coordinator.py` |
| Sweep controls | `conductress sweep pause|focus|resume|reset-series|close-generation` act on files on the runner host (`sweep_config.json`, the series state). They are not reachable through the control service. | `__main__.py`, `sweep_config.py`, `sweep/reset_series.py`, `sweep/generations.py` |
| Durations | `estimate_task_duration_seconds` gives an expected duration per task, with a calibration factor from observed durations. A runner writes `observed_duration_sec` and `expected_duration_sec` on every result. | `duration_estimator.py`, `file_protocol.py` |
| Results | Completed tasks are stored in the database and published as static JSON. Dashboard data under the published directories is public. | `control/service.py` (`_publish_outcome`), `public_fields.py` |
| Remote client | `FleetClient.from_env` reads the token from `CONDUCTRESS_OPERATOR_TOKEN` or `~/.config/conductress/operator.token`. A user token is passed in the same variable today. | `fleet_client.py` |

## 3. Where the decisions differ from today

| Decision | Today | Change |
|---|---|---|
| The server decides `submitted_by` | The client sets `submitted_by` in the envelope. The token login is used for quota, but the stored `submitted_by` is client text. | Overwrite `submitted_by` and the `submitter` block from the token. |
| Roles: submit, read, operator | Token roles are `operator`, `runner`, `user`. A `user` can submit, list own tasks, and cancel. Directory roles `owner` and `approver` add more. | A `user` token gets a `scope` of `submit` or `read`. Cancel-any, priority overrides, and runner or sweep control are operator-only. |
| Only the operator cancels a running task | An owner or the task's own submitter can set `cancel-requested` on a running task. | Own-and-queued only for non-operators. |
| Quotas off by default | A daily runner-minute quota (`quota_runner_minutes_per_day`) holds a task in `pending-approval` when the account passed it. | The hold stays as an optional limit, but it applies only to a login that has an explicit quota. No quota set means no limit. Usage is always logged and tracked, whether or not a quota is set. |
| Accounting | No per-task usage record. Runner seconds can only be guessed from `accepted_at` and `completed_at`. Local sweep and canary time is not in the database. | New usage table and report. |
| Fair share | FIFO by `submitted_at`. One agent with 500 queued tasks blocks everyone behind it. | Pick the agent with the least recent runner time. |
| Sweep share | The sweep runs only when no remote task can be claimed. A long queue starves the sweep, including the nightly tip and bisection. | Reserved share while there is need. |
| Agent access | CLI only. | MCP server. |

## 4. Design

### 4.1 Identity and roles

One token per agent, or per team. A team is one login in `users.toml` that the team members share (an `agent` entry with a `sponsor`). Accounting is per login, so a team that wants separate numbers needs separate logins.

The operator issues tokens with the existing `conductress-control hash-token` flow and adds a record to `tokens.json`:

```json
{"token_hash": "<sha256>", "role": "user", "label": "example-agent", "login": "example-agent", "scope": "submit"}
```

- `scope` is new. It is `submit` (the default for a `user` token, so existing tokens keep working) or `read`. It is rejected on `operator` and `runner` tokens.
- The login must exist in `users.toml`. The service refuses to start if a token names an unknown login. This removes the "unknown login goes to `pending-approval`" case for token callers; the approval flow stays for envelopes that arrive on an operator token naming another login.
- `submitted_by` is set by the server on every submission:
  - a `user` token: the token's login;
  - an operator token: `operator:<label>`, where the label comes from `tokens.json`.
  The client value is ignored. If it differs from the server value, the server stores the server value and records the client value in the audit entry (`detail.client_submitted_by`). The `submitter` block (`login`, `kind`, `sponsor`) is rebuilt from the directory in the same way.
- `priority` from a non-operator token is clamped to the default (100). A different value is accepted only on an operator token. The audit entry records the clamp.

| Role | Token | May do |
|---|---|---|
| `submit` | `role: user`, `scope: submit` | Submit tasks and batches. Read all results and published data. Read own tasks and own usage. Cancel own tasks that are still `queued` (or `pending-approval`). |
| `read` | `role: user`, `scope: read` | Read results, task and batch status, fleet status, and published dashboard data. Nothing that changes state. |
| `operator` | `role: operator` | Everything: cancel any task (including a running one), set priority, deploy, read all usage, and the runner-side controls below. |

The directory roles `owner` and `approver` keep their current meaning for provenance bypass and approval. They no longer grant cancel-any. That power moves to the operator token only.

Runner tokens are unchanged.

Files to change: `control/auth.py` (scope), `control/users.py` (startup check that token logins exist; no field change), `control/app.py` (`_require_submitter`, `_is_owner`, the submit and cancel handlers), `control/service.py` (`submit_task`, `_task_from_row`), `fleet_client.py` (a `from_user_env` that reads `CONDUCTRESS_USER_TOKEN` and `CONDUCTRESS_USER_TOKEN_FILE`; the old operator variable still works), `cli.py` (`_remote_client`).

### 4.2 No interference

- A running task (`claimed` or `accepted`) is cancelled only by an operator token. The `cancel-requested` path stays, but only the operator can trigger it.
- A `submit` token cancels only its own tasks, and only while they are `queued` or `pending-approval`. A batch selector cancels the caller's own queued tasks in that batch and skips the rest, as it does now for non-owners. The response lists what was skipped and why (`not_yours`, `already_running`).
- Sweep and runner controls are operator-only because they never go through the control service: `conductress sweep pause|focus|resume|reset-series|close-generation` run on the runner host and write local files. The MCP server (4.6) has no tool for them, and no route will be added to the control service.
- Deploys stay at task boundaries. The deploy procedure waits until the runner is idle between tasks and aborts if it never sees idle; it never interrupts a task. This note does not change that procedure.
- No route lets a `submit` token change a task that belongs to someone else, set a priority, or touch a runner or its status.

Files to change: `control/service.py` (`cancel_task`), `control/app.py` (the delete handler passes the role), `docs/control-service.md` (the cancel paragraph and the lifecycle table).

### 4.3 Usage accounting

Every task gets one usage row, written in the same transaction as the state change that ends it. A new database schema version adds the table:

```text
task_usage(
  task_id PRIMARY KEY, submitted_by, batch_id, note, runner_id, platform,
  task_class,                 -- manual | canary | sweep | local
  queued_at, started_at, ended_at,   -- started_at = accepted time
  expected_seconds, runner_seconds,
  outcome                     -- completed | failed | cancelled-queued | cancelled-running
)
```

- `runner_seconds` is `ended_at - started_at` for tasks that ran, and 0 for a task cancelled while queued. `expected_seconds` comes from `estimate_task_duration_seconds` at submit time, so the report can show estimate against actual.
- `platform` comes from the fleet manifest entry of `runner_id`.
- `note` is copied from the task. It is private data (see 4.3.2).
- A migration fills the table from the existing `tasks` rows, using `accepted_at` and `completed_at`.
- Canary tasks are created by the control service and claimed like any other, so they already have a row. Sweep tasks, and tasks that someone queues by hand on the runner host, run from the runner's local queue, so the control service does not see them. After each local task the runner posts one record to a new route, `POST /api/v1/runners/{runner_id}/usage` (runner token). The record has the task id, class, started and ended times, seconds, and outcome. The service stores it with `task_class` `sweep` or `local`, and `submitted_by` set to `sweep` or `local`. The post is best effort and uses the runner's existing delivery journal pattern, so a lost post never blocks a task. A task that is posted twice is stored once (the primary key).

#### 4.3.1 Report command

```text
conductress usage --by agent|batch|platform --since <window> [--runner ID] [--json]
```

`<window>` is a duration such as `24h` or `7d`, or a date. Columns: task count, runner seconds, share of the total, mean wait (`started_at - queued_at`), failed count, and cancelled count. `--by platform` includes the sweep and canary rows, so it shows how a runner's time splits between agents, sweep, and canaries. The command calls a new route, `GET /api/v1/usage?by=&since=&runner_id=`.

- An operator token sees every row.
- A `submit` or `read` token sees its own rows in full and every other agent's time folded into one row named `others`. It never sees another agent's login, batch id, or note.

#### 4.3.2 Exported JSON for the dashboard

The service writes `usage/summary.json` next to the canary files, after each task end, with the same allowlist method as `public_fields.py`. It holds only data that is safe to publish: per platform and per day, the runner seconds by class (`manual`, `canary`, `sweep`, `local`), the task counts, the median queue wait, and the current queue depth. It has no login, batch id, or note. A new key must be added to the allowlist before it is published, and the existing unknown-key test fails otherwise.

Per-agent and per-batch data is operator-only. It is available through `GET /api/v1/usage` and the CLI, and it is never written to the published directory. The production web server already answers 404 for the task directory; the per-agent data uses no static path at all.

Files to change: `control/db.py` (schema version 5), `control/service.py` (writes at `record_outcome` and `cancel_task`; `usage_report`), `control/app.py` (routes), `public_fields.py` (allowlist), `runner_mailbox.py` and `task_runner.py` (local usage post), `fleet_client.py` and `cli.py` (`usage` command), `schemas/` (a schema for the usage route and the exported file).

### 4.4 Fair-share scheduling

Fair share decides the order inside one runner's `queued` manual tasks. It runs in `claim_task`, in the same transaction that picks the task, so two claims never use different state.

Order, from strongest to weakest:

1. Class: `manual`, then `canary`, then `sweep` (unchanged).
2. `priority`, higher first. Only an operator can raise it (4.1), so this is the operator override and nothing else.
3. Fair share across agents (this section).
4. First in, first out inside one agent: `submitted_at`, then `task_id`.

**Recent usage.** For agent `a` on runner `r` at time `t`:

```text
U(a, r, t) = sum over tasks i of a on r:  s_i * 2 ^ ( -(t - e_i) / H )
```

- `s_i` is `runner_seconds` of task `i`, and `e_i` is its end time.
- A task that is running now counts with the seconds used so far and `e_i = t`, so an agent cannot start a long task and look idle.
- `H` is the half-life. Default: 24 hours (`FAIR_SHARE_HALF_LIFE_HOURS`). A task's weight halves every day, so a day of heavy use is mostly forgiven in two days. Tasks older than `8 * H` are ignored (their weight is under 0.4 percent).
- Usage is counted per runner. Each runner is a separate resource, and an agent that used all of one platform last week is not slower on another. A global variant (sum over all runners) is possible; it is not recommended because it couples runners that do not otherwise depend on each other.

**Weighting by expected duration.** For each agent that has a queued task on `r`, take its first-in-line task and its expected duration `D(a)` from `estimate_task_duration_seconds` (with the calibration factor; 300 seconds when the type is unknown). The agent's score is:

```text
S(a) = U(a, r, t) + D(a)
```

The agent with the lowest `S` supplies the next task. A tie goes to the older `submitted_at`. In words: the next task is charged to its agent before it runs, so a very long task has to wait until its agent's decayed usage plus the task itself is the lowest, and an agent with small tasks is not held back by a neighbour's large one.

Example, `t` fixed, no decay since the last task ended:

| Agent | `U` (seconds) | Head task `D` | `S` | Order |
|---|---|---|---|---|
| A | 3600 | 600 | 4200 | first |
| B | 900 | 7200 | 8100 | second |
| C | 0 (new) | 300 | 300 | before both |

After A's task runs, `U(A)` is 4200 and A is compared again.

Properties: a new agent starts at `U = 0` and goes first; a queue of any length is allowed because the agent's own tasks only compete with each other by FIFO; the wait of any agent is finite because `U` of the agents ahead of it grows with every task they run and decays with the half-life; idle capacity is never held back (if only one agent has queued tasks, it gets all of them).

The existing `idx_tasks_runner_state_priority` index stays. The claim query reads the per-agent heads and the recent rows of `task_usage` for that runner (an index on `(runner_id, ended_at)`). It does not scan the whole table.

Files to change: `control/service.py` (`claim_task` and a small `fair_share.py` with the pure formula, so the maths is unit tested without a database), `control/db.py` (index), `docs/control-service.md`.

### 4.5 Sweep share, driven by bisection need

This applies to the current Conductress runner hosts only. The sweep stays a local feature of each runner. The control service does not schedule it.

**Need.** A series has need while at least one adjacent pair of its completed points differs by more than the series' threshold and still has commits between them. That is the set `SweepPlanner.get_unresolved_segments()` already returns once the threshold is the one defined below. Bisection measures the middle commit of the largest such pair, and need ends when all gaps are under the threshold or have no commits left to test. Two other kinds of task are not bisection but also cannot wait behind an unbounded queue: the nightly tip (`NIGHTLY`) and a missing release landmark (`LANDMARK`). They count as need too, because without them the dashboard goes stale. `BACKFILL` and `CONFIRM` never count; they run on spare capacity only.

A coordinator gets a method `has_need() -> bool`, built from the planner (`get_unresolved_segments()`, nightly due, landmark missing). It is cheap: it reads state the planner already holds.

**Threshold.** Two options.

*Option A, a multiple of the measured noise floor, with a floor.*

```text
threshold(series) = max( F, k * sigma(series) )
```

- `sigma(series)` is the median per-point CV of the last 30 completed points of the series, as a fraction. CV is the spread between the repetitions of one point, which the planner already stores (`BenchmarkPoint.cv`).
- `k = 2`. The difference of two points that each average three repetitions has a standard deviation of about 0.8 times the per-repetition CV, so `k = 2` is about 2.4 standard deviations of the difference.
- For throughput series, `F = 2 percent`, the value used today. A quiet series keeps its current behaviour; a noisy one gets a higher bar.
- For a deterministic metric such as memory bytes per item, the CV is close to zero and `k * sigma` would be zero. The floor is absolute, in the metric's unit: `F_abs = 0.5 byte per item` of overhead. For comparison, 2 percent of a 50-byte overhead is 1 byte, so this is as sensitive as today for small overheads and no longer loosens as overhead grows.
- The existing Welch significance test stays as a second condition.

With the median CVs recorded in `docs/layout-noise.md`, Option A gives 2.0 percent (the floor) on Graviton 4 (CV 0.45, so 0.9 percent from noise) and on Graviton 3 (CV 0.9, so 1.8 percent), 3.8 percent on AMD (CV 1.9), and 4.6 percent on Intel (CV 2.31). These numbers come from one GET series and are examples, not a promise.

*Option B, a fixed table per metric class.* A configured threshold per class (for example 2 percent for throughput, 0.5 byte per item for memory), with no measurement of the series. It is simple and predictable, and it keeps the code as it is today. It treats a noisy x86 series like a quiet Graviton one, so it spends bisection tasks on gaps that are inside the noise.

**Recommendation: Option A.** It spends bisection effort where a gap can be told from noise, it keeps today's values on quiet series, and it removes the memory special case from the planner (the floor is the only memory-specific number). One caveat applies to both options: `docs/layout-noise.md` shows that binary layout moves single points on Graviton 3 by up to 12 percent, so a gap over the threshold is a reason to look, not proof of a change. That is already true of bisection today.

`threshold(series)` is computed when the planner evaluates segments. It is stored in `SweepState` (replacing the fixed `threshold` field when it is auto, and kept as an explicit override).

**Reserved share.** While a runner has need, the sweep gets a reserved share of that runner's busy time.

- Proposed default share `R = 25 percent` when the feature is on (see PR 6 for how it is switched on), configurable per runner in `sweep_config.json` (`sweep_share`, 0 to 100; 0 turns the reservation off, which is today's behaviour).
- The runner measures the sweep's actual fraction `f` over a decaying window with the same half-life as 4.4: `f = S / (S + M)`, where `S` is decayed sweep seconds and `M` is decayed seconds of every other class on this runner. It reads these from its own result records (`observed_duration_sec`), the same file the duration calibration already reads.
- In `_choose_next`, before the remote claim: if any coordinator `has_need()` and `f < R` and nothing is running, call `_schedule_next()` first, so the sweep task is queued and runs next.
- If `f >= R`, remote tasks go first as today.
- A task that is already `claimed` or `accepted` is never displaced. The share applies only at a boundary.
- When no coordinator has need, `_choose_next` behaves as today: the sweep runs only when nothing else can be claimed.
- If a runner has need but no remote task is waiting, the sweep runs at once; the reservation never holds capacity idle.

Why 25 percent: a bisection of one regression is about 6 to 10 sequential points; a runner that does a few hundred minutes of agent work a day then spends about a quarter of it on bisection while need lasts. The value is a default to measure against the usage report, not a derived constant. If the report shows need staying open for days, raise it.

Files to change: `sweep/planner.py` (threshold and `sigma`), `sweep/coordinator.py`, `sweep/coordinator_v3.py`, `sweep/memory_coordinator.py` (`has_need`), `sweep_config.py` (`sweep_share`), `task_runner.py` (`_choose_next`, the window), `docs/configuration.md`.

### 4.6 Access for agents: an MCP server

A small MCP server wraps the CLI so an agent has typed tools and never handles the token.

- Entry point: `conductress-mcp` (an optional extra, `[mcp]`, so runners do not install it). Stdio transport.
- The server reads the user token from `CONDUCTRESS_USER_TOKEN_FILE` (mode 0600, checked as `fleet_client._load_token` does) when it starts. The token goes only to the control service, as the `Authorization` header of the CLI client it calls in process. No tool returns it, logs it, or accepts it as a parameter.
- Tools, each with a JSON schema and no free-form shell:

| Tool | Does | Calls |
|---|---|---|
| `queue_add` | Queue a task of a fixed kind (`perf`, `memory`, `cachecannon`, `scenario`) with validated fields: `platform` or `runner_id`, `source`, `specifier`, `note`, `batch`, and the options the CLI already accepts. No `priority` field. | the `queue add*` handlers |
| `task_status` | State of one task or batch. | `GET /api/v1/tasks/{selector}` |
| `results_get` | The published result of a finished task. | the published task file or `GET /api/v1/tasks/{id}` |
| `usage_status` | Own usage and queue position, and the fleet totals (4.3.1 view). | `GET /api/v1/usage` |
| `cancel_own` | Cancel own queued tasks by task id or batch id. | `DELETE /api/v1/tasks/{selector}` |

- There is no tool for sweep control, deploy, priority, or cancel-any, and the server refuses a token with the `operator` role at start-up so that it cannot become an operator tool by mistake.
- Every call is written to an audit log, one JSON line: time, tool, validated arguments, control-service response code, and the task ids involved. The path is `~/.local/state/conductress-mcp/audit.jsonl`, mode 0600, append only. The `note` text is kept in the log; the token is never in it. The control service writes its own audit entry for the same request (`actor` is the token label), so the two logs can be compared.
- The server applies the same limits as the CLI (batch size, note length) before it sends anything.

Files to add: `src/conductress/mcp_server.py`, `tests/test_mcp_server.py`, and a short `docs/agent-access.md` (or a section in `docs/fleet-cli.md`). `pyproject.toml` gets the `mcp` extra.

## 5. Interfaces that change

| Interface | Change |
|---|---|
| `tokens.json` | New optional field `scope` on `user` tokens. |
| `users.toml` | No format change. `quota_runner_minutes_per_day` stays as the optional limit; a login without it has no limit. No login gets a default quota. |
| Database | Schema version 5: `task_usage`, an index on `(runner_id, ended_at)`. |
| API | New: `GET /api/v1/usage`, `POST /api/v1/runners/{id}/usage`. Changed: submit (server-set `submitted_by`, clamped priority), cancel (own and queued only for non-operators), claim (fair-share order). |
| CLI | New: `conductress usage`. Changed: token variables for the remote commands. |
| Published files | New: `usage/summary.json` (allowlist, no private fields). |
| Runner | `_choose_next` share rule, usage post, `sweep_share` in `sweep_config.json`. |
| Sweep | `has_need()`, threshold from series noise. |

## 6. Rollout

Seven pull requests, in this order. Each one is useful alone and can be deployed on its own; each deploy happens at a task boundary through the existing procedure. Every PR updates the documents it changes.

1. **Server-set identity and token scope.** Overwrite `submitted_by` and the submitter block from the token, add `scope`, clamp `priority` for non-operators, add `from_user_env`. Tests: unit tests for the token store (scope parsing, unknown login refused at start), submit tests that send a forged `submitted_by` and a high `priority` with a user token and check the stored values and the audit detail, an operator token that keeps its priority, and a client test for the new token variable. Risk: existing user tokens must keep working, so the default scope is `submit` and a test pins that.
2. **Cancel rules.** Own-and-queued only for non-operators. Tests: a user cancels own queued (ok), own running (refused with `already_running`), another user's queued (refused), a mixed batch (partial result with reasons); an operator cancels a running task and the runner sees `cancel-requested` (existing mailbox test). Also removes `owner` from the cancel check; a test pins that an owner login without an operator token cannot cancel another login's task.
3. **Usage accounting.** Schema version 5, the migration, writes at end of task and at cancel, the runner usage post, tests. Tests: migration on a database built from the current schema with tasks in every state; one row per task for completed, failed, cancelled-queued and cancelled-running; a duplicate post stores one row; runner seconds equal `ended_at - started_at`; a runner with no network keeps working and posts later (journal test).
4. **Usage report and export.** `GET /api/v1/usage`, `conductress usage`, `usage/summary.json`, and the allowlist entries. Tests: aggregation by agent, batch, and platform on a fixed fixture; the window filter; a `submit` token sees its own rows and an `others` row; a `read` token and a `submit` token never get another login, batch id, or note (checked by scanning the whole response); the exported file contains only allowlisted keys, and the unknown-key test fails when one is added.
5. **Fair-share claim order, and optional quotas.** `fair_share.py`, the new `claim_task` order, and the quota hold only for a login that has an explicit quota (no default quota). Tests: a login without a quota is never held, however much runner time it used; a login with a quota is held once it passes it; the pure formula against hand-computed values (including the example table, a running task, decay at one half-life, and the 8 half-life cut-off); a simulation of two agents with queues of 500 and 5 tasks that checks the small agent's first task starts within one task of submission and the long queue still drains; operator priority still wins; FIFO inside one agent; a single agent gets every claim; claims on two runners do not affect each other.
6. **Sweep need and share.** `has_need()`, the threshold options, `sweep_share`, and the `_choose_next` rule. Tests: the planner with a fixed set of points (a gap over the threshold has need; a gap inside the noise has none; a deterministic memory series uses the absolute floor); the share rule with a fake clock and fake results (sweep goes first while `f < R` and need; remote goes first when `f >= R`; no need means today's order; `sweep_share = 0` means today's order; a claimed task is never displaced). Because this changes what the production runners do, it ships with `sweep_share` set to 0 and is switched on per runner after the usage report (PR 4) shows the current split.
7. **MCP server.** `mcp_server.py` and the `mcp` extra. Tests: schema validation per tool; the token never appears in a result or in the audit log (a test greps both for the token string); an `operator` token is refused at start; `cancel_own` refuses another login's task; every call writes one audit line; no tool exists for priority, sweep control, or deploy.

## 7. Open points

- The half-life (24 hours) and the sweep share (25 percent) are defaults. The usage report from PR 4 gives the data to check them before PR 5 and PR 6 are switched on.
- Option A for the sweep threshold changes which gaps count as need on noisy platforms. Before it is enabled, compare the list of unresolved gaps under both options on the current series and look at the difference.
- A usage window for the dashboard (7 days or 30 days) is a presentation choice and is not fixed here.
- Team logins hide who inside the team used the time. If that matters, give each member a login with the team as its `sponsor`; no code change is needed.
