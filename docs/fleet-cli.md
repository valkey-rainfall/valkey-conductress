# Fleet-aware CLI

The fleet-aware CLI lets humans and agents discover the Conductress fleet and manage the central remote queue without SSH access to benchmark hosts.

Runners import remote tasks only between local tasks, so a submitted remote task stays `queued` in the control service until a runner reaches a boundary and imports it. Existing local queue commands remain the execution path.

## Configuration

The client uses only Python's standard library and verifies TLS normally.

Set an operator token through the environment:

```bash
export CONDUCTRESS_OPERATOR_TOKEN='...'
```

Or store it in an owner-only file:

```bash
install -d -m 0700 ~/.config/conductress
install -m 0600 /dev/stdin ~/.config/conductress/operator.token
```

Select a different file with `CONDUCTRESS_OPERATOR_TOKEN_FILE`. Group/world-readable token files are rejected.

Configuration variables:

```text
CONDUCTRESS_CONTROL_URL          default: https://data.conductress.rainsupreme.net/api/v1
CONDUCTRESS_OPERATOR_TOKEN      operator bearer token
CONDUCTRESS_OPERATOR_TOKEN_FILE alternate owner-only token file
CONDUCTRESS_CONTROL_TIMEOUT      request timeout in seconds, default 10
CONDUCTRESS_CONTROL_CA_BUNDLE    optional custom CA bundle; TLS verification remains enabled
```

The CLI is short-lived and keeps the operator token in memory only for the command duration. A long-lived runner poller must re-evaluate secret loading rather than blindly reusing this lifetime model.

Plain HTTP is rejected except for `localhost` and `127.0.0.1` development servers.

## Fleet discovery

```bash
conductress fleet list
conductress fleet status
conductress fleet show <runner>
```

Every command supports `--json` for a versioned machine contract:

```bash
conductress fleet status --json
```

`fleet list` shows runner IDs, canonical platforms, aliases, and enabled state. `fleet status` adds queued/active task counts and status age. `fleet show` displays one runner in detail, including the runner's last control-plane error (`Last error:`) and the last outcome it could not deliver in full (`Last outcome: <stage> for <task> at <time>: <reason>`; see `control-service.md`, "Outcome size"). The same fields are `fleet_control.last_error` and `fleet_control.last_outcome_downgrade` in the `--json` output.

## Remote task management

```bash
conductress remote list
conductress remote list --runner armbench --state queued
conductress remote show TASK_ID
conductress remote cancel TASK_ID
```

Cancellation is intentionally limited to tasks that have not been claimed.

## Submit existing task types remotely

Every task-producing local queue command accepts exactly one routing selector:

```bash
conductress queue add --tests get --platform graviton3
conductress queue add-insertion --insertions 20M --size 16 --key-size 16 \
  --maxmemory 8GB --max-rss 12GB --perf-stat --runner armbench
conductress queue add-memory --types zadd --runner armbench
conductress queue add-mixed --set-ratio 20 --runner g4bench
conductress queue add-scenario --scenario eval-storm --runner bench
conductress queue add-latency valkey-rainfall COMMIT 500000 --runner g4bench
conductress queue add-cachecannon --runner armbench
conductress queue add-replica-read --io-threads 8 --write-rate 50000 --runner g4bench
```

Options:

```text
--runner RUNNER      explicit runner ID
--platform PLATFORM  unique enabled platform or alias
--priority INTEGER   remote queue priority, default 100
--json               machine-readable submission result
```

Platform resolution is deliberately strict. If future same-platform runners make an alias ambiguous, the CLI refuses to guess and requires `--runner`.

Remote retries use `runner_id:task_id` as an idempotency key. Repeating the same submission returns the existing task rather than creating a duplicate.

## Local compatibility

Without `--runner` or `--platform`, behavior is unchanged:

```bash
conductress queue add --tests get
conductress queue list
conductress queue remove TASK_ID
conductress queue clear
```

Local and remote queues are intentionally separate. `queue list/remove/clear` remain local; use the `remote` group for the control-service queue.

## Exit codes

```text
0 success
1 invalid input, unknown runner/platform/task, or schema error
2 missing/invalid credentials or authorization failure
3 network, TLS, malformed response, or server failure
4 conflict, disabled runner, ambiguous platform, or non-cancellable task
```

Human errors go to stderr. With `--json`, errors are emitted as JSON containing stable `code`, `message`, and `exit_code` fields.
