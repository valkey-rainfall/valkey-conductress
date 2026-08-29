# Fleet-aware CLI

The Phase 3 CLI lets humans and agents discover the Conductress fleet and manage the central remote queue without SSH access to benchmark hosts.

Remote tasks will remain `queued` in the control service until Phase 4 enables between-task mailbox polling on runners. Existing local queue commands remain the execution path until then.

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

The Phase 3 CLI is short-lived and keeps the operator token in memory only for the command duration. Phase 4 must re-evaluate secret loading for the long-lived runner poller rather than blindly reusing this lifetime model.

Plain HTTP is rejected except for `localhost` and `127.0.0.1` development servers.

## Fleet discovery

```bash
conductress fleet list
conductress fleet status
conductress fleet show armbench
```

Every command supports `--json` for a versioned machine contract:

```bash
conductress fleet status --json
```

`fleet list` shows runner IDs, canonical platforms, aliases, and enabled state. `fleet status` adds queued/active task counts and status age. `fleet show` displays one runner in detail.

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
conductress queue add-memory --types zadd --runner armbench
conductress queue add-mixed --set-ratio 20 --runner g4bench
conductress queue add-scenario --scenario eval-storm --runner bench
conductress queue add-latency valkey-rainfall COMMIT 500000 --runner g4bench
conductress queue add-cachecannon --runner armbench
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
