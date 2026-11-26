# Agent access: the Conductress MCP server

`conductress-mcp` lets an agent use the benchmark queue through typed tools, with the agent's own user token. It is a thin client of the control service: it holds no queue logic and no database access, and it makes the same API calls as the CLI (`queue add`, `queue mine`, `queue cancel`, `usage`, and the task and batch reads). What a token may see or do is decided by the control service, as for the CLI: the server sets the submitter and the priority, a `read` token cannot submit or cancel, a user sees its own tasks and finished tasks, and a user cancels only its own queued tasks.

## Transport and safety

- **Stdio only.** The agent runtime starts `conductress-mcp` as a local process and talks to it on standard input and output (newline-delimited JSON-RPC 2.0). The server opens no port and no socket and has no network listener. Its only network traffic is the HTTPS calls to the control service.
- **No MCP library.** The official SDK needs Python 3.10 or newer (this package supports 3.9) and pulls in an HTTP server stack that this process must never start. The protocol subset used here (`initialize`, `ping`, `tools/list`, `tools/call`; protocol versions 2025-06-18, 2025-03-26 and 2024-11-05) is small and covered by tests. Nothing needs to be installed beyond the package itself.
- **The token.** It is read from `CONDUCTRESS_USER_TOKEN` or `CONDUCTRESS_USER_TOKEN_FILE` (an owner-only file, default `~/.config/conductress/user.token`) when the server starts. The operator variables are never read. The token goes only to the control service. No tool takes it, returns it or logs it, and the server replaces it with `[redacted]` in any text it returns or logs.
- **Which token.** At start the server asks the control service who the token is. A user token is accepted. An operator token is refused unless `CONDUCTRESS_MCP_ALLOW_OPERATOR=1` is set on purpose (the tools then show only what that token allows; an operator has no task list of its own). A runner token or an invalid token is always refused. The server exits with status 2 and a message on standard error.
- **Audit log.** Every tool call, also a rejected one, adds one JSON line to `~/.local/state/conductress-mcp/audit.jsonl` (or the path in `CONDUCTRESS_MCP_AUDIT_LOG`): time, tool, the validated arguments, result, HTTP status or error code, and the task ids. The file is mode 0600 in a 0700 directory and is opened for append only. If it cannot be opened, the server does not start. The control service writes its own audit entry for the same request (the actor is the token label), so the two logs can be compared.
- **Data, not instructions.** Notes, specifiers, results and error messages are written by people and programs. The tool descriptions and the server instructions say so. The server never runs anything from them: each tool makes fixed API calls, and the arguments are checked against a schema before anything is sent.

## Tools

| Tool | Arguments | Does |
|---|---|---|
| `queue_add` | `kind` (`perf` or `memory`), one of `runner_id` or `platform`, optional `source`, `specifier`, `note` (300 characters), `batch`; for `perf`: `tests`, optional `sizes`, `io_threads`, `pipelining`, `warmup`, `duration`, `repetitions`; for `memory`: `types`, optional `sizes` | Runs `conductress queue add` or `queue add-memory` with those values. At most 50 tasks per call, `duration` and `warmup` at most 60 minutes, `repetitions` at most 10. There is no `priority`, no build or server arguments, and no path or CPU option. A call always goes to a remote runner, never to a local queue. |
| `queue_mine` | optional `limit` (1 to 100) | Your own tasks, newest first. |
| `task_status` | `task_id` or `batch_id` | State and result of one task or batch. Your own tasks and finished tasks only. This is also how a finished result is read. |
| `cancel_own` | `selector` (a task id or a batch id) | `DELETE /api/v1/tasks/{selector}`, as `queue cancel` does. Only your own queued tasks change; others are reported as skipped. There is no way to cancel everything. |
| `usage_status` | optional `by` (`agent`, `batch`, `platform`), `since`, `runner_id` | `GET /api/v1/usage`: your rows and one row `others`. |

There is no tool for sweep control, deploy, priority, or cancelling other logins' tasks. `queue_add` supports the `perf` and `memory` kinds only; the `cachecannon` and `scenario` kinds have many expert options and are not offered.

## Configure an agent

Create a user token with the operator (see the control service guide), keep it in an owner-only file, and add the server to the agent's MCP configuration. For a runtime that uses the common `mcpServers` format:

```json
{
  "mcpServers": {
    "conductress": {
      "command": "conductress-mcp",
      "env": {
        "CONDUCTRESS_USER_TOKEN_FILE": "/path/to/owner-only/user.token",
        "CONDUCTRESS_CONTROL_URL": "https://control.example.test/api/v1"
      }
    }
  }
}
```

`CONDUCTRESS_CONTROL_URL` must be HTTPS (plain HTTP is accepted only for localhost). The other client variables of the CLI (`CONDUCTRESS_CONTROL_TIMEOUT`, `CONDUCTRESS_CONTROL_CA_BUNDLE`) apply too.

## Deploy

Install the package where the agent runs. The control service needs the version that lets a user token read the fleet directory (`GET /api/v1/fleet`: runner id, display name, platform, aliases and enabled flag only), because `queue_add` and `queue add` with a user token look up the runner there. The runners need nothing.
