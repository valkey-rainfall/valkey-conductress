"""Human- and agent-friendly fleet control CLI."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Optional

from .fleet_client import FleetClient, FleetClientError

CLI_SCHEMA_VERSION = 1


def resolve_runner(client: FleetClient, *, runner_id: Optional[str], platform: Optional[str]) -> str:
    if bool(runner_id) == bool(platform):
        raise FleetClientError(
            "ROUTING_REQUIRED",
            "select exactly one of --runner or --platform",
            1,
        )
    runners = client.fleet().get("runners", [])
    if runner_id:
        matches = [runner for runner in runners if runner["runner_id"] == runner_id]
        if not matches:
            raise FleetClientError("RUNNER_NOT_FOUND", f"unknown runner: {runner_id}", 1)
        if not matches[0]["enabled"]:
            raise FleetClientError("RUNNER_DISABLED", f"runner is disabled: {runner_id}", 4)
        return runner_id

    matches = [
        runner
        for runner in runners
        if runner["enabled"] and (platform == runner["platform"] or platform in runner.get("platform_aliases", []))
    ]
    if not matches:
        raise FleetClientError("PLATFORM_NOT_FOUND", f"no enabled runner matches platform: {platform}", 1)
    if len(matches) > 1:
        raise FleetClientError(
            "PLATFORM_AMBIGUOUS",
            f"platform {platform} matches multiple runners; use --runner explicitly",
            4,
        )
    return matches[0]["runner_id"]


def _json_output(command: str, data: dict[str, Any]) -> None:
    print(
        json.dumps(
            {"schema_version": CLI_SCHEMA_VERSION, "command": command, "data": data},
            indent=2,
            sort_keys=True,
        )
    )


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _age(value: Optional[str]) -> str:
    parsed = _parse_time(value)
    if parsed is None:
        return "never"
    seconds = max(0, int((datetime.now(timezone.utc) - parsed).total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _runner_state(runner: dict[str, Any]) -> str:
    updated = _parse_time(runner.get("status_updated_at"))
    ttl = runner.get("status_ttl_seconds", 900)
    if updated is None or (datetime.now(timezone.utc) - updated).total_seconds() > ttl:
        return "offline"
    counts = runner.get("task_counts", {})
    if counts.get("accepted", 0) or counts.get("claimed", 0):
        return "active"
    return "idle"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="conductress", description="Conductress fleet control")
    groups = parser.add_subparsers(dest="command", required=True)

    fleet = groups.add_parser("fleet", help="Discover and inspect benchmark runners")
    fleet_sub = fleet.add_subparsers(dest="fleet_command", required=True)
    for name, help_text in (
        ("list", "List fleet runners and platform aliases"),
        ("status", "Show runner status and task counts"),
    ):
        command = fleet_sub.add_parser(name, help=help_text)
        command.add_argument("--json", action="store_true")
    show = fleet_sub.add_parser("show", help="Show one runner in detail")
    show.add_argument("runner_id")
    show.add_argument("--json", action="store_true")

    remote = groups.add_parser("remote", help="Inspect and cancel remote tasks")
    remote_sub = remote.add_subparsers(dest="remote_command", required=True)
    remote_list = remote_sub.add_parser("list", help="List remote tasks")
    remote_list.add_argument("--runner")
    remote_list.add_argument("--state")
    remote_list.add_argument("--limit", type=int, default=50)
    remote_list.add_argument("--offset", type=int, default=0)
    remote_list.add_argument("--json", action="store_true")
    remote_show = remote_sub.add_parser("show", help="Show a remote task")
    remote_show.add_argument("task_id")
    remote_show.add_argument("--json", action="store_true")
    remote_cancel = remote_sub.add_parser("cancel", help="Cancel a queued remote task")
    remote_cancel.add_argument("task_id")
    remote_cancel.add_argument("--json", action="store_true")
    return parser


def _fleet_list(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.fleet()
    if args.json:
        _json_output("fleet.list", document)
        return 0
    print(f"{'RUNNER':<14} {'PLATFORM':<42} {'ALIASES':<24} ENABLED")
    for runner in document.get("runners", []):
        aliases = ", ".join(runner.get("platform_aliases", [])) or "-"
        enabled = "yes" if runner["enabled"] else "no"
        print(f"{runner['runner_id']:<14} {runner['platform']:<42} {aliases:<24} {enabled}")
    return 0


def _fleet_status(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.fleet()
    if args.json:
        _json_output("fleet.status", document)
        return 0
    print(f"{'RUNNER':<14} {'STATE':<9} {'QUEUED':>7} {'ACTIVE':>7} {'AGE':>8}  PLATFORM")
    for runner in document.get("runners", []):
        counts = runner.get("task_counts", {})
        active = counts.get("claimed", 0) + counts.get("accepted", 0)
        print(
            f"{runner['runner_id']:<14} {_runner_state(runner):<9} "
            f"{counts.get('queued', 0):>7} {active:>7} "
            f"{_age(runner.get('status_updated_at')):>8}  {runner['platform']}"
        )
    return 0


def _fleet_show(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.runner(args.runner_id)
    if args.json:
        _json_output("fleet.show", document)
        return 0
    runner = document["runner"]
    print(f"Runner:      {runner['runner_id']} ({runner['display_name']})")
    print(f"Platform:    {runner['platform']}")
    print(f"Aliases:     {', '.join(runner.get('platform_aliases', [])) or '-'}")
    print(f"Enabled:     {'yes' if runner['enabled'] else 'no'}")
    print(f"State:       {_runner_state(runner)}")
    print(f"Status age:  {_age(runner.get('status_updated_at'))}")
    counts = runner.get("task_counts", {})
    print(f"Task counts: {', '.join(f'{key}={value}' for key, value in sorted(counts.items())) or '-'}")
    if runner.get("status"):
        current = runner["status"].get("current_task")
        print(f"Current task: {current.get('id') if current else '-'}")
    return 0


def _remote_list(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.list_tasks(
        runner_id=args.runner,
        state=args.state,
        limit=args.limit,
        offset=args.offset,
    )
    if args.json:
        _json_output("remote.list", document)
        return 0
    tasks = document.get("tasks", [])
    if not tasks:
        print("No remote tasks found.")
        return 0
    print(f"{'TASK ID':<31} {'RUNNER':<14} {'CLASS':<9} {'STATE':<10} PRIORITY  SUBMITTED")
    for task in tasks:
        print(
            f"{task['task_id']:<31} {task['runner_id']:<14} {task['task_class']:<9} "
            f"{task['state']:<10} {task['priority']:>8}  {task['submitted_at']}"
        )
    return 0


def _remote_show(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.task(args.task_id)
    if args.json:
        _json_output("remote.show", document)
        return 0
    task = document["task"]
    print(f"Task:        {task['task_id']}")
    print(f"Runner:      {task['runner_id']}")
    print(f"Class/state: {task['task_class']} / {task['state']}")
    print(f"Priority:    {task['priority']}")
    print(f"Submitted:   {task['submitted_at']} by {task['submitted_by']}")
    print(f"Task type:   {task['envelope']['task']['task_type']}")
    if task.get("outcome"):
        print(f"Outcome:     {task['outcome']['state']} at {task['outcome']['completed_at']}")
    return 0


def _remote_cancel(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.cancel_task(args.task_id)
    if args.json:
        _json_output("remote.cancel", document)
        return 0
    task = document["task"]
    changed = "cancelled" if document.get("changed") else "already cancelled"
    print(f"Remote task {task['task_id']}: {changed}")
    return 0


def main(argv: Optional[list[str]] = None, *, client: Optional[FleetClient] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    json_mode = bool(getattr(args, "json", False))
    try:
        client = client or FleetClient.from_env()
        if args.command == "fleet":
            if args.fleet_command == "list":
                return _fleet_list(client, args)
            if args.fleet_command == "status":
                return _fleet_status(client, args)
            return _fleet_show(client, args)
        if args.remote_command == "list":
            return _remote_list(client, args)
        if args.remote_command == "show":
            return _remote_show(client, args)
        return _remote_cancel(client, args)
    except FleetClientError as exc:
        if json_mode:
            payload = {
                "schema_version": CLI_SCHEMA_VERSION,
                "error": True,
                "code": exc.code,
                "message": exc.message,
                "exit_code": exc.exit_code,
            }
            if exc.details is not None:
                payload["details"] = exc.details
            print(json.dumps(payload, sort_keys=True))
        else:
            print(f"Error [{exc.code}]: {exc.message}", file=sys.stderr)
        return exc.exit_code
