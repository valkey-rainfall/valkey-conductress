"""Command-line entry point for the fleet control service."""

import argparse
import getpass
import subprocess
import sys
from pathlib import Path

from aiohttp import web

from .app import create_app
from .auth import hash_token
from .config import ControlConfig


def _service_is_active(service: str = "conductress-control.service") -> bool:
    try:
        result = subprocess.run(["systemctl", "is-active", service], capture_output=True, text=True, check=False)
    except (FileNotFoundError, OSError):
        return False
    return result.stdout.strip() == "active"


def _archive_runner(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    from .archive import archive_runner, format_manifest_entry
    from .db import ControlDatabase
    from .fleet_registry import FleetRegistry

    config = ControlConfig.from_env()
    if not args.dry_run and not args.force and _service_is_active():
        parser.error(
            "conductress-control.service is active; stop it first (an ingest could race the rename), or pass --force"
        )
    database = ControlDatabase(config.database_path, config.audit_jsonl_path)
    if not config.database_path.exists():
        parser.error(f"database not found: {config.database_path}")
    registry = FleetRegistry.from_file(config.fleet_manifest_path)
    live_runner = registry.get_runner(args.runner, require_enabled=False)
    if registry.is_archived(args.runner):
        parser.error(f"{args.runner} is itself an archive")
    backup = None
    if not args.dry_run:
        backup = (
            Path(args.backup)
            if args.backup
            else config.database_path.with_name(f"{config.database_path.name}.pre-archive-{args.archive_id}.db")
        )
    try:
        result = archive_runner(
            database,
            runner_id=args.runner,
            archive_id=args.archive_id,
            until=args.until,
            note=args.note,
            actor=args.actor,
            live_runner=live_runner,
            dry_run=args.dry_run,
            backup=backup,
        )
    except (ValueError, FileExistsError) as exc:
        print(f"error: {exc}")
        sys.exit(2)
    for line in result.summary_lines():
        print(line)
    print("Add this entry to the fleet manifest (fleet.json) and restart the service:")
    print(format_manifest_entry(result.manifest_entry))


def main() -> None:
    parser = argparse.ArgumentParser(prog="conductress-control")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="Run the localhost-only control service")
    serve.add_argument("--port", type=int, default=8390)
    subparsers.add_parser("hash-token", help="Read a token securely and print its SHA-256 hash")
    archive = subparsers.add_parser(
        "archive-runner",
        help=(
            "Manual: rename a runner's canary series rows up to a date to an archive id, so the live "
            "runner starts a fresh series. Backs the database up first; nothing is deleted."
        ),
    )
    archive.add_argument("--runner", required=True, help="Live runner id whose history is archived")
    archive.add_argument(
        "--archive-id", required=True, help="New runner id for the history, e.g. intelbench-pre-2026-11-07"
    )
    archive.add_argument("--until", required=True, help="Last UTC date (YYYY-MM-DD) that belongs to the archive")
    archive.add_argument("--note", required=True, help="What changed (stored in the audit entry and the manifest)")
    archive.add_argument("--actor", default=f"operator:{getpass.getuser()}", help="Audit actor")
    archive.add_argument("--backup", default=None, help="Backup path (default: <db>.pre-archive-<archive-id>.db)")
    archive.add_argument("--dry-run", action="store_true", help="Only count the rows that would move")
    archive.add_argument("--force", action="store_true", help="Run even if the control service is active (unsafe)")
    args = parser.parse_args()

    if args.command == "hash-token":
        token = getpass.getpass("Token: ")
        if not token:
            parser.error("token must not be empty")
        print(hash_token(token))
        return
    if args.command == "archive-runner":
        _archive_runner(args, parser)
        return

    config = ControlConfig.from_env()
    # Deliberately hardcoded: the reverse proxy is the only network listener.
    web.run_app(create_app(config), host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
