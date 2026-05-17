"""Unified entry point for Conductress."""

import argparse
import logging
import sys

from src.config import CONDUCTRESS_LOG


def main() -> None:
    parser = argparse.ArgumentParser(prog="conductress", description="Valkey Conductress")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("tui", help="Launch the TUI")
    subparsers.add_parser("run", help="Start the task runner worker")
    subparsers.add_parser("setup", help="Run setup/bootstrap")
    subparsers.add_parser("perf", help="Queue perf tasks via CLI")
    subparsers.add_parser("queue", help="List queued tasks")
    subparsers.add_parser("compare", help="Run analysis/comparison")
    subparsers.add_parser("status", help="Show runner and task status (non-blocking)")

    args, remaining = parser.parse_known_args()

    # Configure logging for all subcommands
    logging.basicConfig(
        filename=str(CONDUCTRESS_LOG),
        encoding="utf-8",
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("asyncssh").setLevel(logging.WARNING)

    if args.command is None:
        parser.print_usage()
        sys.exit(0)

    if args.command == "tui":
        from src.tui import BenchmarkApp

        app = BenchmarkApp()
        app.run()

    elif args.command == "run":
        import asyncio
        import json
        import traceback
        from datetime import datetime

        from src.config import PROJECT_ROOT
        from src.task_runner import TaskRunner

        crash_file = PROJECT_ROOT / "last_crash.json"
        runner = TaskRunner()
        try:
            asyncio.run(runner.run())
        except KeyboardInterrupt:
            print("Runner stopped by user.")
        except Exception:
            tb = traceback.format_exc()
            timestamp = datetime.utcnow().isoformat() + "Z"
            task_desc = str(runner.task) if runner.task else None

            # Log to main log file
            logger = logging.getLogger("conductress.crash")
            logger.critical("Runner crashed!\n%s", tb)

            # Write crash file for status command
            crash_info = {
                "timestamp": timestamp,
                "traceback": tb,
                "task": task_desc,
            }
            crash_file.write_text(json.dumps(crash_info, indent=2))

            # Also print to stderr for nohup captures
            print(f"[{timestamp}] RUNNER CRASHED:", file=sys.stderr)
            print(tb, file=sys.stderr)
            sys.exit(1)

    elif args.command == "setup":
        import asyncio

        from src import config
        from src.bootstrap import (
            SERVERS,
            ensure_server_ssh_fingerprints,
            ensure_ssh_key,
            update_host_list,
        )

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        logger.addHandler(ch)
        logger.info("⊹˚₊‧───Starting update/setup───‧₊˚⊹")

        ensure_ssh_key()
        asyncio.run(ensure_server_ssh_fingerprints())

        update_servers = SERVERS.copy()
        if config.ServerInfo("localhost", "", "localhost") not in update_servers:
            update_servers.append(config.ServerInfo("localhost", "", "localhost"))

        asyncio.run(update_host_list(update_servers))
        logger.info("Update/setup complete!")

    elif args.command == "perf":
        from src.cli import main as cli_main

        sys.exit(cli_main(["perf"] + remaining))

    elif args.command == "queue":
        from src.cli import main as cli_main

        sys.exit(cli_main(["queue"] + remaining))

    elif args.command == "compare":
        from src.analysis import main as analysis_main

        sys.exit(analysis_main(remaining))

    elif args.command == "status":
        from src.status import print_status

        sys.exit(print_status())


if __name__ == "__main__":
    main()
