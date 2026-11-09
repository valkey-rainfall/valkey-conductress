"""Unified entry point for Conductress."""

import argparse
import logging
import sys

from conductress.banner import VersionAction
from conductress.config import CONDUCTRESS_LOG


def main() -> None:
    parser = argparse.ArgumentParser(prog="conductress", description="Valkey Conductress")
    parser.add_argument("--version", action=VersionAction)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Start the task runner worker")
    run_parser.add_argument(
        "--sweep",
        action="store_true",
        help="Enable sweep mode: auto-generate historical benchmark tasks when queue is empty",
    )
    run_parser.add_argument(
        "--memory-sweep",
        action="store_true",
        help="Enable memory sweep: track per-item memory overhead across history",
    )
    run_parser.add_argument(
        "--repo",
        type=str,
        default=None,
        help="Path to valkey git repo for sweep (default: ~/valkey)",
    )
    run_parser.add_argument(
        "--publish",
        type=str,
        default=None,
        help="Publish dashboard data to this rsync target after each task (e.g. user@host:/path)",
    )
    run_parser.add_argument(
        "--nudge-url",
        type=str,
        default=None,
        help="HTTP endpoint to POST task-completion 'nudge' notifications to (e.g. http://localhost:8080/hooks/nudge).",
    )
    run_parser.add_argument(
        "--nudge-on",
        type=str,
        default="completed,failed,empty",
        help="Comma-separated event types that trigger a nudge: completed, failed, empty (default: all). Unrecognized event names are silently ignored.",
    )
    run_parser.add_argument(
        "--fleet-mode",
        choices=["off", "shadow", "live"],
        default="off",
        help="Fleet mailbox mode: off, shadow status-only, or live task import",
    )
    run_parser.add_argument(
        "--management-settle",
        type=float,
        default=None,
        help="Seconds to settle after boundary network activity before task execution",
    )
    subparsers.add_parser(
        "setup",
        help="Provision this machine and every servers.json host as a benchmark runner "
        "(sudo: system package upgrade, sysctl and limits changes, systemd service). "
        "Not needed to develop or run the tests.",
    )
    subparsers.add_parser("queue", help="Manage the task queue (list, add, remove)", add_help=False)
    subparsers.add_parser("plot", help="Render a figure from a task's results", add_help=False)
    subparsers.add_parser("fleet", help="Discover and inspect benchmark runners", add_help=False)
    subparsers.add_parser("remote", help="Inspect and cancel remote tasks", add_help=False)
    subparsers.add_parser("canary", help="Canary drift monitoring", add_help=False)
    subparsers.add_parser("compare", help="Run analysis/comparison", add_help=False)
    runner_info_parser = subparsers.add_parser("runner-info", help="Show stable runner identity and environment")
    runner_info_parser.add_argument("--json", action="store_true", help="Emit versioned machine-readable JSON")
    subparsers.add_parser("status", help="Show runner and task status (non-blocking)")
    status_export_parser = subparsers.add_parser("status-export", help="Export status to JSON for remote monitoring")
    status_export_parser.add_argument(
        "--publish",
        type=str,
        default=None,
        help="Publish status to this rsync target (e.g. user@host:/path)",
    )
    sweep_parser = subparsers.add_parser("sweep", help="Sweep management (export, status)")
    sweep_sub = sweep_parser.add_subparsers(dest="sweep_command")
    export_parser = sweep_sub.add_parser("export", help="Export sweep results to dashboard JSON")
    export_parser.add_argument(
        "--platform",
        required=True,
        help="Platform identifier (e.g. amd64, arm64, intel)",
    )
    export_parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path (default: ./series-{platform}.json)",
    )
    export_parser.add_argument(
        "--push",
        type=str,
        default=None,
        help="Git repo path to commit+push the exported file to",
    )
    export_parser.add_argument(
        "--metric",
        type=str,
        default=None,
        choices=["throughput", "memory"],
        help="Export only this metric (default: all available)",
    )
    sweep_sub.add_parser("status", help="Show sweep progress summary")

    export_archived_parser = sweep_sub.add_parser(
        "export-archived",
        help="One-time: load an archived epoch's state files, export its dashboard files to the "
        "persistent publish export dir, and exit. Run once on a runner whose export dir was wiped; "
        "the runner service never loads archived state itself.",
    )
    export_archived_parser.add_argument(
        "--epoch",
        default="v1",
        help="Archived epoch to regenerate (only 'v1' is supported)",
    )
    export_archived_parser.add_argument(
        "--generation",
        default=None,
        help=(
            "Export a closed v3 generation instead of an archived epoch: the id printed by "
            "'sweep close-generation' (its cut date, e.g. 2026-11-06)"
        ),
    )
    export_archived_parser.add_argument(
        "--repo",
        type=str,
        default=None,
        help="Path to valkey git repo (default: ~/valkey)",
    )
    export_archived_parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to export into (default: the persistent publish export dir)",
    )

    # Sweep control commands
    focus_parser = sweep_sub.add_parser("focus", help="Focus on a single workload (others paused)")
    focus_parser.add_argument(
        "workload",
        help=(
            "Selector to focus on. 'throughput' (all epochs), "
            "'v3:get-k16-v16-t7-p10' (one epoch), or 'v3:*' (a whole epoch)"
        ),
    )
    pause_parser = sweep_sub.add_parser("pause", help="Pause specific sweeps")
    pause_parser.add_argument(
        "workloads",
        nargs="+",
        help=(
            "Selectors to pause. A bare workload id pauses it in EVERY epoch "
            "(v1 and v3 share ids); qualify as 'v1:<workload>' or 'v1:*' to pause one epoch"
        ),
    )
    sweep_sub.add_parser("resume", help="Resume all sweeps (remove focus/pause)")
    sweep_sub.add_parser("list", help="List all workload IDs and current scheduling config")

    reset_series_parser = sweep_sub.add_parser(
        "reset-series",
        help="Clear a series' history: back up and delete its state file and relocate its queued task files",
    )
    reset_series_parser.add_argument(
        "--epoch",
        default="v3",
        help="Epoch the series belongs to (only 'v3' is supported)",
    )
    reset_series_target = reset_series_parser.add_mutually_exclusive_group(required=True)
    reset_series_target.add_argument(
        "--workload",
        help=(
            "Workload label: a throughput series such as get-k16-v16-t7-p1, or a memory series such as "
            "memory-sadd-m20 (unprefixed; --engine adds the engine prefix)"
        ),
    )
    reset_series_target.add_argument(
        "--all-memory",
        action="store_true",
        help="Reset every memory series on the roster for the given engine, under one timestamp",
    )
    reset_series_parser.add_argument(
        "--engine",
        default=None,
        help="Comparison engine source whose prefixed series to reset (e.g. redis); omit for the default Valkey series",
    )
    reset_series_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without touching any file",
    )
    reset_series_parser.add_argument(
        "--force",
        action="store_true",
        help="Reset even if the runner service appears active (unsafe: the runner may be mid-task on this series)",
    )

    close_gen_parser = sweep_sub.add_parser(
        "close-generation",
        help=(
            "Manual: close this platform's live v3 generation at a cut time. Points from tasks that started "
            "before the cut move to a read-only archived generation; tasks that started between the cut and "
            "--resume are discarded (listed in excluded_tasks.json); the live series restart from the cut."
        ),
    )
    close_gen_parser.add_argument("--epoch", default="v3", help="Epoch of the series (only 'v3' is supported)")
    close_gen_parser.add_argument(
        "--cut",
        required=True,
        help="UTC instant of the cut, ISO 8601 (e.g. 2026-11-06T16:14:23Z); tasks started before it are archived",
    )
    close_gen_parser.add_argument(
        "--resume",
        default=None,
        help="UTC instant from which tasks are live again (default: the cut); tasks started in between are discarded",
    )
    close_gen_parser.add_argument("--label", required=True, help="What changed, shown on the dashboard")
    close_gen_parser.add_argument(
        "--reason", required=True, help="Why the generation is closed (e.g. the canary alarm)"
    )
    close_gen_parser.add_argument(
        "--workloads",
        default=None,
        help="Comma-separated series labels to split (default: every v3 throughput and latency series)",
    )
    close_gen_parser.add_argument("--by", default="operator", help="Who decided (recorded in the generation entry)")
    close_gen_parser.add_argument("--dry-run", action="store_true", help="Print what would happen; touch nothing")
    close_gen_parser.add_argument(
        "--force", action="store_true", help="Run even if the runner service appears active (unsafe)"
    )

    add_event_parser = sweep_sub.add_parser(
        "add-event",
        help=(
            "Record an environment change the provenance fields cannot see (sweep_data/environment_events.json). "
            "Published in the manifest and used by the canary findings. The label is public."
        ),
    )
    add_event_parser.add_argument("--at", required=True, help="UTC instant the change began, ISO 8601")
    add_event_parser.add_argument("--until", default=None, help="UTC instant the change ended (e.g. back from reboot)")
    add_event_parser.add_argument("--label", required=True, help="What changed, for a public reader")
    add_event_parser.add_argument(
        "--fields", default="", help="Comma-separated words, e.g. glibc,microcode_ctl,reboot (kinds of change)"
    )
    add_event_parser.add_argument("--by", default="operator", help="Who recorded it (kept on the runner only)")
    add_event_parser.add_argument("--dry-run", action="store_true", help="Validate and print; write nothing")

    explain_parser = sweep_sub.add_parser(
        "explain-event",
        help=(
            "Explain the canary event of a date on this runner (it becomes 'explained' on the canary page). "
            "The text is public."
        ),
    )
    explain_parser.add_argument("--date", required=True, help="Event date as shown on the canary page (YYYY-MM-DD)")
    explain_parser.add_argument("--text", required=True, help="The explanation, for a public reader")
    explain_parser.add_argument("--by", default="operator", help="Who recorded it (kept on the runner only)")
    explain_parser.add_argument("--dry-run", action="store_true", help="Validate and print; write nothing")

    args, remaining = parser.parse_known_args()
    if args.command == "run" and args.management_settle is not None and args.management_settle < 0:
        parser.error("--management-settle must not be negative")

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

    if args.command == "run":
        import asyncio
        import json
        import traceback
        from datetime import datetime
        from pathlib import Path

        from conductress.config import PROJECT_ROOT
        from conductress.task_runner import TaskRunner

        crash_file = PROJECT_ROOT / "last_crash.json"
        repo_path = Path(args.repo) if args.repo else None
        # Parse nudge events from comma-separated string
        nudge_events = None
        if args.nudge_url:
            if args.nudge_on:
                nudge_events = {e.strip() for e in args.nudge_on.split(",") if e.strip()}
            else:
                nudge_events = {"completed", "failed", "empty"}
        runner = TaskRunner(
            sweep=args.sweep,
            memory_sweep=args.memory_sweep,
            repo_path=repo_path,
            publish_target=args.publish,
            nudge_url=args.nudge_url,
            nudge_on=nudge_events,
            fleet_mode=args.fleet_mode,
            management_settle_seconds=args.management_settle,
        )
        if args.sweep:
            print("Sweep mode enabled — will auto-generate tasks when queue is empty")
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

        from conductress import config
        from conductress.bootstrap import SERVERS, ensure_server_ssh_fingerprints, ensure_ssh_key, update_host_list

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

    elif args.command == "queue":
        from conductress.cli import main as cli_main

        sys.exit(cli_main(["queue"] + remaining))

    elif args.command == "plot":
        from conductress.cli import main as cli_main

        sys.exit(cli_main(["plot"] + remaining))

    elif args.command in {"fleet", "remote", "canary"}:
        from conductress.fleet_cli import main as fleet_cli_main

        sys.exit(fleet_cli_main([args.command] + remaining))

    elif args.command == "compare":
        from conductress.analysis import main as analysis_main

        sys.exit(analysis_main(remaining))

    elif args.command == "runner-info":
        import json

        from conductress.runner_identity import get_runner_info

        info = get_runner_info()
        if args.json:
            print(json.dumps(info, indent=2, sort_keys=True))
        else:
            environment = info["environment"]
            print(f"Runner:      {info['runner_id']} ({info['display_name']})")
            print(f"Platform:    {info['platform']['label']}")
            print(f"Hostname:    {info['hostname']}")
            print(f"Fingerprint: {environment['host_fingerprint']}")
            print(f"Kernel:      {environment['kernel_release']}")
            print(f"Conductress: {environment['conductress_revision']}")

    elif args.command == "status":
        from conductress.status import print_status

        sys.exit(print_status())

    elif args.command == "status-export":
        from conductress.status_export import export_status

        path = export_status(publish_target=args.publish or "")
        print(f"Exported to {path}")

    elif args.command == "sweep":
        from pathlib import Path

        from conductress.config import (
            SWEEP_IO_THREADS,
            SWEEP_PIPELINING,
            SWEEP_STATE_DIR,
            SWEEP_STATE_FILE,
            SWEEP_THROUGHPUT_WORKLOADS,
            SWEEP_V3_ENABLED,
        )
        from conductress.sweep.coordinator import BaseSweepCoordinator, SweepCoordinator
        from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS, MemorySweepCoordinator
        from conductress.sweep.planner import SweepState

        if args.sweep_command == "export":
            platform = args.platform
            platform_labels = {
                "amd64": "amd64/epyc-9r14/zen4",
                "arm64": "arm64/c7g.metal/graviton3",
                "intel": "intel/sapphire-rapids/8488c",
            }
            platform_str = platform_labels.get(platform, platform)
            repo_path = Path(args.repo) if getattr(args, "repo", None) else Path.home() / "valkey"

            # Build list of coordinators to export
            coordinators: list[BaseSweepCoordinator] = []
            if not args.metric or args.metric == "throughput":
                primary = SweepCoordinator(repo_path)
                if primary.state_file.exists():
                    coordinators.append(primary)
                for wl in SWEEP_THROUGHPUT_WORKLOADS:
                    wl_coord = SweepCoordinator(
                        repo_path,
                        val_size=wl["val_size"],
                        test=wl.get("test", "get"),
                        io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                        pipelining=wl.get("pipelining", SWEEP_PIPELINING),
                    )
                    if wl_coord.state_file.exists():
                        coordinators.append(wl_coord)
            if SWEEP_V3_ENABLED and (not args.metric or args.metric == "throughput"):
                from conductress.sweep.coordinator_v3 import create_v3_coordinators

                for v3_coord in create_v3_coordinators(repo_path):
                    if v3_coord.state_file.exists():
                        coordinators.append(v3_coord)

            if not args.metric or args.metric == "memory":
                for mem_wl in MEMORY_WORKLOADS:
                    if mem_wl.state_file.exists():
                        coordinators.append(MemorySweepCoordinator(repo_path, mem_wl))

            # Additional engines (e.g. Redis)
            if not args.metric or args.metric == "throughput":
                from conductress.config import SWEEP_ENGINES

                for engine in SWEEP_ENGINES:
                    if engine.source == "valkey":
                        continue
                    engine_repo = Path(args.repo) if getattr(args, "repo", None) else Path.home() / engine.source
                    primary = SweepCoordinator(engine_repo, engine=engine)
                    if primary.state_file.exists():
                        coordinators.append(primary)
                    for wl in SWEEP_THROUGHPUT_WORKLOADS:
                        engine_coord = SweepCoordinator(
                            engine_repo,
                            val_size=wl["val_size"],
                            test=wl.get("test", "get"),
                            io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                            pipelining=wl.get("pipelining", SWEEP_PIPELINING),
                            engine=engine,
                        )
                        if engine_coord.state_file.exists():
                            coordinators.append(engine_coord)

            if not coordinators:
                print("No sweep data to export.")
                sys.exit(1)

            exported_files = []
            for coord in coordinators:
                epoch_id = getattr(coord, "epoch_id", "v1")
                default_name = f"series-{platform}-{coord.workload_id}-{coord.metric_id}"
                if epoch_id != "v1":
                    default_name += f".epoch-{epoch_id}"
                output = Path(args.output) if args.output else Path(f"{default_name}.json")
                count = coord.export(output, platform=platform_str)
                if count > 0:
                    print(f"Exported {count} {coord.metric_id} points to {output}")
                    exported_files.append(output)

            if args.push and exported_files:
                import shutil
                import subprocess

                repo_path_push = Path(args.push)
                for output in exported_files:
                    dest = repo_path_push / "data" / output.name
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(output, dest)
                    subprocess.run(
                        ["git", "-C", str(repo_path_push), "add", str(dest)],
                        check=True,
                    )
                result = subprocess.run(
                    ["git", "-C", str(repo_path_push), "diff", "--cached", "--quiet"],
                    capture_output=True,
                )
                if result.returncode != 0:
                    msg = ", ".join(f.name for f in exported_files)
                    subprocess.run(
                        [
                            "git",
                            "-C",
                            str(repo_path_push),
                            "commit",
                            "-m",
                            f"Update {msg}",
                        ],
                        check=True,
                    )
                    subprocess.run(
                        ["git", "-C", str(repo_path_push), "push"],
                        check=True,
                    )
                    print(f"Pushed to {repo_path_push}")
                    print(f"Pushed to {repo_path}")
                else:
                    print("No changes to push (data unchanged)")

        elif args.sweep_command == "export-archived":
            from conductress.sweep.archived_export import export_archived_epoch

            repo_path = Path(args.repo) if getattr(args, "repo", None) else None
            output_dir = Path(args.output_dir) if getattr(args, "output_dir", None) else None
            what = f"generation {args.generation}" if args.generation else f"{args.epoch} epoch"
            try:
                if args.generation:
                    from conductress.sweep.archived_export import export_archived_generation

                    written = export_archived_generation(args.generation, repo_path=repo_path, output_dir=output_dir)
                else:
                    written = export_archived_epoch(args.epoch, repo_path=repo_path, output_dir=output_dir)
            except ValueError as exc:
                print(f"error: {exc}")
                sys.exit(2)
            if written == 0:
                print(f"No state found for the {what}; nothing exported.")
                sys.exit(1)
            print(f"Exported {written} series of the {what} to the publish export dir.")

        elif args.sweep_command == "close-generation":
            from datetime import datetime, timezone

            from conductress.sweep.generations import close_generation

            def _utc(text):
                value = datetime.fromisoformat(text.replace("Z", "+00:00"))
                return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

            if args.epoch != "v3":
                print(f"error: only the v3 epoch is supported, got {args.epoch!r}")
                sys.exit(2)
            try:
                close_result = close_generation(
                    cut=_utc(args.cut),
                    resume=_utc(args.resume) if args.resume else None,
                    label=args.label,
                    reason=args.reason,
                    recorded_by=args.by,
                    workloads=[w.strip() for w in args.workloads.split(",") if w.strip()] if args.workloads else None,
                    dry_run=args.dry_run,
                    force=args.force,
                )
            except ValueError as exc:
                print(f"error: {exc}")
                sys.exit(2)
            for line in close_result.summary_lines():
                print(line)
            if close_result.refused_reason:
                sys.exit(1)
            if not args.dry_run:
                print(
                    f"Next: 'conductress sweep export-archived --generation {close_result.generation_id}' "
                    "to publish the archived files, then start the runner service."
                )

        elif args.sweep_command in ("add-event", "explain-event"):
            from conductress.sweep.environment_events import add_event, add_explanation, default_path, parse_utc

            try:
                if args.sweep_command == "add-event":
                    entry = add_event(
                        at=parse_utc(args.at),
                        until=parse_utc(args.until) if args.until else None,
                        label=args.label,
                        fields=[f for f in args.fields.split(",") if f.strip()],
                        recorded_by=args.by,
                        dry_run=args.dry_run,
                    )
                else:
                    entry = add_explanation(
                        event_date=args.date, text=args.text, recorded_by=args.by, dry_run=args.dry_run
                    )
            except ValueError as exc:
                print(f"error: {exc}")
                sys.exit(2)
            verb = "Would record" if args.dry_run else "Recorded"
            print(f"{verb} in {default_path()}: {entry}")
            if not args.dry_run:
                print("It is published with the next sweep publication.")

        elif args.sweep_command == "status":
            state = SweepState.load(SWEEP_STATE_FILE)
            from conductress.sweep.planner import SweepPlanner

            planner = SweepPlanner(state)
            completed = sum(1 for p in state.points.values() if p.value is not None)
            failed = sum(1 for p in state.points.values() if p.status.name == "BUILD_FAILED")
            segments = planner.get_unresolved_segments()
            print(f"Commits tracked: {len(state.merge_commits)}")
            print(f"Points completed: {completed}")
            print(f"Build failures: {failed}")
            print(f"Landmarks: {len(state.landmarks)}")
            print(f"Unresolved segments (>{state.threshold*100:.0f}%): {len(segments)}")
            if segments:
                top = segments[0]
                print(f"Largest gap: {top.abs_delta*100:.1f}% ({top.commit_count} commits)")

        elif args.sweep_command == "focus":
            from conductress.sweep_config import focus, load_sweep_config

            focus(args.workload)
            print(f"Sweep focused on: {args.workload}")
            print("Only this workload will queue new tasks. Use 'conductress sweep resume' to restore.")

        elif args.sweep_command == "pause":
            from conductress.sweep_config import pause

            pause(args.workloads)
            print(f"Paused: {', '.join(args.workloads)}")
            print("These sweeps won't queue. Use 'conductress sweep resume' to restore.")

        elif args.sweep_command == "resume":
            from conductress.sweep_config import resume

            resume()
            print("All sweeps resumed (normal scheduling).")

        elif args.sweep_command == "list":
            from conductress.config import SWEEP_EPOCH_PRECEDENCE
            from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS
            from conductress.sweep_config import load_sweep_config

            cfg = load_sweep_config()
            workloads = ["throughput"] + [f"memory-{wl.label}" for wl in MEMORY_WORKLOADS]
            print(f"Mode: {cfg.mode}" + (f" (target: {cfg.target})" if cfg.target else ""))
            if cfg.paused:
                print(f"Paused selectors: {', '.join(cfg.paused)}")
            print(f"Epoch precedence: {' > '.join(SWEEP_EPOCH_PRECEDENCE)} (highest first)")
            print("\nWorkload IDs (static hint; the live roster depends on enabled epochs):")
            for wid in workloads:
                status = "✓" if cfg.is_allowed(wid) else "✗ paused"
                print(f"  {wid:<30} {status}")
            print(
                "\nWorkload ids are shared across epochs, so a bare selector pauses every epoch."
                "\nQualify a selector to target one: 'v1:<workload>', or 'v1:*' for a whole epoch."
            )

        elif args.sweep_command == "reset-series":
            from conductress.sweep.reset_series import reset_all_memory_series, reset_series

            if args.epoch != "v3":
                print(f"error: only the v3 epoch is supported, got {args.epoch!r}")
                sys.exit(2)
            if args.all_memory:
                reset_results = reset_all_memory_series(engine=args.engine, dry_run=args.dry_run, force=args.force)
            else:
                reset_results = [
                    reset_series(args.workload, engine=args.engine, dry_run=args.dry_run, force=args.force)
                ]
            for reset_result in reset_results:
                for line in reset_result.summary_lines():
                    print(line)
            if any(r.refused_reason for r in reset_results):
                sys.exit(1)

        else:
            sweep_parser.print_usage()
            sys.exit(1)


if __name__ == "__main__":
    main()
