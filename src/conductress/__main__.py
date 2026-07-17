"""Unified entry point for Conductress."""

import argparse
import logging
import sys

from conductress.config import CONDUCTRESS_LOG


def _configure_setup_console_logging() -> logging.Logger:
    """Mirror INFO+ from the whole conductress package to the console.

    bootstrap.py logs through its module logger (conductress.bootstrap);
    configuring only __main__'s logger here left fatal errors (e.g. the
    missing-keyfile sys.exit in ensure_ssh_key) invisible on the console.
    Attaching the handler to the package logger covers every module.
    """
    logger = logging.getLogger("conductress")
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)
    return logger


def main() -> None:
    parser = argparse.ArgumentParser(prog="conductress", description="Valkey Conductress")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("tui", help="Launch the TUI")
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
    subparsers.add_parser("setup", help="Run setup/bootstrap")
    subparsers.add_parser("queue", help="Manage the task queue (list, add, remove)")
    subparsers.add_parser("compare", help="Run analysis/comparison")
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

    # Sweep control commands
    focus_parser = sweep_sub.add_parser("focus", help="Focus on a single workload (others paused)")
    focus_parser.add_argument("workload", help="Workload ID to focus on (e.g. memory-set-64b, throughput)")
    pause_parser = sweep_sub.add_parser("pause", help="Pause specific sweeps")
    pause_parser.add_argument("workloads", nargs="+", help="Workload IDs to pause")
    sweep_sub.add_parser("resume", help="Resume all sweeps (remove focus/pause)")
    sweep_sub.add_parser("list", help="List all workload IDs and current scheduling config")

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
        from conductress.tui import BenchmarkApp

        app = BenchmarkApp()
        app.run()

    elif args.command == "run":
        import asyncio
        import json
        import traceback
        from datetime import datetime
        from pathlib import Path

        from conductress.config import PROJECT_ROOT
        from conductress.task_runner import TaskRunner

        crash_file = PROJECT_ROOT / "last_crash.json"
        repo_path = Path(args.repo) if args.repo else None
        runner = TaskRunner(
            sweep=args.sweep,
            memory_sweep=args.memory_sweep,
            repo_path=repo_path,
            publish_target=args.publish,
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

        logger = _configure_setup_console_logging()
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

    elif args.command == "compare":
        from conductress.analysis import main as analysis_main

        sys.exit(analysis_main(remaining))

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
                output = (
                    Path(args.output)
                    if args.output
                    else Path(f"series-{platform}-{coord.workload_id}-{coord.metric_id}.json")
                )
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
            from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS
            from conductress.sweep_config import load_sweep_config

            cfg = load_sweep_config()
            workloads = ["throughput"] + [f"memory-{wl.label}" for wl in MEMORY_WORKLOADS]
            print(f"Mode: {cfg.mode}" + (f" (target: {cfg.target})" if cfg.target else ""))
            print(f"\nWorkload IDs:")
            for wid in workloads:
                status = "✓" if cfg.is_allowed(wid) else "✗ paused"
                print(f"  {wid:<30} {status}")

        else:
            sweep_parser.print_usage()
            sys.exit(1)


if __name__ == "__main__":
    main()
