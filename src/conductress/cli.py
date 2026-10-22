"""Non-interactive CLI for queuing and managing benchmark tasks."""

import argparse
import itertools
import json
import logging
import sys
import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from . import config
from .cachecannon import DEFAULT_CACHECANNON_BINARY
from .fleet_client import FleetClientError
from .task_queue import BaseTaskData, TaskQueue
from .tasks.task_perf_benchmark import PerfTaskData
from .tasks.task_replica_read import (
    DEFAULT_MAX_LAG_SECONDS,
    DEFAULT_MAX_LAG_SLOPE,
    DEFAULT_READ_RATE_MAX,
    DEFAULT_READ_RATE_START,
    DEFAULT_READ_RATE_STEP,
    DEFAULT_READ_RATE_TOLERANCE,
)
from .topology import TopologySpec
from .utility import HumanByte, HumanTime, validate_cpulist

if TYPE_CHECKING:
    from .sweep.memory_coordinator import MemoryWorkload

logger = logging.getLogger(__name__)


def validate_source(source: str) -> bool:
    """Check whether a source string is a recognized repository name or manually uploaded."""
    return source in config.REPO_NAMES or source == config.MANUALLY_UPLOADED


def generate_task_combinations(
    tests: List[str],
    sizes: List[int],
    io_threads: List[int],
    pipelining: List[int],
    key_sizes: List[int],
) -> List[Tuple[str, int, int, int, int]]:
    """Compute the Cartesian product of multi-valued benchmark parameters."""
    return list(itertools.product(tests, sizes, io_threads, pipelining, key_sizes))


def _parse_comma_separated_ints(value: str, name: str) -> List[int]:
    """Parse a comma-separated string of integers."""
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if not parts:
        raise ValueError(f"--{name} cannot be empty")
    result = []
    for part in parts:
        try:
            result.append(int(part))
        except ValueError:
            raise ValueError(f"Invalid integer in --{name}: '{part}'")
    return result


def _parse_comma_separated_bytes(value: str, name: str) -> List[int]:
    """Parse a comma-separated string of human-readable byte values."""
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if not parts:
        raise ValueError(f"--{name} cannot be empty")
    result = []
    for part in parts:
        try:
            val = HumanByte.from_human(part)
            result.append(int(val))
        except ValueError:
            raise ValueError(f"Invalid byte value in --{name}: '{part}'")
    return result


def _parse_human_time(value: str, name: str) -> int:
    """Parse a human-readable time value."""
    try:
        return int(HumanTime.from_human(value))
    except ValueError:
        raise ValueError(f"Invalid time value for --{name}: '{value}'")


def _parse_tests(value: str) -> List[str]:
    """Parse a comma-separated list of test names."""
    tests = [t.strip() for t in value.split(",") if t.strip()]
    if not tests:
        raise ValueError("No tests specified")
    return tests


def _parse_bytes(value: str, name: str) -> int:
    """Parse one human-readable byte value, naming the flag in the error."""
    try:
        return int(HumanByte.from_human(value))
    except ValueError as exc:
        raise ValueError(f"--{name}: {exc}") from exc


def _check_cpulist(value: str, name: str) -> None:
    """Validate a cpulist flag, naming the flag in the error."""
    try:
        validate_cpulist(value)
    except ValueError as exc:
        raise ValueError(f"--{name}: {exc}") from exc


def _source_is_valid(source: str) -> bool:
    """True if ``source`` is a known repository; otherwise print the error and return False."""
    if validate_source(source):
        return True
    valid_sources = config.REPO_NAMES + [config.MANUALLY_UPLOADED]
    print(f"Error: Invalid source '{source}'. Valid: {', '.join(valid_sources)}", file=sys.stderr)
    return False


# --- arguments shared by every queue add-* command -------------------------------
#
# Every task type takes the same build identity (--source/--specifier), the same
# run-length knobs (--warmup/--duration/--repetitions) and the same footer
# (--note/--make-args). Each helper takes the per-command help text where the
# wording carries information (what "duration" means for that task) and fixes
# the flag names, types and defaults so the commands cannot drift apart.

SOURCE_HELP = "Repository source name (default: valkey)"
SPECIFIER_HELP = "Branch, tag, or commit (default: unstable)"
WARMUP_HELP = "Warmup duration (e.g., 30s, 1m)"
DURATION_HELP = "Test duration (e.g., 5m, 30s)"
REPETITIONS_HELP = "Number of repetitions"


def _add_source_args(parser: argparse.ArgumentParser, specifier_help: str = SPECIFIER_HELP) -> None:
    """--source / --specifier: which build to test."""
    parser.add_argument("--source", default="valkey", help=SOURCE_HELP)
    parser.add_argument("--specifier", default="unstable", help=specifier_help)


def _add_run_length_args(
    parser: argparse.ArgumentParser,
    *,
    warmup: Optional[str] = WARMUP_HELP,
    duration: Optional[str] = DURATION_HELP,
    repetitions: str = REPETITIONS_HELP,
    warmup_default: int = config.DEFAULT_WARMUP,
    duration_default: int = config.DEFAULT_DURATION,
    repetitions_default: int = config.DEFAULT_REPETITIONS,
) -> None:
    """--warmup / --duration / --repetitions. Pass ``None`` to omit a flag the task has no use for.

    The ``*_default`` overrides exist for a command whose cells should match a
    sweep's shape out of the box; the flag names, types and help format stay
    shared either way.
    """
    if warmup is not None:
        parser.add_argument("--warmup", default=f"{warmup_default}s", help=f"{warmup}. Default: {warmup_default}s")
    if duration is not None:
        parser.add_argument(
            "--duration", default=f"{duration_default}s", help=f"{duration}. Default: {duration_default}s"
        )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=repetitions_default,
        help=f"{repetitions}. Default: {repetitions_default}",
    )


def _add_note_and_build_args(parser: argparse.ArgumentParser, *, plural: bool = False) -> None:
    """--note / --make-args: the footer every task carries into its result row."""
    parser.add_argument("--note", default="", help=f"Optional note for the task{'s' if plural else ''}")
    parser.add_argument(
        "--make-args",
        default=config.DEFAULT_MAKE_ARGS,
        help=f"Build arguments. Default: '{config.DEFAULT_MAKE_ARGS}'",
    )


def _add_role_args(parser: argparse.ArgumentParser) -> None:
    """--primary-args / --replica-args: per-role server arguments, appended after --server-args."""
    parser.add_argument(
        "--primary-args", default="", help="Extra raw server arguments for the primary only (after --server-args)"
    )
    parser.add_argument(
        "--replica-args",
        default="",
        help="Extra raw server arguments for replicas only (after --server-args); the natural A/B lever for a "
        "replica-side config change",
    )


def _add_topology_args(parser: argparse.ArgumentParser) -> None:
    """--topology plus the per-role arguments, for tasks that bring servers up through TopologyGroup."""
    parser.add_argument(
        "--topology",
        default="standalone",
        help="Server layout: 'standalone' (one instance; the sweep-comparable shape, default) or 'replica:N' "
        "(primary plus N replicas on consecutive ports of the runner host; the generator still targets the "
        "primary, so this measures the primary while it replicates). Non-standalone results are NOT "
        "sweep-comparable.",
    )
    _add_role_args(parser)


def _parse_topology(args: argparse.Namespace) -> TopologySpec:
    """Build the TopologySpec for --topology / --primary-args / --replica-args; raises ValueError on bad input."""
    value = args.topology.strip().lower()
    if value == "standalone":
        replicas = 0
    elif value.startswith("replica:"):
        replicas = int(value.split(":", 1)[1])
    else:
        raise ValueError(f"--topology must be 'standalone' or 'replica:N', got {args.topology!r}")
    return TopologySpec.on_host_replicas(replicas, primary_args=args.primary_args, replica_args=args.replica_args)


def _describe_topology(spec: TopologySpec) -> str:
    """One line for the submission summary; flags the comparability consequence."""
    if spec.is_standalone:
        return "standalone"
    ports = ", ".join(f":{i.port}" for i in spec.replicas)
    return f"primary :{spec.primary.port} + {len(spec.replicas)} replica(s) ({ports}) -- NOT sweep-comparable"


def _topology_or_none(args: argparse.Namespace) -> Optional[TopologySpec]:
    """The handler-side boundary for --topology: print the error and return None on bad input."""
    try:
        return _parse_topology(args)
    except ValueError as exc:
        print(f"Error (--topology): {exc}", file=sys.stderr)
        return None


def _add_cachecannon_binary_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--cachecannon-binary",
        default=DEFAULT_CACHECANNON_BINARY,
        help=f"Path to cachecannon binary (default: {DEFAULT_CACHECANNON_BINARY})",
    )


def _add_perf_args(parser: argparse.ArgumentParser) -> None:
    """Add performance benchmark arguments to a parser."""
    _add_source_args(parser)
    parser.add_argument("--tests", required=True, help="Comma-separated test names (e.g., get,set)")
    parser.add_argument(
        "--sizes",
        default=str(config.DEFAULT_VAL_SIZE),
        help=f"Comma-separated value sizes (e.g., 16,512,1KB). Default: {config.DEFAULT_VAL_SIZE}",
    )
    parser.add_argument(
        "--io-threads",
        default=str(config.DEFAULT_IO_THREADS),
        help=f"Comma-separated IO thread counts (e.g., 1,9). Default: {config.DEFAULT_IO_THREADS}",
    )
    parser.add_argument(
        "--pipelining",
        default=str(config.DEFAULT_PIPELINING),
        help=f"Comma-separated pipelining values (e.g., 1,4,10). Default: {config.DEFAULT_PIPELINING}",
    )
    _add_run_length_args(
        parser, duration="Test duration (e.g., 5m, 15m)", repetitions="Number of repetitions per config"
    )
    parser.add_argument(
        "--key-sizes",
        default=str(config.DEFAULT_KEY_SIZE),
        help=f"Comma-separated key sizes in bytes (0=standard). Default: {config.DEFAULT_KEY_SIZE}",
    )
    _add_note_and_build_args(parser, plural=True)
    _add_topology_args(parser)
    parser.add_argument(
        "--perf-stat",
        action="store_true",
        help="Enable perf stat hardware counter collection",
    )
    parser.add_argument("--no-preload", action="store_true", help="Disable key preloading")
    parser.add_argument(
        "--server-cpus",
        default="",
        help="Expert: explicit cpulist override for server (e.g. '0-3,8-11'), " "bypasses topology-aware allocation",
    )
    parser.add_argument(
        "--client-cpus",
        default="",
        help="Expert: explicit cpulist override for benchmark client (e.g. '16-23'), "
        "bypasses topology-aware allocation",
    )
    parser.add_argument(
        "--bench-threads",
        type=int,
        default=0,
        help="Expert: valkey-benchmark --threads override (0 = default 16). "
        "Also sizes the benchmark CPU allocation.",
    )
    parser.add_argument(
        "--bench-clients",
        type=int,
        default=0,
        help="Expert: valkey-benchmark total connections override (0 = default 1200)",
    )
    parser.add_argument(
        "--client-netns",
        type=str,
        default="",
        help="Expert: run the benchmark client inside this network namespace "
        "(dual-ENI real-NIC hairpin; requires host setup per docs/real-nic-hairpin.md). "
        "Empty = default namespace (loopback path).",
    )
    parser.add_argument(
        "--bench-binary",
        type=str,
        default="",
        help="Expert: absolute path to an alternative benchmark binary (generator A/Bs). "
        "The client is part of the workload definition -- results are NOT comparable with "
        "sweep history; the override is recorded in result metadata. Empty = repo default.",
    )
    parser.add_argument(
        "--server-args",
        default="",
        help="Extra raw server arguments appended to the valkey-server command line (e.g. '--io-threads-ownership yes'). Appended last, overriding generated defaults",
    )


def _add_remote_routing_args(parser: argparse.ArgumentParser) -> None:
    routing = parser.add_mutually_exclusive_group()
    routing.add_argument("--runner", help="Submit to a specific fleet runner instead of the local queue")
    routing.add_argument("--platform", help="Submit to the unique enabled runner matching this platform")
    parser.add_argument("--priority", type=int, default=100, help="Remote queue priority (default: 100)")
    parser.add_argument(
        "--batch",
        dest="batch_id",
        default=None,
        help="Group this submission's tasks under a batch id (for remote cancel/status)",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable submission output")


def _submit_tasks(tasks: List[BaseTaskData], args: argparse.Namespace) -> dict:
    runner_arg = getattr(args, "runner", None)
    platform_arg = getattr(args, "platform", None)
    if not runner_arg and not platform_arg:
        queue = TaskQueue()
        for task in tasks:
            queue.submit_task(task)
        return {
            "destination": "local",
            "runner_id": None,
            "tasks": [{"task_id": task.task_id, "created": True, "state": "queued"} for task in tasks],
        }

    from .fleet_cli import resolve_runner
    from .fleet_client import FleetClient
    from .task_envelope import build_task_envelope

    client = FleetClient.from_env()
    runner_id = resolve_runner(client, runner_id=runner_arg, platform=platform_arg)
    # Every remote submission belongs to a batch so a sequence can be cancelled
    # or inspected as a unit. Honour an explicit --batch; otherwise mint one.
    batch_id = getattr(args, "batch_id", None) or f"batch-{uuid.uuid4().hex[:16]}"
    submitted: list[dict[str, Any]] = []
    for task in tasks:
        envelope = build_task_envelope(task, runner_id=runner_id, priority=args.priority, batch_id=batch_id)
        try:
            document = client.submit_task(envelope, idempotency_key=f"{runner_id}:{task.task_id}")
        except FleetClientError as exc:
            if not submitted:
                raise
            submitted_ids = [item["task_id"] for item in submitted]
            details = {"runner_id": runner_id, "submitted": submitted}
            raise FleetClientError(
                exc.code,
                f"{exc.message}; {len(submitted)} task(s) were already submitted: " f"{', '.join(submitted_ids)}",
                exc.exit_code,
                exc.status,
                details,
            ) from exc
        remote_task = document["task"]
        submitted.append(
            {
                "task_id": remote_task["task_id"],
                "created": document["created"],
                "state": remote_task["state"],
            }
        )
    return {"destination": "remote", "runner_id": runner_id, "batch_id": batch_id, "tasks": submitted}


def _finish_submission(result: dict, args: argparse.Namespace) -> bool:
    """Print generic JSON or remote destination details; return True if done."""
    if getattr(args, "json", False):
        print(json.dumps({"schema_version": 1, "command": "queue.submit", "data": result}, indent=2, sort_keys=True))
        return True
    if result["destination"] == "remote":
        created = sum(1 for task in result["tasks"] if task["created"])
        replayed = len(result["tasks"]) - created
        suffix = f" ({replayed} idempotent replay)" if replayed else ""
        batch = result.get("batch_id")
        batch_note = f" [batch {batch}]" if batch else ""
        print(f"Remote destination: {result['runner_id']} — {created} task(s) submitted{suffix}{batch_note}")
    return False


class _TaskSubmitter:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.tasks: List[BaseTaskData] = []

    def submit_task(self, task: BaseTaskData) -> None:
        self.tasks.append(task)

    def finish(self) -> dict:
        return _submit_tasks(self.tasks, self.args)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="conductress",
        description="Conductress CLI for queuing and managing benchmark tasks",
    )
    subparsers = parser.add_subparsers(dest="command")

    # queue subcommand with its own subcommands
    queue_parser = subparsers.add_parser("queue", help="Manage the task queue")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", title="commands")

    # queue list
    queue_sub.add_parser("list", help="List all pending tasks")

    # queue add
    add_parser = queue_sub.add_parser("add", help="Add performance benchmark tasks to the queue")
    _add_perf_args(add_parser)

    # queue add-insertion
    insertion_parser = queue_sub.add_parser(
        "add-insertion", help="Add a finite new-key-only SET task with explicit memory bounds"
    )
    _add_source_args(insertion_parser)
    insertion_parser.add_argument(
        "--insertions", required=True, help="Exact unique key count (supports K/M/G suffixes)"
    )
    insertion_parser.add_argument("--size", default="16", help="Value size (default: 16 bytes)")
    insertion_parser.add_argument("--key-size", default="16", help="Key size (default: 16 bytes)")
    insertion_parser.add_argument("--io-threads", type=int, default=config.DEFAULT_IO_THREADS)
    insertion_parser.add_argument("--pipelining", type=int, default=config.DEFAULT_PIPELINING)
    _add_run_length_args(insertion_parser, warmup=None, duration=None)
    insertion_parser.add_argument("--maxmemory", required=True, help="Valkey maxmemory bound (for example 8GB)")
    insertion_parser.add_argument("--max-rss", required=True, help="RSS abort ceiling (for example 12GB)")
    insertion_parser.add_argument("--perf-stat", action="store_true", help="Enable perf stat hardware counters")
    insertion_parser.add_argument("--server-cpus", default="", help="Expert: explicit server cpulist")
    insertion_parser.add_argument("--client-cpus", default="", help="Expert: explicit benchmark-client cpulist")
    insertion_parser.add_argument("--bench-threads", type=int, default=0, help="Expert: client thread count")
    insertion_parser.add_argument("--bench-clients", type=int, default=0, help="Expert: total client connections")
    insertion_parser.add_argument("--server-args", default="", help="Extra raw server arguments")
    _add_note_and_build_args(insertion_parser)
    _add_topology_args(insertion_parser)

    # queue add-memory
    mem_parser = queue_sub.add_parser("add-memory", help="Add memory efficiency tasks to the queue")
    _add_source_args(mem_parser, specifier_help="Branch, tag, commit, or path (default: unstable)")
    mem_parser.add_argument(
        "--types",
        default="set,sadd,zadd,hset",
        help="Comma-separated data types (default: set,sadd,zadd,hset)",
    )
    mem_parser.add_argument(
        "--sizes",
        default="",
        help="Comma-separated value/member sizes in bytes (e.g., 8,20,64). One task is queued per "
        "type per size, with per-item user data derived per type (set: key+value, zadd: member+8 "
        "score bytes, sadd: member, hset: field+value). Default: sizes from the standard workload "
        "config (set v64, zadd m20, sadd m20, hset f64-v64).",
    )
    mem_parser.add_argument(
        "--key-size",
        default="",
        help="Key size in bytes for set (e.g., 32). Overrides the standard workload's key size; "
        "per-item user data is recomputed as key+value. Only affects set.",
    )
    mem_parser.add_argument(
        "--field-size",
        default="",
        help="Field name size in bytes for hset (e.g., 16). Overrides the standard workload's "
        "field size; per-item user data is recomputed as field+value. Only affects hset.",
    )
    mem_parser.add_argument("--expire", action="store_true", help="Also test with expiration enabled")
    mem_parser.add_argument(
        "--populate-mode",
        choices=["random", "sequential", "churn"],
        default="random",
        help="zadd insertion pattern (default: random). sequential=dense/best-case, "
        "churn=50/50 add-delete steady state. Only affects zadd.",
    )
    _add_note_and_build_args(mem_parser, plural=True)
    mem_parser.add_argument(
        "--settle",
        action="store_true",
        help="Measure steady state instead of the post-fill transient: quiesce until "
        "used_memory plateaus (background reclamation, e.g. zset compaction), then read "
        "every item once so any incremental rehash the fill left behind completes, and "
        "fail the task if DEBUG HTSTATS still shows a rehash in progress. Default off.",
    )

    # queue add-mixed
    mixed_parser = queue_sub.add_parser(
        "add-mixed",
        help="Add a mixed GET/SET throughput task (cachecannon, the epoch-3 sweep shape by default)",
    )
    _add_source_args(mixed_parser)
    mixed_parser.add_argument(
        "--set-ratio",
        type=int,
        required=True,
        help="Percentage of SET commands (0-100). E.g. 20 = 20%% SET / 80%% GET.",
    )
    mixed_parser.add_argument(
        "--sizes",
        default=str(config.SWEEP_V3_VAL_SIZE),
        help=f"Comma-separated value sizes (e.g., 16,512,1KB). Default: {config.SWEEP_V3_VAL_SIZE}",
    )
    mixed_parser.add_argument(
        "--io-threads",
        default=str(config.SWEEP_V3_IO_THREADS),
        help=f"Comma-separated IO thread counts. Default: {config.SWEEP_V3_IO_THREADS}",
    )
    mixed_parser.add_argument(
        "--pipelining",
        default=str(config.SWEEP_V3_PIPELINING),
        help=f"Comma-separated pipelining values. Default: {config.SWEEP_V3_PIPELINING}",
    )
    _add_run_length_args(
        mixed_parser,
        warmup="Warmup before the scored window (0s disables)",
        duration="Scored window per repetition",
        warmup_default=config.SWEEP_V3_WARMUP,
        duration_default=config.SWEEP_V3_DURATION,
        repetitions_default=config.SWEEP_V3_REPETITIONS,
    )
    _add_note_and_build_args(mixed_parser, plural=True)
    _add_topology_args(mixed_parser)
    mixed_parser.add_argument("--perf-stat", action="store_true", help="Enable perf stat hardware counter collection")
    mixed_parser.add_argument(
        "--server-cpus",
        default="",
        help="Expert: explicit cpulist override for server",
    )
    mixed_parser.add_argument(
        "--client-cpus",
        default="",
        help="Expert: explicit cpulist override for benchmark client",
    )
    mixed_parser.add_argument(
        "--server-args",
        default="",
        help="Extra raw server arguments appended to the valkey-server command line (e.g. '--io-threads-ownership yes'). Appended last, overriding generated defaults",
    )
    mixed_parser.add_argument(
        "--connections",
        type=int,
        default=config.SWEEP_V3_CONNECTIONS,
        help=f"Total client connections. Default: {config.SWEEP_V3_CONNECTIONS}",
    )
    mixed_parser.add_argument(
        "--threads",
        type=int,
        default=config.SWEEP_V3_CLIENT_THREADS,
        help=f"Client worker threads. Default: {config.SWEEP_V3_CLIENT_THREADS}",
    )
    mixed_parser.add_argument(
        "--keyspace",
        type=int,
        default=config.SWEEP_V3_KEYSPACE,
        help=f"Number of keys. Default: {config.SWEEP_V3_KEYSPACE}",
    )

    # queue add-scenario
    scenario_parser = queue_sub.add_parser(
        "add-scenario", help="Add a pathological-workload scenario task (background GET + overlay)"
    )
    scenario_parser.add_argument(
        "--scenario",
        required=True,
        choices=[
            "eval-storm",
            "scan-churn",
            "multi-exec",
            "flushall-spike",
            "expiry-heavy",
            "bgsave",
            "large-value-reader",
            "connection-storm",
        ],
        help="Pathological workload scenario to run. 'bgsave' fires a single BGSAVE at ~40%% of "
        "duration; fork+COW impact shows up in the interval timeseries. Dataset size (prefill) "
        "drives the fork cost. 'large-value-reader' measures how continuous large-value GETs "
        "from a dedicated keyset degrade background workload throughput/latency. "
        "'connection-storm' overlays a burst of reconnecting clients (optionally while a stall "
        "blocks the main thread); parameterise it with the --storm-* flags.",
    )
    _add_source_args(scenario_parser)
    scenario_parser.add_argument(
        "--io-threads",
        default=str(config.DEFAULT_IO_THREADS),
        help=f"IO thread count. Default: {config.DEFAULT_IO_THREADS}",
    )
    scenario_parser.add_argument(
        "--pipelining",
        default=str(config.DEFAULT_PIPELINING),
        help=f"Pipeline depth for background GET. Default: {config.DEFAULT_PIPELINING}",
    )
    _add_run_length_args(scenario_parser, warmup=None)
    _add_note_and_build_args(scenario_parser)
    _add_topology_args(scenario_parser)
    scenario_parser.add_argument(
        "--perf-stat", action="store_true", help="Enable perf stat hardware counter collection"
    )
    scenario_parser.add_argument(
        "--server-cpus",
        default="",
        help="Expert: explicit cpulist override for server",
    )
    scenario_parser.add_argument(
        "--client-cpus",
        default="",
        help="Expert: explicit cpulist override for benchmark client",
    )
    scenario_parser.add_argument(
        "--server-args",
        default="",
        help="Extra raw server arguments appended to the valkey-server command line (e.g. '--io-threads-ownership yes'). Appended last, overriding generated defaults",
    )
    scenario_parser.add_argument(
        "--background-set-ratio",
        type=int,
        default=0,
        help="Percentage of SET commands in background load (0-100, default 0 = pure GET). "
        "Higher values add write pressure alongside the scenario overlay. "
        "0 preserves comparability with existing pure-GET baselines.",
    )
    scenario_parser.add_argument(
        "--overlay-value-size",
        type=int,
        default=0,
        help="Value size in bytes for the large-value-reader overlay's dedicated keyset "
        "(default: 10240 = 10KB). Only used when --scenario=large-value-reader; ignored otherwise.",
    )
    scenario_parser.add_argument(
        "--server-sample-ms",
        type=int,
        default=None,
        help="Poll the server's INFO counters every N ms during the measurement (0 = off, else >= 20). "
        "connection-storm defaults to 100 when this flag is absent; other scenarios default to off.",
    )
    scenario_parser.add_argument(
        "--tls",
        action="store_true",
        help="connection-storm only: run the herd over TLS on a dedicated TLS port (plaintext port + 1). "
        "The plaintext port still carries prefill/INFO/memtier/probe; builds with BUILD_TLS=yes.",
    )
    scenario_parser.add_argument(
        "--background",
        choices=["memtier", "none"],
        default="memtier",
        help="connection-storm only: background load. 'memtier' (default) runs the steady GET load; "
        "'none' skips memtier so the fixed-rate probe is the goodput measure and dip metrics are empty.",
    )
    scenario_parser.add_argument(
        "--server-sample-fields",
        default="",
        help="Comma-separated extra INFO fields the sampler records per tick (any section); an unknown "
        "field records null, so a patched build's counter appears without a code change.",
    )
    # connection-storm overlay parameters -- serialized into overlay_spec (JSON).
    # Only valid with --scenario connection-storm; rejected otherwise.
    storm_group = scenario_parser.add_argument_group(
        "connection-storm overlay", "Only valid with --scenario connection-storm"
    )
    storm_group.add_argument("--storm-clients", type=int, default=None, help="Storm client population (default: 2000)")
    storm_group.add_argument(
        "--storm-burst-ms", type=int, default=None, help="Window first-attempts are spread over (default: 200)"
    )
    storm_group.add_argument(
        "--storm-connect-timeout-ms", type=float, default=None, help="Per-connect timeout in ms (default: 1000)"
    )
    storm_group.add_argument(
        "--storm-reply-timeout-ms", type=float, default=None, help="Per-reply timeout in ms (default: 500)"
    )
    storm_group.add_argument(
        "--storm-policy",
        default=None,
        help="Reconnect policy: immediate, fixed:<ms>, exp:<base_ms>:<max_ms>[:jitter] (default: fixed:200)",
    )
    storm_group.add_argument(
        "--storm-stall", default=None, help="Stall injector: none or debug-sleep:<seconds> (default: none)"
    )
    storm_group.add_argument(
        "--storm-burst-after-stall-ms",
        type=int,
        default=None,
        help="Stall-first: start the burst this many ms after the stall is issued (default: 200)",
    )
    storm_group.add_argument(
        "--storm-start-delay-s",
        type=float,
        default=None,
        help="Launch the storm generator this many seconds after the overlay starts, so the background "
        "series has an undisturbed baseline before the stall (default: 5)",
    )
    storm_group.add_argument(
        "--storm-stall-after-s",
        type=float,
        default=None,
        help="Seconds into the storm generator's run at which the stall is injected, in both orderings "
        "(default: 1). Use with --storm-burst-first and a large value (e.g. 30) to have the herd connected "
        "and the probe running a baseline before the stall lands.",
    )
    storm_group.add_argument(
        "--storm-burst-first",
        action="store_true",
        help="Legacy ordering: burst first, then stall (default is stall-first)",
    )
    storm_group.add_argument(
        "--storm-prewarm-connections",
        type=int,
        default=None,
        help="Throwaway connections opened before the baseline (default: = --storm-clients; 0 disables)",
    )
    storm_group.add_argument(
        "--storm-workers",
        type=int,
        default=None,
        help="Worker processes clients fan out across (default: 0 = auto, min(8, cpu_count))",
    )
    storm_group.add_argument(
        "--storm-handshake",
        action="append",
        default=None,
        help="Handshake command each client sends after connect, repeatable (default: 'HELLO 3')",
    )
    storm_group.add_argument(
        "--storm-first-command",
        default=None,
        help="First command each client issues after the handshake (default: 'GET storm:key')",
    )
    storm_group.add_argument(
        "--storm-bind-addrs",
        default=None,
        help="','-separated loopback source addresses to spread ephemeral ports across",
    )
    storm_group.add_argument(
        "--storm-herd-command-interval-ms",
        type=float,
        default=None,
        help="Active herd: a connected client re-sends --storm-first-command every N ms and applies the "
        "reply timeout; a timeout reconnects (outcome reply_timeout_steady). 0 (default) is the idle hold.",
    )
    storm_group.add_argument(
        "--storm-probe-clients",
        type=int,
        default=None,
        help="Fixed-rate goodput probe: this many open-loop clients (0 disables). Requires --storm-probe-rate.",
    )
    storm_group.add_argument(
        "--storm-probe-rate",
        type=int,
        default=None,
        help="Aggregate commands/s the probe sends across its clients (open-loop). Requires --storm-probe-clients.",
    )
    storm_group.add_argument(
        "--storm-probe-tls",
        action="store_true",
        help="Run the probe over TLS too (default: the probe uses the plaintext port, measuring main-thread goodput)",
    )

    # queue add-latency
    lat_parser = queue_sub.add_parser(
        "add-latency",
        help="Add a latency measurement task (cachecannon at a fixed request rate, scored on p99)",
    )
    lat_parser.add_argument("source", help="Source repo name (e.g. 'valkey')")
    lat_parser.add_argument("specifier", help="Commit hash or branch to test")
    lat_parser.add_argument(
        "target_rps",
        type=int,
        help=f"Fixed request rate shared across all connections (the epoch-3 sweep uses {config.SWEEP_V3_LATENCY_RATE})",
    )
    lat_parser.add_argument("--note", default="", help="Optional note for the task")
    lat_parser.add_argument(
        "--server-args",
        default="",
        help="Extra raw server arguments appended to the valkey-server command line "
        "(e.g. '--io-threads-ownership yes'). Appended last, overriding generated defaults",
    )
    lat_parser.add_argument(
        "--set-ratio",
        type=int,
        default=0,
        help="Percentage of SET commands (0-100, default 0 = GET-only). The recorded p99 is the dominant command's.",
    )
    lat_parser.add_argument(
        "--value-size",
        type=int,
        default=config.SWEEP_V3_VAL_SIZE,
        help=f"Value size in bytes (default: {config.SWEEP_V3_VAL_SIZE})",
    )
    lat_parser.add_argument(
        "--io-threads",
        type=int,
        default=config.SWEEP_V3_IO_THREADS,
        help=f"Server io-threads (default: {config.SWEEP_V3_IO_THREADS})",
    )
    lat_parser.add_argument(
        "--connections",
        type=int,
        default=config.SWEEP_V3_CONNECTIONS,
        help=f"Total client connections (default: {config.SWEEP_V3_CONNECTIONS})",
    )
    lat_parser.add_argument(
        "--threads",
        type=int,
        default=config.SWEEP_V3_CLIENT_THREADS,
        help=f"Client worker threads (default: {config.SWEEP_V3_CLIENT_THREADS})",
    )
    lat_parser.add_argument(
        "--repetitions",
        type=int,
        default=config.SWEEP_V3_LATENCY_REPETITIONS,
        help=f"Repetitions, each with a fresh server (default: {config.SWEEP_V3_LATENCY_REPETITIONS})",
    )

    _add_topology_args(lat_parser)
    # queue add-cachecannon
    cc_parser = queue_sub.add_parser(
        "add-cachecannon",
        help="Add a cachecannon benchmark task (second-opinion generator, NOT sweep-comparable)",
    )
    _add_source_args(cc_parser)
    cc_parser.add_argument(
        "--test",
        default="get",
        choices=["get", "set"],
        help="Command to bench (default: get)",
    )
    cc_parser.add_argument(
        "--sizes",
        default=str(config.DEFAULT_VAL_SIZE),
        help=f"Value size in bytes (e.g., 512, 1KB). Default: {config.DEFAULT_VAL_SIZE}",
    )
    cc_parser.add_argument(
        "--pipelining",
        type=int,
        default=config.DEFAULT_PIPELINING,
        help=f"Pipeline depth. Default: {config.DEFAULT_PIPELINING}",
    )
    cc_parser.add_argument(
        "--connections",
        type=int,
        default=1200,
        help="Total connections (default: 1200)",
    )
    cc_parser.add_argument(
        "--threads",
        type=int,
        default=config.PERF_BENCH_THREADS,
        help=f"Worker threads (default: {config.PERF_BENCH_THREADS})",
    )
    cc_parser.add_argument(
        "--io-threads",
        type=int,
        default=config.DEFAULT_IO_THREADS,
        help=f"Server IO threads (default: {config.DEFAULT_IO_THREADS})",
    )
    _add_run_length_args(cc_parser)
    cc_parser.add_argument(
        "--keyspace",
        type=int,
        default=config.PERF_BENCH_KEYSPACE,
        help=f"Keyspace count (default: {config.PERF_BENCH_KEYSPACE})",
    )
    cc_parser.add_argument(
        "--set-ratio",
        type=int,
        default=0,
        help="Percentage of SET commands (0-100) for a mixed GET/SET workload. "
        "0 (default) = pure workload per --test; >0 overrides --test "
        "(e.g. 30 = 30%% SET / 70%% GET).",
    )
    cc_parser.add_argument(
        "--distribution",
        default="uniform",
        choices=["uniform", "zipf"],
        help="Key distribution (default: uniform). 'zipf' concentrates traffic "
        "on hot keys (cachecannon-native zipf, no exponent knob).",
    )
    _add_cachecannon_binary_arg(cc_parser)
    cc_parser.add_argument(
        "--server-args",
        default="",
        help="Extra raw server arguments (e.g. '--io-threads-ownership yes')",
    )
    cc_parser.add_argument(
        "--server-cpus",
        default="",
        help="Expert: explicit cpulist override for server",
    )
    cc_parser.add_argument(
        "--client-cpus",
        default="",
        help="Expert: explicit cpulist override for cachecannon client",
    )
    cc_parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Concurrent cachecannon processes forming the generator (default: 1). "
        "--connections and --threads stay the totals and are divided across them; "
        "an explicit --client-cpus list is cut into disjoint slices. One process "
        "walls at roughly 22-25M ops/s; use >1 to drive a server past that.",
    )
    cc_parser.add_argument(
        "--client-netns",
        type=str,
        default="",
        help="Expert: run cachecannon inside this network namespace "
        "(dual-ENI real-NIC hairpin; requires host setup per docs/real-nic-hairpin.md). "
        "Empty = default namespace (loopback path).",
    )
    _add_note_and_build_args(cc_parser)
    _add_topology_args(cc_parser)
    cc_parser.add_argument(
        "--rate",
        type=int,
        default=0,
        help="Fixed total request rate in req/s across all connections (open loop). "
        "Default 0 = closed loop (each connection sends as fast as replies return)",
    )
    cc_parser.add_argument(
        "--perf-stat",
        action="store_true",
        help="Collect perf stat hardware counters per thread (main vs I/O threads) on every rep "
        "and a CPU flamegraph on the last rep",
    )
    cc_parser.add_argument(
        "--info-sections",
        default="",
        help="Comma-separated INFO sections (e.g. 'stats,io_uring') to snapshot at the start and "
        "end of each scored window; numeric field deltas are stored in the result",
    )

    # queue add-replica-read
    rr_parser = queue_sub.add_parser(
        "add-replica-read",
        help="Add a replica-read task: reads served by a replica while the primary ingests writes at a fixed rate",
    )
    _add_source_args(rr_parser)
    rr_parser.add_argument(
        "--replicas", type=int, default=1, help="Replica count; reads are measured at the first (default: 1)"
    )
    rr_parser.add_argument(
        "--io-threads", type=int, default=8, help="Replica io-threads -- the measured instance (default: 8)"
    )
    rr_parser.add_argument(
        "--primary-io-threads", type=int, default=1, help="Primary io-threads (default: 1; it only ingests writes)"
    )
    rr_parser.add_argument(
        "--write-rate",
        type=int,
        default=50_000,
        help="Fixed write rate at the primary in SET/s, the replication-stream rate (default: 50000)",
    )
    rr_parser.add_argument("--write-connections", type=int, default=16, help="Writer connections (default: 16)")
    rr_parser.add_argument("--write-threads", type=int, default=4, help="Writer generator threads (default: 4)")
    rr_parser.add_argument("--write-pipelining", type=int, default=1, help="Writer pipeline depth (default: 1)")
    rr_parser.add_argument(
        "--sizes",
        default=str(config.DEFAULT_VAL_SIZE),
        help=f"Value size in bytes (e.g., 512, 1KB). Default: {config.DEFAULT_VAL_SIZE}",
    )
    rr_parser.add_argument("--pipelining", type=int, default=1, help="Reader pipeline depth (default: 1)")
    rr_parser.add_argument("--connections", type=int, default=400, help="Reader connections (default: 400)")
    rr_parser.add_argument("--threads", type=int, default=8, help="Reader generator threads (default: 8)")
    rr_parser.add_argument(
        "--keyspace",
        type=int,
        default=config.PERF_BENCH_KEYSPACE,
        help=f"Keyspace count, shared by writer and reader (default: {config.PERF_BENCH_KEYSPACE})",
    )
    _add_run_length_args(rr_parser, warmup="Reader warmup (e.g., 10s)", duration="Reader measured duration (e.g., 30s)")
    rr_parser.add_argument(
        "--base-port", type=int, default=6379, help="Primary port; replicas take the following ports (default: 6379)"
    )
    rr_parser.add_argument("--server-args", default="", help="Extra raw server arguments for EVERY instance")
    _add_role_args(rr_parser)
    rr_parser.add_argument(
        "--sample-interval", type=float, default=1.0, help="INFO sampling cadence in seconds (default: 1.0)"
    )
    rr_parser.add_argument(
        "--info-fields",
        default="",
        help="Comma-separated extra INFO fields to sample from every instance, from any INFO section "
        "(e.g. counters a build under test exposes)",
    )
    rr_parser.add_argument(
        "--max-lag-seconds",
        type=float,
        default=DEFAULT_MAX_LAG_SECONDS,
        help="Guard: fail a rep whose replica sat more than this far behind the primary on average over the "
        f"scored window, measured in seconds of replication stream (default: {DEFAULT_MAX_LAG_SECONDS}). "
        "With --read-rate-search a probe is judged on its lag at the end of the window instead, so a probe "
        "that drains a startup backlog passes",
    )
    _add_cachecannon_binary_arg(rr_parser)
    rr_parser.add_argument(
        "--read-rate-search",
        action="store_true",
        help="Instead of one closed-loop reader, probe open-loop reader rates (climb then bisect) and score the "
        "highest rate at which the replica still keeps up with its primary; --warmup/--duration apply per probe",
    )
    rr_parser.add_argument(
        "--read-rate-start",
        type=int,
        default=DEFAULT_READ_RATE_START,
        help=f"Search: first reader rate to probe, GET/s (default: {DEFAULT_READ_RATE_START})",
    )
    rr_parser.add_argument(
        "--read-rate-max",
        type=int,
        default=DEFAULT_READ_RATE_MAX,
        help=f"Search: never probe above this reader rate; passing it ends the search (default: {DEFAULT_READ_RATE_MAX})",
    )
    rr_parser.add_argument(
        "--read-rate-step",
        type=float,
        default=DEFAULT_READ_RATE_STEP,
        help=f"Search: climb multiplier between passing probes (default: {DEFAULT_READ_RATE_STEP})",
    )
    rr_parser.add_argument(
        "--read-rate-tolerance",
        type=float,
        default=DEFAULT_READ_RATE_TOLERANCE,
        help="Search: stop bisecting when the pass/fail bracket is within this fraction of its upper end "
        f"(default: {DEFAULT_READ_RATE_TOLERANCE})",
    )
    rr_parser.add_argument(
        "--max-lag-slope",
        type=float,
        default=DEFAULT_MAX_LAG_SLOPE,
        help="Search: a probe fails when the replica's lag grows faster than this over the scored window, in "
        f"seconds of replication stream per second (default: {DEFAULT_MAX_LAG_SLOPE})",
    )
    rr_parser.add_argument("--client-cpus", default="", help="Expert: explicit cpulist override for both generators")
    rr_parser.add_argument(
        "--cpu-profile",
        action="store_true",
        help="perf record the measured replica (main + io threads) over the scored window of the last rep; "
        "collapsed stacks land on the row as cpu_stacks_main/cpu_stacks_io",
    )
    _add_note_and_build_args(rr_parser)

    for task_parser in (
        add_parser,
        insertion_parser,
        mem_parser,
        mixed_parser,
        scenario_parser,
        lat_parser,
        cc_parser,
        rr_parser,
    ):
        _add_remote_routing_args(task_parser)

    # queue remove
    remove_parser = queue_sub.add_parser("remove", help="Remove a task from the queue")
    remove_parser.add_argument("task_id", help="Task ID to remove (from 'queue list' output)")

    # queue clear
    queue_sub.add_parser("clear", help="Remove all pending tasks from the queue")

    # queue cancel — cancel a remote task or a whole batch
    cancel_parser = queue_sub.add_parser("cancel", help="Cancel a remote task or batch (by task id or batch id)")
    cancel_parser.add_argument("selector", help="Task id or batch id to cancel")
    cancel_parser.add_argument("--json", action="store_true")

    # queue mine — list the caller's remote tasks
    mine_parser = queue_sub.add_parser("mine", help="List your remote tasks and their state")
    mine_parser.add_argument("--limit", type=int, default=100)
    mine_parser.add_argument("--json", action="store_true")

    # queue pending — list tasks awaiting approval (approver only)
    pending_parser = queue_sub.add_parser("pending", help="List remote tasks awaiting approval")
    pending_parser.add_argument("--json", action="store_true")

    # queue approve / reject — resolve a pending task or batch (approver only)
    approve_parser = queue_sub.add_parser("approve", help="Approve a pending task or batch")
    approve_parser.add_argument("selector", help="Task id or batch id to approve")
    approve_parser.add_argument("--json", action="store_true")
    reject_parser = queue_sub.add_parser("reject", help="Reject a pending task or batch")
    reject_parser.add_argument("selector", help="Task id or batch id to reject")
    reject_parser.add_argument("--json", action="store_true")

    # plot subcommand
    plot_parser = subparsers.add_parser("plot", help="Render a figure from a task's results")
    plot_sub = plot_parser.add_subparsers(dest="plot_command", title="figures")
    storm_plot = plot_sub.add_parser(
        "connection-storm",
        help="Render the connection-storm figure (one column per task id, max 3)",
    )
    storm_plot.add_argument("task_ids", nargs="+", help="Task id(s) whose scenario result to plot (max 3)")
    storm_plot.add_argument("--out", required=True, help="Output PNG path")
    storm_plot.add_argument(
        "--rep", type=int, default=None, help="Plot a single repetition index (default: all, median bold)"
    )
    storm_plot.add_argument("--xrange", default=None, help="Axis range in seconds, e.g. -3:8")

    return parser


def handle_queue_add_insertion(args: argparse.Namespace) -> int:
    """Submit an exact finite new-key SET task with explicit memory bounds."""
    from conductress.tasks.task_perf_benchmark import BoundedInsertionTaskData
    from conductress.utility import HumanNumber

    if not _source_is_valid(args.source):
        return 1
    topology = _topology_or_none(args)
    if topology is None:
        return 1
    try:
        insertions = int(HumanNumber.from_human(args.insertions))
        val_size = int(HumanByte.from_human(args.size))
        key_size = int(HumanByte.from_human(args.key_size))
        maxmemory_bytes = int(HumanByte.from_human(args.maxmemory))
        max_rss_bytes = int(HumanByte.from_human(args.max_rss))
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    numeric = {
        "insertions": insertions,
        "size": val_size,
        "key-size": key_size,
        "io-threads": args.io_threads,
        "pipelining": args.pipelining,
        "repetitions": args.repetitions,
        "maxmemory": maxmemory_bytes,
        "max-rss": max_rss_bytes,
    }
    for name, value in numeric.items():
        if value < 1:
            print(f"Error: --{name} must be at least 1", file=sys.stderr)
            return 1
    if key_size < 16:
        print("Error: --key-size must be at least 16", file=sys.stderr)
        return 1
    if insertions > config.BENCHMARK_MAX_ITERATIONS:
        print(
            f"Error: --insertions must not exceed {config.BENCHMARK_MAX_ITERATIONS}",
            file=sys.stderr,
        )
        return 1
    if args.bench_threads < 0 or args.bench_clients < 0:
        print("Error: --bench-threads and --bench-clients must not be negative", file=sys.stderr)
        return 1
    if max_rss_bytes < maxmemory_bytes:
        print("Error: --max-rss must be greater than or equal to --maxmemory", file=sys.stderr)
        return 1
    try:
        validate_cpulist(args.server_cpus)
        validate_cpulist(args.client_cpus)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    try:
        task = BoundedInsertionTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            topology=topology,
            note=args.note,
            requirements={},
            val_size=val_size,
            key_size=key_size,
            io_threads=args.io_threads,
            pipelining=args.pipelining,
            insertions=insertions,
            repetitions=args.repetitions,
            maxmemory_bytes=maxmemory_bytes,
            max_rss_bytes=max_rss_bytes,
            perf_stat_enabled=args.perf_stat,
            server_cpu_override=args.server_cpus,
            benchmark_cpu_override=args.client_cpus,
            server_args=args.server_args,
            bench_threads=args.bench_threads,
            bench_clients=args.bench_clients,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    queue = _TaskSubmitter(args)
    queue.submit_task(task)
    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0
    print(f"Queued bounded insertion task: {insertions} unique SETs per repetition")
    print(f"  source={args.source} specifier={args.specifier}")
    print(f"  topology: {_describe_topology(topology)}")
    print(f"  key={key_size}B value={val_size}B io-threads={args.io_threads} pipeline={args.pipelining}")
    print(f"  maxmemory={maxmemory_bytes} max-rss={max_rss_bytes} reps={args.repetitions}")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def handle_queue_add(args: argparse.Namespace) -> int:
    """Handle 'queue add': validate inputs, generate tasks, and submit them."""
    if not _source_is_valid(args.source):
        return 1
    topology = _topology_or_none(args)
    if topology is None:
        return 1

    try:
        tests = _parse_tests(args.tests)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        sizes = _parse_comma_separated_bytes(args.sizes, "sizes")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        io_threads = _parse_comma_separated_ints(args.io_threads, "io-threads")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        pipelining = _parse_comma_separated_ints(args.pipelining, "pipelining")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        key_sizes = _parse_comma_separated_bytes(args.key_sizes, "key-sizes")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        warmup = _parse_human_time(args.warmup, "warmup")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        duration = _parse_human_time(args.duration, "duration")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.repetitions < 1:
        print("Error: Repetitions must be at least 1", file=sys.stderr)
        return 1

    # Validate CPU override syntax (reject malformed, don't validate topology)
    try:
        validate_cpulist(args.server_cpus)
    except ValueError as e:
        print(f"Error (--server-cpus): {e}", file=sys.stderr)
        return 1
    try:
        validate_cpulist(args.client_cpus)
    except ValueError as e:
        print(f"Error (--client-cpus): {e}", file=sys.stderr)
        return 1

    combinations = generate_task_combinations(tests, sizes, io_threads, pipelining, key_sizes)

    queue = _TaskSubmitter(args)
    for test, size, io_thread, pipeline, key_size in combinations:
        task = PerfTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            topology=topology,
            note=args.note,
            requirements={},
            test=test,
            val_size=size,
            io_threads=io_thread,
            pipelining=pipeline,
            warmup=warmup,
            duration=duration,
            perf_stat_enabled=args.perf_stat,
            has_expire=False,
            preload_keys=not args.no_preload,
            key_size=key_size,
            repetitions=args.repetitions,
            server_cpu_override=args.server_cpus,
            benchmark_cpu_override=args.client_cpus,
            server_args=args.server_args,
            bench_threads=args.bench_threads,
            bench_clients=args.bench_clients,
            client_netns=args.client_netns,
            bench_binary=args.bench_binary,
        )
        queue.submit_task(task)

    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0
    print(f"Queued {len(combinations)} task(s):")
    print(f"  source={args.source} specifier={args.specifier}")
    print(f"  topology: {_describe_topology(topology)}")
    print(f"  tests={tests} sizes={sizes} io-threads={io_threads} pipeline={pipelining}")
    print(f"  duration={duration}s warmup={warmup}s reps={args.repetitions}")
    if args.make_args:
        print(f"  make-args: {args.make_args}")
    if args.server_args:
        print(f"  server-args: {args.server_args}")
    if args.bench_threads or args.bench_clients:
        print(f"  bench-threads={args.bench_threads or 'default'} bench-clients={args.bench_clients or 'default'}")
    if args.client_netns:
        print(f"  client-netns: {args.client_netns} (real-NIC hairpin path)")
    if args.bench_binary:
        print(f"  bench-binary: {args.bench_binary} (NOT sweep-comparable)")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def handle_queue_add_latency(args: argparse.Namespace) -> int:
    """Handle 'queue add-latency': submit a fixed-rate latency task.

    A cachecannon cell driven at ``target_rps`` with no pipelining, scored on
    the dominant command's p99 in microseconds. The defaults are the epoch-3
    latency sweep's, so a cell queued without overrides has the same shape as
    the sweep's points.
    """
    from conductress.tasks.task_cachecannon import CachecannonTaskData

    if not _source_is_valid(args.source):
        return 1
    topology = _topology_or_none(args)
    if topology is None:
        return 1

    if args.target_rps <= 0:
        print(f"Error: target_rps must be > 0, got {args.target_rps}", file=sys.stderr)
        return 1
    if not (0 <= args.set_ratio <= 100):
        print(f"Error: --set-ratio must be 0-100, got {args.set_ratio}", file=sys.stderr)
        return 1
    if args.value_size < 1:
        print(f"Error: --value-size must be >= 1, got {args.value_size}", file=sys.stderr)
        return 1
    if args.repetitions < 1:
        print("Error: Repetitions must be at least 1", file=sys.stderr)
        return 1

    ratio_note = f", SET={args.set_ratio}%" if args.set_ratio > 0 else ""
    default_note = f"manual latency @ {args.target_rps} rps{ratio_note}"

    task = CachecannonTaskData(
        source=args.source,
        specifier=args.specifier,
        make_args=config.DEFAULT_MAKE_ARGS,
        topology=topology,
        note=args.note or default_note,
        requirements={},
        test="get",
        set_ratio=args.set_ratio,
        val_size=args.value_size,
        pipelining=config.SWEEP_V3_LATENCY_PIPELINING,
        connections=args.connections,
        threads=args.threads,
        io_threads=args.io_threads,
        warmup=config.SWEEP_V3_WARMUP,
        duration=config.SWEEP_V3_DURATION,
        repetitions=args.repetitions,
        keyspace_count=config.SWEEP_V3_KEYSPACE,
        server_args=args.server_args,
        rate_limit=args.target_rps,
        score_metric="p99",
    )

    queue = _TaskSubmitter(args)
    queue.submit_task(task)
    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0
    print(f"Queued latency task: {args.specifier[:8]} @ {args.target_rps} rps (id: {task.task_id})")
    print(f"  topology: {_describe_topology(topology)}")
    print(f"  score: p99 (us)  pipeline=1  connections={args.connections} threads={args.threads}")
    print(
        f"  io-threads={args.io_threads} duration={config.SWEEP_V3_DURATION}s warmup={config.SWEEP_V3_WARMUP}s reps={args.repetitions}"
    )
    if args.server_args:
        print(f"  server-args: {args.server_args}")
    if args.set_ratio > 0:
        print(f"  set-ratio: {args.set_ratio}%")
    if args.value_size != config.SWEEP_V3_VAL_SIZE:
        print(f"  value-size: {args.value_size}B")
    return 0


def handle_queue_add_mixed(args: argparse.Namespace) -> int:
    """Handle 'queue add-mixed': submit mixed GET/SET throughput tasks.

    One cachecannon cell per (size, io-threads, pipelining) combination, scored
    on throughput. The defaults are the epoch-3 mixed sweep's shape.
    """
    from conductress.tasks.task_cachecannon import CachecannonTaskData

    if not _source_is_valid(args.source):
        return 1
    topology = _topology_or_none(args)
    if topology is None:
        return 1

    if not (0 <= args.set_ratio <= 100):
        print(f"Error: --set-ratio must be 0-100, got {args.set_ratio}", file=sys.stderr)
        return 1

    try:
        sizes = _parse_comma_separated_bytes(args.sizes, "sizes")
        io_threads = _parse_comma_separated_ints(args.io_threads, "io-threads")
        pipelining = _parse_comma_separated_ints(args.pipelining, "pipelining")
        duration = _parse_human_time(args.duration, "duration")
        warmup = _parse_human_time(args.warmup, "warmup")
        validate_cpulist(args.server_cpus)
        validate_cpulist(args.client_cpus)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.repetitions < 1:
        print("Error: Repetitions must be at least 1", file=sys.stderr)
        return 1
    if args.connections < 1 or args.threads < 1:
        print("Error: --connections and --threads must be >= 1", file=sys.stderr)
        return 1
    if args.keyspace < 1:
        print(f"Error: --keyspace must be >= 1, got {args.keyspace}", file=sys.stderr)
        return 1

    combinations = list(itertools.product(sizes, io_threads, pipelining))

    queue = _TaskSubmitter(args)
    for val_size, io_thread, pipeline in combinations:
        task = CachecannonTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            topology=topology,
            note=args.note,
            requirements={},
            test="get",
            set_ratio=args.set_ratio,
            val_size=val_size,
            io_threads=io_thread,
            pipelining=pipeline,
            connections=args.connections,
            threads=args.threads,
            warmup=warmup,
            duration=duration,
            repetitions=args.repetitions,
            keyspace_count=args.keyspace,
            perf_stat_enabled=args.perf_stat,
            server_cpu_override=args.server_cpus,
            benchmark_cpu_override=args.client_cpus,
            server_args=args.server_args,
        )
        queue.submit_task(task)

    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0
    ratio_str = f"{args.set_ratio}%SET/{100-args.set_ratio}%GET"
    print(f"Queued {len(combinations)} mixed task(s) ({ratio_str}):")
    print(f"  source={args.source} specifier={args.specifier}")
    print(f"  topology: {_describe_topology(topology)}")
    print(f"  sizes={sizes} io-threads={io_threads} pipeline={pipelining}")
    print(f"  connections={args.connections} threads={args.threads} keyspace={args.keyspace}")
    print(f"  duration={duration}s warmup={warmup}s reps={args.repetitions}")
    if args.server_args:
        print(f"  server-args: {args.server_args}")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def build_scenario_overlay_spec(args: argparse.Namespace) -> str:
    """Assemble the connection-storm overlay_spec JSON from --storm-* args.

    Pure function over ``args``: returns a JSON object string containing only
    the storm keys the user set (unset flags fall back to the spec defaults at
    parse time). Enforces that --storm-* flags are given only with
    --scenario connection-storm. Raises ValueError on a gating violation.
    """
    import json as _json

    storm_map = {
        "clients": args.storm_clients,
        "burst_ms": args.storm_burst_ms,
        "connect_timeout_ms": args.storm_connect_timeout_ms,
        "reply_timeout_ms": args.storm_reply_timeout_ms,
        "policy": args.storm_policy,
        "stall": args.storm_stall,
        "burst_after_stall_ms": args.storm_burst_after_stall_ms,
        "stall_after_s": args.storm_stall_after_s,
        "start_delay_s": args.storm_start_delay_s,
        "prewarm_connections": args.storm_prewarm_connections,
        "workers": args.storm_workers,
        "first_command": args.storm_first_command,
        "herd_command_interval_ms": args.storm_herd_command_interval_ms,
        "probe_clients": args.storm_probe_clients,
        "probe_rate": args.storm_probe_rate,
    }
    spec: dict = {k: v for k, v in storm_map.items() if v is not None}
    if args.storm_burst_first:
        spec["burst_first"] = True
    if args.storm_probe_tls:
        spec["probe_tls"] = True
    if args.storm_handshake is not None:
        spec["handshake"] = [h for h in args.storm_handshake if h.strip()]
    if args.storm_bind_addrs is not None:
        spec["bind_addrs"] = [a.strip() for a in args.storm_bind_addrs.split(",") if a.strip()]

    given = bool(spec)
    if given and args.scenario != "connection-storm":
        raise ValueError("--storm-* flags are only valid with --scenario connection-storm")
    return _json.dumps(spec) if spec else ""


def handle_queue_add_scenario(args: argparse.Namespace) -> int:
    """Handle 'queue add-scenario': submit a pathological-workload scenario task."""
    from conductress.tasks.task_scenario import SCENARIO_CHOICES, ScenarioTaskData

    if not _source_is_valid(args.source):
        return 1
    topology = _topology_or_none(args)
    if topology is None:
        return 1

    if args.scenario not in SCENARIO_CHOICES:
        print(f"Error: Unknown scenario '{args.scenario}'. Valid: {', '.join(SCENARIO_CHOICES)}", file=sys.stderr)
        return 1

    try:
        io_threads = int(args.io_threads)
    except ValueError:
        print(f"Error: --io-threads must be an integer, got '{args.io_threads}'", file=sys.stderr)
        return 1

    try:
        pipelining = int(args.pipelining)
    except ValueError:
        print(f"Error: --pipelining must be an integer, got '{args.pipelining}'", file=sys.stderr)
        return 1

    try:
        duration = _parse_human_time(args.duration, "duration")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.repetitions < 1:
        print("Error: Repetitions must be at least 1", file=sys.stderr)
        return 1

    try:
        validate_cpulist(args.server_cpus)
    except ValueError as e:
        print(f"Error (--server-cpus): {e}", file=sys.stderr)
        return 1
    try:
        validate_cpulist(args.client_cpus)
    except ValueError as e:
        print(f"Error (--client-cpus): {e}", file=sys.stderr)
        return 1

    if not (0 <= args.background_set_ratio <= 100):
        print(
            f"Error: --background-set-ratio must be 0-100, got {args.background_set_ratio}",
            file=sys.stderr,
        )
        return 1

    if args.overlay_value_size < 0:
        print(
            f"Error: --overlay-value-size must be >= 0, got {args.overlay_value_size}",
            file=sys.stderr,
        )
        return 1

    try:
        overlay_spec = build_scenario_overlay_spec(args)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # connection-storm samples the server by default (100 ms) unless the flag
    # says otherwise; other scenarios stay off unless explicitly asked.
    if args.server_sample_ms is not None:
        server_sample_ms = args.server_sample_ms
    elif args.scenario == "connection-storm":
        server_sample_ms = 100
    else:
        server_sample_ms = 0

    queue = _TaskSubmitter(args)
    try:
        task = ScenarioTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            topology=topology,
            note=args.note,
            requirements={},
            scenario=args.scenario,
            val_size=config.DEFAULT_VAL_SIZE,
            io_threads=io_threads,
            pipelining=pipelining,
            duration=duration,
            repetitions=args.repetitions,
            perf_stat_enabled=args.perf_stat,
            server_cpu_override=args.server_cpus,
            benchmark_cpu_override=args.client_cpus,
            server_args=args.server_args,
            background_set_ratio=args.background_set_ratio,
            overlay_value_size=args.overlay_value_size,
            overlay_spec=overlay_spec,
            server_sample_ms=server_sample_ms,
            tls=args.tls,
            background=args.background,
            server_sample_fields=args.server_sample_fields,
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    queue.submit_task(task)
    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0

    print(f"Queued scenario task: {args.scenario}")
    print(f"  source={args.source} specifier={args.specifier}")
    print(f"  topology: {_describe_topology(topology)}")
    print(f"  io-threads={io_threads} pipeline={pipelining}")
    print(f"  duration={duration}s reps={args.repetitions}")
    if args.background_set_ratio > 0:
        print(f"  background-set-ratio={args.background_set_ratio}%")
    if args.overlay_value_size > 0:
        print(f"  overlay-value-size={args.overlay_value_size}B")
    if overlay_spec:
        print(f"  overlay-spec: {overlay_spec}")
    if args.server_args:
        print(f"  server-args: {args.server_args}")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def handle_queue_add_cachecannon(args: argparse.Namespace) -> int:
    """Handle 'queue add-cachecannon': submit a cachecannon benchmark task.

    cachecannon is a second-opinion generator -- results are NOT comparable
    with the valkey-benchmark sweep history.
    """
    from conductress.tasks.task_cachecannon import CachecannonTaskData

    if not _source_is_valid(args.source):
        return 1
    topology = _topology_or_none(args)
    if topology is None:
        return 1

    try:
        warmup = _parse_human_time(args.warmup, "warmup")
        duration = _parse_human_time(args.duration, "duration")
        val_size = _parse_bytes(args.sizes, "sizes")
        _check_cpulist(args.server_cpus, "server-cpus")
        _check_cpulist(args.client_cpus, "client-cpus")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.repetitions < 1:
        print("Error: Repetitions must be at least 1", file=sys.stderr)
        return 1

    if not 0 <= args.set_ratio <= 100:
        print(f"Error: --set-ratio must be 0-100, got {args.set_ratio}", file=sys.stderr)
        return 1
    if args.rate < 0:
        print(f"Error: --rate must be >= 0 (0 = closed loop), got {args.rate}", file=sys.stderr)
        return 1
    try:
        from conductress.tasks.task_cachecannon import parse_info_sections

        parse_info_sections(args.info_sections)
    except ValueError as e:
        print(f"Error (--info-sections): {e}", file=sys.stderr)
        return 1

    queue = _TaskSubmitter(args)
    task = CachecannonTaskData(
        source=args.source,
        specifier=args.specifier,
        make_args=args.make_args,
        topology=topology,
        note=args.note,
        requirements={},
        test=args.test,
        val_size=val_size,
        pipelining=args.pipelining,
        connections=args.connections,
        threads=args.threads,
        io_threads=args.io_threads,
        warmup=warmup,
        duration=duration,
        repetitions=args.repetitions,
        keyspace_count=args.keyspace,
        cachecannon_binary=args.cachecannon_binary,
        server_args=args.server_args,
        server_cpu_override=args.server_cpus,
        benchmark_cpu_override=args.client_cpus,
        set_ratio=args.set_ratio,
        distribution=args.distribution,
        rate_limit=args.rate,
        perf_stat_enabled=args.perf_stat,
        info_sections=args.info_sections,
        client_netns=args.client_netns,
        processes=args.processes,
    )
    queue.submit_task(task)
    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0

    print(f"Queued cachecannon task (NOT sweep-comparable):")
    print(f"  source={args.source} specifier={args.specifier}")
    print(f"  topology: {_describe_topology(topology)}")
    if args.rate > 0:
        print(f"  rate={args.rate} req/s (open loop)")
    if args.perf_stat or args.info_sections:
        print(f"  perf-stat={'on' if args.perf_stat else 'off'} info-sections={args.info_sections or '-'}")
    if args.set_ratio > 0:
        print(
            f"  workload=mixed {args.set_ratio}%SET/{100 - args.set_ratio}%GET size={val_size} pipeline={args.pipelining}"
        )
    else:
        print(f"  test={args.test} size={val_size} pipeline={args.pipelining}")
    if args.distribution != "uniform":
        print(f"  distribution={args.distribution}")
    print(f"  connections={args.connections} threads={args.threads}")
    print(f"  io-threads={args.io_threads} duration={duration}s warmup={warmup}s reps={args.repetitions}")
    if args.server_args:
        print(f"  server-args: {args.server_args}")
    if args.cachecannon_binary != DEFAULT_CACHECANNON_BINARY:
        print(f"  binary: {args.cachecannon_binary}")
    if args.client_netns:
        print(f"  client-netns: {args.client_netns} (real-NIC hairpin path)")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def handle_queue_add_replica_read(args: argparse.Namespace) -> int:
    """Handle 'queue add-replica-read': submit a replica-read benchmark task."""
    from conductress.tasks.task_replica_read import ReplicaReadTaskData

    if not _source_is_valid(args.source):
        return 1

    try:
        warmup = _parse_human_time(args.warmup, "warmup")
        duration = _parse_human_time(args.duration, "duration")
        val_size = _parse_bytes(args.sizes, "sizes")
        _check_cpulist(args.client_cpus, "client-cpus")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        task = ReplicaReadTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            topology=TopologySpec.replica_read(
                replicas=args.replicas,
                replica_io_threads=args.io_threads,
                primary_io_threads=args.primary_io_threads,
                server_args=args.server_args,
                primary_args=args.primary_args,
                replica_args=args.replica_args,
                base_port=args.base_port,
            ),
            note=args.note,
            requirements={},
            val_size=val_size,
            pipelining=args.pipelining,
            connections=args.connections,
            threads=args.threads,
            keyspace_count=args.keyspace,
            warmup=warmup,
            duration=duration,
            repetitions=args.repetitions,
            write_rate=args.write_rate,
            write_connections=args.write_connections,
            write_threads=args.write_threads,
            write_pipelining=args.write_pipelining,
            sample_interval=args.sample_interval,
            info_fields=args.info_fields,
            max_lag_seconds=args.max_lag_seconds,
            read_rate_search=args.read_rate_search,
            read_rate_start=args.read_rate_start,
            read_rate_max=args.read_rate_max,
            read_rate_step=args.read_rate_step,
            read_rate_tolerance=args.read_rate_tolerance,
            max_lag_slope=args.max_lag_slope,
            cachecannon_binary=args.cachecannon_binary,
            benchmark_cpu_override=args.client_cpus,
            cpu_profile=args.cpu_profile,
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    queue = _TaskSubmitter(args)
    queue.submit_task(task)
    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0

    print("Queued replica-read task:")
    print(f"  source={args.source} specifier={args.specifier}")
    print(
        f"  topology: primary :{args.base_port} (io-threads={args.primary_io_threads}) + {args.replicas} replica(s) (io-threads={args.io_threads})"
    )
    print(
        f"  writer: {args.write_rate} SET/s, {args.write_connections}c P{args.write_pipelining} {args.write_threads}t at the primary"
    )
    print(f"  reader: GET size={val_size} P{args.pipelining} {args.connections}c {args.threads}t at the first replica")
    if args.read_rate_search:
        print(
            f"  read-rate search: from {args.read_rate_start}/s x{args.read_rate_step:g} up to {args.read_rate_max}/s, "
            f"bisect to {args.read_rate_tolerance:.0%}; a probe fails on lag slope > {args.max_lag_slope} s/s or "
            f"lag at the end of the window > {args.max_lag_seconds} s; warmup/duration are per probe"
        )
    print(f"  duration={duration}s warmup={warmup}s reps={args.repetitions} keyspace={args.keyspace}")
    print(f"  lag guard: scored-window mean <= {args.max_lag_seconds} s of replication stream")
    for label, value in (
        ("server-args", args.server_args),
        ("primary-args", args.primary_args),
        ("replica-args", args.replica_args),
    ):
        if value:
            print(f"  {label}: {value}")
    if args.info_fields:
        print(f"  info-fields: {args.info_fields}")
    if args.cpu_profile:
        print(f"  cpu-profile: replica main + io threads, last rep (rep {args.repetitions}), scored window only")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def _memory_user_data_bytes(workload: "MemoryWorkload", value_size: int) -> int:
    """Per-item user data bytes for a memory workload at a custom value/member size.

    set: key + value; zadd: member + score double; sadd: member; hset: field + value.
    """
    if workload.command == "set":
        return workload.key_size + value_size
    if workload.command == "zadd":
        return value_size + config.MEM_TEST_SCORE_SIZE
    if workload.command == "sadd":
        return value_size
    if workload.command == "hset":
        return workload.field_size + value_size
    raise ValueError(f"Unknown memory workload command: {workload.command}")


def _memory_workload_label(workload: "MemoryWorkload") -> str:
    """Label naming every user-data dimension of a memory workload (set-k16-v64, hset-f16-v16, zadd-m20)."""
    if workload.command == "set":
        label = f"set-k{workload.key_size}-v{workload.value_size}"
    elif workload.command == "hset":
        label = f"hset-f{workload.field_size}-v{workload.value_size}"
    else:
        label = f"{workload.command}-m{workload.value_size}"
    return f"{label}-expire" if workload.has_expire else label


def _validate_memory_workload_sizes(workload: "MemoryWorkload") -> None:
    """Reject sizes the populator cannot encode uniquely for MEM_TEST_ITEM_COUNT items.

    Keys, members and fields carry a prefix plus a zero-padded index, so each has a
    floor below which two items would collide. Values are pure padding and have no floor.
    """
    from conductress.sweep.populator import minimum_item_size

    count = config.MEM_TEST_ITEM_COUNT
    checks: list[tuple[str, str, int]] = []
    if workload.command == "set":
        checks.append(("key", "key", workload.key_size))
    elif workload.command == "hset":
        checks.append(("field", "f", workload.field_size))
    else:
        checks.append(("member", "m", workload.value_size))
    for name, prefix, size in checks:
        floor = minimum_item_size(prefix, count)
        if size < floor:
            raise ValueError(
                f"{workload.command} {name} size {size} is below the {floor}-byte minimum for {count:,} unique items"
            )


def _default_memory_shapes(roster: list, types: list[str], *, has_expire: bool) -> list:
    """The first roster shape for each requested type at the given expire setting, in roster order."""
    seen: set[str] = set()
    shapes = []
    for wl in roster:
        if wl.command in types and wl.has_expire == has_expire and wl.command not in seen:
            seen.add(wl.command)
            shapes.append(wl)
    return shapes


def handle_queue_add_memory(args: argparse.Namespace) -> int:
    """Handle 'queue add-memory': submit memory efficiency tasks."""
    from conductress.heap_profiler import with_jemalloc_prof
    from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS
    from conductress.tasks.task_mem_efficiency import MemTaskData

    if not _source_is_valid(args.source):
        return 1

    types = [t.strip() for t in args.types.split(",") if t.strip()]
    valid_types = ["set", "sadd", "zadd", "hset"]
    for t in types:
        if t not in valid_types:
            print(f"Error: Invalid type '{t}'. Valid: {', '.join(valid_types)}", file=sys.stderr)
            return 1

    # One base shape per requested type, taken from the sweep roster: the first
    # roster entry for each (type, expire) pair.  The roster carries several
    # shapes per type (a 64-byte and a 16-byte string, for instance); the size
    # flags below derive the others from this base, so a request for a type
    # yields one task, not one per roster shape.
    workloads = _default_memory_shapes(MEMORY_WORKLOADS, types, has_expire=False)
    if args.expire:
        workloads += _default_memory_shapes(MEMORY_WORKLOADS, types, has_expire=True)

    if not workloads:
        print("Error: No matching workloads found.", file=sys.stderr)
        return 1

    # Per-type size overrides. --sizes sets the value/member dimension for every type;
    # set has a second dimension (key) and hset has one too (field), each with its own flag.
    # Every override is checked against the smallest string the populator can generate
    # while keeping all MEM_TEST_ITEM_COUNT items unique.
    def _single_size(raw: str, name: str) -> Optional[int]:
        if not raw:
            return None
        values = _parse_comma_separated_bytes(raw, name)
        if len(values) != 1:
            raise ValueError(f"--{name} takes exactly one size")
        return values[0]

    try:
        key_size = _single_size(args.key_size, "key-size")
        field_size = _single_size(args.field_size, "field-size")
        sizes = _parse_comma_separated_bytes(args.sizes, "sizes") if args.sizes else []
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if key_size is not None:
        workloads = [replace(wl, key_size=key_size) if wl.command == "set" else wl for wl in workloads]
    if field_size is not None:
        workloads = [replace(wl, field_size=field_size) if wl.command == "hset" else wl for wl in workloads]

    if sizes:
        # Re-derive one workload per (type, size) pair, keeping each type's key/field
        # sizes and expire variants.
        workloads = [replace(wl, value_size=size) for wl in workloads for size in sizes]

    if key_size is not None or field_size is not None or sizes:
        # Recompute the label and per-item user data for every workload whose shape changed.
        workloads = [
            replace(
                wl,
                label=_memory_workload_label(wl),
                user_data_bytes=_memory_user_data_bytes(wl, wl.value_size),
            )
            for wl in workloads
        ]

    for wl in workloads:
        try:
            _validate_memory_workload_sizes(wl)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    queue = _TaskSubmitter(args)
    for wl in workloads:
        task = MemTaskData(
            source=args.source,
            specifier=args.specifier,
            # enable_profiling below is only effective when the binary is built with
            # jemalloc profiling; without this the task silently records breakdown=None.
            make_args=with_jemalloc_prof(args.make_args),
            topology=TopologySpec.standalone(),
            note=args.note or f"manual mem-{wl.command}",
            requirements={},
            type=wl.command,
            val_sizes=[wl.value_size],
            has_expire=wl.has_expire,
            enable_profiling=True,
            key_size=wl.key_size,
            field_size=wl.field_size,
            user_data_bytes=wl.user_data_bytes,
            populate_mode=args.populate_mode,
            settle=args.settle,
        )
        queue.submit_task(task)

    submission = queue.finish()
    if _finish_submission(submission, args):
        return 0
    print(f"Queued {len(workloads)} memory task(s):")
    print(f"  source={args.source} specifier={args.specifier}")
    for wl in workloads:
        expire_str = " +expire" if wl.has_expire else ""
        print(f"  - {wl.command} v={wl.value_size}B k={wl.key_size}B{expire_str}")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def handle_queue_list(args: argparse.Namespace) -> int:
    """Handle 'queue list': show all pending tasks."""
    queue = TaskQueue()
    tasks = queue.get_all_tasks()

    if not tasks:
        print("No pending tasks in the queue.")
        return 0

    print(f"{'#':<4} {'Task ID':<30} {'Description':<50} {'Note'}")
    print("-" * 110)
    for i, task in enumerate(tasks, 1):
        print(f"{i:<4} {task.task_id:<30} {task.short_description():<50} {task.note}")

    print(f"\nTotal: {len(tasks)} task(s)")
    return 0


def handle_queue_remove(args: argparse.Namespace) -> int:
    """Handle 'queue remove': remove a task by ID."""
    queue = TaskQueue()
    if queue.remove_task(args.task_id):
        print(f"Removed task: {args.task_id}")
        return 0
    else:
        print(f"Error: Task not found: {args.task_id}", file=sys.stderr)
        return 1


def handle_queue_clear(args: argparse.Namespace) -> int:
    """Handle 'queue clear': remove all pending tasks."""
    queue = TaskQueue()
    tasks = queue.get_all_tasks()
    if not tasks:
        print("Queue is already empty.")
        return 0

    for task in tasks:
        queue.remove_task(task.task_id)

    print(f"Cleared {len(tasks)} task(s) from the queue.")
    return 0


def _remote_client():
    from .fleet_client import FleetClient

    return FleetClient.from_env()


def _emit_json(command: str, document: dict) -> None:
    print(json.dumps({"schema_version": 1, "command": command, "data": document}, indent=2, sort_keys=True))


def handle_queue_cancel(args: argparse.Namespace) -> int:
    """Handle 'queue cancel': cancel a remote task or batch."""
    document = _remote_client().cancel_task(args.selector)
    if args.json:
        _emit_json("queue.cancel", document)
        return 0
    changed = document.get("changed", 0)
    print(f"Cancel {args.selector}: {changed} task(s) changed")
    for task in document.get("tasks", []):
        print(f"  {task['task_id']}: {task['state']}")
    return 0


def handle_queue_mine(args: argparse.Namespace) -> int:
    """Handle 'queue mine': list the caller's remote tasks."""
    document = _remote_client().list_mine(limit=args.limit)
    if args.json:
        _emit_json("queue.mine", document)
        return 0
    tasks = document.get("tasks", [])
    if not tasks:
        print("No remote tasks found for your account.")
        return 0
    print(f"{'TASK ID':<31} {'STATE':<17} {'BATCH':<22} SUBMITTED")
    for task in tasks:
        print(
            f"{task['task_id']:<31} {task['state']:<17} " f"{(task.get('batch_id') or '-'):<22} {task['submitted_at']}"
        )
    return 0


def handle_queue_pending(args: argparse.Namespace) -> int:
    """Handle 'queue pending': list tasks awaiting approval."""
    document = _remote_client().list_pending()
    if args.json:
        _emit_json("queue.pending", document)
        return 0
    tasks = document.get("tasks", [])
    if not tasks:
        print("No tasks awaiting approval.")
        return 0
    print(f"{'TASK ID':<31} {'RUNNER':<14} {'SUBMITTER':<16} {'BATCH':<22} SUBMITTED")
    for task in tasks:
        print(
            f"{task['task_id']:<31} {task['runner_id']:<14} "
            f"{(task.get('submitter_login') or '-'):<16} {(task.get('batch_id') or '-'):<22} {task['submitted_at']}"
        )
    return 0


def handle_queue_approve(args: argparse.Namespace) -> int:
    """Handle 'queue approve': approve a pending task or batch."""
    document = _remote_client().approve_tasks(args.selector)
    if args.json:
        _emit_json("queue.approve", document)
        return 0
    print(f"Approve {args.selector}: {document.get('changed', 0)} task(s) queued")
    return 0


def handle_queue_reject(args: argparse.Namespace) -> int:
    """Handle 'queue reject': reject a pending task or batch."""
    document = _remote_client().reject_tasks(args.selector)
    if args.json:
        _emit_json("queue.reject", document)
        return 0
    print(f"Reject {args.selector}: {document.get('changed', 0)} task(s) rejected")
    return 0


def parse_xrange(spec: Optional[str]) -> Optional[Tuple[float, float]]:
    """Parse an ``--xrange`` spec like ``-3:8`` into ``(lo, hi)`` or None.

    Raises ValueError on a malformed spec so the CLI can report it.
    """
    if spec is None:
        return None
    text = spec.strip()
    if not text:
        return None
    # rsplit on the LAST colon so a negative lower bound (-3:8) parses.
    lo_str, sep, hi_str = text.rpartition(":")
    if not sep:
        raise ValueError(f"--xrange must be LO:HI, got {spec!r}")
    try:
        lo, hi = float(lo_str), float(hi_str)
    except ValueError as exc:
        raise ValueError(f"--xrange bounds must be numbers, got {spec!r}") from exc
    if hi <= lo:
        raise ValueError(f"--xrange HI must be greater than LO, got {spec!r}")
    return (lo, hi)


def handle_plot(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Handle 'plot connection-storm': render a task's results to a PNG."""
    if args.plot_command != "connection-storm":
        parser.print_usage()
        return 1

    try:
        xrange = parse_xrange(args.xrange)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if len(args.task_ids) > 3:
        print("Error: at most 3 task ids (columns) are supported", file=sys.stderr)
        return 1

    try:
        from conductress.plots.connection_storm import build_storm_figure, load_run_views
    except ImportError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    try:
        views = load_run_views(args.task_ids)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        figure = build_storm_figure(views, rep=args.rep, xrange=xrange)
    except ImportError as e:
        # matplotlib missing surfaces here too (builder imports it).
        print(f"Error: {e}", file=sys.stderr)
        return 2

    figure.savefig(args.out)
    print(f"wrote {args.out}")
    return 0


def _dispatch(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.command == "queue":
        if args.queue_command is None:
            # Bare 'conductress queue' defaults to list (preserves original
            # behavior relied on by scripts/integration tests). The improved
            # subcommand help remains available via 'conductress queue --help'.
            return handle_queue_list(args)
        if args.queue_command == "list":
            return handle_queue_list(args)
        if args.queue_command == "add":
            return handle_queue_add(args)
        if args.queue_command == "add-insertion":
            return handle_queue_add_insertion(args)
        if args.queue_command == "add-memory":
            return handle_queue_add_memory(args)
        if args.queue_command == "add-mixed":
            return handle_queue_add_mixed(args)
        if args.queue_command == "add-scenario":
            return handle_queue_add_scenario(args)
        if args.queue_command == "add-latency":
            return handle_queue_add_latency(args)
        if args.queue_command == "add-cachecannon":
            return handle_queue_add_cachecannon(args)
        if args.queue_command == "add-replica-read":
            return handle_queue_add_replica_read(args)
        if args.queue_command == "remove":
            return handle_queue_remove(args)
        if args.queue_command == "clear":
            return handle_queue_clear(args)
        if args.queue_command == "cancel":
            return handle_queue_cancel(args)
        if args.queue_command == "mine":
            return handle_queue_mine(args)
        if args.queue_command == "pending":
            return handle_queue_pending(args)
        if args.queue_command == "approve":
            return handle_queue_approve(args)
        if args.queue_command == "reject":
            return handle_queue_reject(args)
    if args.command == "plot":
        return handle_plot(args, parser)
    parser.print_usage()
    return 1


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for the CLI module."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_usage()
        return 1

    try:
        return _dispatch(args, parser)
    except FleetClientError as exc:
        if getattr(args, "json", False):
            payload = {
                "schema_version": 1,
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


if __name__ == "__main__":
    sys.exit(main())
