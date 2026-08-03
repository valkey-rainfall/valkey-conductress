"""Non-interactive CLI for queuing and managing benchmark tasks."""

import argparse
import itertools
import logging
import sys
from dataclasses import replace
from typing import TYPE_CHECKING, List, Optional, Tuple

from . import config
from .task_queue import TaskQueue
from .tasks.task_perf_benchmark import PerfTaskData
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


def _add_perf_args(parser: argparse.ArgumentParser) -> None:
    """Add performance benchmark arguments to a parser."""
    parser.add_argument("--source", default="valkey", help="Repository source name (default: valkey)")
    parser.add_argument(
        "--specifier",
        default="unstable",
        help="Branch, tag, or commit (default: unstable)",
    )
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
    parser.add_argument(
        "--warmup",
        default=f"{config.DEFAULT_WARMUP}s",
        help=f"Warmup duration (e.g., 30s, 1m). Default: {config.DEFAULT_WARMUP}s",
    )
    parser.add_argument(
        "--duration",
        default=f"{config.DEFAULT_DURATION}s",
        help=f"Test duration (e.g., 5m, 15m). Default: {config.DEFAULT_DURATION}s",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=config.DEFAULT_REPETITIONS,
        help=f"Number of repetitions per config. Default: {config.DEFAULT_REPETITIONS}",
    )
    parser.add_argument(
        "--key-sizes",
        default=str(config.DEFAULT_KEY_SIZE),
        help=f"Comma-separated key sizes in bytes (0=standard). Default: {config.DEFAULT_KEY_SIZE}",
    )
    parser.add_argument("--note", default="", help="Optional note for the tasks")
    parser.add_argument(
        "--make-args",
        default=config.DEFAULT_MAKE_ARGS,
        help=f"Build arguments. Default: '{config.DEFAULT_MAKE_ARGS}'",
    )
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

    # queue add-memory
    mem_parser = queue_sub.add_parser("add-memory", help="Add memory efficiency tasks to the queue")
    mem_parser.add_argument("--source", default="valkey", help="Repository source name (default: valkey)")
    mem_parser.add_argument("--specifier", default="unstable", help="Branch, tag, commit, or path (default: unstable)")
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
    mem_parser.add_argument("--expire", action="store_true", help="Also test with expiration enabled")
    mem_parser.add_argument(
        "--populate-mode",
        choices=["random", "sequential", "churn"],
        default="random",
        help="zadd insertion pattern (default: random). sequential=dense/best-case, "
        "churn=50/50 add-delete steady state. Only affects zadd.",
    )
    mem_parser.add_argument("--note", default="", help="Optional note for the tasks")
    mem_parser.add_argument(
        "--make-args",
        default=config.DEFAULT_MAKE_ARGS,
        help=f"Build arguments. Default: '{config.DEFAULT_MAKE_ARGS}'",
    )
    mem_parser.add_argument(
        "--settle",
        action="store_true",
        help="Quiesce until used_memory plateaus before sampling (captures steady-state "
        "memory after background reclamation, e.g. zset compaction). Default off.",
    )

    # queue add-mixed
    mixed_parser = queue_sub.add_parser("add-mixed", help="Add a mixed GET/SET throughput task (memtier)")
    mixed_parser.add_argument("--source", default="valkey", help="Repository source name (default: valkey)")
    mixed_parser.add_argument("--specifier", default="unstable", help="Branch, tag, or commit (default: unstable)")
    mixed_parser.add_argument(
        "--set-ratio",
        type=int,
        required=True,
        help="Percentage of SET commands (0-100). E.g. 20 = 20%% SET / 80%% GET.",
    )
    mixed_parser.add_argument(
        "--sizes",
        default=str(config.DEFAULT_VAL_SIZE),
        help=f"Comma-separated value sizes (e.g., 16,512,1KB). Default: {config.DEFAULT_VAL_SIZE}",
    )
    mixed_parser.add_argument(
        "--key-sizes",
        default=str(config.DEFAULT_KEY_SIZE),
        help=f"Comma-separated key sizes in bytes (0=standard). Default: {config.DEFAULT_KEY_SIZE}",
    )
    mixed_parser.add_argument(
        "--io-threads",
        default=str(config.DEFAULT_IO_THREADS),
        help=f"Comma-separated IO thread counts. Default: {config.DEFAULT_IO_THREADS}",
    )
    mixed_parser.add_argument(
        "--pipelining",
        default=str(config.DEFAULT_PIPELINING),
        help=f"Comma-separated pipelining values. Default: {config.DEFAULT_PIPELINING}",
    )
    mixed_parser.add_argument(
        "--duration",
        default=f"{config.DEFAULT_DURATION}s",
        help=f"Test duration (e.g., 5m, 30s). Default: {config.DEFAULT_DURATION}s",
    )
    mixed_parser.add_argument(
        "--repetitions",
        type=int,
        default=config.DEFAULT_REPETITIONS,
        help=f"Number of repetitions. Default: {config.DEFAULT_REPETITIONS}",
    )
    mixed_parser.add_argument("--note", default="", help="Optional note for the tasks")
    mixed_parser.add_argument(
        "--make-args",
        default=config.DEFAULT_MAKE_ARGS,
        help=f"Build arguments. Default: '{config.DEFAULT_MAKE_ARGS}'",
    )
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

    # queue add-scenario
    scenario_parser = queue_sub.add_parser(
        "add-scenario", help="Add a pathological-workload scenario task (background GET + overlay)"
    )
    scenario_parser.add_argument(
        "--scenario",
        required=True,
        choices=["eval-storm", "scan-churn", "multi-exec", "flushall-spike", "expiry-heavy", "bgsave"],
        help="Pathological workload scenario to run. 'bgsave' fires a single BGSAVE at ~40%% of "
        "duration; fork+COW impact shows up in the interval timeseries. Dataset size (prefill) "
        "drives the fork cost.",
    )
    scenario_parser.add_argument("--source", default="valkey", help="Repository source name (default: valkey)")
    scenario_parser.add_argument("--specifier", default="unstable", help="Branch, tag, or commit (default: unstable)")
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
    scenario_parser.add_argument(
        "--duration",
        default=f"{config.DEFAULT_DURATION}s",
        help=f"Test duration (e.g., 5m, 30s). Default: {config.DEFAULT_DURATION}s",
    )
    scenario_parser.add_argument(
        "--repetitions",
        type=int,
        default=config.DEFAULT_REPETITIONS,
        help=f"Number of repetitions. Default: {config.DEFAULT_REPETITIONS}",
    )
    scenario_parser.add_argument("--note", default="", help="Optional note for the task")
    scenario_parser.add_argument(
        "--make-args",
        default=config.DEFAULT_MAKE_ARGS,
        help=f"Build arguments. Default: '{config.DEFAULT_MAKE_ARGS}'",
    )
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

    # queue add-latency
    lat_parser = queue_sub.add_parser("add-latency", help="Add a latency measurement task")
    lat_parser.add_argument("source", help="Source repo name (e.g. 'valkey')")
    lat_parser.add_argument("specifier", help="Commit hash or branch to test")
    lat_parser.add_argument("target_rps", type=int, help="Target requests/sec (use 70%% of max throughput)")
    lat_parser.add_argument("--note", default="", help="Optional note for the task")

    # queue remove
    remove_parser = queue_sub.add_parser("remove", help="Remove a task from the queue")
    remove_parser.add_argument("task_id", help="Task ID to remove (from 'queue list' output)")

    # queue clear
    queue_sub.add_parser("clear", help="Remove all pending tasks from the queue")

    return parser


def handle_queue_add(args: argparse.Namespace) -> int:
    """Handle 'queue add': validate inputs, generate tasks, and submit them."""
    if not validate_source(args.source):
        valid_sources = config.REPO_NAMES + [config.MANUALLY_UPLOADED]
        print(
            f"Error: Invalid source '{args.source}'. " f"Valid sources: {', '.join(valid_sources)}",
            file=sys.stderr,
        )
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

    queue = TaskQueue()
    for test, size, io_thread, pipeline, key_size in combinations:
        task = PerfTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            replicas=0,
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

    print(f"Queued {len(combinations)} task(s):")
    print(f"  source={args.source} specifier={args.specifier}")
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
    """Handle 'queue add-latency': submit a latency measurement task."""
    from conductress.config import LATENCY_MAKE_ARGS, SWEEP_IO_THREADS
    from conductress.tasks.task_latency import LatencyTaskData

    if not validate_source(args.source):
        valid_sources = config.REPO_NAMES + [config.MANUALLY_UPLOADED]
        print(f"Error: Invalid source '{args.source}'. Valid: {', '.join(valid_sources)}", file=sys.stderr)
        return 1

    task = LatencyTaskData(
        source=args.source,
        specifier=args.specifier,
        make_args=LATENCY_MAKE_ARGS,
        replicas=0,
        note=args.note or f"manual latency @ {args.target_rps} rps",
        requirements={},
        target_rps=args.target_rps,
        io_threads=SWEEP_IO_THREADS,
    )

    queue = TaskQueue()
    queue.submit_task(task)
    print(f"Queued latency task: {args.specifier[:8]} @ {args.target_rps} rps (id: {task.task_id})")
    return 0


def handle_queue_add_mixed(args: argparse.Namespace) -> int:
    """Handle 'queue add-mixed': submit mixed GET/SET throughput tasks."""
    from conductress.tasks.task_mixed import MixedTaskData

    if not validate_source(args.source):
        valid_sources = config.REPO_NAMES + [config.MANUALLY_UPLOADED]
        print(f"Error: Invalid source '{args.source}'. Valid: {', '.join(valid_sources)}", file=sys.stderr)
        return 1

    if not (0 <= args.set_ratio <= 100):
        print(f"Error: --set-ratio must be 0-100, got {args.set_ratio}", file=sys.stderr)
        return 1

    try:
        sizes = _parse_comma_separated_bytes(args.sizes, "sizes")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        key_sizes = _parse_comma_separated_bytes(args.key_sizes, "key-sizes")
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

    import itertools

    combinations = list(itertools.product(sizes, io_threads, pipelining, key_sizes))

    queue = TaskQueue()
    for val_size, io_thread, pipeline, key_size in combinations:
        task = MixedTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            replicas=0,
            note=args.note,
            requirements={},
            set_ratio=args.set_ratio,
            val_size=val_size,
            io_threads=io_thread,
            pipelining=pipeline,
            duration=duration,
            repetitions=args.repetitions,
            perf_stat_enabled=args.perf_stat,
            key_size=key_size,
            server_cpu_override=args.server_cpus,
            benchmark_cpu_override=args.client_cpus,
            server_args=args.server_args,
        )
        queue.submit_task(task)

    ratio_str = f"{args.set_ratio}%SET/{100-args.set_ratio}%GET"
    print(f"Queued {len(combinations)} mixed task(s) ({ratio_str}):")
    print(f"  source={args.source} specifier={args.specifier}")
    print(f"  sizes={sizes} io-threads={io_threads} pipeline={pipelining}")
    print(f"  duration={duration}s reps={args.repetitions}")
    if args.server_args:
        print(f"  server-args: {args.server_args}")
    if args.note:
        print(f"  note: {args.note}")
    return 0


def handle_queue_add_scenario(args: argparse.Namespace) -> int:
    """Handle 'queue add-scenario': submit a pathological-workload scenario task."""
    from conductress.tasks.task_scenario import SCENARIO_CHOICES, ScenarioTaskData

    if not validate_source(args.source):
        valid_sources = config.REPO_NAMES + [config.MANUALLY_UPLOADED]
        print(f"Error: Invalid source '{args.source}'. Valid: {', '.join(valid_sources)}", file=sys.stderr)
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

    queue = TaskQueue()
    task = ScenarioTaskData(
        source=args.source,
        specifier=args.specifier,
        make_args=args.make_args,
        replicas=0,
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
    )
    queue.submit_task(task)

    print(f"Queued scenario task: {args.scenario}")
    print(f"  source={args.source} specifier={args.specifier}")
    print(f"  io-threads={io_threads} pipeline={pipelining}")
    print(f"  duration={duration}s reps={args.repetitions}")
    if args.background_set_ratio > 0:
        print(f"  background-set-ratio={args.background_set_ratio}%")
    if args.server_args:
        print(f"  server-args: {args.server_args}")
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


def handle_queue_add_memory(args: argparse.Namespace) -> int:
    """Handle 'queue add-memory': submit memory efficiency tasks."""
    from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS
    from conductress.tasks.task_mem_efficiency import MemTaskData

    if not validate_source(args.source):
        valid_sources = config.REPO_NAMES + [config.MANUALLY_UPLOADED]
        print(f"Error: Invalid source '{args.source}'. Valid: {', '.join(valid_sources)}", file=sys.stderr)
        return 1

    types = [t.strip() for t in args.types.split(",") if t.strip()]
    valid_types = ["set", "sadd", "zadd", "hset"]
    for t in types:
        if t not in valid_types:
            print(f"Error: Invalid type '{t}'. Valid: {', '.join(valid_types)}", file=sys.stderr)
            return 1

    # Match workloads from MEMORY_WORKLOADS config
    workloads = [w for w in MEMORY_WORKLOADS if w.command in types and not w.has_expire]
    if args.expire:
        workloads += [w for w in MEMORY_WORKLOADS if w.command in types and w.has_expire]

    if not workloads:
        print("Error: No matching workloads found.", file=sys.stderr)
        return 1

    if args.sizes:
        try:
            sizes = _parse_comma_separated_bytes(args.sizes, "sizes")
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
        # Re-derive one workload per (type, size) pair, keeping each type's standard
        # key/field sizes and expire variants, with per-item user data computed per size.
        base_workloads = workloads
        workloads = []
        for wl in base_workloads:
            for size in sizes:
                workloads.append(
                    replace(
                        wl,
                        value_size=size,
                        label=f"{wl.command}-custom-{size}",
                        user_data_bytes=_memory_user_data_bytes(wl, size),
                    )
                )

    queue = TaskQueue()
    for wl in workloads:
        task = MemTaskData(
            source=args.source,
            specifier=args.specifier,
            make_args=args.make_args,
            replicas=0,
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


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for the CLI module."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_usage()
        return 1

    if args.command == "queue":
        if args.queue_command is None:
            # Bare 'conductress queue' defaults to list (preserves original
            # behavior relied on by scripts/integration tests). The improved
            # subcommand help remains available via 'conductress queue --help'.
            return handle_queue_list(args)
        elif args.queue_command == "list":
            return handle_queue_list(args)
        elif args.queue_command == "add":
            return handle_queue_add(args)
        elif args.queue_command == "add-memory":
            return handle_queue_add_memory(args)
        elif args.queue_command == "add-mixed":
            return handle_queue_add_mixed(args)
        elif args.queue_command == "add-scenario":
            return handle_queue_add_scenario(args)
        elif args.queue_command == "add-latency":
            return handle_queue_add_latency(args)
        elif args.queue_command == "remove":
            return handle_queue_remove(args)
        elif args.queue_command == "clear":
            return handle_queue_clear(args)

    parser.print_usage()
    return 1


if __name__ == "__main__":
    sys.exit(main())
