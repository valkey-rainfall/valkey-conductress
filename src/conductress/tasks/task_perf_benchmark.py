"""Throughput benchmark"""

import asyncio
import datetime
import logging
import time
from dataclasses import dataclass
from math import sqrt
from statistics import mean, stdev
from typing import Any, Optional

from scipy.stats import t as t_dist

from conductress.config import (
    BENCHMARK_MAX_ITERATIONS,
    BENCHMARK_UPDATE_INTERVAL,
    HEARTBEAT_INTERVAL,
    PERF_BENCH_CLIENTS,
    PERF_BENCH_KEYSPACE,
    PERF_BENCH_THREADS,
    PROJECT_ROOT,
    VALKEY_BENCHMARK,
    ServerInfo,
    get_sweep_engine,
    should_profile_internals,
)
from conductress.cpu_allocator import AllocationTag
from conductress.file_protocol import BenchmarkResults, BenchmarkStatus, MetricData
from conductress.memory_capture import (
    MemorySampler,
    collect_memory_after,
    fold_memory_records,
    reset_memory_peaks,
    servers_by_role,
)
from conductress.server import Server
from conductress.task_queue import BaseTaskData, BaseTaskRunner
from conductress.topology import TopologyGroup, TopologySpec
from conductress.utility import (
    HumanByte,
    HumanNumber,
    HumanTime,
    RealtimeCommand,
    count_cpu_list,
    get_primary_interface_ip,
    sample_process_tree_cpu,
    summarize_client_cpu,
)

BASE_KEY_PATTERN = "key:__rand_int__"
BASE_KEY_SIZE = len(BASE_KEY_PATTERN)  # 16 bytes


def generate_padded_key(key_size: int) -> str:
    """Generate a padded key pattern that reaches the target byte size.

    Appends deterministic ASCII characters to the base key pattern.
    Returns the base pattern unmodified if key_size <= BASE_KEY_SIZE.
    """
    if key_size <= BASE_KEY_SIZE:
        return BASE_KEY_PATTERN
    padding_needed = key_size - BASE_KEY_SIZE
    padding = "A" * padding_needed
    return BASE_KEY_PATTERN + padding


def compute_aggregated_stats(per_run_rps: list[float]) -> tuple[float, float]:
    """Compute mean RPS and 95% confidence interval from per-run RPS values.

    Args:
        per_run_rps: List of average RPS values from each repetition run.
                     Must contain at least 2 values.

    Returns:
        Tuple of (mean_rps, ci_95) where ci_95 is the half-width of the
        95% confidence interval.

    Raises:
        ValueError: If fewer than 2 values are provided.
    """
    n = len(per_run_rps)
    if n < 2:
        raise ValueError(f"Need at least 2 values for aggregation, got {n}")
    mean_rps = mean(per_run_rps)
    ci_95 = t_dist.ppf(0.975, n - 1) * (stdev(per_run_rps) / sqrt(n))
    return mean_rps, ci_95


def should_stop_adaptive(per_run_rps: list[float], rep: int, min_reps: int, target_cv: float) -> bool:
    """Return True if adaptive precision target is met and we can stop early.

    The stop criterion is the 95% confidence interval half-width of the mean,
    expressed as a percentage of the mean (t-distribution). This accounts for
    small sample sizes: raw CV understates uncertainty at low rep counts (at
    n=3 the t multiplier is 4.30x, so CV 0.5% is really a ±1.24% CI). On
    platforms with a bimodal between-restart distribution (Intel Xeon — see
    docs/benchmark-precision-guide.md), a few reps landing on the same mode
    can produce a deceptively low CV; the CI criterion makes such
    false-certainty early stops much harder.

    Args:
        per_run_rps: RPS values collected so far.
        rep: Current repetition index (0-based).
        min_reps: Minimum number of reps before early exit is allowed.
        target_cv: Target precision: 95% CI half-width as % of mean. 0 = disabled.
    """
    if target_cv <= 0 or rep < min_reps - 1 or len(per_run_rps) < 2:
        return False
    mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)
    if mean_rps == 0:
        return False
    # bool() because scipy's t.ppf yields a numpy float, making <= a numpy bool
    return bool((ci_95 / mean_rps) * 100 <= target_cv)


@dataclass
class PerfTaskData(BaseTaskData):
    """data class for performance benchmark task"""

    test: str
    val_size: int
    io_threads: int
    pipelining: int
    warmup: int
    duration: int
    perf_stat_enabled: bool
    has_expire: bool
    preload_keys: bool
    key_size: int = 0  # target key size in bytes, 0 = standard keys
    repetitions: int = 1  # number of independent benchmark runs (min reps in adaptive mode)
    max_reps: int = 0  # 0 = fixed reps; >0 = adaptive mode upper limit
    target_cv: float = 0.0  # adaptive: stop early when 95% CI half-width (% of mean) <= this; 0 = disabled
    # (field name kept as target_cv for queued-task schema compatibility)
    sweep_commit: str = ""  # non-empty marks this as a sweep task
    server_cpu_override: str = ""  # expert: explicit cpulist for server, bypasses topology-aware allocation
    benchmark_cpu_override: str = ""  # expert: explicit cpulist for benchmark client, bypasses allocation
    server_args: str = ""  # extra raw args appended to the server command line (override defaults)
    bench_threads: int = 0  # valkey-benchmark --threads override; 0 = PERF_BENCH_THREADS default
    bench_clients: int = 0  # valkey-benchmark -c override; 0 = PERF_BENCH_CLIENTS default
    client_netns: str = ""  # run the benchmark client inside this network namespace (dual-ENI
    # real-NIC hairpin topology; see docs/real-nic-hairpin.md). Empty = default namespace.
    bench_binary: str = ""  # expert: absolute path to an alternative benchmark binary. The client
    # is part of the workload definition, so results from an overridden binary are NOT comparable
    # with sweep history; the override is recorded in result metadata. Empty = repo default.
    generator_profile: str = ""  # named generator profile (e.g. "legacy-v1", "scalable-v2").
    # When set, overrides the default bench_binary resolution with a profile-managed binary.
    # Empty = legacy behavior (PROJECT_ROOT/valkey-benchmark from VALKEY_BENCHMARK_COMMIT).
    # bench_binary override still takes absolute precedence over generator_profile.

    def __post_init__(self):
        super().__post_init__()
        self.warmup = int(self.warmup)
        self.duration = int(self.duration)

    def short_description(self) -> str:
        return (
            f"{HumanByte.to_human(self.val_size)} {self.test} items for "
            f"{HumanTime.to_human(self.duration)}, {self.io_threads} threads"
            f", {self.pipelining} pipelined"
            f"{', perf-stat' if self.perf_stat_enabled else ''}"
        )

    def prepare_task_runner(self, server_infos: list[ServerInfo]) -> "PerfTaskRunner":
        """Return the task runner for this task."""
        return PerfTaskRunner(
            self.task_id,
            server_infos,
            self.source,
            self.specifier,
            topology=self.topology,
            io_threads=self.io_threads,
            valsize=self.val_size,
            pipelining=self.pipelining,
            test=self.test,
            warmup=self.warmup,
            duration=self.duration,
            preload_keys=self.preload_keys,
            has_expire=self.has_expire,
            make_args=self.make_args,
            perf_stat_enabled=self.perf_stat_enabled,
            note=self.note,
            key_size=self.key_size,
            repetitions=self.repetitions,
            max_reps=self.max_reps,
            target_cv=self.target_cv,
            server_cpu_override=self.server_cpu_override,
            benchmark_cpu_override=self.benchmark_cpu_override,
            server_args=self.server_args,
            bench_threads=self.bench_threads,
            bench_clients=self.bench_clients,
            client_netns=self.client_netns,
            bench_binary=self.bench_binary,
            generator_profile=self.generator_profile,
        )


@dataclass
class CanaryPerfTaskData(PerfTaskData):
    """Pinned performance task with reproducible key selection."""

    keyspace: int = PERF_BENCH_KEYSPACE
    seed: int = 1

    def prepare_task_runner(self, server_infos: list[ServerInfo]) -> "PerfTaskRunner":
        runner = super().prepare_task_runner(server_infos)
        runner.keyspace = self.keyspace
        runner.seed = self.seed
        return runner


class PerfTaskRunner(BaseTaskRunner):
    """Benchmark the throughput of a Valkey server."""

    @dataclass
    class Test:
        """Defines an available test"""

        name: str
        preload_command: Optional[str]
        test_command: str
        expire_command: Optional[str] = None
        # Optional per-test override for the timed benchmark's -r keyspace.
        # Preload always fills PERF_BENCH_KEYSPACE; this only widens the -r used
        # during measurement (needed by zpop, whose append must draw from a
        # namespace far larger than the resident set to avoid draining it).
        keyspace: Optional[int] = None

    # Append namespace for the zpop sliding-window test. ZPOPMIN never misses,
    # so a 50/50 pop/append drains with time-constant = append-keyspace (in
    # pairs). At ~1.5M pairs/s over a 5min run (~500M pairs) a 2B namespace keeps
    # the ~3M resident set from draining more than ~25%, so it never empties and
    # ZPOPMIN (O(1)) throughput stays representative.
    ZPOP_APPEND_KEYSPACE = 2_000_000_000

    tests: dict[str, Test] = {
        "set": Test(
            name="set",
            preload_command="-t set",
            test_command="-t set",
            expire_command=f"EXPIRE key:__rand_int__ {7*24*60*60}",
        ),
        "get": Test(
            name="get",
            preload_command="-t set",
            test_command="-t get",
            expire_command=f"EXPIRE key:__rand_int__ {7*24*60*60}",
        ),
        "sadd": Test(name="sadd", preload_command="-t sadd", test_command="-t sadd"),
        "hset": Test(name="hset", preload_command="-t hset", test_command="-t hset"),
        "zadd": Test(name="zadd", preload_command="-t zadd", test_command="-t zadd"),
        "zrank": Test(
            name="zrank",
            preload_command="-t zadd",
            test_command=" -- ZRANK myzset element:__rand_int__",
        ),
        "zcount": Test(
            name="zcount",
            preload_command="-t zadd",
            test_command=" -- ZCOUNT myzset __rand_int__ __rand_int__",
        ),
        "zscore": Test(
            name="zscore",
            preload_command="-t zadd",
            test_command=" -- ZSCORE myzset element:__rand_int__",
        ),
        # Scattered range scan of ~100 elements at a random rank offset. --count
        # activates __rand_beg__/__rand_end__ (end = beg + count - 1).
        "zrange": Test(
            name="zrange",
            preload_command="-t zadd",
            test_command="--count 100 -- ZRANGE myzset __rand_beg__ __rand_end__",
        ),
        # Score-range variant: preloaded scores are dense (sequential 0..K-1),
        # so a width-100 score window returns ~100 elements.
        "zrangebyscore": Test(
            name="zrangebyscore",
            preload_command="-t zadd",
            test_command="--count 100 -- ZRANGEBYSCORE myzset __rand_beg__ __rand_end__",
        ),
        # Random-sample read of 100 members (literal COUNT arg; no placeholder).
        "zrandmember": Test(
            name="zrandmember",
            preload_command="-t zadd",
            test_command=" -- ZRANDMEMBER myzset 100",
        ),
        # Scattered delete: 50/50 random add/remove over the same keyspace. ZREM
        # can miss, so the pair self-balances at ~K/2 (birth-death, P=0.5). The
        # ';' is shell-quoted so valkey-benchmark receives it as a sequence
        # separator. ~50% of ZREMs miss (hashtable-only) -> structural signal is
        # directional. Warmup relaxes the preloaded full set K -> K/2.
        "zrem": Test(
            name="zrem",
            preload_command="-t zadd",
            test_command=" -- ZADD myzset __rand_int__ element:__rand_int__ ';' ZREM myzset element:__rand_int__",
        ),
        # Hot-spot delete: sliding window. ZPOPMIN removes the min; ZADD appends
        # a new member (distinct 'nm:' prefix) drawn from a huge namespace so the
        # resident set drains only slowly and never empties (see
        # ZPOP_APPEND_KEYSPACE). Measures pop+append combined (O(1) pop).
        "zpop": Test(
            name="zpop",
            preload_command="-t zadd",
            test_command=" -- ZPOPMIN myzset ';' ZADD myzset __rand_int__ nm:__rand_int__",
            keyspace=ZPOP_APPEND_KEYSPACE,
        ),
        "sismember": Test(
            name="sismember",
            preload_command="-t sadd",
            test_command=" -- SISMEMBER myset element:__rand_int__",
        ),
        "ping": Test(
            name="ping",
            preload_command=None,
            test_command="-t ping",
        ),
        "mget": Test(
            name="mget",
            preload_command="-t set",
            test_command=" -- MGET key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__",
        ),
        # Dante's regression-list shapes (2026-10-22): 10 distinct random keys per
        # command, streams, and lists. MGET/MSET each __rand_int__ occurrence is an
        # independent value, so the 10 keys are 10 distinct random keys.
        "mget10": Test(name="mget10", preload_command="-t set", test_command=" -- MGET key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__"),
        "mset10": Test(name="mset10", preload_command="-t set", test_command=" -- MSET key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__ key:__rand_int__ __rand_field__"),
        "lpush": Test(name="lpush", preload_command=" -- LPUSH key:__rand_int__ __rand_field__", test_command=" -- LPUSH key:__rand_int__ __rand_field__"),
        "lrange": Test(name="lrange", preload_command=" -- LPUSH key:__rand_int__ __rand_field__", test_command=" -- LRANGE key:__rand_int__ 0 99"),
        "xadd": Test(name="xadd", preload_command=" -- XADD key:__rand_int__ MAXLEN ~ 1000 * f __rand_field__", test_command=" -- XADD key:__rand_int__ MAXLEN ~ 1000 * f __rand_field__"),
        "xrange": Test(name="xrange", preload_command=" -- XADD key:__rand_int__ MAXLEN ~ 1000 * f __rand_field__", test_command=" -- XRANGE key:__rand_int__ - + COUNT 100"),
    }

    def __init__(
        self,
        task_name: str,
        server_infos: list[ServerInfo],
        binary_source: str,
        specifier: str,
        io_threads: int,
        valsize: int,
        pipelining: int,
        test: str,
        warmup: int,
        duration: int,
        preload_keys: bool,
        has_expire: bool,
        make_args: str,
        perf_stat_enabled: bool = False,
        note: str = "",
        key_size: int = 0,
        repetitions: int = 1,
        max_reps: int = 0,
        target_cv: float = 0.0,
        server_cpu_override: str = "",
        benchmark_cpu_override: str = "",
        server_args: str = "",
        bench_threads: int = 0,
        bench_clients: int = 0,
        keyspace: int = 0,
        seed: int = 0,
        client_netns: str = "",
        bench_binary: str = "",
        generator_profile: str = "",
        topology: Optional[TopologySpec] = None,
    ):
        super().__init__(task_name)

        self.logger = logging.getLogger(self.__class__.__name__ + "." + test)

        # settings
        self.task_name = task_name
        self.server_infos = server_infos
        self.topology = topology if topology is not None else TopologySpec.standalone()
        self.binary_source = binary_source
        self.specifier = specifier
        # CPU flamegraph stacks expose the server binary's symbols; skip for engines
        # that opt out (Redis). Aggregate perf-stat counters are unaffected.
        self._profile_internals = should_profile_internals(get_sweep_engine(binary_source))
        self.io_threads = io_threads
        self.valsize = valsize
        self.pipelining = pipelining
        self.test: PerfTaskRunner.Test = PerfTaskRunner.tests[test]
        self.warmup = warmup  # seconds
        self.duration = duration  # seconds
        self.preload_keys = preload_keys
        self.has_expire = has_expire
        self.note = note
        self.make_args = make_args
        self.key_size = key_size
        self.repetitions = repetitions
        self.max_reps = max_reps
        self.target_cv = target_cv
        self.server_cpu_override = server_cpu_override
        self.benchmark_cpu_override = benchmark_cpu_override
        self.server_args = server_args
        self.bench_threads = bench_threads or PERF_BENCH_THREADS
        self.bench_clients = bench_clients or PERF_BENCH_CLIENTS
        self.keyspace = keyspace or PERF_BENCH_KEYSPACE
        self.seed = seed
        self.client_netns = client_netns
        # Expert override for the benchmark binary (generator A/Bs). The
        # client is part of the workload definition: overridden results are
        # not sweep-comparable, so the override is recorded in metadata.
        self.bench_binary = bench_binary
        # Generator profile: when set, resolves bench_binary via the profile
        # system at execution time. bench_binary override takes precedence.
        self.generator_profile = generator_profile
        self._generator_provenance: dict = {}  # populated at resolve time

        self.perf_stat_enabled = perf_stat_enabled
        self._is_last_rep = False
        self._current_rep = 0  # 0-indexed current repetition (set by _execute_benchmark_loop)
        self._cpu_stacks_main: list[list] = []
        self._perf_stat_scope: Optional[str] = None
        self._cpu_stacks_io: list[list] = []
        # Client (load generator) CPU telemetry: cores kept busy by the
        # generator process tree during each measurement window, plus the
        # core budget it was confined to (None when unknown, e.g. remote
        # client without an explicit override).
        self._client_cores_busy_per_rep: list[float] = []
        self._client_allocated_cores: Optional[int] = None
        self._perf_rep_count = 0  # reps whose perf counters were summed into perf_counters
        self._perf_duration_seconds: Optional[float] = None

        # Build custom commands when key_size > 0
        if self.key_size > 0:
            padded_key = generate_padded_key(self.key_size)
            preload_custom = self._build_custom_command(self.test, padded_key, is_preload=True)
            test_custom = self._build_custom_command(self.test, padded_key, is_preload=False)
            self.preload_command: Optional[str] = preload_custom
            self.test_command: Optional[str] = test_custom
        else:
            self.preload_command = self.test.preload_command
            self.test_command = self.test.test_command

        self.title = (
            f"{test} throughput, {binary_source}:{specifier}, io-threads={io_threads}, "
            f"pipelining={pipelining}, size={HumanByte.to_human(valsize)}, "
            f"warmup={HumanTime.to_human(warmup)}, "
            f"duration={HumanTime.to_human(duration)}"
        )
        if self.key_size > 0:
            self.title += f", key-size={HumanByte.to_human(self.key_size)}"
        if self.perf_stat_enabled:
            self.title += ", perf-stat"

        # statistics
        self.rps_data: list[float] = []

        self.commit_hash = ""

        # Initialize status
        self.status = BenchmarkStatus(steps_total=self.warmup + self.duration, task_type=f"perf-{test}")

    def _build_custom_command(self, test: "PerfTaskRunner.Test", padded_key: str, is_preload: bool) -> Optional[str]:
        """Build a custom command string for the given test type using the padded key.

        Returns the custom command string, or None if the test has no preload
        and is_preload is True.
        """
        name = test.name

        if is_preload:
            preload_map: dict[str, Optional[str]] = {
                "set": f" -- SET {padded_key} __rand_field__",
                "get": f" -- SET {padded_key} __rand_field__",
                "sadd": f" -- SADD {padded_key} element:__rand_int__",
                "sismember": f" -- SADD {padded_key} element:__rand_int__",
                "hset": f" -- HSET {padded_key} field:__rand_int__ __rand_field__",
                "zadd": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "mget": f" -- SET {padded_key} __rand_field__",
                "mget10": f" -- SET {padded_key} __rand_field__",
                "mset10": f" -- SET {padded_key} __rand_field__",
                "lpush": f" -- LPUSH {padded_key} __rand_field__",
                "lrange": f" -- LPUSH {padded_key} __rand_field__",
                "xadd": f" -- XADD {padded_key} MAXLEN ~ 1000 * f __rand_field__",
                "xrange": f" -- XADD {padded_key} MAXLEN ~ 1000 * f __rand_field__",
                "ping": None,
                "zrank": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "zcount": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "zscore": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "zrange": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "zrangebyscore": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "zrandmember": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "zrem": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "zpop": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
            }
            return preload_map[name]
        else:
            test_map: dict[str, str] = {
                "set": f" -- SET {padded_key} __rand_field__",
                "get": f" -- GET {padded_key}",
                "sadd": f" -- SADD {padded_key} element:__rand_int__",
                "sismember": f" -- SISMEMBER {padded_key} element:__rand_int__",
                "hset": f" -- HSET {padded_key} field:__rand_int__ __rand_field__",
                "zadd": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__",
                "mget": f" -- MGET {padded_key} {padded_key} {padded_key} {padded_key}",
                "mget10": f" -- MGET {padded_key} {padded_key} {padded_key} {padded_key} {padded_key} {padded_key} {padded_key} {padded_key} {padded_key} {padded_key}",
                "mset10": f" -- MSET {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__ {padded_key} __rand_field__",
                "lpush": f" -- LPUSH {padded_key} __rand_field__",
                "lrange": f" -- LRANGE {padded_key} 0 99",
                "xadd": f" -- XADD {padded_key} MAXLEN ~ 1000 * f __rand_field__",
                "xrange": f" -- XRANGE {padded_key} - + COUNT 100",
                "ping": "-t ping",
                "zrank": f" -- ZRANK {padded_key} element:__rand_int__",
                "zcount": f" -- ZCOUNT {padded_key} __rand_int__ __rand_int__",
                "zscore": f" -- ZSCORE {padded_key} element:__rand_int__",
                "zrange": f"--count 100 -- ZRANGE {padded_key} __rand_beg__ __rand_end__",
                "zrangebyscore": f"--count 100 -- ZRANGEBYSCORE {padded_key} __rand_beg__ __rand_end__",
                "zrandmember": f" -- ZRANDMEMBER {padded_key} 100",
                "zrem": f" -- ZADD {padded_key} __rand_int__ element:__rand_int__ ';' ZREM {padded_key} element:__rand_int__",
                "zpop": f" -- ZPOPMIN {padded_key} ';' ZADD {padded_key} __rand_int__ nm:__rand_int__",
            }
            return test_map[name]

    async def __collect_metrics(self, command: RealtimeCommand):
        line, _ = command.poll_output()
        while line is not None and line != "" and not line.isspace():
            if "overall" not in line:
                line, _ = command.poll_output()
                continue
            # line looks like this:
            # "GET: rps=140328.0 (overall: 141165.2) avg_msec=0.193 (overall: 0.191)"
            # or this:
            # ZRANK myzset ele__rand_int__: rps=442912.0 (overall: 436252.6) avg_msec=5.868 (overall: 5.948)
            rps = float(line.split("rps=")[1].split()[0])
            self.rps_data.append(rps)

            # Write metric to file protocol
            metric = MetricData(metrics={"rps": rps}, rep=self._current_rep + 1)
            self.file_protocol.append_metric(metric)

            line, _ = command.poll_output()

    def _store_perf_counters(self, detailed_data: dict, perf_counters: dict) -> None:
        """Write perf counters into detailed_data, splitting per-thread buckets.

        ``perf_counters`` is the bucketed structure produced by the collection path:
        ``{"all": {...}, "main": {...}, "io": {...}}``. The process-wide total goes
        to ``perf_counters`` (preserving the historical key/shape so existing export
        and dashboard code is unaffected); the per-thread groups go to
        ``perf_counters_main`` / ``perf_counters_io`` when non-empty. A legacy flat
        dict (no bucket keys) is treated as the process-wide total.
        """
        if any(k in perf_counters for k in ("all", "main", "io")):
            all_counters = perf_counters.get("all") or {}
            main_counters = perf_counters.get("main") or {}
            io_counters = perf_counters.get("io") or {}
        else:
            all_counters, main_counters, io_counters = perf_counters, {}, {}

        if all_counters:
            detailed_data["perf_counters"] = all_counters
            if self._perf_stat_scope:
                detailed_data["perf_counters_scope"] = self._perf_stat_scope
            detailed_data["perf_duration_seconds"] = (
                self._perf_duration_seconds if self._perf_duration_seconds is not None else float(self.duration)
            )
            # Counters are summed across reps; record how many so the exporter can
            # normalize absolute per-request metrics (instructions-per-req).
            detailed_data["perf_rep_count"] = self._perf_rep_count or 1
        if main_counters:
            detailed_data["perf_counters_main"] = main_counters
        if io_counters:
            detailed_data["perf_counters_io"] = io_counters

    async def __record_result(
        self,
        server,
        per_run_rps: Optional[list[float]] = None,
        perf_counters: Optional[dict] = None,
        memory_records: Optional[list[dict]] = None,
    ):
        completion_time = datetime.datetime.now()

        if len(self.rps_data) == 0 and not per_run_rps:
            raise RuntimeError("No results recorded")

        # Fold per-rep memory records (peak fields -> max) into one data.memory.
        folded_memory = fold_memory_records(memory_records) if memory_records else None

        # Get system information
        lscpu_output, _ = await server.run_host_command("lscpu")

        if per_run_rps is not None and len(per_run_rps) > 1:
            # Aggregated result for repetitions > 1
            mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)
            cv = (stdev(per_run_rps) / mean_rps) * 100 if mean_rps else 0.0
            reps = len(per_run_rps)

            detailed_data = {
                "topology": self.topology.to_dict(),
                "management_cpus": self.management_cpus,
                "warmup": self.warmup,
                "duration": self.duration,
                "io-threads": self.io_threads,
                "pipeline": self.pipelining,
                "has_expire": self.has_expire,
                "size": self.valsize,
                "key_size": self.key_size,
                "keyspace": self.keyspace,
                "seed": self.seed,
                "preload_keys": self.preload_keys,
                "perf_stat_enabled": self.perf_stat_enabled,
                "lscpu": lscpu_output,
                "server_cpus": server.server_cpus,
                "repetitions": self.repetitions,
                "per_run_rps": per_run_rps,
                "mean_rps": mean_rps,
                "ci_95": ci_95,
            }
            if perf_counters:
                self._store_perf_counters(detailed_data, perf_counters)
            if self._cpu_stacks_main:
                detailed_data["cpu_stacks_main"] = self._cpu_stacks_main
                detailed_data["cpu_stacks_io"] = self._cpu_stacks_io
            if self._client_cores_busy_per_rep:
                detailed_data["client_cpu"] = summarize_client_cpu(
                    self._client_cores_busy_per_rep, self._client_allocated_cores
                )
            if self.bench_binary:
                detailed_data["bench_binary"] = self.bench_binary
            if self._generator_provenance:
                detailed_data["generator_provenance"] = self._generator_provenance
            if folded_memory is not None:
                detailed_data["memory"] = folded_memory

            results = BenchmarkResults(
                method=f"perf-{self.test.name}",
                source=self.binary_source,
                specifier=self.specifier,
                commit_hash=self.commit_hash,
                score=mean_rps,
                end_time=completion_time,
                data=detailed_data,
                make_args=self.make_args,
                note=self.note,
                cv=cv,
                reps=reps,
            )
        else:
            # Single-run result (repetitions == 1 or legacy behavior)
            avg_rps = sum(self.rps_data) / len(self.rps_data)

            detailed_data = {
                "topology": self.topology.to_dict(),
                "management_cpus": self.management_cpus,
                "warmup": self.warmup,
                "duration": self.duration,
                "io-threads": self.io_threads,
                "pipeline": self.pipelining,
                "has_expire": self.has_expire,
                "size": self.valsize,
                "key_size": self.key_size,
                "keyspace": self.keyspace,
                "seed": self.seed,
                "preload_keys": self.preload_keys,
                "perf_stat_enabled": self.perf_stat_enabled,
                "avg_rps": avg_rps,
                "lscpu": lscpu_output,
                "server_cpus": server.server_cpus,
            }
            if perf_counters:
                self._store_perf_counters(detailed_data, perf_counters)
            if self._cpu_stacks_main:
                detailed_data["cpu_stacks_main"] = self._cpu_stacks_main
                detailed_data["cpu_stacks_io"] = self._cpu_stacks_io
            if self._client_cores_busy_per_rep:
                detailed_data["client_cpu"] = summarize_client_cpu(
                    self._client_cores_busy_per_rep, self._client_allocated_cores
                )
            if self.bench_binary:
                detailed_data["bench_binary"] = self.bench_binary
            if self._generator_provenance:
                detailed_data["generator_provenance"] = self._generator_provenance
            if folded_memory is not None:
                detailed_data["memory"] = folded_memory

            results = BenchmarkResults(
                method=f"perf-{self.test.name}",
                source=self.binary_source,
                specifier=self.specifier,
                commit_hash=self.commit_hash,
                score=avg_rps,
                end_time=completion_time,
                data=detailed_data,
                make_args=self.make_args,
                note=self.note,
            )

        self.file_protocol.write_results(results)

    async def run(self):
        """Run the benchmark.

        When repetitions > 1 or adaptive mode is enabled, executes the benchmark
        loop N times sequentially with server restarts between reps.
        Otherwise, runs a single benchmark pass.
        """
        effective_reps = self.max_reps if self.max_reps > 0 else self.repetitions
        if effective_reps > 1:
            total_steps = (self.warmup + self.duration) * effective_reps
            self.status.steps_total = total_steps
            self.logger.info(
                "preparing: %s %s",
                self.title,
                (
                    f"({effective_reps} max repetitions, target ±{self.target_cv}% CI95)"
                    if self.target_cv > 0
                    else f"({self.repetitions} repetitions)"
                ),
            )
        else:
            total_steps = self.warmup + self.duration
            self.logger.info("preparing: %s", self.title)

        self.file_protocol.write_status(self.status)

        topology_group = TopologyGroup.for_task(
            self.server_infos,
            self.topology,
            self.binary_source,
            self.specifier,
            io_threads=self.io_threads,
            make_args=self.make_args,
            server_args=self.server_args,
            cpu_override=self.server_cpu_override,
        )

        benchmark_alloc_tag = None
        client = None
        server = None
        per_run_rps: list[float] = []
        perf_counters: Optional[dict] = None
        memory_records: list[dict[str, Any]] = []

        try:
            for rep in range(effective_reps):
                # Between-rep housekeeping (skip on first rep)
                if rep > 0:
                    await topology_group.stop_all_servers()
                    # Drop page caches between reps to prevent drift.
                    # Skip on Intel (large monolithic L3 stays warm).
                    primary_server = topology_group.primary or Server(self.server_infos[0].ip)
                    platform = getattr(primary_server, "_platform_info", None)
                    if platform is None or platform.needs_drop_caches:
                        await primary_server.run_host_command(
                            "sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'",
                            check=False,
                        )

                # Start server
                await topology_group.kill_all_valkey_instances()
                await topology_group.start()
                if not topology_group.primary:
                    raise RuntimeError("Replication group failed to start: no primary server available")

                await topology_group.begin_replication()
                await topology_group.wait_for_repl_sync()
                server = topology_group.primary
                self.commit_hash = server.get_build_hash() or ""

                # Preload data
                if self.preload_keys and self.preload_command is not None:
                    await server.run_valkey_command_over_keyspace(
                        self.keyspace, f"-d {self.valsize} {self.preload_command}"
                    )
                    if self.has_expire:
                        if not self.test.expire_command:
                            self.logger.warning("Expire command not available, skipping expiration")
                        else:
                            await server.run_valkey_command_over_keyspace(self.keyspace, self.test.expire_command)

                # Setup client CPU allocation (once)
                if client is None:
                    client = Server("127.0.0.1")
                    await client.ensure_host_cpu_allocation()
                    benchmark_alloc_tag = self._allocate_benchmark_cpus(client, server)
                    # Record the client's core budget for generator-saturation
                    # telemetry (None = unknown, e.g. cpunodebind fallback).
                    if self.benchmark_cpu_override:
                        self._client_allocated_cores = count_cpu_list(self.benchmark_cpu_override)
                    elif benchmark_alloc_tag is not None:
                        self._client_allocated_cores = self.bench_threads
                    # Preflight for the dual-ENI hairpin: network namespaces do
                    # NOT persist across reboots, so fail loudly and point at
                    # the fix rather than dying later with a cryptic benchmark
                    # error (docs/real-nic-hairpin.md).
                    if self.client_netns:
                        ns_list, _ = await client.run_host_command("ip netns list", check=False)
                        ns_names = {line.split()[0] for line in ns_list.splitlines() if line.strip()}
                        if self.client_netns not in ns_names:
                            raise RuntimeError(
                                f"client_netns '{self.client_netns}' does not exist on this host "
                                f"(namespaces do not survive reboots). Run "
                                f"scripts/setup-loadgen-netns.sh or install the "
                                f"loadgen-netns systemd unit — see docs/real-nic-hairpin.md."
                            )

                # Build and execute benchmark command
                command_string = self._build_benchmark_command(client, server.ip, benchmark_alloc_tag)
                self._is_last_rep = rep == effective_reps - 1
                # 1 Hz peak-memory sampler on its own connection, spanning the
                # generator run (warmup + scored window). It never raises into
                # the cell; a failed connect leaves sample_errors set.
                mem_sampler = MemorySampler(server.ip, server.port)
                mem_sampler.start()
                servers = servers_by_role(server, topology_group.replicas)
                try:
                    avg_rps = await self._execute_benchmark_loop(
                        command_string, server, rep, effective_reps, reset_servers=servers
                    )
                finally:
                    mem_sampler.stop()
                # After the scored window, before the server is stopped: read
                # INFO memory per server and assemble this rep's data.memory.
                # The peaks were re-armed at warmup end (self._memory_resets).
                try:
                    memory_records.append(await collect_memory_after(servers, mem_sampler, self._memory_resets))
                except Exception as exc:  # capture must never fail the cell
                    self.logger.warning("memory capture failed on rep %d: %s", rep + 1, exc)
                per_run_rps.append(avg_rps)
                self.logger.info("Repetition %d/%d avg RPS: %.1f", rep + 1, effective_reps, avg_rps)

                # Update last-rep flag after we have this rep's data
                if not self._is_last_rep:
                    self._is_last_rep = should_stop_adaptive(per_run_rps, rep, self.repetitions, self.target_cv)

                # Collect profiling reports
                rep_counters = await self._collect_profiling_reports(server)
                if rep_counters:
                    # Sum raw counters across all reps for better statistical robustness.
                    # rep_counters is bucketed: {"all": {...}, "main": {...}, "io": {...}}.
                    # NOTE: because counters are SUMMED across reps while rps/duration
                    # describe a single rep, the per-request divisor must multiply by
                    # this rep count (tracked here, persisted as perf_rep_count) — see
                    # exporter instructions-per-req. Ratio metrics (IPC, MPKI) are
                    # unaffected since the rep factor cancels.
                    self._perf_rep_count += 1
                    if perf_counters is None:
                        perf_counters = rep_counters
                    else:
                        for bucket, events in rep_counters.items():
                            acc = perf_counters.setdefault(bucket, {})
                            for k, v in events.items():
                                acc[k] = acc.get(k, 0) + v

                # Collect CPU profile stacks on last rep
                if self._is_last_rep and self.perf_stat_enabled and self._profile_internals:
                    try:
                        cpu_main, cpu_io = await server.cpu_profile_collect()
                        if cpu_main:
                            self._cpu_stacks_main = cpu_main
                            self._cpu_stacks_io = cpu_io
                    except Exception as e:
                        self.logger.warning("CPU profile collection failed: %s", e)

                # Adaptive early exit
                if should_stop_adaptive(per_run_rps, rep, self.repetitions, self.target_cv):
                    mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)
                    self.logger.info(
                        "Precision target reached: ±%.2f%% (95%% CI) <= %.2f%% after %d reps",
                        ci_95 / mean_rps * 100,
                        self.target_cv,
                        rep + 1,
                    )
                    break

            # Record results
            if server is None:
                raise RuntimeError("No server available for recording results")
            if effective_reps > 1:
                await self.__record_result(
                    server, per_run_rps=per_run_rps, perf_counters=perf_counters, memory_records=memory_records
                )
            else:
                await self.__record_result(server, perf_counters=perf_counters, memory_records=memory_records)

            # Write final status
            self.status.state = "completed"
            self.status.end_time = time.time()
            self.status.steps_completed = total_steps
            self.file_protocol.write_status(self.status)

        finally:
            await topology_group.stop_all_servers()
            if benchmark_alloc_tag and client:
                client.release_cpus(benchmark_alloc_tag)

    def _allocate_benchmark_cpus(self, client: "Server", server: "Server") -> Optional[AllocationTag]:
        """Allocate CPUs for the benchmark client. Returns the tag or None."""
        if self.benchmark_cpu_override:
            self.logger.info("Using explicit benchmark CPU override: %s", self.benchmark_cpu_override)
            return None

        target_ip = server.ip
        if not self._is_local_benchmark(target_ip):
            return None

        self.logger.info("Local benchmark detected - optimizing CPU allocation")
        server_tag = AllocationTag(task_id=f"server_{server.ip}_{server.port}", purpose="server")
        platform = getattr(server, "_platform_info", None)
        is_chiplet = platform is not None and platform.needs_single_cache_pinning
        benchmark_alloc_tag = AllocationTag(task_id=self.task_name, purpose="benchmark")
        benchmark_cpus = client.allocate_client_cpus(
            benchmark_alloc_tag,
            self.bench_threads,
            avoid_tags=[server_tag],
            minimize_cache_groups=is_chiplet,
        )
        self.logger.info(
            "Allocated CPUs %s for benchmark client (NUMA node %s)",
            benchmark_cpus,
            client.net_numa_node(),
        )
        return benchmark_alloc_tag

    def _resolve_generator_binary(self) -> str:
        """Resolve an explicit generator profile to a local binary path.

        No profile preserves the legacy default path.  An explicit profile is
        part of the workload identity, so bootstrap or verification failures
        must fail the task rather than silently running the legacy generator.
        """
        if not self.generator_profile:
            return ""

        from conductress.generator_profiles import resolve_bench_binary

        path, provenance = resolve_bench_binary(
            generator_profile=self.generator_profile,
            bench_binary_override="",
        )
        self._generator_provenance = provenance
        return path

    def _build_benchmark_command(
        self,
        client: "Server",
        target_ip: str,
        benchmark_alloc_tag: Optional[AllocationTag],
    ) -> str:
        """Build the numactl + valkey-benchmark command string."""
        net_numa = client.net_numa_node()
        bench_bin = self.bench_binary or self._resolve_generator_binary() or str(PROJECT_ROOT / VALKEY_BENCHMARK)

        # Dual-ENI real-NIC hairpin (docs/real-nic-hairpin.md): run the client
        # inside a network namespace holding the secondary ENI. The namespace
        # has its own loopback, so a 127.0.0.1 target must be rewritten to the
        # host's primary-interface IP — the request then exits the secondary
        # ENI, traverses the VPC fabric, and re-enters via the primary ENI
        # (real driver/IRQ/NAPI path both directions). Locality decisions
        # (_is_local_benchmark, CPU allocation) still use the ORIGINAL
        # target_ip: the client process runs on this host either way.
        netns_prefix = ""
        bench_target = target_ip
        if self.client_netns:
            netns_prefix = f"sudo ip netns exec {self.client_netns} "
            if self._is_local_benchmark(target_ip):
                bench_target = get_primary_interface_ip()

        # Per-test overrides (currently zpop) take precedence over the
        # task-level keyspace. A nonzero seed freezes random key selection.
        finite_request_count = int(getattr(self, "finite_request_count", 0))
        keyspace = finite_request_count or self.test.keyspace or getattr(self, "keyspace", PERF_BENCH_KEYSPACE)
        seed = getattr(self, "seed", 0)
        seed_arg = f" --seed {seed}" if seed else ""
        iteration_args = (
            f"--sequential -n {finite_request_count}" if finite_request_count else f"-l -n {BENCHMARK_MAX_ITERATIONS}"
        )

        if self.benchmark_cpu_override:
            # Expert override: use the explicit cpulist verbatim. Bind memory
            # to the NUMA node(s) of the override CPUs (NOT the NIC node) so a
            # cross-socket placement doesn't silently run with 100% remote memory.
            from conductress.utility import parse_cpulist

            override_cpus = parse_cpulist(self.benchmark_cpu_override)
            override_nodes = client.cpu_allocator.get_numa_nodes_for_cpus(client.ip, override_cpus)
            membind = ",".join(map(str, override_nodes)) if override_nodes else str(net_numa)
            return (
                f"{netns_prefix}numactl --physcpubind={self.benchmark_cpu_override} --membind={membind} "
                f"{bench_bin} -h {bench_target} -d {self.valsize} "
                f"-r {keyspace} -c {self.bench_clients} -P {self.pipelining} "
                f"--threads {self.bench_threads}{seed_arg} -q {iteration_args} {self.test_command}"
            )
        elif benchmark_alloc_tag and self._is_local_benchmark(target_ip):
            benchmark_cpu_list = client.allocated_cpu_list(benchmark_alloc_tag)
            return (
                f"{netns_prefix}numactl --physcpubind={benchmark_cpu_list} --membind={net_numa} "
                f"{bench_bin} -h {bench_target} -d {self.valsize} "
                f"-r {keyspace} -c {self.bench_clients} -P {self.pipelining} "
                f"--threads {self.bench_threads}{seed_arg} -q {iteration_args} {self.test_command}"
            )
        else:
            return (
                f"{netns_prefix}numactl --cpunodebind={net_numa} --membind={net_numa} "
                f"{bench_bin} -h {bench_target} -d {self.valsize} "
                f"-r {keyspace} -c {self.bench_clients} -P {self.pipelining} "
                f"--threads {self.bench_threads}{seed_arg} -q {iteration_args} {self.test_command}"
            )

    async def _execute_benchmark_loop(
        self,
        command_string: str,
        server: "Server",
        rep: int,
        total_reps: int,
        reset_servers: Optional[dict[str, Any]] = None,
    ) -> float:
        """Execute one benchmark run (warmup + measurement). Returns avg RPS.

        ``reset_servers`` (role -> Server) has its memory peaks re-armed the
        moment warmup ends, so ``data.memory`` describes the scored window
        only; the reset records land in ``self._memory_resets``.
        """
        benchmark_update_interval = BENCHMARK_UPDATE_INTERVAL
        self._current_rep = rep
        self.rps_data = []
        self._memory_resets: Optional[dict[str, dict[str, Any]]] = None

        command = RealtimeCommand(command_string)
        self.logger.info(
            "Starting realtime command (rep %d/%d): %s",
            rep + 1,
            total_reps,
            command_string,
        )
        command.start()
        start_time = time.monotonic()
        test_start_time = start_time + self.warmup
        end_time = test_start_time + self.duration
        warming_up = True

        self.status.state = "running"
        self.file_protocol.write_status(self.status)

        self.logger.info(f"started rt cmd (rep {rep + 1}/{total_reps})")
        last_heartbeat = time.time()
        client_cpu_start: Optional[float] = None
        client_cpu_start_time = 0.0
        while command.is_running():
            await self.__collect_metrics(command)
            time.sleep(benchmark_update_interval)
            now = time.monotonic()

            if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                elapsed_total_time = now - start_time
                steps_this_rep = min(int(elapsed_total_time), self.warmup + self.duration)
                self.status.steps_completed = rep * (self.warmup + self.duration) + steps_this_rep
                self.file_protocol.write_status(self.status)
                last_heartbeat = time.time()

            if now > end_time:
                # Close the generator CPU sample over the measurement window
                # before killing the process tree.
                if client_cpu_start is not None and command.p is not None:
                    client_cpu_end = sample_process_tree_cpu(command.p.pid)
                    elapsed = time.monotonic() - client_cpu_start_time
                    if client_cpu_end is not None and elapsed > 0:
                        cores_busy = (client_cpu_end - client_cpu_start) / elapsed
                        self._client_cores_busy_per_rep.append(cores_busy)
                        if self._client_allocated_cores and cores_busy / self._client_allocated_cores >= 0.9:
                            self.logger.warning(
                                "Load generator used %.2f of %d allocated cores (>=90%%): "
                                "throughput may reflect CLIENT capacity, not the server's.",
                                cores_busy,
                                self._client_allocated_cores,
                            )
                if self.perf_stat_enabled:
                    await server.perf_stat_stop()
                command.kill()
            elif warming_up and now >= test_start_time:
                self.rps_data = []
                warming_up = False
                # Scored window opens: reset the kernel/cgroup peaks and record
                # the engine floors so data.memory excludes prefill and warmup.
                # Done before perf stat arms so the INFO round-trip is not
                # counted in the profiled window.
                if reset_servers:
                    self._memory_resets = await reset_memory_peaks(reset_servers)
                if command.p is not None:
                    client_cpu_start = sample_process_tree_cpu(command.p.pid)
                    client_cpu_start_time = time.monotonic()
                if self.perf_stat_enabled:
                    await server.perf_stat_start()
                if self.perf_stat_enabled and self._profile_internals:
                    server.cpu_profile_start(self.duration)

        await self.__collect_metrics(command)

        if len(self.rps_data) == 0:
            raise RuntimeError(f"No results recorded for repetition {rep + 1}")
        return sum(self.rps_data) / len(self.rps_data)

    async def _collect_profiling_reports(self, server: "Server") -> Optional[dict]:
        """Collect perf stat and CPU profile reports. Returns perf counters dict or None."""
        if self.perf_stat_enabled:
            server.perf_stat_wait()
            result_dir = self.file_protocol.get_result_dir()
            counters = await server.perf_stat_report(result_dir)
            self._perf_stat_scope = server.perf_stat_scope or self._perf_stat_scope
            return counters
        return None

    def _is_local_benchmark(self, target_ip: str) -> bool:
        """Check if benchmark is running locally (server and client on same host)."""
        # Normalize localhost variations
        local_ips = {"127.0.0.1", "localhost", "::1"}
        return target_ip in local_ips


RSS_SAMPLE_INTERVAL_SECONDS = 0.5


def checked_memory_snapshot(info: dict[str, str], maxmemory_bytes: int, max_rss_bytes: int) -> tuple[int, int]:
    """Return ``(used_memory, rss)`` and reject a crossed safety bound."""
    try:
        used_memory = int(info["used_memory"])
        rss = int(info["used_memory_rss"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("INFO memory did not provide numeric used_memory and used_memory_rss") from exc
    if used_memory > maxmemory_bytes:
        raise RuntimeError(f"used_memory safety limit exceeded: {used_memory} > {maxmemory_bytes}")
    if rss > max_rss_bytes:
        raise RuntimeError(f"RSS safety limit exceeded: {rss} > {max_rss_bytes}")
    return used_memory, rss


@dataclass
class BoundedInsertionTaskData(BaseTaskData):
    """Finite new-key-only SET benchmark with explicit memory bounds."""

    val_size: int
    key_size: int
    io_threads: int
    pipelining: int
    insertions: int
    repetitions: int
    maxmemory_bytes: int
    max_rss_bytes: int
    perf_stat_enabled: bool = False
    server_cpu_override: str = ""
    benchmark_cpu_override: str = ""
    server_args: str = ""
    bench_threads: int = 0
    bench_clients: int = 0

    def __post_init__(self):
        super().__post_init__()
        positive = {
            "val_size": self.val_size,
            "key_size": self.key_size,
            "io_threads": self.io_threads,
            "pipelining": self.pipelining,
            "insertions": self.insertions,
            "repetitions": self.repetitions,
            "maxmemory_bytes": self.maxmemory_bytes,
            "max_rss_bytes": self.max_rss_bytes,
        }
        for name, value in positive.items():
            if int(value) < 1:
                raise ValueError(f"{name} must be at least 1")
        if self.key_size < BASE_KEY_SIZE:
            raise ValueError(f"key_size must be at least {BASE_KEY_SIZE}")
        if self.insertions > BENCHMARK_MAX_ITERATIONS:
            raise ValueError(f"insertions must not exceed {BENCHMARK_MAX_ITERATIONS}")
        if self.bench_threads < 0 or self.bench_clients < 0:
            raise ValueError("bench_threads and bench_clients must not be negative")
        if self.max_rss_bytes < self.maxmemory_bytes:
            raise ValueError("max_rss_bytes must be greater than or equal to maxmemory_bytes")
        if self.topology.replicas:
            raise ValueError("bounded insertion tasks do not support replicas")

    def short_description(self) -> str:
        return (
            f"{HumanNumber.to_human(self.insertions)} unique SETs, "
            f"key={HumanByte.to_human(self.key_size)}, value={HumanByte.to_human(self.val_size)}, "
            f"maxmemory={HumanByte.to_human(self.maxmemory_bytes)}, "
            f"RSS ceiling={HumanByte.to_human(self.max_rss_bytes)}"
        )

    def prepare_task_runner(self, server_infos: list[ServerInfo]) -> "BoundedInsertionTaskRunner":
        return BoundedInsertionTaskRunner(
            task_name=self.task_id,
            server_infos=server_infos,
            topology=self.topology,
            binary_source=self.source,
            specifier=self.specifier,
            val_size=self.val_size,
            key_size=self.key_size,
            io_threads=self.io_threads,
            pipelining=self.pipelining,
            insertions=self.insertions,
            repetitions=self.repetitions,
            maxmemory_bytes=self.maxmemory_bytes,
            max_rss_bytes=self.max_rss_bytes,
            make_args=self.make_args,
            note=self.note,
            perf_stat_enabled=self.perf_stat_enabled,
            server_cpu_override=self.server_cpu_override,
            benchmark_cpu_override=self.benchmark_cpu_override,
            server_args=self.server_args,
            bench_threads=self.bench_threads,
            bench_clients=self.bench_clients,
        )


class BoundedInsertionTaskRunner(PerfTaskRunner):
    """Run exact finite fills while reusing standard CPU/perf placement."""

    def __init__(
        self,
        *,
        task_name: str,
        server_infos: list[ServerInfo],
        binary_source: str,
        specifier: str,
        val_size: int,
        key_size: int,
        io_threads: int,
        pipelining: int,
        insertions: int,
        repetitions: int,
        maxmemory_bytes: int,
        max_rss_bytes: int,
        make_args: str,
        note: str,
        perf_stat_enabled: bool,
        server_cpu_override: str,
        benchmark_cpu_override: str,
        server_args: str,
        bench_threads: int,
        bench_clients: int,
        topology: Optional[TopologySpec] = None,
    ):
        bounded_server_args = (f"{server_args} --maxmemory {maxmemory_bytes} --maxmemory-policy noeviction").strip()
        super().__init__(
            task_name=task_name,
            server_infos=server_infos,
            topology=topology,
            binary_source=binary_source,
            specifier=specifier,
            io_threads=io_threads,
            valsize=val_size,
            pipelining=pipelining,
            test="set",
            warmup=0,
            duration=0,
            preload_keys=False,
            has_expire=False,
            make_args=make_args,
            perf_stat_enabled=perf_stat_enabled,
            note=note,
            key_size=key_size,
            repetitions=repetitions,
            server_cpu_override=server_cpu_override,
            benchmark_cpu_override=benchmark_cpu_override,
            server_args=bounded_server_args,
            bench_threads=bench_threads,
            bench_clients=bench_clients,
        )
        self.finite_request_count = insertions
        self.insertions = insertions
        self.maxmemory_bytes = maxmemory_bytes
        self.max_rss_bytes = max_rss_bytes
        self.status = BenchmarkStatus(steps_total=repetitions, task_type="perf-set-insertion")

    async def _run_finite_fill(self, command_string: str, server: Server, rep: int) -> dict[str, Any]:
        before_info = await server.info("memory")
        before_used, before_rss = checked_memory_snapshot(before_info, self.maxmemory_bytes, self.max_rss_bytes)
        command = RealtimeCommand(command_string)
        output_tail = ""
        perf_started = False
        if self.perf_stat_enabled:
            await server.perf_stat_start()
            perf_started = True
        try:
            command.start()
            process = command.p
            if process is None:
                raise RuntimeError("finite insertion generator did not create a process")
        except Exception:
            if perf_started:
                await server.perf_stat_stop()
            raise
        start = time.monotonic()
        client_cpu_start = sample_process_tree_cpu(process.pid)
        client_cpu_last = client_cpu_start
        client_cpu_last_time = start
        last_status = time.time()
        last_rss_sample = 0.0
        peak_rss = before_rss
        try:
            while command.is_running():
                stdout, stderr = command.poll_output()
                output_tail = (output_tail + (stdout or "") + (stderr or ""))[-4096:]
                now = time.monotonic()
                if now - last_rss_sample >= RSS_SAMPLE_INTERVAL_SECONDS:
                    memory_info = await server.info("memory")
                    _, rss = checked_memory_snapshot(memory_info, self.maxmemory_bytes, self.max_rss_bytes)
                    peak_rss = max(peak_rss, rss)
                    live_cpu = sample_process_tree_cpu(process.pid)
                    if live_cpu is not None:
                        client_cpu_last = live_cpu
                        client_cpu_last_time = now
                    last_rss_sample = now
                if time.time() - last_status >= HEARTBEAT_INTERVAL:
                    self.file_protocol.write_status(self.status)
                    last_status = time.time()
                await asyncio.sleep(BENCHMARK_UPDATE_INTERVAL)
            stdout, stderr = command.poll_output()
            output_tail = (output_tail + (stdout or "") + (stderr or ""))[-4096:]
        except Exception:
            if command.is_running():
                command.kill()
            raise
        finally:
            if perf_started:
                await server.perf_stat_stop()

        elapsed = time.monotonic() - start
        if process.returncode != 0:
            raise RuntimeError(f"finite insertion generator exited {process.returncode}: {output_tail}")
        if elapsed <= 0:
            raise RuntimeError("finite insertion elapsed time was not positive")

        if client_cpu_start is not None and client_cpu_last is not None:
            sampled_elapsed = client_cpu_last_time - start
            if sampled_elapsed > 0:
                self._client_cores_busy_per_rep.append((client_cpu_last - client_cpu_start) / sampled_elapsed)

        after_info = await server.info("memory")
        after_used, after_rss = checked_memory_snapshot(after_info, self.maxmemory_bytes, self.max_rss_bytes)
        peak_rss = max(peak_rss, after_rss)
        key_count, expire_count = await server.count_items_expires()
        if expire_count != 0:
            raise RuntimeError(f"finite insertion created {expire_count} unexpected expirations")
        if key_count != self.insertions:
            raise RuntimeError(
                f"finite insertion expected {self.insertions} keys but DBSIZE reported {key_count}; "
                "the maxmemory bound may have rejected writes"
            )

        return {
            "rps": self.insertions / elapsed,
            "elapsed_seconds": elapsed,
            "inserted_keys": key_count,
            "used_memory_before": before_used,
            "used_memory_after": after_used,
            "used_memory_rss_after": after_rss,
            "peak_rss": peak_rss,
            "bytes_per_inserted_key": (after_used - before_used) / key_count,
        }

    async def run(self) -> None:
        self.file_protocol.write_status(self.status)
        topology_group = TopologyGroup.for_task(
            self.server_infos,
            self.topology,
            self.binary_source,
            self.specifier,
            io_threads=self.io_threads,
            make_args=self.make_args,
            server_args=self.server_args,
            cpu_override=self.server_cpu_override,
        )
        benchmark_alloc_tag = None
        client: Optional[Server] = None
        server: Optional[Server] = None
        samples: list[dict[str, Any]] = []
        perf_counters: Optional[dict[str, dict[str, int]]] = None

        try:
            for rep in range(self.repetitions):
                if rep > 0:
                    await topology_group.stop_all_servers()
                    primary = topology_group.primary or Server(self.server_infos[0].ip)
                    platform = getattr(primary, "_platform_info", None)
                    if platform is None or platform.needs_drop_caches:
                        await primary.run_host_command("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'", check=False)

                await topology_group.kill_all_valkey_instances()
                await topology_group.start()
                if not topology_group.primary:
                    raise RuntimeError("Replication group failed to start: no primary server available")
                await topology_group.begin_replication()
                await topology_group.wait_for_repl_sync()
                server = topology_group.primary
                self.commit_hash = server.get_build_hash() or ""

                empty_count, _ = await server.count_items_expires()
                if empty_count != 0:
                    raise RuntimeError(f"finite insertion repetition did not start empty: DBSIZE={empty_count}")

                if client is None:
                    client = Server("127.0.0.1")
                    await client.ensure_host_cpu_allocation()
                    benchmark_alloc_tag = self._allocate_benchmark_cpus(client, server)
                    if self.benchmark_cpu_override:
                        self._client_allocated_cores = count_cpu_list(self.benchmark_cpu_override)
                    elif benchmark_alloc_tag is not None:
                        self._client_allocated_cores = self.bench_threads

                command_string = self._build_benchmark_command(client, server.ip, benchmark_alloc_tag)
                sample = await self._run_finite_fill(command_string, server, rep)
                samples.append(sample)
                self.status.steps_completed = rep + 1
                self.file_protocol.write_status(self.status)

                rep_counters = await self._collect_profiling_reports(server)
                if rep_counters:
                    self._perf_rep_count += 1
                    if perf_counters is None:
                        perf_counters = rep_counters
                    else:
                        for bucket, events in rep_counters.items():
                            accumulated = perf_counters.setdefault(bucket, {})
                            for name, value in events.items():
                                accumulated[name] = accumulated.get(name, 0) + value

            if server is None or not samples:
                raise RuntimeError("bounded insertion produced no samples")

            per_run_rps = [float(sample["rps"]) for sample in samples]
            if len(per_run_rps) > 1:
                mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)
            else:
                mean_rps, ci_95 = per_run_rps[0], 0.0
            mean_elapsed = sum(float(sample["elapsed_seconds"]) for sample in samples) / len(samples)
            self._perf_duration_seconds = mean_elapsed
            data: dict[str, Any] = {
                "topology": self.topology.to_dict(),
                "management_cpus": self.management_cpus,
                "insertions_per_rep": self.insertions,
                "key_size": self.key_size,
                "size": self.valsize,
                "io-threads": self.io_threads,
                "pipeline": self.pipelining,
                "repetitions": self.repetitions,
                "maxmemory_bytes": self.maxmemory_bytes,
                "max_rss_bytes": self.max_rss_bytes,
                "per_run_rps": per_run_rps,
                "mean_rps": mean_rps,
                "ci_95": ci_95,
                "per_run_elapsed_seconds": [sample["elapsed_seconds"] for sample in samples],
                "per_run_memory": samples,
                "perf_stat_enabled": self.perf_stat_enabled,
            }
            if perf_counters:
                self._store_perf_counters(data, perf_counters)
            if self._client_cores_busy_per_rep:
                data["client_cpu"] = summarize_client_cpu(self._client_cores_busy_per_rep, self._client_allocated_cores)

            self.file_protocol.write_results(
                BenchmarkResults(
                    method="perf-set-insertion",
                    source=self.binary_source,
                    specifier=self.specifier,
                    commit_hash=self.commit_hash,
                    score=mean_rps,
                    end_time=datetime.datetime.now(),
                    data=data,
                    make_args=self.make_args,
                    note=self.note,
                )
            )
            self.status.state = "completed"
            self.status.end_time = time.time()
            self.file_protocol.write_status(self.status)
        finally:
            await topology_group.stop_all_servers()
            if benchmark_alloc_tag and client:
                client.release_cpus(benchmark_alloc_tag)
