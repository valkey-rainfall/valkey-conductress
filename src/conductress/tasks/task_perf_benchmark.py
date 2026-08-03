"""Throughput benchmark"""

import datetime
import logging
import time
from dataclasses import dataclass
from math import sqrt
from statistics import mean, stdev
from typing import List, Optional, Sequence, Union

from scipy.stats import t as t_dist

from conductress.base_task_visualizer import PlotTaskVisualizer
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
from conductress.file_protocol import BenchmarkResults, BenchmarkStatus, FileProtocol, MetricData
from conductress.replication_group import ReplicationGroup
from conductress.server import Server
from conductress.task_queue import BaseTaskData, BaseTaskRunner
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
        )


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
        client_netns: str = "",
        bench_binary: str = "",
    ):
        super().__init__(task_name)

        self.logger = logging.getLogger(self.__class__.__name__ + "." + test)

        # settings
        self.task_name = task_name
        self.server_infos = server_infos
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
        self.client_netns = client_netns
        # Expert override for the benchmark binary (generator A/Bs). The
        # client is part of the workload definition: overridden results are
        # not sweep-comparable, so the override is recorded in metadata.
        self.bench_binary = bench_binary

        self.perf_stat_enabled = perf_stat_enabled
        self._is_last_rep = False
        self._current_rep = 0  # 0-indexed current repetition (set by _execute_benchmark_loop)
        self._cpu_stacks_main: list[list] = []
        self._cpu_stacks_io: list[list] = []
        # Client (load generator) CPU telemetry: cores kept busy by the
        # generator process tree during each measurement window, plus the
        # core budget it was confined to (None when unknown, e.g. remote
        # client without an explicit override).
        self._client_cores_busy_per_rep: list[float] = []
        self._client_allocated_cores: Optional[int] = None
        self._perf_rep_count = 0  # reps whose perf counters were summed into perf_counters

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
            detailed_data["perf_duration_seconds"] = float(self.duration)
            # Counters are summed across reps; record how many so the exporter can
            # normalize absolute per-request metrics (instructions-per-req).
            detailed_data["perf_rep_count"] = self._perf_rep_count or 1
        if main_counters:
            detailed_data["perf_counters_main"] = main_counters
        if io_counters:
            detailed_data["perf_counters_io"] = io_counters

    async def __record_result(
        self, server, per_run_rps: Optional[list[float]] = None, perf_counters: Optional[dict] = None
    ):
        completion_time = datetime.datetime.now()

        if len(self.rps_data) == 0 and not per_run_rps:
            raise RuntimeError("No results recorded")

        # Get system information
        lscpu_output, _ = await server.run_host_command("lscpu")

        if per_run_rps is not None and len(per_run_rps) > 1:
            # Aggregated result for repetitions > 1
            mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)

            detailed_data = {
                "warmup": self.warmup,
                "duration": self.duration,
                "io-threads": self.io_threads,
                "pipeline": self.pipelining,
                "has_expire": self.has_expire,
                "size": self.valsize,
                "key_size": self.key_size,
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
            )
        else:
            # Single-run result (repetitions == 1 or legacy behavior)
            avg_rps = sum(self.rps_data) / len(self.rps_data)

            detailed_data = {
                "warmup": self.warmup,
                "duration": self.duration,
                "io-threads": self.io_threads,
                "pipeline": self.pipelining,
                "has_expire": self.has_expire,
                "size": self.valsize,
                "key_size": self.key_size,
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

        replication_group = ReplicationGroup(
            self.server_infos,
            self.binary_source,
            self.specifier,
            self.io_threads,
            self.make_args,
            server_cpu_override=self.server_cpu_override,
            server_args=self.server_args,
        )

        benchmark_alloc_tag = None
        client = None
        server = None
        per_run_rps: list[float] = []
        perf_counters: Optional[dict] = None

        try:
            for rep in range(effective_reps):
                # Between-rep housekeeping (skip on first rep)
                if rep > 0:
                    await replication_group.stop_all_servers()
                    # Drop page caches between reps to prevent drift.
                    # Skip on Intel (large monolithic L3 stays warm).
                    primary_server = replication_group.primary or Server(self.server_infos[0].ip)
                    platform = getattr(primary_server, "_platform_info", None)
                    if platform is None or platform.needs_drop_caches:
                        await primary_server.run_host_command(
                            "sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'",
                            check=False,
                        )

                # Start server
                await replication_group.kill_all_valkey_instances()
                await replication_group.start()
                if not replication_group.primary:
                    raise RuntimeError("Replication group failed to start: no primary server available")

                await replication_group.begin_replication()
                await replication_group.wait_for_repl_sync()
                server = replication_group.primary
                self.commit_hash = server.get_build_hash() or ""

                # Preload data
                if self.preload_keys and self.preload_command is not None:
                    await server.run_valkey_command_over_keyspace(
                        PERF_BENCH_KEYSPACE, f"-d {self.valsize} {self.preload_command}"
                    )
                    if self.has_expire:
                        if not self.test.expire_command:
                            self.logger.warning("Expire command not available, skipping expiration")
                        else:
                            await server.run_valkey_command_over_keyspace(PERF_BENCH_KEYSPACE, self.test.expire_command)

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
                avg_rps = await self._execute_benchmark_loop(command_string, server, rep, effective_reps)
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
                await self.__record_result(server, per_run_rps=per_run_rps, perf_counters=perf_counters)
            else:
                await self.__record_result(server, perf_counters=perf_counters)

            # Write final status
            self.status.state = "completed"
            self.status.end_time = time.time()
            self.status.steps_completed = total_steps
            self.file_protocol.write_status(self.status)

        finally:
            await replication_group.stop_all_servers()
            if benchmark_alloc_tag and client:
                client._cpu_allocator.release(client.ip, benchmark_alloc_tag)

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
        net_numa = client._cpu_allocator.get_net_interface_numa(client.ip)
        benchmark_cpus = client._cpu_allocator.allocate(
            client.ip,
            benchmark_alloc_tag,
            count=self.bench_threads,
            require_numa=net_numa,
            avoid_tags=[server_tag],
            prefer_different_cache=True,
            minimize_cache_groups=is_chiplet,
        )
        self.logger.info(
            "Allocated CPUs %s for benchmark client (NUMA node %d)",
            benchmark_cpus,
            net_numa,
        )
        return benchmark_alloc_tag

    def _build_benchmark_command(
        self,
        client: "Server",
        target_ip: str,
        benchmark_alloc_tag: Optional[AllocationTag],
    ) -> str:
        """Build the numactl + valkey-benchmark command string."""
        net_numa = client._cpu_allocator.get_net_interface_numa(client.ip)
        bench_bin = self.bench_binary or str(PROJECT_ROOT / VALKEY_BENCHMARK)

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

        # Per-test override for the timed -r keyspace (preload always uses
        # PERF_BENCH_KEYSPACE); only zpop widens this today.
        keyspace = self.test.keyspace or PERF_BENCH_KEYSPACE

        if self.benchmark_cpu_override:
            # Expert override: use the explicit cpulist verbatim. Bind memory
            # to the NUMA node(s) of the override CPUs (NOT the NIC node) so a
            # cross-socket placement doesn't silently run with 100% remote memory.
            from conductress.utility import parse_cpulist

            override_cpus = parse_cpulist(self.benchmark_cpu_override)
            override_nodes = client._cpu_allocator.get_numa_nodes_for_cpus(client.ip, override_cpus)
            membind = ",".join(map(str, override_nodes)) if override_nodes else str(net_numa)
            return (
                f"{netns_prefix}numactl --physcpubind={self.benchmark_cpu_override} --membind={membind} "
                f"{bench_bin} -h {bench_target} -d {self.valsize} "
                f"-r {keyspace} -c {self.bench_clients} -P {self.pipelining} "
                f"--threads {self.bench_threads} -q -l -n {BENCHMARK_MAX_ITERATIONS} {self.test_command}"
            )
        elif benchmark_alloc_tag and self._is_local_benchmark(target_ip):
            allocated = client._cpu_allocator.get_allocation(client.ip, benchmark_alloc_tag)
            benchmark_cpu_list = ",".join(map(str, allocated)) if allocated else ""
            return (
                f"{netns_prefix}numactl --physcpubind={benchmark_cpu_list} --membind={net_numa} "
                f"{bench_bin} -h {bench_target} -d {self.valsize} "
                f"-r {keyspace} -c {self.bench_clients} -P {self.pipelining} "
                f"--threads {self.bench_threads} -q -l -n {BENCHMARK_MAX_ITERATIONS} {self.test_command}"
            )
        else:
            return (
                f"{netns_prefix}numactl --cpunodebind={net_numa} --membind={net_numa} "
                f"{bench_bin} -h {bench_target} -d {self.valsize} "
                f"-r {keyspace} -c {self.bench_clients} -P {self.pipelining} "
                f"--threads {self.bench_threads} -q -l -n {BENCHMARK_MAX_ITERATIONS} {self.test_command}"
            )

    async def _execute_benchmark_loop(self, command_string: str, server: "Server", rep: int, total_reps: int) -> float:
        """Execute one benchmark run (warmup + measurement). Returns avg RPS."""
        benchmark_update_interval = BENCHMARK_UPDATE_INTERVAL
        self._current_rep = rep
        self.rps_data = []

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
            return await server.perf_stat_report(result_dir)
        return None

    def _is_local_benchmark(self, target_ip: str) -> bool:
        """Check if benchmark is running locally (server and client on same host)."""
        # Normalize localhost variations
        local_ips = {"127.0.0.1", "localhost", "::1"}
        return target_ip in local_ips


class PerfTaskVisualizer(PlotTaskVisualizer):
    """Visualizer for performance benchmark tasks."""

    def __init__(self, task_id: str, file_protocol: FileProtocol, *args, **kwargs):
        super().__init__(task_id, *args, **kwargs)
        self.file_protocol = file_protocol

    def format_x_tick(self, value: float) -> str:
        return HumanTime.to_human(value / 4)

    def format_y_tick(self, value: float) -> str:
        return HumanNumber.to_human(value, 3)

    def get_plot_data(self) -> "List[Optional[float]]":
        datapoints = self.file_protocol.read_metrics()
        data = [dp.metrics.get("rps", 0.0) for dp in datapoints]

        if len(data) < 4:
            return data  # type: ignore[return-value]

        sorted_data = sorted(data)
        q1_idx: int = len(sorted_data) // 4
        q3_idx: int = 3 * len(sorted_data) // 4
        q1, q3 = sorted_data[q1_idx], sorted_data[q3_idx]
        iqr = q3 - q1
        lower, upper = q1 - 3 * iqr, q3 + 3 * iqr

        return [x if lower <= x <= upper else None for x in data]
