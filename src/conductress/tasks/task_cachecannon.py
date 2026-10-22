"""Cachecannon benchmark task -- second-opinion generator instrument.

cachecannon is a Rust load generator using io_uring (ringline framework).
Its workload definition differs from valkey-benchmark -- results are a SEPARATE
series, never sweep-comparable. Use for generator-wall cross-checks and
absolute ceiling validation.

Binary: config.CACHECANNON_BINARY (default ~/cachecannon/target/release/cachecannon
on bench hosts; override with CONDUCTRESS_CACHECANNON_BINARY).
Config: TOML file generated per-run in the result directory.
"""

import datetime
import json
import logging
import time
from dataclasses import dataclass
from statistics import median, stdev
from typing import Any, Literal, Optional

from conductress.cachecannon import (  # noqa: F401  (re-exported for callers/tests)
    DEFAULT_CACHECANNON_BINARY,
    generate_toml_config,
    merge_parsed_results,
    parse_json_results,
    split_generator,
    workload_issues_gets,
)
from conductress.config import (
    DEFAULT_DURATION,
    DEFAULT_MAKE_ARGS,
    DEFAULT_PIPELINING,
    DEFAULT_REPETITIONS,
    DEFAULT_VAL_SIZE,
    DEFAULT_WARMUP,
    PERF_BENCH_KEYSPACE,
    PERF_BENCH_THREADS,
    SWEEP_V3_TLS_PORT,
    TLS_CERT_DIR,
    ServerInfo,
    get_sweep_engine,
    should_profile_internals,
)
from conductress.cpu_allocator import AllocationTag
from conductress.file_protocol import BenchmarkResults, BenchmarkStatus
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
    HumanTime,
    RealtimeCommand,
    count_cpu_list,
    get_primary_interface_ip,
    sample_process_tree_cpu,
    summarize_client_cpu,
)

logger = logging.getLogger(__name__)

# Measurements a task may record as its ``score``.  "throughput" is ops/s over the
# scored window; "p99" is the p99 latency of the dominant command in microseconds
# (the unit the latency series has always used), for rate-limited runs whose
# throughput is fixed by configuration.
SCORE_METRICS = frozenset({"throughput", "p99"})

# How the recorded ``score`` aggregates the per-rep score series.  "mean" (the
# default, and every existing task's behaviour) records the arithmetic mean;
# "median" records the median, which reports the mode most reps landed in when
# a workload is bimodal across server restarts -- unpipelined P1 throughput
# behaves this way, and a mean of two modes is a value no rep produced.  Either
# way ``cv`` is still the coefficient of variation of the per-rep series
# (stdev/mean), so the recorded spread is comparable across both aggregates and
# a mode split stays visible through ``cv`` and the score bounds.
SCORE_AGGREGATES = frozenset({"mean", "median"})


def score_from_parsed(parsed: dict, score_metric: str) -> float:
    """Pick the recorded score out of one rep's parsed cachecannon result.

    Raises ValueError when a latency score is requested and the result carries
    no latency block, rather than recording a zero the bisection would trust.
    """
    if score_metric == "throughput":
        return float(parsed["throughput_rps"])
    latency = parsed.get("latency")
    if not latency:
        raise ValueError("cachecannon result carries no latency block; cannot record a p99 score")
    return float(latency["p99_ms"]) * 1000.0


def _compute_aggregated_stats(per_run_rps: list) -> tuple:
    """Compute mean and 95% CI. Deferred import to avoid circular dependency."""
    from conductress.tasks.task_perf_benchmark import compute_aggregated_stats

    return compute_aggregated_stats(per_run_rps)


def _should_stop_adaptive(per_run_rps: list, rep: int, min_reps: int, target_cv: float) -> bool:
    """Adaptive stopping test. Deferred import to avoid circular dependency.

    Shares the perf-benchmark implementation so both generators converge on
    precision identically rather than drifting apart.
    """
    from conductress.tasks.task_perf_benchmark import should_stop_adaptive

    return should_stop_adaptive(per_run_rps, rep, min_reps, target_cv)


def parse_info_sections(spec: str) -> list[str]:
    """Normalize a comma-separated INFO section list ('stats, io_uring' -> ['stats','io_uring']).

    Section names are passed to `INFO <section>`; only [a-z0-9_] is accepted so a
    task payload cannot smuggle anything else into the command line.
    """
    sections: list[str] = []
    for raw in (spec or "").split(","):
        name = raw.strip().lower()
        if not name:
            continue
        if not all(ch.isalnum() or ch == "_" for ch in name):
            raise ValueError(f"invalid INFO section name: {raw!r}")
        if name not in sections:
            sections.append(name)
    return sections


def is_scored_sample_line(line: str) -> bool:
    """True for a cachecannon per-second NDJSON sample, emitted only during the scored window."""
    stripped = line.strip()
    if not stripped.startswith("{") or '"sample"' not in stripped:
        return False
    try:
        doc = json.loads(stripped)
    except ValueError:
        return False
    return isinstance(doc, dict) and doc.get("type") == "sample"


async def snapshot_info(server: "Server", sections: list[str]) -> dict[str, dict[str, str]]:
    """Fetch `INFO <section>` for each section; a failing section is recorded as {}."""
    out: dict[str, dict[str, str]] = {}
    for section in sections:
        try:
            out[section] = await server.info(section)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("INFO %s snapshot failed: %s", section, e)
            out[section] = {}
    return out


def info_window_deltas(t0: dict[str, dict[str, str]], t1: dict[str, dict[str, str]], window_secs: float) -> dict:
    """Numeric field deltas (t1 - t0) per section over the scored window.

    Non-numeric fields are dropped (they are configuration, not counters);
    numeric fields that did not change are kept as 0 so consumers can tell
    "counter exists and was idle" from "counter absent on this build".
    """
    deltas: dict[str, dict[str, float]] = {}
    for section, fields1 in t1.items():
        fields0 = t0.get(section, {})
        sec: dict[str, float] = {}
        for key, v1 in fields1.items():
            v0 = fields0.get(key)
            if v0 is None:
                continue
            try:
                d = float(v1) - float(v0)
            except ValueError:
                continue
            sec[key] = int(d) if d.is_integer() else d
        deltas[section] = sec
    return {"window_secs": round(window_secs, 3), "sections": deltas}


@dataclass
class CachecannonTaskData(BaseTaskData):
    """Data class for cachecannon benchmark task.

    cachecannon is a second-opinion generator instrument -- its workload
    definition differs from valkey-benchmark. Results are a SEPARATE series,
    never directly comparable with the valkey-benchmark sweep history.
    """

    test: str = "get"
    val_size: int = DEFAULT_VAL_SIZE
    pipelining: int = DEFAULT_PIPELINING
    connections: int = 1200
    threads: int = PERF_BENCH_THREADS
    warmup: int = DEFAULT_WARMUP
    duration: int = DEFAULT_DURATION
    repetitions: int = DEFAULT_REPETITIONS
    keyspace_count: int = PERF_BENCH_KEYSPACE
    io_threads: int = 9  # server io-threads
    cachecannon_binary: str = DEFAULT_CACHECANNON_BINARY
    server_args: str = ""
    server_cpu_override: str = ""
    benchmark_cpu_override: str = ""
    set_ratio: int = 0  # 0 = pure workload per 'test'; >0 = mixed GET/SET
    distribution: str = "uniform"  # key distribution: 'uniform' or 'zipf'
    max_reps: int = 0  # 0 = fixed reps; >0 = adaptive mode upper limit
    target_cv: float = 0.0  # adaptive: stop early when 95% CI half-width (% of mean) <= this; 0 = disabled
    sweep_commit: str = ""  # non-empty marks this as a sweep task
    rate_limit: int = 0  # fixed total req/s (open loop); 0 = closed loop (unlimited)
    perf_stat_enabled: bool = False  # perf stat per-thread counters every rep; CPU flamegraph on the last rep
    info_sections: str = ""  # comma-separated INFO sections to snapshot at the scored-window edges
    # Which measurement the recorded ``score`` carries, and which one adaptive
    # stopping bounds: "throughput" (ops/s, the default) or "p99" (p99 latency of
    # the dominant command in microseconds).  A rate-limited run measures
    # latency, so its throughput is the configured rate and says nothing.
    score_metric: str = "throughput"
    # How the recorded ``score`` aggregates the per-rep score series: "mean"
    # (default, unchanged for every existing task) or "median" (robust to a
    # single bad server restart).  ``cv`` is the coefficient of variation of the
    # per-rep series regardless, so the two aggregates carry comparable spread.
    score_aggregate: Literal["mean", "median"] = "mean"

    # Serve the workload over TLS.  When set, the server opens a TLS listener on
    # SWEEP_V3_TLS_PORT with the bootstrap test certificates and cachecannon
    # connects over it (endpoint pointed at the TLS port, [target] tls keys
    # emitted), so the cell measures the encrypted read path.  It is a real
    # dataclass field, not a dynamic attribute: the task is serialized through
    # asdict(), which silently drops any attribute that is not a field, so a
    # dynamic flag would deserialize to False on the runner and the cell would
    # run plaintext.  A TLS cell also builds a BUILD_TLS=yes binary (a distinct
    # build-cache entry, keyed by make_args); see the task runner.
    tls: bool = False

    # Run cachecannon inside this network namespace (dual-ENI real-NIC hairpin,
    # docs/real-nic-hairpin.md).  Empty = default namespace, loopback path.  Same
    # lever as the valkey-benchmark task's client_netns: the generator is wrapped
    # in ``sudo ip netns exec`` and a loopback target is rewritten to the primary
    # ENI's address, so both directions traverse the real driver/IRQ/NAPI path.
    # Topology is part of the workload definition: a hairpin cell is a separate
    # series, never compared cell-for-cell against a loopback cell.
    client_netns: str = ""

    # Number of concurrent cachecannon processes that together form the
    # generator (default 1 = unchanged behaviour). One process walls at roughly
    # 22-25M ops/s on a c8gn.48xlarge whatever its thread/connection count;
    # several processes on the same host scale almost linearly (2 -> 2.2x,
    # 4 -> 4.3x, measured 2026-10-22). ``connections`` and ``threads`` stay the
    # TOTALS and are divided across the processes; an explicit client cpulist is
    # cut into disjoint slices. Per-rep throughput is the sum, hit rate is
    # recomputed from summed hits/misses, error_pct is the worst process, and
    # the recorded latency block is the process with the highest p99 (with the
    # per-process p50/p99 kept alongside). See cachecannon.split_generator.
    processes: int = 1

    def __post_init__(self):
        super().__post_init__()
        self.warmup = int(self.warmup)
        self.duration = int(self.duration)
        if self.client_netns and self.tls:
            # The bootstrap server certificate's SAN carries only 127.0.0.1 /
            # localhost; a hairpin endpoint is the primary ENI's address, so
            # tls_verify would fail at connect.  Refuse rather than silently
            # measure a failed-handshake cell.
            raise ValueError("client_netns and tls cannot be combined: the bootstrap certificate only covers loopback")
        if self.score_metric not in SCORE_METRICS:
            raise ValueError(f"score_metric must be one of {sorted(SCORE_METRICS)}, got {self.score_metric!r}")
        if self.score_aggregate not in SCORE_AGGREGATES:
            raise ValueError(f"score_aggregate must be one of {sorted(SCORE_AGGREGATES)}, got {self.score_aggregate!r}")
        # These must be REAL controls. An accepted-but-ignored value is a fake
        # lever: it makes the operator believe they configured a precision
        # target that never took effect. Adaptive stopping can only ever fire
        # between `repetitions` and `max_reps`, so a target with no headroom
        # above the minimum is rejected rather than silently doing nothing.
        if self.max_reps and self.max_reps < self.repetitions:
            raise ValueError(
                f"max_reps ({self.max_reps}) must be >= repetitions ({self.repetitions}); "
                "repetitions is the adaptive minimum and max_reps the ceiling"
            )
        if self.target_cv < 0:
            raise ValueError(f"target_cv must be >= 0, got {self.target_cv}")
        if self.rate_limit < 0:
            raise ValueError(f"rate_limit must be >= 0, got {self.rate_limit}")
        self.info_sections = ",".join(parse_info_sections(self.info_sections))
        if self.target_cv > 0 and self.max_reps <= self.repetitions:
            raise ValueError(
                f"target_cv={self.target_cv} requires max_reps > repetitions "
                f"(got max_reps={self.max_reps}, repetitions={self.repetitions}); "
                "otherwise adaptive stopping has no reps to skip and the target is a no-op"
            )

    def workload_label(self) -> str:
        """Human label for the workload: 'get', 'set', or 'mixed s<N>'."""
        label = f"mixed s{self.set_ratio}" if self.set_ratio > 0 else self.test
        if self.distribution != "uniform":
            label += f" {self.distribution}"
        return label

    def short_description(self) -> str:
        rate = f", {self.rate_limit} req/s" if self.rate_limit > 0 else ""
        netns = f", netns {self.client_netns}" if self.client_netns else ""
        return (
            f"cachecannon {self.workload_label()}, {HumanByte.to_human(self.val_size)} values, "
            f"P{self.pipelining}, {self.connections}c, {self.threads}t{rate}{netns}, "
            f"{HumanTime.to_human(self.duration)} x{self.repetitions}"
        )

    def prepare_task_runner(self, server_infos: list[ServerInfo]) -> "CachecannonTaskRunner":
        return CachecannonTaskRunner(
            task_id=self.task_id,
            server_infos=server_infos,
            topology=self.topology,
            source=self.source,
            specifier=self.specifier,
            make_args=self.make_args,
            io_threads=self.io_threads,
            test=self.test,
            val_size=self.val_size,
            pipelining=self.pipelining,
            connections=self.connections,
            threads=self.threads,
            warmup=self.warmup,
            duration=self.duration,
            repetitions=self.repetitions,
            max_reps=self.max_reps,
            target_cv=self.target_cv,
            keyspace_count=self.keyspace_count,
            cachecannon_binary=self.cachecannon_binary,
            server_args=self.server_args,
            server_cpu_override=self.server_cpu_override,
            benchmark_cpu_override=self.benchmark_cpu_override,
            set_ratio=self.set_ratio,
            distribution=self.distribution,
            note=self.note,
            rate_limit=self.rate_limit,
            perf_stat_enabled=self.perf_stat_enabled,
            info_sections=parse_info_sections(self.info_sections),
            score_metric=self.score_metric,
            score_aggregate=self.score_aggregate,
            tls=self.tls,
            client_netns=self.client_netns,
            processes=self.processes,
        )


class CachecannonTaskRunner(BaseTaskRunner):
    """Run a cachecannon benchmark against a Valkey server.

    Builds and starts the server through the shared TopologyGroup machinery,
    allocates client CPUs, generates a TOML config, launches cachecannon, and
    parses results.
    """

    def __init__(
        self,
        task_id: str,
        server_infos: list[ServerInfo],
        source: str,
        specifier: str,
        make_args: str,
        io_threads: int,
        test: str,
        val_size: int,
        pipelining: int,
        connections: int,
        threads: int,
        warmup: int,
        duration: int,
        repetitions: int,
        keyspace_count: int,
        cachecannon_binary: str,
        server_args: str,
        server_cpu_override: str,
        benchmark_cpu_override: str,
        note: str,
        set_ratio: int = 0,
        distribution: str = "uniform",
        max_reps: int = 0,
        target_cv: float = 0.0,
        rate_limit: int = 0,
        perf_stat_enabled: bool = False,
        info_sections: Optional[list[str]] = None,
        topology: Optional[TopologySpec] = None,
        score_metric: str = "throughput",
        score_aggregate: str = "mean",
        tls: bool = False,
        client_netns: str = "",
        processes: int = 1,
    ):
        super().__init__(task_id)
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{test}")
        if score_metric not in SCORE_METRICS:
            raise ValueError(f"score_metric must be one of {sorted(SCORE_METRICS)}, got {score_metric!r}")
        if score_aggregate not in SCORE_AGGREGATES:
            raise ValueError(f"score_aggregate must be one of {sorted(SCORE_AGGREGATES)}, got {score_aggregate!r}")
        self.score_metric = score_metric
        self.score_aggregate = score_aggregate

        self.server_infos = server_infos
        self.topology = topology if topology is not None else TopologySpec.standalone()
        self.source = source
        self.specifier = specifier
        self.make_args = make_args
        self.io_threads = io_threads
        self.test = test
        self.val_size = val_size
        self.pipelining = pipelining
        self.connections = connections
        self.threads = threads
        self.warmup = warmup
        self.duration = duration
        self.repetitions = repetitions
        self.max_reps = max_reps
        self.target_cv = target_cv
        self.keyspace_count = keyspace_count
        self.cachecannon_binary = cachecannon_binary
        self.server_args = server_args
        self.server_cpu_override = server_cpu_override
        self.benchmark_cpu_override = benchmark_cpu_override
        self.set_ratio = set_ratio
        self.distribution = distribution
        self.note = note
        self.rate_limit = rate_limit
        self.perf_stat_enabled = perf_stat_enabled
        self.info_sections = list(info_sections or [])
        self.tls = tls
        self.client_netns = client_netns
        if processes < 1:
            raise ValueError(f"processes must be >= 1, got {processes}")
        self.processes = processes
        # TLS transport: the server opens a second listener on the TLS port with
        # the bootstrap test certificates and the generator connects over it
        # (endpoint pointed at self._tls_port, [target] tls keys emitted below).
        # The plaintext port keeps serving the server's own housekeeping.
        #
        # A TLS cell needs a BUILD_TLS=yes binary (Valkey and Redis both compile
        # TLS out by default), so BUILD_TLS=yes is appended to make_args when it
        # is not already there.  The build cache is keyed by md5(make_args)
        # (BinaryManager.get_cached_build_path), so a TLS binary and a plaintext
        # binary of the same commit occupy DISTINCT cache entries and neither is
        # ever served to the other transport.  Without --tls the make_args are
        # unchanged, so a plaintext cell of the same commit still builds and
        # caches its own byte-identical non-TLS binary.
        self._tls_port = SWEEP_V3_TLS_PORT
        if tls:
            tls_flags = (
                f"--tls-port {self._tls_port} "
                f"--tls-cert-file {TLS_CERT_DIR}/server.crt "
                f"--tls-key-file {TLS_CERT_DIR}/server.key "
                f"--tls-ca-cert-file {TLS_CERT_DIR}/ca.crt "
                f"--tls-auth-clients no"
            )
            self.server_args = " ".join(a for a in (self.server_args, tls_flags) if a)
            if "BUILD_TLS=yes" not in self.make_args:
                self.make_args = f"{self.make_args} BUILD_TLS=yes".strip()
        # CPU flamegraph stacks expose the server binary's symbols; skipped for
        # engines that opt out (same gate as the memtier mixed task).
        self._profile_internals = should_profile_internals(get_sweep_engine(source))
        self._cpu_stacks_main: list[list] = []
        self._cpu_stacks_io: list[list] = []
        self._perf_stat_scope: Optional[str] = None
        # Reps whose counters were summed into perf_counters, and the measured
        # length of each counting window. Both are needed to normalise absolute
        # counters per request (the exporter divides by rps * duration * reps).
        self._perf_rep_count = 0
        self._perf_windows_seconds: list[float] = []
        self._info_deltas_per_rep: list[dict] = []

        self.commit_hash = ""
        self._client_cores_busy_per_rep: list[float] = []
        self._client_allocated_cores: Optional[int] = None

        workload = f"mixed s{set_ratio}" if set_ratio > 0 else test
        if distribution != "uniform":
            workload += f" {distribution}"
        self.workload = workload
        effective_reps = max_reps if max_reps > 0 else repetitions
        rep_label = f"x{repetitions}" if effective_reps == repetitions else f"x{repetitions}-{effective_reps}"
        rate_label = f", {rate_limit} req/s" if rate_limit > 0 else ""
        self.title = (
            f"cachecannon {workload}, {source}:{specifier}, io-threads={io_threads}, "
            f"P{pipelining}, {connections}c, {threads}t{rate_label}, "
            f"{HumanTime.to_human(duration)} {rep_label}"
        )

        # Status tracking. Budget for the adaptive ceiling so progress never
        # exceeds 100% when extra reps are needed to hit the precision target.
        self.status = BenchmarkStatus(
            steps_total=(warmup + duration) * effective_reps,
            task_type=f"cachecannon-{workload.replace(' ', '-')}",
        )

    def _allocate_benchmark_cpus(self, client: "Server", server: "Server") -> Optional[AllocationTag]:
        """Allocate CPUs for the cachecannon client. Returns the tag or None."""
        if self.benchmark_cpu_override:
            self.logger.info("Using explicit benchmark CPU override: %s", self.benchmark_cpu_override)
            return None

        target_ip = server.ip
        if target_ip not in {"127.0.0.1", "localhost", "::1"}:
            return None

        self.logger.info("Local benchmark detected - allocating client CPUs")
        server_tag = AllocationTag(task_id=f"server_{server.ip}_{server.port}", purpose="server")
        benchmark_alloc_tag = AllocationTag(task_id=self.task_name, purpose="benchmark")
        platform = getattr(server, "_platform_info", None)
        is_chiplet = platform is not None and platform.needs_single_cache_pinning
        benchmark_cpus = client.allocate_client_cpus(
            benchmark_alloc_tag,
            self.threads,
            avoid_tags=[server_tag],
            minimize_cache_groups=is_chiplet,
        )
        self.logger.info(
            "Allocated CPUs %s for cachecannon (NUMA node %s)",
            benchmark_cpus,
            client.net_numa_node(),
        )
        return benchmark_alloc_tag

    def _get_cpu_list(self, client: "Server", benchmark_alloc_tag: Optional[AllocationTag]) -> str:
        """Get the comma-separated CPU list for cachecannon's cpu_list config ("" lets the OS schedule)."""
        if self.benchmark_cpu_override:
            return self.benchmark_cpu_override
        return client.allocated_cpu_list(benchmark_alloc_tag) if benchmark_alloc_tag else ""

    def _build_command(self, toml_path: str, cpu_list: str) -> str:
        """Build the cachecannon launch command.

        With ``client_netns`` set the whole invocation is wrapped in
        ``sudo ip netns exec <ns>`` so the generator's sockets live in the
        load-generator namespace (dual-ENI hairpin).  CPU pinning is unaffected:
        a network namespace only scopes the network stack.
        """
        netns_prefix = f"sudo ip netns exec {self.client_netns} " if self.client_netns else ""
        if cpu_list:
            # Use numactl for memory binding even though cachecannon pins its own threads
            numa_node = 0  # sensible default
            return f"{netns_prefix}numactl --membind={numa_node} {self.cachecannon_binary} {toml_path}"
        return f"{netns_prefix}{self.cachecannon_binary} {toml_path}"

    async def run(self):
        """Run the cachecannon benchmark with repetitions."""
        self.logger.info("preparing: %s", self.title)
        self.file_protocol.write_status(self.status)

        topology_group = TopologyGroup.for_task(
            self.server_infos,
            self.topology,
            self.source,
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
        per_run_scores: list[float] = []  # the score_metric measurement, one per rep
        all_results: list[dict] = []
        perf_counters: Optional[dict] = None
        memory_records: list[dict] = []

        try:
            effective_reps = self.max_reps if self.max_reps > 0 else self.repetitions
            for rep in range(effective_reps):
                # Between-rep housekeeping
                if rep > 0:
                    await topology_group.stop_all_servers()
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
                    raise RuntimeError("Replication group failed to start: no primary")

                await topology_group.begin_replication()
                await topology_group.wait_for_repl_sync()
                server = topology_group.primary
                self.commit_hash = server.get_build_hash() or ""

                # Setup client CPU allocation (once)
                if client is None:
                    client = Server("127.0.0.1")
                    await client.ensure_host_cpu_allocation()
                    benchmark_alloc_tag = self._allocate_benchmark_cpus(client, server)
                    if self.benchmark_cpu_override:
                        self._client_allocated_cores = count_cpu_list(self.benchmark_cpu_override)
                    elif benchmark_alloc_tag is not None:
                        self._client_allocated_cores = self.threads
                    # Preflight for the dual-ENI hairpin: network namespaces do
                    # NOT persist across reboots, so fail loudly and point at
                    # the fix rather than dying later with a cryptic connect
                    # error from inside cachecannon (docs/real-nic-hairpin.md).
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

                # Get CPU list for TOML config
                cpu_list = self._get_cpu_list(client, benchmark_alloc_tag)

                # Generate TOML config
                server_port = server.port or 6379
                # Over TLS the generator targets the server's TLS listener (a
                # second port); the plaintext port keeps serving the server's
                # own housekeeping.  The CA is the bootstrap test CA; the server
                # certificate's SAN carries 127.0.0.1/localhost (bootstrap
                # _TLS_SERVER_EXT), so tls_hostname is the loopback address the
                # endpoint uses and tls_verify stays on.
                target_port = self._tls_port if self.tls else server_port
                # Hairpin: the load-generator namespace has its own loopback, so
                # a loopback target is unreachable from inside it.  Point the
                # generator at the primary ENI's address instead; traffic leaves
                # via the secondary ENI, crosses the VPC fabric and re-enters via
                # the primary ENI.  Locality decisions above (CPU allocation)
                # still use server.ip: the client process runs on this host
                # either way.  Prefill goes over the hairpin too (correctness
                # only; only the scored window is measured).
                target_ip = server.ip
                if self.client_netns and target_ip in {"127.0.0.1", "localhost", "::1"}:
                    target_ip = get_primary_interface_ip()
                endpoint = f"{target_ip}:{target_port}"

                # One TOML per generator process. processes == 1 reproduces the
                # single-process files byte for byte (same name, same content).
                plan = split_generator(self.processes, self.connections, self.threads, cpu_list)
                toml_paths: list[str] = []
                command_strings: list[str] = []
                for pidx, part in enumerate(plan):
                    toml_content = generate_toml_config(
                        duration=self.duration,
                        warmup=self.warmup,
                        threads=part["threads"],
                        cpu_list=part["cpu_list"],
                        endpoint=endpoint,
                        connections=part["connections"],
                        pipeline_depth=self.pipelining,
                        keyspace_count=self.keyspace_count,
                        val_size=self.val_size,
                        test=self.test,
                        set_ratio=self.set_ratio,
                        distribution=self.distribution,
                        rate_limit=self.rate_limit // self.processes if self.rate_limit else 0,
                        tls=self.tls,
                        tls_hostname=server.ip if self.tls else "",
                        tls_ca_file=f"{TLS_CERT_DIR}/ca.crt" if self.tls else "",
                    )
                    suffix = "" if self.processes == 1 else f"_p{pidx+1}"
                    toml_path = str(self.file_protocol.work_dir / f"cachecannon_rep{rep+1}{suffix}.toml")
                    with open(toml_path, "w") as f:
                        f.write(toml_content)
                    toml_paths.append(toml_path)
                    command_strings.append(self._build_command(toml_path, part["cpu_list"]))
                command_string = command_strings[0]
                self.logger.info(
                    "Starting cachecannon (rep %d/%d, %d process%s): %s",
                    rep + 1,
                    effective_reps,
                    self.processes,
                    "" if self.processes == 1 else "es",
                    command_string,
                )

                self.status.state = "running"
                self.file_protocol.write_status(self.status)

                # Profiling: perf stat every rep and a CPU flamegraph on the last
                # scheduled rep, plus optional INFO snapshots at both edges of the
                # scored window. cachecannon runs prefill + warmup + scored window
                # in one process and only emits per-second "sample" lines during
                # the scored window, so the first sample line (not a fixed sleep,
                # which would start during a 3M-key prefill) arms the collectors;
                # they are read at process exit. The edge is placed within one poll
                # period (1 s) of the true boundary and the actual window length is
                # recorded with the deltas. Same arm/stop/collect discipline as the
                # memtier mixed task: each collector is stopped exactly once, in the
                # finally block if need be.
                is_last_rep = rep == effective_reps - 1
                perf_armed = False
                perf_stopped = False
                perf_t0: Optional[float] = None
                cpu_profile_armed = False
                window_started = False
                info_t0: Optional[dict[str, dict[str, str]]] = None
                info_t0_time: Optional[float] = None
                info_t1: Optional[dict[str, dict[str, str]]] = None
                info_t1_time: Optional[float] = None
                output_lines: list[str] = []
                outputs: list[list[str]] = [[] for _ in command_strings]
                commands: list[RealtimeCommand] = []
                mem_sampler: Optional[MemorySampler] = None
                memory_resets: Optional[dict[str, dict[str, Any]]] = None
                try:
                    # Launch every generator process -- each handles its own
                    # prefill (idempotent SETs of the same keyspace) and warmup.
                    for cs in command_strings:
                        c = RealtimeCommand(cs)
                        c.start()
                        commands.append(c)
                    command = commands[0]

                    # Collect output (cachecannon runs to completion).
                    # CPU telemetry: sample_process_tree_cpu returns None once the
                    # root exits, so refresh the last-known sample each poll cycle.
                    # With several processes the samples are summed across trees.
                    def _sample_all() -> Optional[float]:
                        total = 0.0
                        seen = False
                        for c in commands:
                            if c.p:
                                s = sample_process_tree_cpu(c.p.pid)
                                if s is not None:
                                    total += s
                                    seen = True
                        return total if seen else None

                    client_cpu_t0 = time.monotonic()
                    client_cpu_s0 = _sample_all()
                    client_cpu_t1: Optional[float] = None
                    client_cpu_s1: Optional[float] = None
                    while any(c.is_running() for c in commands):
                        for pidx, c in enumerate(commands):
                            line, _ = c.poll_output()
                            while line is not None and line != "":
                                outputs[pidx].append(line)
                                if not window_started and is_scored_sample_line(line):
                                    window_started = True
                                    # Scored window just opened: reset the kernel/
                                    # cgroup peaks and record the engine floors so
                                    # data.memory excludes cachecannon's prefill and
                                    # warmup, then begin 1 Hz memory sampling on its
                                    # own connection (independent of the perf/info
                                    # opt-ins below).
                                    if mem_sampler is None:
                                        memory_resets = await reset_memory_peaks(
                                            servers_by_role(server, topology_group.replicas)
                                        )
                                        mem_sampler = MemorySampler(server.ip, server.port)
                                        mem_sampler.start()
                                line, _ = c.poll_output()
                        sample = _sample_all()
                        if sample is not None:
                            client_cpu_s1 = sample
                            client_cpu_t1 = time.monotonic()
                        if window_started and not perf_armed and not cpu_profile_armed and info_t0 is None:
                            # First poll after the scored window opened: arm everything.
                            if self.perf_stat_enabled:
                                await server.perf_stat_start()
                                perf_armed = True
                                perf_t0 = time.monotonic()
                            if is_last_rep and self.perf_stat_enabled and self._profile_internals:
                                # Leave a margin so the record ends before the load does.
                                server.cpu_profile_start(max(1, self.duration - 2))
                                cpu_profile_armed = True
                            if self.info_sections:
                                info_t0 = await snapshot_info(server, self.info_sections)
                                info_t0_time = time.monotonic()
                            if not (self.perf_stat_enabled or self.info_sections):
                                window_started = False  # nothing to arm; stop re-checking
                        time.sleep(1)

                    # INFO snapshot at the end of the scored window (server still up).
                    if self.info_sections and info_t0 is not None:
                        info_t1 = await snapshot_info(server, self.info_sections)
                        info_t1_time = time.monotonic()

                    # Peak-memory: stop the sampler and read INFO memory per
                    # server while it is still up (before topology stop).
                    if mem_sampler is not None:
                        mem_sampler.stop()
                    try:
                        servers = servers_by_role(server, topology_group.replicas)
                        memory_records.append(await collect_memory_after(servers, mem_sampler, memory_resets))
                    except Exception as exc:  # capture must never fail the cell
                        self.logger.warning("memory capture failed on rep %d: %s", rep + 1, exc)

                    # Perf stat: stop counting (scored phase is over).
                    if perf_armed:
                        await server.perf_stat_stop()
                        perf_stopped = True
                        if perf_t0 is not None:
                            self._perf_windows_seconds.append(time.monotonic() - perf_t0)

                    # Drain remaining output from every process
                    for pidx, c in enumerate(commands):
                        line, _ = c.poll_output()
                        while line is not None and line != "":
                            outputs[pidx].append(line)
                            line, _ = c.poll_output()
                    output_lines = [ln for out in outputs for ln in out]

                    # Collect perf stat counters per rep and sum across reps.
                    if perf_armed:
                        server.perf_stat_wait()
                        rep_counters = await server.perf_stat_report(self.file_protocol.get_result_dir())
                        self._perf_stat_scope = server.perf_stat_scope or self._perf_stat_scope
                        if rep_counters:
                            self._perf_rep_count += 1
                            if perf_counters is None:
                                perf_counters = rep_counters
                            else:
                                for bucket, events in rep_counters.items():
                                    acc = perf_counters.setdefault(bucket, {})
                                    for k, v in events.items():
                                        acc[k] = acc.get(k, 0) + v
                        perf_armed = False  # stopped, joined, and reported

                    # Collect CPU profile stacks on the last rep.
                    if cpu_profile_armed:
                        try:
                            cpu_main, cpu_io = await server.cpu_profile_collect()
                            if cpu_main:
                                self._cpu_stacks_main = cpu_main
                                self._cpu_stacks_io = cpu_io
                        except Exception as e:  # pylint: disable=broad-exception-caught
                            self.logger.warning("CPU profile collection failed: %s", e)
                        cpu_profile_armed = False  # fully consumed
                finally:
                    if perf_armed:
                        if not perf_stopped:
                            try:
                                await server.perf_stat_stop()
                            except Exception:  # pylint: disable=broad-exception-caught
                                pass
                        try:
                            server.perf_stat_wait()
                        except Exception:  # pylint: disable=broad-exception-caught
                            pass
                    if cpu_profile_armed:
                        try:
                            server.cpu_profile_cancel()
                        except Exception:  # pylint: disable=broad-exception-caught
                            pass
                    if mem_sampler is not None:
                        try:
                            mem_sampler.stop()  # idempotent; no-op if already stopped
                        except Exception:  # pylint: disable=broad-exception-caught
                            pass

                if info_t0 is not None and info_t1 is not None and info_t0_time and info_t1_time:
                    self._info_deltas_per_rep.append(info_window_deltas(info_t0, info_t1, info_t1_time - info_t0_time))

                # Check exit codes (is_running() returned False for all, so p.poll() has run)
                for pidx, c in enumerate(commands):
                    exit_code = c.p.returncode if c.p else None
                    if exit_code != 0:
                        full_output = "\n".join(outputs[pidx])
                        raise RuntimeError(
                            f"cachecannon process {pidx+1}/{len(commands)} exited with code {exit_code}. "
                            f"Output:\n{full_output[-2000:]}"
                        )

                # Parse results (exact values from cachecannon's JSON output), one
                # per process, then merge (sum throughput, worst error_pct, hit
                # rate from summed hits/misses, pessimistic latency block).
                full_output = "\n".join(output_lines)
                parsed = merge_parsed_results([parse_json_results("\n".join(out)) for out in outputs])

                # Fail loudly on errors or low hit rate
                if parsed["error_pct"] > 0:
                    raise RuntimeError(
                        f"cachecannon reported {parsed['error_pct']}% errors "
                        f"(threshold: 0%). Output:\n{full_output[-1000:]}"
                    )
                # Prefill sanity guard -- only meaningful when the workload
                # issues GETs (see workload_issues_gets): a pure-SET run has
                # hit rate 0.0 by definition, and the unconditional check
                # failed every valid pure-SET task.
                if (
                    workload_issues_gets(self.test, self.set_ratio)
                    and parsed["hit_rate"]
                    and parsed["hit_rate"]["percent"] < 99.0
                ):
                    raise RuntimeError(
                        f"cachecannon hit rate {parsed['hit_rate']['percent']}% "
                        f"(threshold: 99%). Prefill may have failed."
                    )

                per_run_rps.append(parsed["throughput_rps"])
                per_run_scores.append(score_from_parsed(parsed, self.score_metric))
                all_results.append(parsed)
                self.logger.info(
                    "Rep %d/%d: %.0f rps, %.2f%% errors, hit rate %.1f%%",
                    rep + 1,
                    effective_reps,
                    parsed["throughput_rps"],
                    parsed["error_pct"],
                    parsed["hit_rate"]["percent"] if parsed["hit_rate"] else 0,
                )

                # Client CPU telemetry (approximate -- spans cachecannon's
                # internal warmup as well as the measurement window)
                if (
                    self._client_allocated_cores
                    and client_cpu_s0 is not None
                    and client_cpu_s1 is not None
                    and client_cpu_t1 is not None
                    and client_cpu_t1 > client_cpu_t0
                ):
                    cores_busy = (client_cpu_s1 - client_cpu_s0) / (client_cpu_t1 - client_cpu_t0)
                    self._client_cores_busy_per_rep.append(cores_busy)

                # Adaptive stop: once the 95% CI half-width of the scored
                # measurement is inside the precision target there is nothing to
                # gain from more reps, and each one costs a full server restart
                # plus a 3M-key prefill.
                if _should_stop_adaptive(per_run_scores, rep, self.repetitions, self.target_cv):
                    mean_score, ci_95 = _compute_aggregated_stats(per_run_scores)
                    self.logger.info(
                        "Adaptive stop after %d reps: 95%% CI half-width %.3f%% <= target %.3f%%",
                        rep + 1,
                        (ci_95 / mean_score) * 100 if mean_score else 0.0,
                        self.target_cv,
                    )
                    self.status.steps_total = (self.warmup + self.duration) * (rep + 1)
                    break

            # Record aggregated results
            if server is None:
                raise RuntimeError("No server available for recording results")
            await self._record_result(
                server, per_run_rps, all_results, toml_content, perf_counters, per_run_scores, memory_records
            )

            # Final status
            self.status.state = "completed"
            self.status.end_time = time.time()
            self.status.steps_completed = self.status.steps_total
            self.file_protocol.write_status(self.status)

        finally:
            await topology_group.stop_all_servers()
            if benchmark_alloc_tag and client:
                client.release_cpus(benchmark_alloc_tag)

    async def _record_result(
        self,
        server: "Server",
        per_run_rps: list[float],
        all_results: list[dict],
        toml_content: str,
        perf_counters: Optional[dict] = None,
        per_run_scores: Optional[list[float]] = None,
        memory_records: Optional[list[dict]] = None,
    ):
        """Record the final benchmark result.

        ``score``, ``cv`` and ``reps`` describe the ``score_metric``
        measurement; the throughput fields are recorded regardless, since a
        rate-limited run's achieved rate is how one checks the generator held
        the requested rate.
        """
        completion_time = datetime.datetime.now()
        lscpu_output, _ = await server.run_host_command("lscpu")

        # Compute aggregated stats
        if len(per_run_rps) >= 2:
            mean_rps, ci_95 = _compute_aggregated_stats(per_run_rps)
        else:
            mean_rps = per_run_rps[0] if per_run_rps else 0
            ci_95 = 0.0
        scores = per_run_scores if per_run_scores is not None else per_run_rps
        if len(scores) >= 2:
            mean_score, score_ci_95 = _compute_aggregated_stats(scores)
            # cv is the coefficient of variation of the per-rep series and is
            # always taken against the MEAN, so switching the recorded score to
            # a median (score_aggregate="median") leaves the reported spread
            # unchanged -- only the central value differs.
            cv = (stdev(scores) / mean_score) * 100 if mean_score else 0.0
            score = median(scores) if self.score_aggregate == "median" else mean_score
        else:
            score = scores[0] if scores else 0
            score_ci_95 = 0.0
            cv = 0.0
        reps = len(scores)
        score_min = min(scores) if scores else 0
        score_max = max(scores) if scores else 0

        # Build detailed data
        detailed_data = {
            "topology": self.topology.to_dict(),
            "management_cpus": self.management_cpus,
            "warmup": self.warmup,
            "duration": self.duration,
            "io-threads": self.io_threads,
            "pipeline": self.pipelining,
            "connections": self.connections,
            "threads": self.threads,
            "processes": self.processes,
            "size": self.val_size,
            "keyspace_count": self.keyspace_count,
            "set_ratio": self.set_ratio,
            "distribution": self.distribution,
            "rate_limit": self.rate_limit,
            # Whether the cell ran over TLS.  Recorded so a reader of the result
            # row can tell an encrypted-path cell from a plaintext one without
            # parsing the TOML; the sweep exporter also publishes it in the
            # series metadata.
            "tls": self.tls,
            # Network namespace the generator ran in ("" = default namespace,
            # loopback path).  Non-empty marks a real-NIC hairpin cell; such a
            # row is a separate series from loopback rows.
            "client_netns": self.client_netns,
            "cachecannon_binary": self.cachecannon_binary,
            "toml_config": toml_content,
            "lscpu": lscpu_output,
            "server_cpus": server.server_cpus,
            "repetitions": self.repetitions,
            "per_run_rps": per_run_rps,
            "mean_rps": mean_rps,
            "ci_95": ci_95,
            "score_metric": self.score_metric,
            "score_aggregate": self.score_aggregate,
            # min/max of the per-rep score series, recorded for every cachecannon
            # task so a reader can see the between-restart spread the score was
            # drawn from (the v3 exporter publishes these alongside rps/cv/reps).
            "score_min": score_min,
            "score_max": score_max,
        }
        if self.score_metric != "throughput":
            # The score series in its own unit, so a reader of the record (and the
            # sweep coordinator computing CV) never has to re-derive it from the
            # per-rep latency blocks.
            detailed_data[f"per_run_{self.score_metric}_us"] = scores
            detailed_data[f"mean_{self.score_metric}_us"] = score
            detailed_data[f"{self.score_metric}_ci_95_us"] = score_ci_95

        # Latency from last rep (most representative after warmup effects)
        if all_results and all_results[-1].get("latency"):
            detailed_data["latency"] = all_results[-1]["latency"]
            for per_command in ("latency_get", "latency_set"):
                if all_results[-1].get(per_command):
                    detailed_data[per_command] = all_results[-1][per_command]

        # Hit rate from last rep
        if all_results and all_results[-1].get("hit_rate"):
            detailed_data["hit_rate"] = all_results[-1]["hit_rate"]

        # Per-rep results for full transparency
        detailed_data["per_rep_results"] = [
            {
                "throughput_rps": r["throughput_rps"],
                "error_pct": r["error_pct"],
                "hit_rate": r.get("hit_rate"),
                "latency": r.get("latency"),
            }
            for r in all_results
        ]

        if self._client_cores_busy_per_rep:
            detailed_data["client_cpu"] = summarize_client_cpu(
                self._client_cores_busy_per_rep, self._client_allocated_cores
            )

        # Profiling (opt-in): summed per-thread hardware counters, flamegraph
        # stacks from the last rep, and INFO deltas over each scored window.
        if perf_counters:
            detailed_data["perf_counters"] = perf_counters
            if self._perf_stat_scope:
                detailed_data["perf_counters_scope"] = self._perf_stat_scope
            # Counters cover the measured counting window (armed on the first
            # scored sample line, stopped at process exit), not the nominal
            # duration; the sweep exporter divides by this value and by the
            # number of reps summed. Same keys as the memtier tasks.
            detailed_data["perf_duration_seconds"] = (
                sum(self._perf_windows_seconds) / len(self._perf_windows_seconds)
                if self._perf_windows_seconds
                else float(self.duration)
            )
            detailed_data["perf_rep_count"] = self._perf_rep_count or 1
        if self._cpu_stacks_main:
            detailed_data["cpu_stacks_main"] = self._cpu_stacks_main
            detailed_data["cpu_stacks_io"] = self._cpu_stacks_io
        if self._info_deltas_per_rep:
            detailed_data["info_sections"] = self.info_sections
            detailed_data["info_deltas_per_rep"] = self._info_deltas_per_rep

        folded_memory = fold_memory_records(memory_records) if memory_records else None
        if folded_memory is not None:
            detailed_data["memory"] = folded_memory

        results = BenchmarkResults(
            method=f"cachecannon-{self.workload.replace(' ', '-')}",
            source=self.source,
            specifier=self.specifier,
            commit_hash=self.commit_hash,
            score=score,
            end_time=completion_time,
            data=detailed_data,
            make_args=self.make_args,
            note=self.note,
            cv=cv if reps >= 2 else None,
            reps=reps,
        )

        self.file_protocol.write_results(results)
