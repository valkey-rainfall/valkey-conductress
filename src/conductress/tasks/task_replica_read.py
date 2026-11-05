"""Replica-read benchmark task.

Measures read throughput served by a REPLICA while its primary ingests writes
at a fixed rate over a real replication link. On a replica the only writer is
the replication stream, executed on the main thread, so no client connection
ever interleaves writes with reads. That makes it the cleanest topology for
measuring any change to how a server serves reads while writes are arriving.

Topology (one host, loopback, all instances pinned by the CPU allocator):

    primary   127.0.0.1:<base>      <- writer:  cachecannon SET, rate_limit=W
    replica   127.0.0.1:<base+1>    <- reader:  cachecannon GET, closed loop
    [replica  127.0.0.1:<base+2> ...]  extra replicas add fan-out load only

Phases per repetition:
    1. kill stray servers, start topology, REPLICAOF, wait for link up
    2. preload: cachecannon prefill of the keyspace against the primary,
       then wait until every replica's offset equals the primary's
    3. measure: start the fixed-rate writer at the primary, then the reader
       at the first replica (warmup + duration); sample INFO and CPU counters
       from every instance and both generators on a fixed cadence for the
       whole reader window
    4. guards: reader 0% errors and >= 99% hit rate; writer 0% errors and
       achieved rate within tolerance of the target (an under-delivering
       writer silently turns the run into a lower-write-rate run); replica
       mean lag over the scored window within --max-lag-seconds of stream (a
       replica that falls behind serves stale data at a full hit rate)
    5. bottleneck verdict: was anything other than the server the limit?
       Recorded on the result, never a failure (see cpu_sampling)

Score = mean reader throughput (rps). Results carry the writer's achieved
rate, replication-lag statistics (primary minus replica offset, bytes), the
per-instance INFO series, the verdict with its evidence and both TOML
configs, so a run can be audited without repeating it.

Read-rate search (``--read-rate-search``): instead of one closed-loop reader
at saturation, each repetition runs a series of open-loop reader probes at
fixed rates and finds the highest rate the replica serves while still
keeping up with its primary. A closed-loop reader at saturation measures
what happens when the client offers unlimited load; a stock replica then
starves its replication link and the cell fails a guard instead of producing
a number. The search measures the operating envelope instead: how many reads
this replica can serve, at this write rate, without falling behind. Probes
follow a climb (x``read_rate_step`` per pass) then a bisection between the
last pass and the first fail (see rate_search.py). A probe passes when the
reader delivered its target rate, the replica's mean lag stayed under
``--max-lag-seconds`` and its lag was not growing (``--max-lag-slope``,
seconds of stream per second). The rep's score is the reader throughput
measured at the highest passing probe; the row records every probe and the
bracket the knee was found in. The writer restarts with every probe and the
replica is caught up before the next one starts, so each probe begins from
zero lag and its lag trend is its own.

Independent variables the task is designed around: replica io-threads, write
rate, and replica-only server arguments. An A/B is two queued tasks that
differ in --specifier (two builds) or --replica-args (one build, two configs).

Cluster mode: the TopologySpec can describe it but the bootstrap is a
follow-up (see topology.py).
"""

import asyncio
import datetime
import logging
import sys
import time
from dataclasses import dataclass
from statistics import mean, stdev
from typing import Optional

from conductress import cpu_sampling
from conductress.cachecannon import DEFAULT_CACHECANNON_BINARY, generate_toml_config, parse_json_results
from conductress.config import (
    DEFAULT_DURATION,
    DEFAULT_REPETITIONS,
    DEFAULT_VAL_SIZE,
    DEFAULT_WARMUP,
    PERF_BENCH_KEYSPACE,
    ServerInfo,
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
from conductress.rate_search import Done, Probe, RateSearch
from conductress.server import Server
from conductress.task_queue import BaseTaskData, BaseTaskRunner
from conductress.topology import DEFAULT_BASE_PORT, TopologyGroup, TopologySpec, replication_lag_stats
from conductress.utility import HumanByte, HumanTime, RealtimeCommand, parse_cpulist

logger = logging.getLogger(__name__)

METHOD = "replica-read"

# Writer runs longer than the reader so the reader's whole window sees the
# write stream, and the writer still exits on its own (its JSON result is how
# we learn the achieved rate).
WRITER_SLACK_SECONDS = 5
# Fraction of the target write rate the writer must achieve for a valid cell.
WRITE_RATE_TOLERANCE = 0.10
# Preload: a short cachecannon run whose only job is the prefill.
PRELOAD_DURATION_SECONDS = 1
# Reader must hit: every key was written to the primary and replicated.
READER_MIN_HIT_RATE_PCT = 99.0
# Replica must be keeping up: mean lag over the scored window, in seconds of
# replication stream. Sampling skew puts the noise floor at a few
# milliseconds, so one second is far above it at any write rate while still
# catching a replica whose main thread is starving the replication link.
DEFAULT_MAX_LAG_SECONDS = 1.0
# Read-rate search: a probe also fails when the replica's lag TREND over the
# scored window exceeds this, in seconds of stream per second (the fraction of
# the write stream the replica is not applying). Offset-sampling skew is a few
# milliseconds per sample, so two percent over a window of ten or more
# samples is well above the noise while catching a replica that is slowly
# but steadily losing ground.
DEFAULT_MAX_LAG_SLOPE = 0.02
# Read-rate search: a probe whose reader delivered less than this fraction of
# its target rate did not offer that rate at all (an open-loop generator with
# a finite connection count turns closed-loop when the server's latency fills
# its in-flight budget). Such a probe fails as a read-path ceiling.
READ_RATE_MIN_DELIVERY = 0.97
DEFAULT_READ_RATE_START = 500_000
DEFAULT_READ_RATE_MAX = 20_000_000
DEFAULT_READ_RATE_STEP = 1.5
DEFAULT_READ_RATE_TOLERANCE = 0.05
DEFAULT_READ_RATE_MAX_BISECT_STEPS = 8


def _io_threads_of(inst) -> int:
    if inst.io_threads is None:
        raise ValueError(f"instance on port {inst.port} has no io_threads")
    return inst.io_threads


def _fmt_seconds(value: Optional[float]) -> str:
    return "n/a" if value is None else f"{value:.3f}"


@dataclass
class ReplicaReadTaskData(BaseTaskData):
    """Task data for the replica-read benchmark. See module docstring."""

    # Reader (measured) side -- aimed at the first replica
    val_size: int = DEFAULT_VAL_SIZE
    pipelining: int = 1
    connections: int = 400
    threads: int = 8
    keyspace_count: int = PERF_BENCH_KEYSPACE
    warmup: int = DEFAULT_WARMUP
    duration: int = DEFAULT_DURATION
    repetitions: int = DEFAULT_REPETITIONS
    # Writer side -- fixed rate at the primary
    write_rate: int = 50_000
    write_connections: int = 16
    write_threads: int = 4
    write_pipelining: int = 1
    # Topology: the inherited ``topology`` field, built with
    # ``TopologySpec.replica_read`` (primary + replicas on consecutive ports of
    # the runner host, each with its own working directory). The properties
    # below read the levers back out of it.
    # Sampling
    sample_interval: float = 1.0
    info_fields: str = ""  # comma-separated extra INFO fields to sample (e.g. counters a build under test exposes)
    # Guard: a rep fails when the replica's mean lag over the scored window,
    # expressed as seconds of replication stream, exceeds this. A replica that
    # falls behind serves the reader stale data at a full hit rate, so the run
    # would silently measure a different experiment (see _check_guards).
    max_lag_seconds: float = DEFAULT_MAX_LAG_SECONDS
    # Read-rate search (see module docstring). Off: one closed-loop reader per
    # rep. On: open-loop reader probes at fixed rates, climb x``read_rate_step``
    # from ``read_rate_start`` then bisect, never above ``read_rate_max``;
    # ``warmup`` and ``duration`` are per probe.
    read_rate_search: bool = False
    read_rate_start: int = DEFAULT_READ_RATE_START
    read_rate_max: int = DEFAULT_READ_RATE_MAX
    read_rate_step: float = DEFAULT_READ_RATE_STEP
    read_rate_tolerance: float = DEFAULT_READ_RATE_TOLERANCE
    read_rate_max_bisect_steps: int = DEFAULT_READ_RATE_MAX_BISECT_STEPS
    max_lag_slope: float = DEFAULT_MAX_LAG_SLOPE
    cachecannon_binary: str = DEFAULT_CACHECANNON_BINARY
    benchmark_cpu_override: str = ""
    # perf-record the measured replica (main + io threads, frame-pointer call
    # graph) over the scored window of the LAST rep; collapsed stacks land on
    # the row as cpu_stacks_main / cpu_stacks_io. Sampling interrupts cost the
    # replica a little, so the profiled rep is named on the row.
    cpu_profile: bool = False

    def __post_init__(self):
        super().__post_init__()
        self.warmup = int(self.warmup)
        self.duration = int(self.duration)
        if not self.topology.replicas:
            raise ValueError("replica-read needs at least one replica in its topology")
        if self.topology.host_count() != 1:
            raise ValueError(
                "replica-read runs every instance on the runner host; the topology must not name other hosts"
            )
        if any(i.io_threads is None for i in self.topology.instances):
            raise ValueError("every replica-read instance must set io_threads")
        if self.write_rate < 1:
            raise ValueError(f"write_rate must be >= 1 req/s, got {self.write_rate}")
        if self.repetitions < 1:
            raise ValueError(f"repetitions must be >= 1, got {self.repetitions}")
        if self.duration < 1:
            raise ValueError(f"duration must be >= 1s, got {self.duration}")
        self.max_lag_seconds = float(self.max_lag_seconds)
        if not self.max_lag_seconds > 0:
            raise ValueError(f"max_lag_seconds must be > 0, got {self.max_lag_seconds}")
        if self.sample_interval <= 0:
            raise ValueError(f"sample_interval must be > 0, got {self.sample_interval}")
        for name in ("connections", "threads", "write_connections", "write_threads", "pipelining", "write_pipelining"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1, got {getattr(self, name)}")
        self.read_rate_search = bool(self.read_rate_search)
        self.max_lag_slope = float(self.max_lag_slope)
        if not self.max_lag_slope > 0:
            raise ValueError(f"max_lag_slope must be > 0, got {self.max_lag_slope}")
        if self.read_rate_search:
            # RateSearch validates the search parameters; build one so a bad
            # combination is refused at queue time rather than on the runner.
            self.rate_search()
            if self.cpu_profile:
                # The profile is one perf.data per task, taken on the last
                # measurement; a search does not know which probe is the knee
                # until it is over, so there is no single window to profile.
                raise ValueError("cpu_profile cannot be combined with read_rate_search")

    def rate_search(self) -> RateSearch:
        """A fresh search state machine from the task's parameters."""
        return RateSearch(
            start_rate=int(self.read_rate_start),
            step_multiplier=float(self.read_rate_step),
            max_rate=int(self.read_rate_max),
            tolerance=float(self.read_rate_tolerance),
            max_bisect_steps=int(self.read_rate_max_bisect_steps),
        )

    def max_probes_per_rep(self) -> int:
        """Probes a rep can take: the search's bound, or one closed-loop reader."""
        return self.rate_search().max_probes if self.read_rate_search else 1

    def reader_mode_description(self) -> str:
        if self.read_rate_search:
            return f"read-rate search from {self.read_rate_start}/s x{self.read_rate_step:g}"
        return "closed-loop reader"

    @property
    def replica_count(self) -> int:
        """Replica instances on the runner host; reads are measured at the first."""
        return len(self.topology.replicas)

    @property
    def io_threads(self) -> int:
        """Replica io-threads (the measured instance)."""
        return _io_threads_of(self.topology.replicas[0])

    @property
    def primary_io_threads(self) -> int:
        return _io_threads_of(self.topology.primary)

    @property
    def base_port(self) -> int:
        return self.topology.primary.port

    def topology_spec(self) -> TopologySpec:
        """The instance layout (the inherited ``topology`` field)."""
        return self.topology

    def extra_info_fields(self) -> list:
        """Extra INFO field names to sample from every instance."""
        return [f.strip() for f in self.info_fields.split(",") if f.strip()]

    def short_description(self) -> str:
        return (
            f"replica-read {self.replica_count}r io{self.io_threads}, writes {self.write_rate}/s, "
            f"{HumanByte.to_human(self.val_size)} values, P{self.pipelining}, {self.connections}c, "
            f"{self.threads}t, {HumanTime.to_human(self.duration)} x{self.repetitions}"
            f"{', read-rate search' if self.read_rate_search else ''}"
            f"{', cpu-profile' if self.cpu_profile else ''}"
        )

    def prepare_task_runner(self, server_infos: list[ServerInfo]) -> "ReplicaReadTaskRunner":
        return ReplicaReadTaskRunner(self, server_infos)


@dataclass(frozen=True)
class _Placement:
    """Where the generators and the runner itself run (cpulists; empty means unpinned).

    ``runner_cpus`` are the management cores the runner process is pinned to
    for this task (``BaseTaskRunner.management_cpus``, set by the task runner
    loop); the bottleneck verdict counts them as claimed rather than foreign,
    and the sampler shell is pinned there too.
    """

    reader_cpus: str
    writer_cpus: str
    runner_cpus: str = ""


@dataclass(frozen=True)
class _Measurement:
    """Everything one measure phase produced, handed to the judge phase."""

    reader: dict
    writer: dict
    samples: list
    reader_started: float  # monotonic; the verdict window opens at reader_started + warmup
    reader_toml: str
    writer_toml: str
    memory: Optional[dict] = None  # data.memory for this measure phase (primary + replica)


def _primary_and_replica(group: TopologyGroup) -> tuple:
    """The primary and the measured (first) replica of a started topology."""
    if group.primary is None or not group.replicas:
        raise RuntimeError("topology not started")
    return group.primary, group.replicas[0]


def _endpoint(server: Server) -> str:
    return f"{server.ip}:{server.port}"


class ReplicaReadTaskRunner(BaseTaskRunner):
    """Run the replica-read benchmark. See module docstring for the phases."""

    def __init__(self, task: ReplicaReadTaskData, server_infos: list[ServerInfo]):
        super().__init__(task.task_id)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.task = task
        if not server_infos:
            raise ValueError("replica-read needs a server host")
        self.host = server_infos[0]
        self.spec = task.topology_spec()
        self.commit_hash = ""
        # Generator placement: allocated on the first rep, released when the run ends.
        self._client: Optional[Server] = None
        self._reader_tag: Optional[AllocationTag] = None
        self._writer_tag: Optional[AllocationTag] = None
        # CPU profile of the measured replica, collected on the last rep when enabled.
        self._cpu_stacks_main: list = []
        self._cpu_stacks_io: list = []
        self.title = (
            f"replica-read, {task.source}:{task.specifier}, {task.replica_count} replica(s), "
            f"replica io-threads={task.io_threads}, writes {task.write_rate}/s, "
            f"P{task.pipelining}, {task.connections}c, {task.threads}t, "
            f"{HumanTime.to_human(task.duration)} x{task.repetitions}"
            f"{', read-rate search' if task.read_rate_search else ''}"
            f"{', cpu-profile' if task.cpu_profile else ''}"
        )
        self.status = BenchmarkStatus(
            steps_total=(task.warmup + task.duration + WRITER_SLACK_SECONDS)
            * task.max_probes_per_rep()
            * task.repetitions,
            task_type=METHOD,
        )

    # ------------------------------------------------------------------ CPU placement

    def _allocate_client_cpus(self, client: Server, purpose: str, count: int) -> Optional[AllocationTag]:
        """Allocate ``count`` CPUs for a generator, away from every server instance (None under --client-cpus)."""
        if self.task.benchmark_cpu_override:
            return None
        return self._allocate(client, purpose, count)

    def _allocate(self, client: Server, purpose: str, count: int) -> AllocationTag:
        """Allocate ``count`` client-side CPUs tagged ``purpose``, away from every server instance."""
        server_tags = [
            AllocationTag(task_id=f"server_{self.host.ip}_{i.port}", purpose="server") for i in self.spec.instances
        ]
        tag = AllocationTag(task_id=f"{self.task_name}_{purpose}", purpose="benchmark")
        cpus = client.allocate_client_cpus(tag, count, avoid_tags=server_tags)
        self.logger.info("Allocated CPUs %s for %s", cpus, purpose)
        return tag

    def _cpu_list(self, client: Server, tag: Optional[AllocationTag]) -> str:
        if self.task.benchmark_cpu_override:
            return self.task.benchmark_cpu_override
        return client.allocated_cpu_list(tag) if tag else ""

    # ------------------------------------------------------------------ generators

    def _toml(self, name: str, content: str) -> str:
        path = self.file_protocol.work_dir / name
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return str(path)

    def _launch(self, toml_path: str, cpus: str) -> RealtimeCommand:
        """Start cachecannon confined to ``cpus``, the cores allocated for that generator.

        cachecannon pins only its ringline workers to the TOML's ``cpu_list``;
        its main and admin (metrics) threads stay on the launch mask. Launching
        under the allocation keeps those threads on claimed cores, so the
        bottleneck verdict does not see them as foreign work (smoke cell take
        3 read 0.16 of a core on an unallocated node-1 core for exactly this).
        Empty ``cpus`` (no allocation, e.g. a bare ``--client-cpus``) defers to
        the runner-wide launch mask.
        """
        command = RealtimeCommand(f"{self.task.cachecannon_binary} {toml_path}", launch_cpus=cpus or None)
        command.start()
        return command

    @staticmethod
    def _pid(command: Optional[RealtimeCommand]) -> Optional[int]:
        """OS pid of a launched generator, or None if it has not started."""
        if command is None:
            return None
        return getattr(command.p, "pid", None)

    @staticmethod
    def _drain(command: RealtimeCommand, lines: list) -> None:
        line, _ = command.poll_output()
        while line:
            lines.append(line)
            line, _ = command.poll_output()

    async def _wait_and_parse(self, command: RealtimeCommand, label: str) -> dict:
        """Wait for a generator to exit, then parse its JSON result."""
        lines: list = []
        while command.is_running():
            self._drain(command, lines)
            await asyncio.sleep(0.5)
        self._drain(command, lines)
        output = "\n".join(lines)
        exit_code = getattr(command.p, "returncode", None)
        if exit_code != 0:
            raise RuntimeError(f"{label} cachecannon exited with code {exit_code}. Output:\n{output[-2000:]}")
        parsed = parse_json_results(output)
        if parsed["error_pct"] > 0:
            raise RuntimeError(f"{label} cachecannon reported {parsed['error_pct']}% errors. Output:\n{output[-1000:]}")
        return parsed

    # ------------------------------------------------------------------ run

    async def run(self):
        """Bring up, preload, measure, judge, repeat; record once at the end."""
        task = self.task
        self.logger.info("preparing: %s", self.title)
        self.file_protocol.write_status(self.status)

        group = TopologyGroup(self.host, self.spec, task.source, task.specifier, task.make_args)
        reps: list = []
        last: Optional[_Measurement] = None
        memory_records: list = []
        try:
            for rep in range(1, task.repetitions + 1):
                await self._bring_up(group, first=rep == 1)
                placement = await self._place_generators()
                await self._preload(group, rep, placement)
                if task.read_rate_search:
                    entry, last = await self._search(group, rep, placement)
                else:
                    last = await self._measure(group, rep, placement)
                    entry = self._judge(group, rep, placement, last)
                if last is not None and last.memory is not None:
                    memory_records.append(last.memory)
                reps.append(entry)
            assert last is not None
            await self._record_result(group, reps, last.reader_toml, last.writer_toml, memory_records)
            self.status.state = "completed"
            self.status.end_time = time.time()
            self.status.steps_completed = self.status.steps_total
            self.file_protocol.write_status(self.status)
        finally:
            await group.stop_all_servers()
            self._release_generators()

    # ------------------------------------------------------------------ phases

    async def _bring_up(self, group: TopologyGroup, first: bool) -> None:
        """Phase 1: fresh instances, replication wired and synced; records the build hash."""
        if not first:
            await group.stop_all_servers()
            await Server(self.host.ip, username=self.host.username).run_host_command(
                "sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'", check=False
            )
        await group.kill_all_valkey_instances()
        await group.start()
        await group.begin_replication()
        await group.wait_for_repl_sync()
        assert group.primary is not None
        self.commit_hash = group.primary.get_build_hash() or ""

    async def _place_generators(self) -> "_Placement":
        """Allocate generator CPUs once (first call) and return their cpulists.

        The allocation outlives the per-rep topology so that every rep's
        generators sit on the same cores; ``_release_generators`` undoes it.
        The runner's own management cores come from the task runner loop
        (``self.management_cpus``) and are reported alongside.
        """
        if self._client is None:
            self._client = Server("127.0.0.1")
            await self._client.ensure_host_cpu_allocation()
            self._reader_tag = self._allocate_client_cpus(self._client, "reader", self.task.threads)
            self._writer_tag = self._allocate_client_cpus(self._client, "writer", self.task.write_threads)
        return _Placement(
            reader_cpus=self._cpu_list(self._client, self._reader_tag),
            writer_cpus=self._cpu_list(self._client, self._writer_tag),
            runner_cpus=self.management_cpus,
        )

    def _release_generators(self) -> None:
        if self._client is None:
            return
        for tag in (self._reader_tag, self._writer_tag):
            if tag:
                self._client.release_cpus(tag)

    async def _preload(self, group: TopologyGroup, rep: int, placement: "_Placement") -> None:
        """Phase 2: fill the keyspace at the primary and wait until every replica has all of it."""
        task = self.task
        primary, replica = _primary_and_replica(group)
        self.status.state = "running"
        self.file_protocol.write_status(self.status)
        preload_toml = self._toml(
            f"preload_rep{rep}.toml",
            generate_toml_config(
                duration=PRELOAD_DURATION_SECONDS,
                warmup=0,
                threads=task.write_threads,
                cpu_list=placement.writer_cpus,
                endpoint=_endpoint(primary),
                connections=max(task.write_connections, 32),
                pipeline_depth=16,
                keyspace_count=task.keyspace_count,
                val_size=task.val_size,
                test="get",
                prefill=True,
            ),
        )
        await self._wait_and_parse(self._launch(preload_toml, placement.writer_cpus), "preload")
        await group.wait_for_offsets_caught_up()
        keys_primary = (await primary.count_items_expires())[0]
        keys_replica = (await replica.count_items_expires())[0]
        if keys_replica != keys_primary or keys_primary < task.keyspace_count:
            raise RuntimeError(
                f"preload mismatch: primary {keys_primary} keys, replica {keys_replica}, "
                f"expected >= {task.keyspace_count}"
            )

    def _generator_configs(
        self, placement: "_Placement", primary: Server, replica: Server, read_rate: int = 0
    ) -> tuple:
        """(writer_toml, reader_toml) for the measure phase.

        The writer outlives the reader by ``WRITER_SLACK_SECONDS`` so the whole
        reader window sees the write stream, and still exits on its own.
        ``read_rate`` > 0 makes the reader open-loop at that rate (a search
        probe); 0 is the closed-loop reader.
        """
        task = self.task
        writer_toml = generate_toml_config(
            duration=task.warmup + task.duration + WRITER_SLACK_SECONDS,
            warmup=0,
            threads=task.write_threads,
            cpu_list=placement.writer_cpus,
            endpoint=_endpoint(primary),
            connections=task.write_connections,
            pipeline_depth=task.write_pipelining,
            keyspace_count=task.keyspace_count,
            val_size=task.val_size,
            test="set",
            rate_limit=task.write_rate,
            prefill=False,
        )
        reader_toml = generate_toml_config(
            duration=task.duration,
            warmup=task.warmup,
            threads=task.threads,
            cpu_list=placement.reader_cpus,
            endpoint=_endpoint(replica),
            connections=task.connections,
            pipeline_depth=task.pipelining,
            keyspace_count=task.keyspace_count,
            val_size=task.val_size,
            test="get",
            rate_limit=read_rate,
            prefill=False,
        )
        return writer_toml, reader_toml

    async def _measure(
        self, group: TopologyGroup, rep: int, placement: "_Placement", probe: int = 0, read_rate: int = 0
    ) -> "_Measurement":
        """Phase 3: fixed-rate writer at the primary, reader at the replica, sampled throughout.

        ``probe`` and ``read_rate`` are set by the read-rate search: the reader
        is then open-loop at ``read_rate`` and the TOML files carry the probe
        index so every probe of a rep is auditable.
        """
        task = self.task
        primary, replica = _primary_and_replica(group)
        writer_toml, reader_toml = self._generator_configs(placement, primary, replica, read_rate=read_rate)
        suffix = f"_rep{rep}" + (f"_p{probe}" if probe else "")
        writer_path = self._toml(f"writer{suffix}.toml", writer_toml)
        reader_path = self._toml(f"reader{suffix}.toml", reader_toml)
        self.logger.info(
            "rep %d/%d%s: writer %d/s at %s, reader %s at %s",
            rep,
            task.repetitions,
            f" probe {probe}" if probe else "",
            task.write_rate,
            _endpoint(primary),
            f"{read_rate}/s" if read_rate else "closed-loop",
            _endpoint(replica),
        )

        samples: list = []
        writer_cmd = self._launch(writer_path, placement.writer_cpus)
        await asyncio.sleep(1.0)  # let the write stream reach steady state before reads start
        reader_cmd = self._launch(reader_path, placement.reader_cpus)
        reader_started = time.monotonic()
        profiling = self._profile_this_rep(rep)
        if profiling:
            # perf record on the replica for the scored window only: it sleeps
            # through the reader's warmup, then records ``duration`` seconds.
            replica.cpu_profile_start(task.duration, delay_seconds=task.warmup)
        # 1 Hz peak-memory sampler on the measured replica, over the reader
        # window; INFO is read on both primary and replica after it closes.
        # The peaks on both servers are re-armed when the reader's warmup ends
        # so data.memory covers the scored window only.
        mem_servers = servers_by_role(primary, [replica])
        mem_sampler = MemorySampler(replica.ip, replica.port)
        mem_sampler.start()
        reset_task = asyncio.create_task(self._reset_peaks_after(task.warmup, mem_servers))
        sampler = asyncio.create_task(
            self._sample_loop(group, samples, reader_cmd, writer_cmd, pin_cpus=placement.runner_cpus)
        )
        memory_record: Optional[dict] = None
        try:
            reader = await self._wait_and_parse(reader_cmd, "reader")
        finally:
            sampler.cancel()
            try:
                await sampler
            except asyncio.CancelledError:
                pass
            mem_sampler.stop()
            memory_resets: Optional[dict] = None
            if reset_task.done() and not reset_task.cancelled():
                memory_resets = reset_task.result()
            else:  # reader ended before its warmup did: no scored window, no reset
                reset_task.cancel()
            try:
                memory_record = await collect_memory_after(mem_servers, mem_sampler, memory_resets)
            except Exception as exc:  # capture must never fail the cell
                self.logger.warning("memory capture failed on rep %d: %s", rep, exc)
            if profiling:
                await self._collect_cpu_profile(replica)
        writer = await self._wait_and_parse(writer_cmd, "writer")
        return _Measurement(
            reader=reader,
            writer=writer,
            samples=samples,
            reader_started=reader_started,
            reader_toml=reader_toml,
            writer_toml=writer_toml,
            memory=memory_record,
        )

    def _profile_this_rep(self, rep: int) -> bool:
        """Profile the last rep only: one perf.data per task, like the perf task."""
        return bool(self.task.cpu_profile) and rep == self.task.repetitions

    async def _collect_cpu_profile(self, replica: Server) -> None:
        """Best effort: a failed profile never fails the cell, it just leaves the row without stacks.

        Called from the measure phase's ``finally``; when the reader itself
        failed, perf is cancelled rather than waited for, so the failure
        surfaces promptly and no half-window profile is recorded.
        """
        if sys.exc_info()[1] is not None:
            replica.cpu_profile_cancel()
            return
        try:
            main_stacks, io_stacks = await replica.cpu_profile_collect()
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.warning("CPU profile collection failed: %s", e)
            return
        if not main_stacks:
            self.logger.warning("CPU profile produced no main-thread stacks")
            return
        self._cpu_stacks_main = main_stacks
        self._cpu_stacks_io = io_stacks
        self.logger.info("CPU profile: %d main-thread stacks, %d io-thread stacks", len(main_stacks), len(io_stacks))

    def _check_guards(self, reader: dict, writer: dict, lag: dict) -> None:
        """Phase 4: conditions under which the rep is not the experiment it claims to be.

        ``lag`` is ``replication_lag_stats`` over the scored window only: the
        warmup is the reader ramping, the scored window is what the score is
        made of.
        """
        self._check_cell_guards(reader, writer)
        self._check_lag_guard(lag)

    def _check_cell_guards(self, reader: dict, writer: dict) -> None:
        """Guards that fail the cell whatever the reader mode: the dataset or the write stream was not as configured."""
        hit_rate = reader["hit_rate"] or {}
        hit = hit_rate.get("percent", 0.0)
        if hit < READER_MIN_HIT_RATE_PCT:
            raise RuntimeError(
                f"reader hit rate {hit}% < {READER_MIN_HIT_RATE_PCT}% "
                f"({hit_rate.get('hits')} hits, {hit_rate.get('misses')} misses): replica dataset incomplete"
            )
        achieved = writer["throughput_rps"]
        target = self.task.write_rate
        if achieved < target * (1 - WRITE_RATE_TOLERANCE):
            raise RuntimeError(
                f"writer achieved {achieved:.0f}/s, below {1 - WRITE_RATE_TOLERANCE:.0%} of target "
                f"{target}/s: primary or writer could not sustain the rate"
            )

    def _probe_verdict(self, read_rate: int, reader: dict, lag: dict) -> tuple:
        """(passed, reason) for one search probe at ``read_rate``.

        A probe passes when the reader actually offered its rate, the replica's
        lag trend over the scored window is within ``max_lag_slope`` and the
        lag where the window closed is within ``max_lag_seconds``. The reason
        names the limit that was hit so the knee's bracket says what bounded
        it: ``reader`` (the read path, or the generator, could not deliver the
        rate) or ``lag`` (the replica could not keep up while serving it).

        The slope is the keeping-up test; the level is judged at the window's
        end rather than as its mean because a probe can open on a backlog and
        drain it: several hundred reader connections reconnecting while the
        replica's main thread is near saturation leave it seconds behind for
        the first part of the window. A window mean carries that backlog after
        it is gone and would fail a rate the replica sustains, biasing the
        knee low; the closing level says where the queue stood once the
        probe's own load had run for the whole window. Cell-level guards (hit
        rate, writer) are not probe outcomes and are checked separately.
        """
        achieved = reader["throughput_rps"]
        if achieved < read_rate * READ_RATE_MIN_DELIVERY:
            return False, (
                f"reader: delivered {achieved:,.0f}/s of {read_rate:,}/s target "
                f"({achieved / read_rate:.1%} < {READ_RATE_MIN_DELIVERY:.0%})"
            )
        if lag.get("samples", 0) >= 2 and lag.get("mean_bytes", 0) > 0:
            stream = lag.get("stream_bytes_per_second") or 0
            mean_seconds = lag.get("mean_seconds")
            end_seconds = lag.get("end_seconds")
            slope = lag.get("slope_seconds_per_second")
            evidence = (
                f"mean {lag['mean_bytes']:,.0f} bytes, max {lag['max_bytes']:,} bytes, stream {stream:,.0f} bytes/s"
            )
            if mean_seconds is None or end_seconds is None:
                return False, f"lag: replica behind a stream that did not advance ({evidence})"
            if slope is not None and slope > self.task.max_lag_slope:
                return False, (
                    f"lag: growing {slope:.3f} s of stream per second > max_lag_slope {self.task.max_lag_slope} "
                    f"(replica not applying {slope:.1%} of the writes; {evidence})"
                )
            if end_seconds > self.task.max_lag_seconds:
                return False, (
                    f"lag: {end_seconds:.2f} s of stream at the end of the window > max_lag_seconds "
                    f"{self.task.max_lag_seconds} (window mean {mean_seconds:.2f} s; {evidence})"
                )
        return True, ""

    def _check_lag_guard(self, lag: dict) -> None:
        """Fail the rep when the replica sat behind its primary during the scored window.

        The threshold is in seconds of replication stream (``mean_seconds``),
        the unit that means the same thing at every write rate. A replica that
        cannot keep up still answers every GET, at a full hit rate, from a
        dataset the primary has already moved past; the reader's throughput is
        then a number about a different experiment.
        """
        if lag.get("samples", 0) < 2:
            self.logger.warning("lag guard skipped: %d offset samples in the scored window", lag.get("samples", 0))
            return
        mean_bytes = lag["mean_bytes"]
        if mean_bytes == 0:
            return
        seconds = lag["mean_seconds"]
        evidence = (
            f"mean {mean_bytes:,.0f} bytes, max {lag['max_bytes']:,} bytes over {lag['samples']} samples, "
            f"stream {lag['stream_bytes_per_second'] or 0:,.0f} bytes/s"
        )
        if seconds is None:
            raise RuntimeError(f"replica lag behind a stream that did not advance ({evidence}): replica not keeping up")
        if seconds > self.task.max_lag_seconds:
            raise RuntimeError(
                f"replica lag {seconds:.2f} s mean over the scored window > max_lag_seconds "
                f"{self.task.max_lag_seconds} ({evidence}): reads measured a replica behind its primary"
            )

    def _judge(self, group: TopologyGroup, rep: int, placement: "_Placement", m: "_Measurement") -> dict:
        """Phases 4-5: apply the guards, then reduce the rep to its result row entry with a verdict."""
        lag, window_start = self._lag(group, m)
        self._check_guards(m.reader, m.writer, lag["scored"])
        entry = self._entry(group, placement, m, lag, window_start)
        log = self.logger.info if entry["bottleneck"]["valid"] else self.logger.warning
        log(
            "rep %d/%d: reader %.0f rps, writer %.0f rps (target %d), lag max %s bytes (scored mean %s s), "
            "bottleneck=%s (%s)",
            rep,
            self.task.repetitions,
            m.reader["throughput_rps"],
            m.writer["throughput_rps"],
            self.task.write_rate,
            lag.get("max_bytes"),
            _fmt_seconds(lag["scored"].get("mean_seconds")),
            entry["bottleneck"]["verdict"],
            entry["bottleneck"]["reason"],
        )
        return entry

    def _lag(self, group: TopologyGroup, m: "_Measurement") -> tuple:
        """(lag statistics with their scored-window subset, scored window start) for a measurement."""
        primary, replica = _primary_and_replica(group)
        window_start = m.reader_started + self.task.warmup
        lag = replication_lag_stats(m.samples, primary.port, replica.port)
        lag["scored"] = replication_lag_stats(
            [s for s in m.samples if s.get("t", 0.0) >= window_start], primary.port, replica.port
        )
        return lag, window_start

    def _entry(
        self, group: TopologyGroup, placement: "_Placement", m: "_Measurement", lag: dict, window_start: float
    ) -> dict:
        """One measurement reduced to its result-row entry, with the bottleneck verdict."""
        primary, replica = _primary_and_replica(group)
        parties = cpu_sampling.Parties(
            replica=cpu_sampling.ServerIdentity(replica.port, replica.valkey_pid),
            primary=cpu_sampling.ServerIdentity(primary.port, primary.valkey_pid),
            allocated_cpus=self._allocated_cpus(group, placement),
        )
        bottleneck = cpu_sampling.bottleneck_verdict(m.samples, parties, window_start=window_start)
        return {
            "reader_rps": m.reader["throughput_rps"],
            "reader_latency": m.reader["latency"],
            "reader_hit_rate": m.reader["hit_rate"],
            "writer_rps": m.writer["throughput_rps"],
            "writer_latency": m.writer["latency"],
            "lag": lag,
            "bottleneck": bottleneck,
            "series": self._series(m.samples),
        }

    async def _search(self, group: TopologyGroup, rep: int, placement: "_Placement") -> tuple:
        """Phases 3-5 in read-rate-search mode: probe reader rates until the knee is bracketed.

        Every probe is a full measure phase (fresh writer, open-loop reader at
        the probe's rate, sampled throughout), judged by ``_probe_verdict``.
        The replica is caught up with its primary before each probe after the
        first, so a probe's lag trend is its own and not the previous probe's
        backlog draining. Returns the rep's entry (the knee probe's entry plus
        a ``search`` record of every probe and the bracket) and the last
        measurement.
        """
        task = self.task
        search = task.rate_search()
        step = search.first()
        probes: list = []
        last: Optional[_Measurement] = None
        while isinstance(step, Probe):
            number = len(probes) + 1
            if number > 1:
                await group.wait_for_offsets_caught_up()
            last = await self._measure(group, rep, placement, probe=number, read_rate=step.rate)
            lag, window_start = self._lag(group, last)
            self._check_cell_guards(last.reader, last.writer)
            passed, reason = self._probe_verdict(step.rate, last.reader, lag["scored"])
            entry = self._entry(group, placement, last, lag, window_start)
            entry.update({"probe": number, "target_rps": step.rate, "passed": passed, "reason": reason})
            self.logger.info(
                "rep %d/%d probe %d: %d/s -> %s (reader %.0f rps, lag scored mean %s s, end %s s, slope %s/s, "
                "bottleneck=%s)%s",
                rep,
                task.repetitions,
                number,
                step.rate,
                "pass" if passed else "FAIL",
                last.reader["throughput_rps"],
                _fmt_seconds(lag["scored"].get("mean_seconds")),
                _fmt_seconds(lag["scored"].get("end_seconds")),
                _fmt_seconds(lag["scored"].get("slope_seconds_per_second")),
                entry["bottleneck"]["verdict"],
                f": {reason}" if reason else "",
            )
            probes.append(entry)
            step = search.advance(passed)
        assert isinstance(step, Done) and last is not None
        if step.knee is None:
            lowest = min(probes, key=lambda p: p["target_rps"])
            raise RuntimeError(
                f"no read rate passed: {lowest['target_rps']}/s failed ({lowest['reason']}); "
                f"lower --read-rate-start or raise the lag limits"
            )
        knee = next(p for p in probes if p["target_rps"] == step.knee and p["passed"])
        hi = next((p for p in probes if p["target_rps"] == step.hi), None)
        if hi is None:
            bounded_by = "ceiling"
        else:
            bounded_by = hi["reason"].split(":", 1)[0]
        record = {
            "knee_target_rps": step.knee,
            "knee_rps": knee["reader_rps"],
            "hi_target_rps": step.hi,
            "hi_reason": hi["reason"] if hi else "",
            "bounded_by": bounded_by,
            "ceiling_reached": step.ceiling_reached,
            "probes": probes,
        }
        log = self.logger.warning if step.ceiling_reached else self.logger.info
        log(
            "rep %d/%d: knee %d/s (reader %.0f rps), bracket (%s, %s], bounded by %s, %d probes%s",
            rep,
            task.repetitions,
            step.knee,
            knee["reader_rps"],
            step.knee,
            step.hi,
            bounded_by,
            len(probes),
            "; read_rate_max passed, the true knee is higher" if step.ceiling_reached else "",
        )
        entry = {k: v for k, v in knee.items() if k not in ("probe", "target_rps", "passed", "reason")}
        entry["search"] = record
        return entry, last

    @staticmethod
    def _allocated_cpus(group: TopologyGroup, placement: "_Placement") -> dict:
        """Label -> CPU list for every pinned party; cores in none of them are foreign."""
        primary, replica = _primary_and_replica(group)
        allocated = {
            "primary": list(primary.server_cpus),
            "replica": list(replica.server_cpus),
            "reader": parse_cpulist(placement.reader_cpus) if placement.reader_cpus else [],
            "writer": parse_cpulist(placement.writer_cpus) if placement.writer_cpus else [],
            "runner": parse_cpulist(placement.runner_cpus) if placement.runner_cpus else [],
        }
        for extra in group.replicas[1:]:
            allocated[f"replica:{extra.port}"] = list(extra.server_cpus)
        return allocated

    async def _reset_peaks_after(self, delay_s: float, servers: dict) -> dict:
        """Sleep through the reader's warmup, then re-arm the memory peaks on ``servers``.

        Returns the per-role reset records for ``collect_memory_after``. Runs
        as a task alongside the sample loop; a cancelled sleep (reader ended
        early) simply means no reset was made.
        """
        await asyncio.sleep(max(0.0, float(delay_s)))
        return await reset_memory_peaks(servers)

    async def _sample_loop(
        self,
        group: TopologyGroup,
        samples: list,
        reader_cmd: RealtimeCommand,
        writer_cmd: Optional[RealtimeCommand] = None,
        pin_cpus: str = "",
    ) -> None:
        """Sample the topology (remote shell) and the two local generators once per interval.

        The generators run on THIS host, so their thread ticks are read straight
        from ``/proc`` rather than through the remote shell; ``t_local`` stamps
        that read so the two clocks are never mixed.
        """
        fields = self.task.extra_info_fields()
        reader_pid = self._pid(reader_cmd)
        writer_pid = self._pid(writer_cmd)
        while reader_cmd.is_running():
            try:
                sample = await group.sample(fields, pin_cpus=pin_cpus, host_threads=True)
                sample["t_local"] = time.monotonic()
                sample["generators"] = {
                    "reader": cpu_sampling.read_local_thread_stats(reader_pid),
                    "writer": cpu_sampling.read_local_thread_stats(writer_pid),
                }
                samples.append(sample)
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning("INFO sample failed: %s", exc)
            await asyncio.sleep(self.task.sample_interval)

    # Decimals kept on the sample clocks in the stored series. Samples are a
    # second apart; a millisecond is far below the sampling skew.
    _SERIES_CLOCK_DECIMALS = 3

    @classmethod
    def _series(cls, samples: list) -> dict:
        """The INFO series of one measurement in columnar form, for the result row.

        The in-memory samples (one dict per tick, every instance's fields
        repeated under it) feed the lag statistics and the bottleneck verdict.
        Stored as they are, 35 ticks x 2 instances x 15 fields cost about
        30 KB per rep, mostly repeated key names; five reps made a 200 KB row
        that the control service refused (HTTP 413). The row keeps the same
        numbers as one list per field::

            {"count": 35,
             "t": [...], "t_local": [...],
             "instances": {6379: {"master_repl_offset": [...], "role": [...], ...},
                           6380: {...}}}

        Every field list has ``count`` entries, in tick order; a field an
        instance did not report on a tick is ``None`` there. The cumulative
        per-core and per-thread counters (``cores``, ``threads``,
        ``host_threads``, ``generators``) are not stored at all: on a 96-core
        host at 1 Hz they would add about 1 MB per rep, and the verdict's
        ``foreign_attribution`` already keeps the part worth keeping.
        """
        count = len(samples)
        series: dict = {
            "count": count,
            "t": [cls._clock(s.get("t")) for s in samples],
            "t_local": [cls._clock(s.get("t_local")) for s in samples],
            "instances": {},
        }
        ports: list = []
        for sample in samples:
            for port in sample.get("instances") or {}:
                if port not in ports:
                    ports.append(port)
        for port in ports:
            fields: dict = {}
            for index, sample in enumerate(samples):
                for key, value in ((sample.get("instances") or {}).get(port) or {}).items():
                    fields.setdefault(key, [None] * count)[index] = value
            series["instances"][port] = fields
        return series

    @classmethod
    def _clock(cls, value):
        return round(value, cls._SERIES_CLOCK_DECIMALS) if isinstance(value, (int, float)) else None

    # ------------------------------------------------------------------ results

    @staticmethod
    def _aggregate_bottleneck(reps: list) -> dict:
        """One verdict for the row: the score is only a server number if EVERY rep was.

        Anything else names the worst rep so a reader of the row does not have
        to open ``per_rep_results`` to find out why ``valid`` is false.
        """
        verdicts = [r["bottleneck"]["verdict"] for r in reps]
        valid = all(r["bottleneck"]["valid"] for r in reps)
        if valid:
            verdict = cpu_sampling.VERDICT_SERVER
        else:
            verdict = next(v for v in verdicts if v != cpu_sampling.VERDICT_SERVER)
        worst = next((r["bottleneck"] for r in reps if r["bottleneck"]["verdict"] == verdict), reps[0]["bottleneck"])
        duties = [v for r in reps if (v := (r["bottleneck"].get("replica") or {}).get("loop_duty")) is not None]
        reader_peaks = [
            v for r in reps if (v := (r["bottleneck"].get("reader") or {}).get("max_thread_util")) is not None
        ]
        return {
            "verdict": verdict,
            "valid": valid,
            "per_rep": verdicts,
            "reason": worst["reason"],
            "replica_loop_duty_mean": mean(duties) if duties else None,
            "reader_max_thread_util": max(reader_peaks, default=None),
        }

    async def _record_result(
        self,
        group: TopologyGroup,
        reps: list,
        reader_toml: str,
        writer_toml: str,
        memory_records: Optional[list] = None,
    ) -> None:
        task = self.task
        completion_time = datetime.datetime.now()
        assert group.primary is not None
        lscpu_output, _ = await group.primary.run_host_command("lscpu")

        reader_rps = [r["reader_rps"] for r in reps]
        mean_rps = mean(reader_rps)
        cv = (stdev(reader_rps) / mean_rps) * 100 if len(reader_rps) >= 2 and mean_rps else None

        detailed = {
            "topology": self.spec.to_dict(),
            "management_cpus": self.management_cpus,
            "server_cpus": {s.port: s.server_cpus for s in group.servers},
            "warmup": task.warmup,
            "duration": task.duration,
            "io-threads": task.io_threads,
            "primary_io_threads": task.primary_io_threads,
            "pipeline": task.pipelining,
            "connections": task.connections,
            "threads": task.threads,
            "size": task.val_size,
            "keyspace_count": task.keyspace_count,
            "write_rate_target": task.write_rate,
            "write_connections": task.write_connections,
            "write_threads": task.write_threads,
            "write_pipeline": task.write_pipelining,
            "write_rate_achieved_mean": mean(r["writer_rps"] for r in reps),
            "replication_lag": {
                "max_bytes": max((r["lag"].get("max_bytes", 0) or 0) for r in reps),
                "mean_bytes": mean((r["lag"].get("mean_bytes", 0.0) or 0.0) for r in reps),
                # Worst rep's mean lag over its scored window, in seconds of
                # stream: the quantity the lag guard judged, next to its limit.
                "max_scored_mean_seconds": max((r["lag"].get("scored", {}).get("mean_seconds") or 0.0) for r in reps),
                "max_lag_seconds": task.max_lag_seconds,
            },
            "per_run_rps": reader_rps,
            "mean_rps": mean_rps,
            "latency": reps[-1]["reader_latency"],
            "bottleneck": self._aggregate_bottleneck(reps),
            "per_rep_results": reps,
            "sample_interval": task.sample_interval,
            "info_fields": task.extra_info_fields(),
            "cachecannon_binary": task.cachecannon_binary,
            "reader_toml": reader_toml,
            "writer_toml": writer_toml,
            "lscpu": lscpu_output,
            "cpu_profile": task.cpu_profile,
        }
        if task.read_rate_search:
            # The score is the reader throughput at each rep's knee; this block
            # says how the knee was found and how sharp the bracket is.
            detailed["read_rate_search"] = {
                "start_rps": task.read_rate_start,
                "max_rps": task.read_rate_max,
                "step": task.read_rate_step,
                "tolerance": task.read_rate_tolerance,
                "max_bisect_steps": task.read_rate_max_bisect_steps,
                "max_lag_slope": task.max_lag_slope,
                "min_delivery": READ_RATE_MIN_DELIVERY,
                "per_rep_knee_target_rps": [r["search"]["knee_target_rps"] for r in reps],
                "per_rep_hi_target_rps": [r["search"]["hi_target_rps"] for r in reps],
                "per_rep_bounded_by": [r["search"]["bounded_by"] for r in reps],
                "per_rep_probes": [len(r["search"]["probes"]) for r in reps],
                "ceiling_reached": any(r["search"]["ceiling_reached"] for r in reps),
            }
        if self._cpu_stacks_main:
            # Same keys the perf task uses, so file_protocol also writes them to
            # cpu_stacks_main.json / cpu_stacks_io.json and the flamegraph
            # tooling reads them unchanged. Profiled rep named so a reader can
            # discount its throughput (perf sampling interrupts the replica).
            detailed["cpu_profile_rep"] = task.repetitions
            detailed["cpu_stacks_main"] = self._cpu_stacks_main
            detailed["cpu_stacks_io"] = self._cpu_stacks_io

        folded_memory = fold_memory_records(memory_records) if memory_records else None
        if folded_memory is not None:
            detailed["memory"] = folded_memory

        results = BenchmarkResults(
            method=METHOD,
            source=task.source,
            specifier=task.specifier,
            commit_hash=self.commit_hash,
            score=mean_rps,
            end_time=completion_time,
            data=detailed,
            make_args=task.make_args,
            note=task.note,
            cv=cv,
            reps=len(reps),
        )
        self.file_protocol.write_results(results)
