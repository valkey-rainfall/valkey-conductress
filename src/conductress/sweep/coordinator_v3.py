"""Cachecannon sweep coordinators (v3 epoch).

The v3 epoch fixes cachecannon as the load generator for every series that
needs one: pure-GET throughput, mixed GET/SET throughput, and GET latency at a
fixed request rate.  It replaced the retired ``scalable-v2`` epoch for two
reasons:

* The v2 mixed workload could not run at all -- it emitted a ``--warmup-period``
  flag that memtier has never implemented, and memtier has no warmup feature to
  wire it to. Cachecannon has native same-process warmup that keeps connections,
  pipelines, and key-generator state alive across the boundary and atomically
  resets statistics, which is the semantics a warmup actually requires.
* The v2 GET series came from a patched valkey-benchmark whose arrival shape
  differs from stock by roughly 5% even when both are server-bound, so those
  points could never share a chart line with cachecannon output.

State files, published filenames, and ownership predicates are disjoint from
both legacy v1 and the retired v2 namespace. Ownership is additionally disjoint
by construction: cachecannon tasks are ``CachecannonTaskData``, a sibling of
``PerfTaskData`` under ``BaseTaskData``, so the v1 ``isinstance`` predicates
cannot match a v3 task even accidentally.  Within v3, the ownership predicate
covers every workload-defining field, rate limit and score metric included, so
the three series cannot absorb one another's cells.

Protocol values live in ``config`` and were confirmed by measurement -- see
``SWEEP_V3_*`` and ``conductress-cachecannon-v3/epoch-specification.md``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from statistics import stdev
from typing import Literal, Optional

from conductress import config
from conductress.config import (
    CONDUCTRESS_RESULTS,
    SWEEP_KEY_SIZE,
    SWEEP_STATE_DIR,
    SWEEP_V3_CACHECANNON_COMMIT,
    SWEEP_V3_CLIENT_THREADS,
    SWEEP_V3_CONNECTIONS,
    SWEEP_V3_DISTRIBUTION,
    SWEEP_V3_DURATION,
    SWEEP_V3_EPOCH_ID,
    SWEEP_V3_IO_THREADS,
    SWEEP_V3_KEYSPACE,
    SWEEP_V3_LARGE_VAL_SIZE,
    SWEEP_V3_LARGE_VALUE_FLOOR_TAG,
    SWEEP_V3_LATENCY_MAX_REPS,
    SWEEP_V3_LATENCY_PIPELINING,
    SWEEP_V3_LATENCY_RATE,
    SWEEP_V3_LATENCY_REPETITIONS,
    SWEEP_V3_LATENCY_TARGET_CV,
    SWEEP_V3_MAX_REPS,
    SWEEP_V3_P1_CLIENT_THREADS,
    SWEEP_V3_PIPELINING,
    SWEEP_V3_REPETITIONS,
    SWEEP_V3_SET_RATIO,
    SWEEP_V3_TARGET_CV,
    SWEEP_V3_TLS_FLOOR_TAG,
    SWEEP_V3_VAL_SIZE,
    SWEEP_V3_WARMUP,
)
from conductress.sweep.coordinator import BaseSweepCoordinator, PerfCounterRecord, perf_counters_from_entry
from conductress.sweep.planner import SweepTask
from conductress.task_queue import BaseTaskData
from conductress.tasks.task_cachecannon import CachecannonTaskData
from conductress.topology import TopologySpec

logger = logging.getLogger(__name__)

# v3 state directory: isolated from both v1 (unqualified) and v2.
V3_STATE_DIR = SWEEP_STATE_DIR / "v3"


def _ensure_v3_state_dir() -> Path:
    V3_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return V3_STATE_DIR


class BaseCachecannonSweepCoordinatorV3(BaseSweepCoordinator):
    """Shared behaviour for cachecannon-driven v3 sweeps.

    Subclasses differ in their workload label and in the ``set_ratio`` /
    ``test`` / ``rate_limit`` / ``score_metric`` values that define the
    workload.  Every one of those is part of the ownership predicate, so a
    latency cell (rate-limited, scored on p99) can never be absorbed into a
    throughput series of the same shape, or the reverse.
    """

    metric_id = "throughput"
    metric_unit = "ops/sec"

    def __init__(
        self,
        repo_path: Path,
        label: str,
        test: str,
        set_ratio: int,
        val_size: int = SWEEP_V3_VAL_SIZE,
        io_threads: int = SWEEP_V3_IO_THREADS,
        pipelining: int = SWEEP_V3_PIPELINING,
        connections: int = SWEEP_V3_CONNECTIONS,
        threads: int = SWEEP_V3_CLIENT_THREADS,
        distribution: str = SWEEP_V3_DISTRIBUTION,
        rate_limit: int = 0,
        score_metric: str = "throughput",
        score_aggregate: Literal["mean", "median"] = "mean",
        repetitions: int = SWEEP_V3_REPETITIONS,
        max_reps: int = SWEEP_V3_MAX_REPS,
        target_cv: float = SWEEP_V3_TARGET_CV,
        tls: bool = False,
        engine: Optional[config.SweepEngine] = None,
        floor_tag: Optional[str] = None,
    ):
        self._test = test
        self._set_ratio = set_ratio
        self._val_size = val_size
        self._io_threads = io_threads
        self._pipelining = pipelining
        self._connections = connections
        self._threads = threads
        self._distribution = distribution
        self._rate_limit = rate_limit
        self._score_metric = score_metric
        self._score_aggregate = score_aggregate
        self._repetitions = repetitions
        self._max_reps = max_reps
        self._target_cv = target_cv
        self._tls = tls

        engine_prefix = f"{engine.source}-" if engine and engine.source != "valkey" else ""
        self._label = f"{engine_prefix}{label}"

        _ensure_v3_state_dir()
        state_file = V3_STATE_DIR / f"state_cachecannon-v3_{self._label}.json"
        super().__init__(repo_path, state_file, engine=engine, floor_tag=floor_tag)

    @property
    def epoch_id(self) -> str:
        return SWEEP_V3_EPOCH_ID

    @property
    def workload_id(self) -> str:  # type: ignore[override]
        return self._label

    def _create_task(self, sweep_task: SweepTask) -> CachecannonTaskData:
        return CachecannonTaskData(
            source=self._sweep_source,
            specifier=sweep_task.commit,
            make_args=self._sweep_make_args,
            topology=TopologySpec.standalone(),
            note=f"[cachecannon-sweep-v3:{self._sweep_source}/{self.workload_id}] {sweep_task.reason}",
            requirements={},
            test=self._test,
            set_ratio=self._set_ratio,
            val_size=self._val_size,
            io_threads=self._io_threads,
            pipelining=self._pipelining,
            connections=self._connections,
            threads=self._threads,
            warmup=SWEEP_V3_WARMUP,
            duration=SWEEP_V3_DURATION,
            keyspace_count=SWEEP_V3_KEYSPACE,
            distribution=self._distribution,
            repetitions=self._repetitions,
            max_reps=self._max_reps,
            target_cv=self._target_cv,
            rate_limit=self._rate_limit,
            score_metric=self._score_metric,
            score_aggregate=self._score_aggregate,
            tls=self._tls,
            # Per-thread hardware counters every rep and a CPU flamegraph on the
            # last rep, as the v1 sweep has always collected. Counting-mode perf
            # stat costs well under 1% and is what feeds the dashboard's IPC,
            # cache, stall and syscall series for this epoch.
            perf_stat_enabled=True,
        )

    def _per_run_scores(self, entry: dict) -> list:
        """The recorded per-rep series of this coordinator's score metric."""
        data = entry.get("data", {})
        if self._score_metric == "throughput":
            return data.get("per_run_rps", [])
        return data.get(f"per_run_{self._score_metric}_us", [])

    def _find_task_entry(self, task: BaseTaskData) -> Optional[dict]:
        # Same as the base implementation, read through this module's
        # CONDUCTRESS_RESULTS so a test can redirect the v3 coordinators alone.
        output_file = CONDUCTRESS_RESULTS / "output.jsonl"
        if not output_file.exists():
            return None
        for line in reversed(output_file.read_text().strip().splitlines()):
            try:
                entry = json.loads(line)
                if entry.get("task_id") == task.task_id:
                    return entry
            except (ValueError, KeyError, TypeError):
                continue
        return None

    def _extract_result(self, task: BaseTaskData) -> Optional[tuple[float, float, int]]:
        entry = self._find_task_entry(task)
        if not entry:
            return None
        score = entry.get("score")
        per_run = self._per_run_scores(entry)
        cv = (stdev(per_run) / score) * 100 if len(per_run) >= 2 and score else 0.0
        # Adaptive reps mean the count is not knowable from configuration; it
        # must come from the recorded run list.
        reps = len(per_run) if per_run else self._repetitions
        return (score, cv, reps) if score else None

    def _extract_perf_counters(self, task: BaseTaskData) -> Optional[PerfCounterRecord]:
        entry = self._find_task_entry(task)
        return perf_counters_from_entry(entry) if entry else None

    def _record_score_bounds(self, task: BaseTaskData) -> None:
        """Copy the recorded per-rep score min/max onto this task's point.

        The min/max come straight from the result row (``score_min`` /
        ``score_max``, written for every cachecannon task) so the v3 exporter
        can publish the between-restart spread the score was drawn from.  A row
        without them (a cell queued before the fields existed) leaves the point
        untouched.
        """
        entry = self._find_task_entry(task)
        if not entry:
            return
        commit = getattr(task, "sweep_commit", "")
        point = self.state.points.get(commit) if commit else None
        if point is None:
            return
        data = entry.get("data", {})
        smin = data.get("score_min")
        smax = data.get("score_max")
        if smin is None and smax is None:
            return
        point.score_min = smin
        point.score_max = smax
        self.state.save(self.state_file)

    def _record_client_cpu(self, task: BaseTaskData) -> None:
        """Copy the load-generator CPU utilization onto this task's point.

        The result row's ``data.client_cpu`` block (built by
        ``utility.summarize_client_cpu``) reports the generator process tree's
        cores kept busy per rep, its allocated core budget, and -- when the
        budget is known -- utilization and a saturation flag.  Lifting the block
        onto the point lets the published series say whether a point measured the
        server or the client.  ``client_cores_busy`` is the max over reps of
        cores kept busy, matching how ``summarize_client_cpu`` defines
        utilization.  A row without a ``client_cpu`` block (a cell queued before
        the field existed) leaves the point untouched.

        A saturated point is logged at WARNING: it is the runner-side signal
        that a series line is measuring the load generator rather than the
        server.
        """
        entry = self._find_task_entry(task)
        if not entry:
            return
        commit = getattr(task, "sweep_commit", "")
        point = self.state.points.get(commit) if commit else None
        if point is None:
            return
        client_cpu = entry.get("data", {}).get("client_cpu")
        if not client_cpu:
            return
        cores_busy_per_rep = client_cpu.get("cores_busy_per_rep") or []
        point.client_cores_busy = max(cores_busy_per_rep) if cores_busy_per_rep else None
        point.client_allocated_cores = client_cpu.get("allocated_cores")
        point.client_utilization = client_cpu.get("utilization")
        point.client_saturated = client_cpu.get("saturated")
        self.state.save(self.state_file)
        if point.client_saturated:
            logger.warning(
                "%s %s measured a saturated client: utilization %s of %s allocated cores; "
                "the throughput reflects the load generator's capacity, not the server's",
                self.workload_id,
                commit[:8],
                point.client_utilization,
                point.client_allocated_cores,
            )

    def on_task_completed(self, task: BaseTaskData) -> None:
        """Record the result as the base does, then lift the score spread.

        ``super().on_task_completed`` records value/cv/reps, perf counters and
        CPU stacks; the score min/max and the load-generator CPU utilization are
        the v3-specific point fields, and they are set afterward so a point
        exists to attach them to.

        Before any of that, the completed task's workload shape is checked
        against this coordinator's identity.  ``_is_my_task`` already gates on
        every identity field but ``threads``; a task that drifted from the
        series' client budget (a cell queued at the old 8-thread budget
        completing after a 16-thread deploy) reaches here and is refused with a
        WARNING naming the field, rather than recorded onto a line measured at a
        different client budget.  Refusing is not crashing: the point is simply
        not recorded and the runner carries on.
        """
        if not self._is_my_task(task):
            return
        mismatch = self._identity_mismatch(task)
        if mismatch is not None:
            field_name, task_value, own_value = mismatch
            commit = getattr(task, "sweep_commit", "?")
            logger.warning(
                "Refusing to record %s %s: task %s=%r does not match series %r; "
                "stale queued task from a different client budget, point not recorded",
                self.workload_id,
                commit[:8],
                field_name,
                task_value,
                own_value,
            )
            return
        super().on_task_completed(task)
        self._record_score_bounds(task)
        self._record_client_cpu(task)

    def _extract_cpu_stacks(self, task: BaseTaskData) -> None:
        entry = self._find_task_entry(task)
        if entry:
            self._store_cpu_stacks_from_entry(task, entry)

    def export(self, output_path: Path, platform: str) -> int:
        """Export this v3 series, tagging its metadata with the client budget.

        The v3 throughput series carry a per-series client budget (the P1 GET
        series runs 16 threads, the rest 8) that is part of their identity, so
        the exported ``metadata`` records the connections and client-thread
        count the line was measured at.  Everything else is the base export.
        """
        from conductress import config as _config
        from conductress.sweep.exporter import export_series

        branch = self._sweep_ref.replace("origin/", "") if self.engine else "unstable"
        export_series(
            self.state,
            output_path,
            platform=platform,
            workload=self.workload_id,
            lower_is_better=self.lower_is_better,
            repo=_config.engine_repo_slug(self.engine),
            branch=branch,
            engine=self.engine,
            connections=self._connections,
            client_threads=self._threads,
            tls=self._tls,
        )
        return sum(1 for p in self.state.points.values() if p.value is not None)

    def _is_my_task(self, task: BaseTaskData) -> bool:
        # Selects the tasks this series owns.  ``threads`` is deliberately NOT
        # here: it is the one identity field that drifts across a client-budget
        # change (a task queued at the old 8-thread budget completing after a
        # 16-thread deploy), and it must reach ``on_task_completed``'s identity
        # guard to be flagged and skipped rather than silently dropped by this
        # predicate.  No two v3 series differ only by ``threads`` (P1 throughput
        # vs P10 GET differ by pipelining; P1 throughput vs P1 latency by
        # rate_limit and score_metric), so dropping it here causes no
        # cross-series claiming.
        return (
            isinstance(task, CachecannonTaskData)
            # A manually queued cachecannon cell carries no sweep_commit and must
            # never be absorbed into the sweep series.
            and bool(getattr(task, "sweep_commit", ""))
            and task.source == self._sweep_source
            and task.test == self._test
            and task.set_ratio == self._set_ratio
            and task.val_size == self._val_size
            and task.io_threads == self._io_threads
            and task.pipelining == self._pipelining
            and task.connections == self._connections
            and task.distribution == self._distribution
            and task.rate_limit == self._rate_limit
            and task.score_metric == self._score_metric
            and task.tls == self._tls
        )

    # The workload-shape fields that make up a series' identity.  A completed
    # task must match the coordinator on every one of them before its point is
    # recorded; ``_is_my_task`` already gates on all of these except
    # ``threads``, which the identity guard below checks explicitly so a
    # client-budget drift is flagged rather than silently absorbed.
    _IDENTITY_FIELDS = (
        "threads",
        "connections",
        "pipelining",
        "val_size",
        "io_threads",
        "test",
        "set_ratio",
        "tls",
    )

    def _identity_mismatch(self, task: BaseTaskData) -> Optional[tuple[str, object, object]]:
        """First identity field where ``task`` disagrees with this coordinator, if any.

        Returns ``(field, task_value, coordinator_value)`` on the first
        mismatch, or ``None`` when every identity field matches.  The
        coordinator's own value is read from the private attribute the
        constructor stored (e.g. ``self._threads`` for ``threads``).
        """
        for field_name in self._IDENTITY_FIELDS:
            task_value = getattr(task, field_name, None)
            own_value = getattr(self, f"_{field_name}")
            if task_value != own_value:
                return (field_name, task_value, own_value)
        return None


class CachecannonThroughputSweepCoordinatorV3(BaseCachecannonSweepCoordinatorV3):
    """Canonical v3 pure-GET throughput sweep."""

    def __init__(
        self,
        repo_path: Path,
        test: str = "get",
        val_size: int = SWEEP_V3_VAL_SIZE,
        io_threads: int = SWEEP_V3_IO_THREADS,
        pipelining: int = SWEEP_V3_PIPELINING,
        connections: int = SWEEP_V3_CONNECTIONS,
        threads: int = SWEEP_V3_CLIENT_THREADS,
        distribution: str = SWEEP_V3_DISTRIBUTION,
        score_aggregate: Literal["mean", "median"] = "mean",
        tls: bool = False,
        engine: Optional[config.SweepEngine] = None,
        floor_tag: Optional[str] = None,
    ):
        label = f"{test}-k{SWEEP_KEY_SIZE}-v{val_size}-t{io_threads}-p{pipelining}"
        if distribution != SWEEP_V3_DISTRIBUTION:
            label += f"-{distribution}"
        # The encrypted-transport series carries a -tls suffix so its label,
        # state file and published series never collide with the plaintext GET
        # series of the same shape.
        if tls:
            label += "-tls"
        super().__init__(
            repo_path,
            label=label,
            test=test,
            set_ratio=0,
            val_size=val_size,
            io_threads=io_threads,
            pipelining=pipelining,
            connections=connections,
            threads=threads,
            distribution=distribution,
            score_aggregate=score_aggregate,
            tls=tls,
            engine=engine,
            floor_tag=floor_tag,
        )


class CachecannonMixedSweepCoordinatorV3(BaseCachecannonSweepCoordinatorV3):
    """Canonical v3 mixed GET/SET throughput sweep (default 80:20)."""

    def __init__(
        self,
        repo_path: Path,
        set_ratio: int = SWEEP_V3_SET_RATIO,
        val_size: int = SWEEP_V3_VAL_SIZE,
        io_threads: int = SWEEP_V3_IO_THREADS,
        pipelining: int = SWEEP_V3_PIPELINING,
        connections: int = SWEEP_V3_CONNECTIONS,
        threads: int = SWEEP_V3_CLIENT_THREADS,
        distribution: str = SWEEP_V3_DISTRIBUTION,
        engine: Optional[config.SweepEngine] = None,
    ):
        label = f"mixed-s{set_ratio}-k{SWEEP_KEY_SIZE}-v{val_size}-t{io_threads}-p{pipelining}"
        if distribution != SWEEP_V3_DISTRIBUTION:
            label += f"-{distribution}"
        super().__init__(
            repo_path,
            label=label,
            test="get",
            set_ratio=set_ratio,
            val_size=val_size,
            io_threads=io_threads,
            pipelining=pipelining,
            connections=connections,
            threads=threads,
            distribution=distribution,
            engine=engine,
        )


class CachecannonLatencySweepCoordinatorV3(BaseCachecannonSweepCoordinatorV3):
    """Canonical v3 latency sweep: p99 of GET at a fixed request rate.

    Same generator, connections, client threads, key space and scored window as
    the v3 throughput workloads, driven at ``SWEEP_V3_LATENCY_RATE`` with no
    pipelining.  Bisects on p99 (lower is better).  The exported series carries
    p50/p99/p99.9/p100 and the achieved rate for every point, so a reader can
    tell a slow server from a generator that failed to hold the rate.
    """

    metric_id = "latency"
    metric_unit = "µs"
    lower_is_better = True

    def __init__(
        self,
        repo_path: Path,
        rate_limit: int = SWEEP_V3_LATENCY_RATE,
        val_size: int = SWEEP_V3_VAL_SIZE,
        io_threads: int = SWEEP_V3_IO_THREADS,
        pipelining: int = SWEEP_V3_LATENCY_PIPELINING,
        connections: int = SWEEP_V3_CONNECTIONS,
        threads: int = SWEEP_V3_CLIENT_THREADS,
        distribution: str = SWEEP_V3_DISTRIBUTION,
        engine: Optional[config.SweepEngine] = None,
    ):
        if rate_limit <= 0:
            raise ValueError(f"a latency sweep needs a fixed request rate, got rate_limit={rate_limit}")
        label = f"get-k{SWEEP_KEY_SIZE}-v{val_size}-t{io_threads}-p{pipelining}-r{rate_limit // 1000}k"
        if distribution != SWEEP_V3_DISTRIBUTION:
            label += f"-{distribution}"
        super().__init__(
            repo_path,
            label=label,
            test="get",
            set_ratio=0,
            val_size=val_size,
            io_threads=io_threads,
            pipelining=pipelining,
            connections=connections,
            threads=threads,
            distribution=distribution,
            rate_limit=rate_limit,
            score_metric="p99",
            repetitions=SWEEP_V3_LATENCY_REPETITIONS,
            max_reps=SWEEP_V3_LATENCY_MAX_REPS,
            target_cv=SWEEP_V3_LATENCY_TARGET_CV,
            engine=engine,
        )

    def get_urgency_score(self) -> float:
        """Priority score, dampened by 0.5x relative to throughput.

        Latency fills in behind the throughput series once both exist, the same
        weighting the latency series has always had.
        """
        completed = sum(1 for p in self.state.points.values() if p.value is not None)
        if completed < 2:
            return float("inf")
        return super().get_urgency_score() * 0.5

    @staticmethod
    def latency_data_from_entry(entry: dict) -> Optional[dict]:
        """The per-point latency detail the exporter publishes, from one result record.

        cachecannon reports p50/p90/p99/p99.9/p99.99/max as exact microseconds;
        the export keeps the four the latency series has always carried and the
        achieved rate.  There is no histogram: cachecannon does not emit one.
        """
        data = entry.get("data", {})
        latency = data.get("latency")
        if not latency:
            return None
        return {
            "p50_us": latency["p50_ms"] * 1000.0,
            "p99_us": latency["p99_ms"] * 1000.0,
            "p99_9_us": latency["p999_ms"] * 1000.0,
            "p100_us": latency["max_ms"] * 1000.0,
            "target_rps": data.get("rate_limit"),
            "actual_rps": data.get("mean_rps"),
        }

    def on_task_completed(self, task: BaseTaskData) -> None:
        """Record p99 for bisection, the counters the cell collected, and the full percentile set.

        The base class records the score and CV, then lifts the perf counters
        and CPU stacks the cell collected (the same fields a throughput point
        carries, so the same per-request series can be published for this
        workload). The percentile detail is the only latency-specific field.
        """
        if not self._is_my_task(task):
            return
        entry = self._find_task_entry(task)
        if not entry or not self._extract_result(task):
            commit = getattr(task, "sweep_commit", "?")
            logger.warning("Could not extract latency result for %s", commit[:8])
            return
        super().on_task_completed(task)
        latency_data = self.latency_data_from_entry(entry)
        if latency_data and task.sweep_commit in self.state.points:  # type: ignore[attr-defined]
            self.state.points[task.sweep_commit].latency_data = latency_data  # type: ignore[attr-defined]
            self.state.save(self.state_file)

    def export(self, output_path: Path, platform: str) -> int:
        from conductress.sweep.exporter import export_latency

        return export_latency(
            self.state,
            output_path,
            platform=platform,
            workload=self.workload_id,
            target_rps=self._rate_limit,
            tool="cachecannon",
            tool_version=SWEEP_V3_CACHECANNON_COMMIT[:8],
            connections=self._connections,
            client_threads=self._threads,
        )


def create_v3_coordinators(
    repo_path: Path, engine: Optional[config.SweepEngine] = None
) -> list[BaseCachecannonSweepCoordinatorV3]:
    """Build the v3 coordinator roster for one engine.

    Seven series, all at the v3 identity (400c / io7 / 3M keys / 16B and 8 client
    threads unless the series says otherwise):

    * GET throughput at P10 -- the canonical pipelined-read ceiling.
    * mixed GET/SET throughput at P10 (default 80:20).
    * SET throughput at P10 -- the write counterpart of the GET series.
    * GET throughput at P1 -- the unpipelined read path, scored on the MEDIAN
      of the per-rep series: P1 throughput lands in two distinct modes across
      server restarts, and the median reports the mode most reps reached where
      a mean would report a value no rep produced.  This series runs a
      16-client-thread budget (``SWEEP_V3_P1_CLIENT_THREADS``), not the 8 the
      P10 series use: at P1 cachecannon spends 4-5x more CPU per request and 8
      threads saturate the client below the server's ceiling (Graviton3 hit
      client utilization 0.941 at 8t).  The budget is part of the series
      identity, so the same factory applies it to the Redis comparison engine's
      P1 series and it never changes within the series.
    * GET throughput at P10 with 1024-byte values -- the raw-encoded,
      copy-dominated reply path, which moves on changes the 16-byte series
      cannot see.  Its commit range starts at ``SWEEP_V3_LARGE_VALUE_FLOOR_TAG``
      rather than the fork point: it guards the reply path going forward and
      does not backfill two years of history at the expense of the other five.
    * GET latency at P1 at a fixed request rate -- p99, lower is better.  It
      stays at 8 client threads: rate-held at 100k/s, the client is under no
      pressure.
    * GET throughput at P10 over TLS -- the encrypted read path.  Same identity
      as the canonical GET series except the transport: the server opens a TLS
      listener with the bootstrap test certificates and the generator connects
      over it.  Like the large-value series it does not backfill the whole
      history: its commit range starts at ``SWEEP_V3_TLS_FLOOR_TAG`` rather than
      the fork point, so it guards the TLS path going forward without slowing
      the other six.  A release-and-tip comparison engine does not read the
      floor (a Valkey tag); its own engine floor stands.

    A comparison engine (Redis) gets the same seven series under its own prefix,
    so the engine comparison reads like-for-like cells; its ``scope`` decides
    how much of its history they measure and bounds the added cost.  A
    release-and-tip engine measures only its latest release and its tip, so a
    series floor tag (a Valkey tag) is not applied to it: its own engine floor
    stands.
    """
    history_floor = config.engine_tracks_history(engine)
    large_value_floor = SWEEP_V3_LARGE_VALUE_FLOOR_TAG if history_floor else None
    tls_floor = SWEEP_V3_TLS_FLOOR_TAG if history_floor else None
    return [
        CachecannonThroughputSweepCoordinatorV3(repo_path, engine=engine),
        CachecannonMixedSweepCoordinatorV3(repo_path, engine=engine),
        CachecannonThroughputSweepCoordinatorV3(repo_path, test="set", engine=engine),
        CachecannonThroughputSweepCoordinatorV3(
            repo_path,
            test="get",
            pipelining=1,
            threads=SWEEP_V3_P1_CLIENT_THREADS,
            score_aggregate="median",
            engine=engine,
        ),
        CachecannonThroughputSweepCoordinatorV3(
            repo_path, test="get", val_size=SWEEP_V3_LARGE_VAL_SIZE, engine=engine, floor_tag=large_value_floor
        ),
        CachecannonLatencySweepCoordinatorV3(repo_path, engine=engine),
        CachecannonThroughputSweepCoordinatorV3(repo_path, test="get", tls=True, engine=engine, floor_tag=tls_floor),
    ]
