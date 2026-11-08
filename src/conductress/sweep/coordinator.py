"""Sweep coordinators: bridge the sweep planner with the Conductress task runner.

BaseSweepCoordinator handles git history, state management, queue interaction,
and the pub/sub protocol. Subclasses define task creation and result extraction
for specific metrics (throughput, memory, etc.).
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from statistics import stdev
from typing import Optional

from conductress import config
from conductress.config import (
    CONDUCTRESS_RESULTS,
    PROJECT_ROOT,
    SWEEP_DURATION,
    SWEEP_FETCH_INTERVAL,
    SWEEP_IO_THREADS,
    SWEEP_MAKE_ARGS,
    SWEEP_MAX_REPS,
    SWEEP_PIPELINING,
    SWEEP_REF,
    SWEEP_REPETITIONS,
    SWEEP_SOURCE,
    SWEEP_STATE_DIR,
    SWEEP_TARGET_CV,
    SWEEP_TEST,
    SWEEP_VAL_SIZE,
    SWEEP_WARMUP,
)
from conductress.sweep.git_ops import fetch_ref, get_head, get_merge_commits, get_release_branch_points
from conductress.sweep.planner import Landmark, SweepPlanner, SweepState, SweepTask, TaskPriority
from conductress.task_queue import BaseTaskData, TaskQueue
from conductress.tasks.task_perf_benchmark import PerfTaskData
from conductress.topology import TopologySpec

logger = logging.getLogger(__name__)

# Git configuration (shared across all sweep types)


@dataclass
class PerfCounterRecord:
    """Perf stat counters lifted from one completed task's result row.

    ``counters`` is process-wide; ``counters_main`` / ``counters_io`` are the
    optional per-thread buckets. ``duration`` is the counting window of one rep
    and ``rep_count`` the number of reps whose raw counters were summed, both
    needed to normalise absolute counters per request. ``scope`` is the
    privilege scope perf counted under (``None`` for rows that predate it, which
    were user-only).
    """

    counters: dict[str, int]
    duration: float
    rps: float
    counters_main: Optional[dict[str, int]] = None
    counters_io: Optional[dict[str, int]] = None
    rep_count: Optional[int] = None
    scope: Optional[str] = None


def perf_counters_from_entry(entry: dict) -> Optional[PerfCounterRecord]:
    """Lift perf stat counters from an ``output.jsonl`` row, whatever its shape.

    Two result shapes exist. The valkey-benchmark task flattens the buckets into
    sibling keys (``perf_counters`` process-wide, ``perf_counters_main``,
    ``perf_counters_io``); the memtier mixed and cachecannon tasks store the
    bucketed dict as written by the profiling manager
    (``perf_counters = {"all": ..., "main": ..., "io": ...}``). Both are
    normalised here so every coordinator records the same point fields.

    ``rps`` is the request rate the counters were collected at, which the
    exporter divides by to get per-request metrics. Rows that record it
    directly (``data.mean_rps``, written by the cachecannon task) supply it;
    for a throughput cell that is the score itself, and for a rate-limited
    latency cell it is the achieved rate where the score is a percentile in
    microseconds. Rows without it fall back to the score, which is only right
    when the score is a throughput.
    """
    data = entry.get("data", {})
    raw = data.get("perf_counters")
    if not raw:
        return None
    if any(k in raw for k in ("all", "main", "io")):
        counters = raw.get("all") or {}
        counters_main = raw.get("main") or None
        counters_io = raw.get("io") or None
    else:
        counters = raw
        counters_main = data.get("perf_counters_main")
        counters_io = data.get("perf_counters_io")
    if not counters:
        return None
    rps = data.get("mean_rps")
    if not rps:
        rps = entry.get("score", 0.0)
    return PerfCounterRecord(
        counters=counters,
        duration=data.get("perf_duration_seconds", 0.0),
        rps=rps,
        counters_main=counters_main,
        counters_io=counters_io,
        rep_count=data.get("perf_rep_count"),
        scope=data.get("perf_counters_scope"),
    )


def latest_release_landmark(landmarks: list[Landmark], merge_commits: list[str]) -> list[Landmark]:
    """The single newest release landmark, by position on the sweep ref.

    Release labels start with a digit (``8.10``, ``9.2``); anything else is a
    descriptive marker and is dropped along with the older releases.  Returns an
    empty list when no release landmark exists.
    """
    index = {c: i for i, c in enumerate(merge_commits)}
    releases = [lm for lm in landmarks if lm.label[:1].isdigit() and lm.commit in index]
    if not releases:
        return []
    return [max(releases, key=lambda lm: index[lm.commit])]


class BaseSweepCoordinator(ABC):
    """Abstract base for sweep coordinators.

    Handles: git history enumeration, state persistence, queue management,
    TaskSubscriber protocol (on_task_completed, on_task_failed, on_queue_empty).

    Subclasses define: task creation, result extraction, task filtering.
    """

    def __init__(
        self,
        repo_path: Path,
        state_file: Path,
        engine: Optional["config.SweepEngine"] = None,
        floor_tag: Optional[str] = None,
    ):
        self.repo_path = repo_path
        self.state_file = state_file
        self.engine = engine
        # A series floor bounds this one series' commit range; it overrides the
        # engine floor (which bounds every series of that engine).
        self.floor_tag = floor_tag
        self.state = SweepState.load(state_file)
        self.planner = self._new_planner()
        self._last_fetch_time: float = 0.0

    def _new_planner(self) -> SweepPlanner:
        """A planner over the current state, shaped by the engine's scope."""
        return SweepPlanner(
            self.state,
            tracks_history=config.engine_tracks_history(self.engine),
            tip_interval_seconds=config.engine_tip_interval_seconds(self.engine),
        )

    @property
    def _floor_tag(self) -> Optional[str]:
        """The tag this series' commit range starts from: series floor, else engine floor."""
        if self.floor_tag:
            return self.floor_tag
        return self.engine.floor_tag if self.engine else None

    @property
    def _sweep_source(self) -> str:
        return self.engine.source if self.engine else SWEEP_SOURCE

    @property
    def _sweep_ref(self) -> str:
        return self.engine.ref if self.engine else SWEEP_REF

    @property
    def _sweep_make_args(self) -> str:
        return self.engine.make_args if self.engine else SWEEP_MAKE_ARGS

    def initialize(self) -> None:
        """Initialize the merge commit list from git history, fetching first."""
        if not self.state.merge_commits:
            self._fetch_and_refresh()

    def record_result(self, commit: str, value: float, cv: float, reps: int) -> None:
        """Record a completed benchmark result and persist state."""
        try:
            head: Optional[str] = get_head(self.repo_path, ref=self._sweep_ref)
        except Exception:
            head = None
        self.planner.record_result(commit, value, cv, reps, sample=self._sample_kind(commit, head))
        if head is not None and commit == head:
            self.state.last_benchmarked_head = commit
            self.state.last_benchmarked_head_at = time.time()
        self.state.save(self.state_file)
        logger.info("Sweep result recorded: %s -> %.0f (CV %.2f%%)", commit[:8], value, cv)

    def _sample_kind(self, commit: str, head: Optional[str]) -> str:
        """Why a commit was measured: a release landmark, the tip, or history."""
        if any(lm.commit == commit for lm in self.state.landmarks):
            return "release"
        if head is not None and commit == head:
            return "tip"
        return "history"

    def record_perf_counters(
        self,
        commit: str,
        counters: dict[str, int],
        duration: float,
        rps: float,
        counters_main: Optional[dict[str, int]] = None,
        counters_io: Optional[dict[str, int]] = None,
        rep_count: Optional[int] = None,
        scope: Optional[str] = None,
    ) -> None:
        """Record perf stat counters for a commit and persist state.

        ``counters`` is the process-wide total; ``counters_main`` / ``counters_io``
        are the optional per-thread (main vs IO) breakdowns. ``rep_count`` is the
        number of reps whose raw counters were summed (needed to normalize absolute
        per-request metrics, which would otherwise scale with the rep count).
        ``scope`` is the privilege scope perf counted under; ``None`` keeps the
        point's existing value so re-recording legacy data cannot erase it.
        """
        point = self.state.points.get(commit)
        if point is None:
            logger.warning("Cannot record perf counters: commit %s not in state", commit[:8])
            return
        point.perf_counters = counters
        point.perf_counters_main = counters_main
        point.perf_counters_io = counters_io
        point.perf_duration_seconds = duration
        point.perf_rps = rps
        point.perf_rep_count = rep_count
        if scope is not None:
            point.perf_counters_scope = scope
        self.state.save(self.state_file)
        logger.info("Perf counters recorded: %s (%d events)", commit[:8], len(counters))

    def record_build_failure(self, commit: str) -> None:
        """Record a build failure and persist state."""
        self.planner.record_build_failure(commit)
        self.state.save(self.state_file)
        logger.info("Sweep build failure: %s", commit[:8])

    # --- TaskSubscriber protocol ---

    def on_queue_empty(self) -> None:
        """Called when the task queue is empty."""
        self.queue_next_if_needed()

    def on_task_completed(self, task: BaseTaskData) -> None:
        """Called on every task completion. Filters to own tasks."""
        if not self._is_my_task(task):
            return
        from conductress.sweep.generations import is_excluded  # local: generations imports this module's subclasses

        if is_excluded(task.task_id):
            # A discarded task (see sweep/generations.py): its result must never
            # become a point, however it is replayed.
            logger.warning(
                "Ignoring excluded sweep task %s for commit %s", task.task_id, getattr(task, "sweep_commit", "?")[:8]
            )
            return
        result = self._extract_result(task)
        if result:
            value, cv, reps = result
            self.record_result(task.sweep_commit, value, cv, reps)  # type: ignore[attr-defined]
            point = self.state.points.get(task.sweep_commit)  # type: ignore[attr-defined]
            if point is not None:
                point.task_id = task.task_id
                self.state.save(self.state_file)
            # Extract perf counters if available
            perf = self._extract_perf_counters(task)  # pylint: disable=assignment-from-none
            if perf:
                self.record_perf_counters(
                    task.sweep_commit,  # type: ignore[attr-defined]
                    perf.counters,
                    perf.duration,
                    perf.rps,
                    perf.counters_main,
                    perf.counters_io,
                    perf.rep_count,
                    scope=perf.scope,
                )
            # Extract CPU profile stacks if available
            self._extract_cpu_stacks(task)
        else:
            commit = getattr(task, "sweep_commit", "?")
            logger.warning("Could not extract result for sweep commit %s", commit[:8])

    def on_task_failed(self, task: BaseTaskData) -> None:
        """Called on every task failure. Filters to own tasks."""
        if not self._is_my_task(task):
            return
        commit = getattr(task, "sweep_commit", "")
        self.record_build_failure(commit)

    def queue_next_if_needed(self) -> bool:
        """Queue the next sweep task if none is already pending.

        A retired series never queues: its history stays published, but the
        series that replaced it is the only one still measuring.
        """
        if self.retired:
            return False
        queue = TaskQueue()
        for queued in queue.get_all_tasks():
            if self._is_my_task(queued):
                return False

        sweep_task = self._get_next_task()
        if sweep_task is None:
            return False

        task = self._create_task(sweep_task)
        task.sweep_commit = sweep_task.commit  # type: ignore[attr-defined]
        queue.submit_task(task)
        logger.info(
            f"[sweep] Queued: {self._sweep_source}/{self.workload_id} {sweep_task.commit[:8]} - {sweep_task.reason}"
        )
        return True

    def get_urgency_score(self) -> float:
        """Return priority score based on expected information gain.

        Higher score = more information to be gained from running this sweeper next.
        Score is gap_magnitude_pct * log2(gap_width), so a new series with no data
        gets infinity (top priority), and a well-covered series with small deltas
        gets a low score.
        """
        import math

        # Brand new series with <2 points = top priority
        completed = sum(1 for p in self.state.points.values() if p.value is not None)
        if completed < 2:
            return float("inf")

        # Check what the planner would do next
        task = self.planner.get_next_task(current_head=None)
        if task is None:
            return 0.0

        # For bisection tasks, score based on the largest unresolved segment
        segments = self.planner.get_unresolved_segments()
        if segments:
            best = max(segments, key=lambda s: s.abs_delta * s.commit_count)
            return best.abs_delta * 100 * math.log2(max(best.commit_count, 2))

        # For backfill/landmark tasks, use gap width only (no magnitude known yet)
        # Lower priority than bisection but still useful
        return math.log2(max(len(self.state.merge_commits) // max(completed, 1), 2))

    # Whether this series' gap-filling backfill yields to every other series'
    # pending work.  A subclass sets it for a metric whose flat regions carry no
    # information anyone is waiting for (memory: deterministic, rarely moves).
    # Only BACKFILL planner tasks are deferred; nightly HEAD, release landmarks
    # and bisection compete on urgency like any other series.
    defer_backfill: bool = False

    def schedule_tier(self) -> int:
        """Scheduling tier for the runner's urgency sort: 0 normal, 1 deferred.

        The runner sorts candidates by tier first and urgency second, so a
        tier-1 series queues only when no tier-0 series has a task to queue.
        A series lands in tier 1 when it defers backfill and the planner's next
        task is BACKFILL.
        """
        if not self.defer_backfill:
            return 0
        task = self.planner.get_next_task(current_head=None)
        return 1 if task is not None and task.priority == TaskPriority.BACKFILL else 0

    def has_nightly_task(self) -> bool:
        """Check if this coordinator would produce a NIGHTLY task (HEAD untested).

        Also triggers a commit list refresh if HEAD is not in the list (stale state).
        """
        try:
            head = get_head(self.repo_path, ref=self._sweep_ref)
        except Exception:
            return False
        if head not in set(self.state.merge_commits):
            # Stale commit list — refresh it so we can check properly
            self._fetch_and_refresh()
        if head not in set(self.state.merge_commits):
            return False
        point = self.state.points.get(head)
        if point is not None and not point.is_complete:
            # A pending or failed tip is re-tried; tip_due would treat it as done.
            return head != self.state.last_benchmarked_head
        return self.planner.tip_due(head)

    # --- Abstract methods (subclass defines) ---

    # --- Abstract properties (subclass defines) ---

    @property
    @abstractmethod
    def metric_id(self) -> str:
        """Identifier for this metric (e.g. 'throughput', 'memory')."""
        ...

    @property
    @abstractmethod
    def workload_id(self) -> str:
        """Workload identifier for file naming (e.g. 'get16b-t7-p10')."""
        ...

    @property
    @abstractmethod
    def metric_unit(self) -> str:
        """Display unit (e.g. 'ops/sec', 'bytes/item')."""
        ...

    @property
    def epoch_id(self) -> str:
        """Measurement epoch used for export namespacing (legacy by default)."""
        return "v1"

    @property
    def epoch_ids(self) -> tuple[str, ...]:
        """Every epoch this series is published under.

        A series measured by a load generator belongs to exactly one epoch, the
        one that fixes that generator.  A generator-independent series (memory
        overhead) is valid in every epoch and overrides this to list them all;
        ``epoch_id`` stays the first entry, which is the epoch the series
        schedules and pauses under.
        """
        return (self.epoch_id,)

    @property
    def retired(self) -> bool:
        """True when a series in another epoch has replaced this one.

        A retired series keeps its state file and keeps exporting, so its
        history stays on the dashboard, but the scheduler never asks it for
        another task.  Only epoch-1 series can be retired; the registry of
        which ones is ``config.SWEEP_V1_RETIRED_SERIES``.
        """
        if self.epoch_id != "v1":
            return False
        return f"{self.metric_id}:{self.workload_id}" in config.SWEEP_V1_RETIRED_SERIES

    @property
    def lower_is_better(self) -> bool:
        """Whether lower values are better (e.g. memory overhead). Default: False (higher is better)."""
        return False

    # --- Export ---

    def export(self, output_path: Path, platform: str) -> int:
        """Export this coordinator's data to a series JSON file. Returns point count."""
        from conductress.sweep.exporter import export_series

        branch = self._sweep_ref.replace("origin/", "") if self.engine else "unstable"
        export_series(
            self.state,
            output_path,
            platform=platform,
            workload=self.workload_id,
            lower_is_better=self.lower_is_better,
            repo=config.engine_repo_slug(self.engine),
            branch=branch,
            engine=self.engine,
        )
        return sum(1 for p in self.state.points.values() if p.value is not None)

    # --- Abstract methods (subclass defines) ---

    @abstractmethod
    def _create_task(self, sweep_task: SweepTask) -> BaseTaskData:
        """Create a concrete task from a SweepTask."""
        ...

    @abstractmethod
    def _extract_result(self, task: BaseTaskData) -> Optional[tuple[float, float, int]]:
        """Extract (value, cv, reps) from a completed task. Returns None on failure."""
        ...

    def _extract_perf_counters(self, task: BaseTaskData) -> Optional[PerfCounterRecord]:
        """Extract perf counters from a completed task.

        Returns ``None`` if perf stat data is not available. Subclasses whose
        tasks can carry counters override to locate the result row and hand it
        to :func:`perf_counters_from_entry`.
        """
        return None

    def _extract_cpu_stacks(self, task: BaseTaskData) -> None:
        """Extract CPU profile stacks from task and store on point. Subclasses override."""

    def _store_cpu_stacks_from_entry(self, task: BaseTaskData, entry: dict) -> None:
        """Copy ``cpu_stacks_main`` / ``cpu_stacks_io`` from a result row onto the task's point."""
        data = entry.get("data", {})
        cpu_main = data.get("cpu_stacks_main")
        if not cpu_main:
            return
        commit = getattr(task, "sweep_commit", "")
        if commit and commit in self.state.points:
            self.state.points[commit].cpu_stacks_main = cpu_main
            self.state.points[commit].cpu_stacks_io = data.get("cpu_stacks_io", [])
            self.state.save(self.state_file)

    @abstractmethod
    def _is_my_task(self, task: BaseTaskData) -> bool:
        """Return True if this task belongs to this coordinator."""
        ...

    # --- Private helpers ---

    def _get_next_task(self) -> Optional[SweepTask]:
        """Get the next sweep task from the planner."""
        if time.time() - self._last_fetch_time >= SWEEP_FETCH_INTERVAL:
            self._fetch_and_refresh()
        try:
            current_head = get_head(self.repo_path, ref=self._sweep_ref)
        except Exception as e:
            logger.warning("Failed to get HEAD: %s", e)
            current_head = None
        return self.planner.get_next_task(current_head)

    def _fetch_and_refresh(self) -> None:
        """Fetch origin and refresh commits if HEAD moved, state is empty, or stale."""
        try:
            old_head = get_head(self.repo_path, ref=self._sweep_ref)
        except Exception:
            old_head = None
        try:
            fetch_ref(self.repo_path)
        except Exception as e:
            logger.warning("Sweep fetch failed: %s", e)
            if self.state.merge_commits:
                return  # Non-fatal if we already have commits
        self._last_fetch_time = time.time()
        try:
            new_head: Optional[str] = get_head(self.repo_path, ref=self._sweep_ref)
        except Exception:
            new_head = None
        head_not_in_list = new_head and new_head not in set(self.state.merge_commits)
        if new_head != old_head or not self.state.merge_commits or head_not_in_list:
            old_count = len(self.state.merge_commits)
            self._refresh_commits()
            logger.info(
                "Sweep commits updated: %d new (%d total)",
                len(self.state.merge_commits) - old_count,
                len(self.state.merge_commits),
            )

    def _refresh_commits(self) -> None:
        """Re-enumerate commits and landmarks from git history, persist and rebuild planner."""
        self._populate_commits()
        self._populate_landmarks()
        self.state.save(self.state_file)
        self.planner = self._new_planner()

    def _populate_commits(self) -> None:
        """Populate merge_commits from git history."""
        floor_tag = self._floor_tag
        if floor_tag:
            from conductress.sweep.git_ops import resolve_floor_commit

            floor_commit = resolve_floor_commit(self.repo_path, floor_tag, self._sweep_ref)
            if floor_commit is None:
                logger.warning(
                    "Floor tag %s does not resolve against %s in %s; sweeping the whole history",
                    floor_tag,
                    self._sweep_ref,
                    self.repo_path,
                )
        else:
            from conductress.sweep.git_ops import find_fork_point

            floor_commit = find_fork_point(self.repo_path)
        commits = get_merge_commits(self.repo_path, since_commit=floor_commit, ref=self._sweep_ref)
        self.state.merge_commits = [c.hash for c in commits]
        self.state.commit_dates = {c.hash: c.date for c in commits}
        self.state.commit_prs = {c.hash: c.pr for c in commits if c.pr is not None}
        self.state.commit_titles = {c.hash: c.pr_title for c in commits if c.pr_title is not None}

    def _populate_landmarks(self) -> None:
        """Populate landmarks from release branch points on unstable.

        The list is rebuilt from scratch on every call: it used to be appended
        to, so each HEAD change added another copy of every landmark to the
        state file and the export.  A release-and-tip engine keeps only its
        latest release, the one point the engine comparison reads.
        """
        self.state.landmarks = []
        PRE_FORK_LANDMARKS = [
            Landmark(
                commit="3431b1f156b05866e4f9a368304216974f047c43",
                date="2023-11-29",
                label="First benchmarkable",
            ),
            Landmark(
                commit="f7b1d0287d62ec9fac72bf14cf789e350d14e52b",
                date="2024-01-09",
                label="7.2.4",
            ),
        ]
        try:
            points = get_release_branch_points(self.repo_path)
            commit_set = set(self.state.merge_commits)
            if not self.engine or self.engine.source == "valkey":
                for lm in PRE_FORK_LANDMARKS:
                    if lm.commit in commit_set:
                        self.state.landmarks.append(lm)
            for commit_hash, date, label in points:
                if commit_hash in commit_set:
                    self.state.landmarks.append(Landmark(commit=commit_hash, date=date, label=label))
        except Exception as e:
            logger.warning("Failed to enumerate release branch points: %s", e)
        if not config.engine_tracks_history(self.engine):
            self.state.landmarks = latest_release_landmark(self.state.landmarks, self.state.merge_commits)


# =============================================================================
# Concrete implementation: throughput sweep
# =============================================================================


class SweepCoordinator(BaseSweepCoordinator):
    """Throughput sweep coordinator (GET/SET, configurable value size, io-threads, pipelining)."""

    metric_id = "throughput"
    metric_unit = "ops/sec"

    def __init__(
        self,
        repo_path: Path,
        val_size: int = SWEEP_VAL_SIZE,
        test: str = SWEEP_TEST,
        io_threads: int = SWEEP_IO_THREADS,
        pipelining: int = SWEEP_PIPELINING,
        engine: Optional[config.SweepEngine] = None,
    ):
        self._val_size = val_size
        self._test = test
        self._io_threads = io_threads
        self._pipelining = pipelining
        self._label = config.sweep_throughput_label(test, val_size, io_threads, pipelining, engine)
        state_file = SWEEP_STATE_DIR / f"state_{self._label}.json"
        super().__init__(repo_path, state_file, engine=engine)

    @property
    def workload_id(self) -> str:  # type: ignore[override]
        return self._label

    def get_next_sweep_task(self) -> Optional[PerfTaskData]:
        """Legacy interface: get next task directly."""
        sweep_task = self._get_next_task()
        if sweep_task is None:
            return None
        logger.info("Sweep task: %s (%s)", sweep_task.commit[:8], sweep_task.reason)
        return self._create_task(sweep_task)

    def _create_task(self, sweep_task: SweepTask) -> PerfTaskData:
        return PerfTaskData(
            source=self._sweep_source,
            specifier=sweep_task.commit,
            make_args=self._sweep_make_args,
            topology=TopologySpec.standalone(),
            note=f"[perf-sweep:{self._sweep_source}/{self.workload_id}] {sweep_task.reason}",
            requirements={},
            test=self._test,
            val_size=self._val_size,
            io_threads=self._io_threads,
            pipelining=self._pipelining,
            warmup=SWEEP_WARMUP,
            duration=SWEEP_DURATION,
            perf_stat_enabled=True,
            has_expire=False,
            preload_keys=True,
            key_size=0,
            repetitions=SWEEP_REPETITIONS,
            max_reps=SWEEP_MAX_REPS,
            target_cv=SWEEP_TARGET_CV,
        )

    def _find_task_entry(self, task: BaseTaskData) -> Optional[dict]:
        """Find the output.jsonl entry for a completed task. Returns parsed dict or None."""

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
        rps = entry.get("score")
        per_run = entry.get("data", {}).get("per_run_rps", [])
        cv = (stdev(per_run) / rps) * 100 if len(per_run) >= 2 and rps else 0.0
        reps = len(per_run) if per_run else 3
        return (rps, cv, reps) if rps else None

    def _extract_perf_counters(self, task: BaseTaskData) -> Optional[PerfCounterRecord]:
        """Extract perf stat counters (process-wide + per-thread) from task output."""
        entry = self._find_task_entry(task)
        return perf_counters_from_entry(entry) if entry else None

    def _extract_cpu_stacks(self, task: BaseTaskData) -> None:
        """Extract CPU profile stacks from task output and store on the point."""
        entry = self._find_task_entry(task)
        if entry:
            self._store_cpu_stacks_from_entry(task, entry)

    def _is_my_task(self, task: BaseTaskData) -> bool:
        return (
            isinstance(task, PerfTaskData)
            and bool(task.sweep_commit)
            and task.source == self._sweep_source
            and task.val_size == self._val_size
            and task.test == self._test
            and task.io_threads == self._io_threads
            and task.pipelining == self._pipelining
            and getattr(task, "generator_profile", "") in ("", "legacy-v1")
        )
