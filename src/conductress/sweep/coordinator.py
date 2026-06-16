"""Sweep coordinators: bridge the sweep planner with the Conductress task runner.

BaseSweepCoordinator handles git history, state management, queue interaction,
and the pub/sub protocol. Subclasses define task creation and result extraction
for specific metrics (throughput, memory, etc.).
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from statistics import stdev
from typing import Optional

from conductress.config import (
    CONDUCTRESS_RESULTS,
    PROJECT_ROOT,
    SWEEP_DURATION,
    SWEEP_FETCH_INTERVAL,
    SWEEP_IO_THREADS,
    SWEEP_KEY_SIZE,
    SWEEP_MAKE_ARGS,
    SWEEP_MAX_REPS,
    SWEEP_PIPELINING,
    SWEEP_REF,
    SWEEP_REPETITIONS,
    SWEEP_SOURCE,
    SWEEP_STATE_DIR,
    SWEEP_STATE_FILE,
    SWEEP_TARGET_CV,
    SWEEP_TEST,
    SWEEP_VAL_SIZE,
    SWEEP_WARMUP,
)
from conductress.sweep.git_ops import fetch_ref, get_head, get_merge_commits, get_release_branch_points
from conductress.sweep.planner import Landmark, SweepPlanner, SweepState, SweepTask
from conductress.task_queue import BaseTaskData, TaskQueue
from conductress.tasks.task_perf_benchmark import PerfTaskData

logger = logging.getLogger(__name__)

# Git configuration (shared across all sweep types)


class BaseSweepCoordinator(ABC):
    """Abstract base for sweep coordinators.

    Handles: git history enumeration, state persistence, queue management,
    TaskSubscriber protocol (on_task_completed, on_task_failed, on_queue_empty).

    Subclasses define: task creation, result extraction, task filtering.
    """

    def __init__(self, repo_path: Path, state_file: Path):
        self.repo_path = repo_path
        self.state_file = state_file
        self.state = SweepState.load(state_file)
        self.planner = SweepPlanner(self.state)
        self._last_fetch_time: float = 0.0

    def initialize(self) -> None:
        """Initialize the merge commit list from git history, fetching first."""
        if not self.state.merge_commits:
            self._fetch_and_refresh()

    def record_result(self, commit: str, value: float, cv: float, reps: int) -> None:
        """Record a completed benchmark result and persist state."""
        self.planner.record_result(commit, value, cv, reps)
        try:
            head = get_head(self.repo_path, ref=SWEEP_REF)
            if commit == head:
                self.state.last_benchmarked_head = commit
        except Exception:
            pass
        self.state.save(self.state_file)
        logger.info("Sweep result recorded: %s -> %.0f (CV %.2f%%)", commit[:8], value, cv)

    def record_perf_counters(self, commit: str, counters: dict[str, int], duration: float, rps: float) -> None:
        """Record perf stat counters for a commit and persist state."""
        point = self.state.points.get(commit)
        if point is None:
            logger.warning("Cannot record perf counters: commit %s not in state", commit[:8])
            return
        point.perf_counters = counters
        point.perf_duration_seconds = duration
        point.perf_rps = rps
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
        result = self._extract_result(task)
        if result:
            value, cv, reps = result
            self.record_result(task.sweep_commit, value, cv, reps)  # type: ignore[attr-defined]
            # Extract perf counters if available
            perf_data = self._extract_perf_counters(task)  # pylint: disable=assignment-from-none
            if perf_data:
                counters, duration, rps = perf_data
                self.record_perf_counters(task.sweep_commit, counters, duration, rps)  # type: ignore[attr-defined]
        else:
            commit = getattr(task, "sweep_commit", "?")
            logger.warning("Could not extract result for sweep commit %s", commit[:8])
        self.queue_next_if_needed()

    def on_task_failed(self, task: BaseTaskData) -> None:
        """Called on every task failure. Filters to own tasks."""
        if not self._is_my_task(task):
            return
        commit = getattr(task, "sweep_commit", "")
        self.record_build_failure(commit)
        self.queue_next_if_needed()

    def queue_next_if_needed(self) -> bool:
        """Queue the next sweep task if none is already pending."""
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
        logger.info(f"[sweep] Queued: {sweep_task.commit[:8]} - {sweep_task.reason}")
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

    def has_nightly_task(self) -> bool:
        """Check if this coordinator would produce a NIGHTLY task (HEAD untested).

        Also triggers a commit list refresh if HEAD is not in the list (stale state).
        """
        try:
            head = get_head(self.repo_path, ref=SWEEP_REF)
        except Exception:
            return False
        if head not in set(self.state.merge_commits):
            # Stale commit list — refresh it so we can check properly
            self._fetch_and_refresh()
        return (
            head != self.state.last_benchmarked_head
            and head not in self.state.points
            and head in set(self.state.merge_commits)
        )

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
    def lower_is_better(self) -> bool:
        """Whether lower values are better (e.g. memory overhead). Default: False (higher is better)."""
        return False

    # --- Export ---

    def export(self, output_path: Path, platform: str) -> int:
        """Export this coordinator's data to a series JSON file. Returns point count."""
        from conductress.sweep.exporter import export_series

        export_series(
            self.state, output_path, platform=platform, workload=self.workload_id, lower_is_better=self.lower_is_better
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

    def _extract_perf_counters(self, task: BaseTaskData) -> Optional[tuple[dict[str, int], float, float]]:
        """Extract (counters, duration_seconds, rps) from a completed task.

        Returns None if perf stat data is not available. Subclasses override
        to provide extraction logic.
        """
        return None

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
            current_head = get_head(self.repo_path, ref=SWEEP_REF)
        except Exception as e:
            logger.warning("Failed to get HEAD: %s", e)
            current_head = None
        return self.planner.get_next_task(current_head)

    def _fetch_and_refresh(self) -> None:
        """Fetch origin and refresh commits if HEAD moved, state is empty, or stale."""
        try:
            old_head = get_head(self.repo_path, ref=SWEEP_REF)
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
            new_head: Optional[str] = get_head(self.repo_path, ref=SWEEP_REF)
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
        self.planner = SweepPlanner(self.state)

    def _populate_commits(self) -> None:
        """Populate merge_commits from git history (Valkey-era only)."""
        from conductress.sweep.git_ops import find_fork_point

        fork_point = find_fork_point(self.repo_path)
        commits = get_merge_commits(self.repo_path, since_commit=fork_point, ref=SWEEP_REF)
        self.state.merge_commits = [c.hash for c in commits]
        self.state.commit_dates = {c.hash: c.date for c in commits}
        self.state.commit_prs = {c.hash: c.pr for c in commits if c.pr is not None}
        self.state.commit_titles = {c.hash: c.pr_title for c in commits if c.pr_title is not None}

    def _populate_landmarks(self) -> None:
        """Populate landmarks from release branch points on unstable."""
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
            for lm in PRE_FORK_LANDMARKS:
                if lm.commit in commit_set:
                    self.state.landmarks.append(lm)
            for commit_hash, date, label in points:
                if commit_hash in commit_set:
                    self.state.landmarks.append(Landmark(commit=commit_hash, date=date, label=label))
        except Exception as e:
            logger.warning("Failed to enumerate release branch points: %s", e)


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
        label: Optional[str] = None,
        test: str = SWEEP_TEST,
        io_threads: int = SWEEP_IO_THREADS,
        pipelining: int = SWEEP_PIPELINING,
    ):
        self._val_size = val_size
        self._test = test
        self._io_threads = io_threads
        self._pipelining = pipelining
        self._label = label or f"get-k{SWEEP_KEY_SIZE}-v{val_size}"
        state_file = SWEEP_STATE_DIR / f"state_{self._label}.json" if label else SWEEP_STATE_FILE
        super().__init__(repo_path, state_file)

    @property
    def workload_id(self) -> str:  # type: ignore[override]
        suffix = f"-t{self._io_threads}-p{self._pipelining}"
        if self._label.endswith(suffix):
            return self._label
        return f"{self._label}{suffix}"

    def get_next_sweep_task(self) -> Optional[PerfTaskData]:
        """Legacy interface: get next task directly."""
        sweep_task = self._get_next_task()
        if sweep_task is None:
            return None
        logger.info("Sweep task: %s (%s)", sweep_task.commit[:8], sweep_task.reason)
        return self._create_task(sweep_task)

    def _create_task(self, sweep_task: SweepTask) -> PerfTaskData:
        return PerfTaskData(
            source=SWEEP_SOURCE,
            specifier=sweep_task.commit,
            make_args=SWEEP_MAKE_ARGS,
            replicas=0,
            note=f"[sweep] {sweep_task.reason}",
            requirements={},
            test=self._test,
            val_size=self._val_size,
            io_threads=self._io_threads,
            pipelining=self._pipelining,
            warmup=SWEEP_WARMUP,
            duration=SWEEP_DURATION,
            profiling_sample_rate=0,
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

    def _extract_perf_counters(self, task: BaseTaskData) -> Optional[tuple[dict[str, int], float, float]]:
        """Extract perf stat counters from the task's output."""
        entry = self._find_task_entry(task)
        if not entry:
            return None
        data = entry.get("data", {})
        counters = data.get("perf_counters")
        if not counters:
            return None
        duration = data.get("perf_duration_seconds", 0.0)
        rps = entry.get("score", 0.0)
        return (counters, duration, rps)

    def _is_my_task(self, task: BaseTaskData) -> bool:
        return (
            isinstance(task, PerfTaskData)
            and bool(task.sweep_commit)
            and task.val_size == self._val_size
            and task.test == self._test
            and task.io_threads == self._io_threads
            and task.pipelining == self._pipelining
        )
