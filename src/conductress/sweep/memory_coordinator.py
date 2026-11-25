"""Memory sweep coordinator: tracks per-item memory overhead across engine history.

Supports multiple workloads (set, zadd, sadd, set+expire) via MemoryWorkload config.
Each workload gets its own state file and sweep planner instance.
Engine-aware: works with any engine (Valkey, Redis) via SweepEngine config.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from conductress.config import (
    CONDUCTRESS_RESULTS,
    MEMORY_STATE_DIR,
    PROJECT_ROOT,
    SWEEP_GENERATOR_INDEPENDENT_EPOCHS,
    SweepEngine,
)
from conductress.heap_profiler import with_jemalloc_prof
from conductress.sweep.coordinator import SWEEP_SOURCE, BaseSweepCoordinator
from conductress.sweep.planner import SweepTask
from conductress.task_queue import BaseTaskData
from conductress.tasks.task_mem_efficiency import MemTaskData
from conductress.topology import TopologySpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MemoryWorkload:
    """Configuration for a single memory sweep workload."""

    command: str  # valkey command: "set", "zadd", "sadd", "hset"
    key_size: int  # key size in bytes (SET) or 0 (single-key commands)
    value_size: int  # value/member size in bytes
    field_size: int = 0  # field name size (HSET only)
    has_expire: bool = False  # whether to apply EXPIRE after populating
    label: str = ""  # unique identifier used in state filename and dashboard
    item_count: int = 5_000_000
    user_data_bytes: int = 0  # per-item user data for dashboard baseline
    populate_mode: str = "random"  # zadd insertion pattern: random | sequential | churn

    @property
    def state_file(self) -> Path:
        return MEMORY_STATE_DIR / f"memory_state_{self.label}.json"

    def state_file_for_engine(self, engine: Optional[SweepEngine] = None) -> Path:
        prefix = f"{engine.source}-" if engine and engine.source != "valkey" else ""
        return MEMORY_STATE_DIR / f"memory_state_{prefix}{self.label}.json"


# All memory workloads to sweep. Add new entries here to extend coverage.
#
# The 16-byte string and hash shapes sit alongside the 64-byte ones because the
# per-item structures behave differently at small element sizes: a small value
# embeds in the key's robj (and moves with robj layout changes), a small
# field+value pair fits in one hash entry, and a TTL is cheapest to see against
# the smallest string.  Set and sorted-set members stay at 20 bytes: the
# populator's member naming needs at least 10, and m20 is the established series.
MEMORY_WORKLOADS: list[MemoryWorkload] = [
    MemoryWorkload(command="set", key_size=16, value_size=64, label="set-k16-v64", user_data_bytes=80),
    MemoryWorkload(
        command="set", key_size=16, value_size=64, has_expire=True, label="set-k16-v64-expire", user_data_bytes=80
    ),
    MemoryWorkload(command="set", key_size=16, value_size=16, label="set-k16-v16", user_data_bytes=32),
    MemoryWorkload(
        command="set", key_size=16, value_size=16, has_expire=True, label="set-k16-v16-expire", user_data_bytes=32
    ),
    MemoryWorkload(command="zadd", key_size=0, value_size=20, label="zadd-m20", user_data_bytes=28),
    MemoryWorkload(command="sadd", key_size=0, value_size=20, label="sadd-m20", user_data_bytes=20),
    MemoryWorkload(command="hset", key_size=0, value_size=64, field_size=64, label="hset-f64-v64", user_data_bytes=128),
    MemoryWorkload(command="hset", key_size=0, value_size=16, field_size=16, label="hset-f16-v16", user_data_bytes=32),
]


class MemorySweepCoordinator(BaseSweepCoordinator):
    """Memory efficiency sweep: tracks bytes/item overhead across history.

    Each instance handles one workload (e.g., set-64b, zadd-64b).
    Multiple instances coexist as subscribers on the same TaskRunner.
    """

    metric_unit = "bytes/item"
    lower_is_better = True
    # Memory is deterministic and its per-item cost moves at a handful of
    # commits per year, so once the landmarks and change-points are in, filling
    # the flat regions between them adds no information anyone is waiting for.
    # Gap-filling backfill therefore yields to every performance series' pending
    # work; nightly HEAD, release landmarks and bisection still compete normally.
    defer_backfill = True

    def __init__(self, repo_path: Path, workload: MemoryWorkload, engine: Optional[SweepEngine] = None):
        self._workload = workload
        state_file = workload.state_file_for_engine(engine)
        super().__init__(repo_path, state_file, engine=engine)

    @property
    def metric_id(self) -> str:  # type: ignore[override]
        return "memory"

    @property
    def epoch_id(self) -> str:
        """The epoch this series schedules under: the first of ``epoch_ids``."""
        return SWEEP_GENERATOR_INDEPENDENT_EPOCHS[0]

    @property
    def epoch_ids(self) -> tuple[str, ...]:
        """Memory overhead needs no load generator, so one series serves every epoch."""
        return SWEEP_GENERATOR_INDEPENDENT_EPOCHS

    @property
    def workload_id(self) -> str:  # type: ignore[override]
        prefix = f"{self.engine.source}-" if self.engine and self.engine.source != "valkey" else ""
        return f"memory-{prefix}{self._workload.label}"

    def export(self, output_path: Path, platform: str) -> int:
        """Override to pass num_keys for export-time re-categorization."""
        from conductress.config import engine_repo_slug, should_profile_internals
        from conductress.sweep.exporter import export_series

        export_series(
            self.state,
            output_path,
            platform=platform,
            workload=self.workload_id,
            lower_is_better=self.lower_is_better,
            num_keys=self._workload.item_count,
            # Opted-out engines (Redis) keep total memory but not the jemalloc breakdown,
            # which exposes the binary's allocation-site symbols. Also stops pre-existing
            # breakdowns in state from being re-published.
            include_breakdown=should_profile_internals(self.engine),
            repo=engine_repo_slug(self.engine),
            engine=self.engine,
            settled=True,
            user_data_bytes=self._workload.user_data_bytes,
            item_count=self._workload.item_count,
            profiled=False,
        )
        return sum(1 for p in self.state.points.values() if p.value is not None)

    def _create_task(self, sweep_task: SweepTask) -> MemTaskData:
        """The measuring task: unprofiled, so used_memory and RSS are both valid."""
        return self._make_task(sweep_task.commit, sweep_task.reason, breakdown=False)

    def _make_task(self, commit: str, reason: str, *, breakdown: bool) -> MemTaskData:
        """A memory task for one commit. ``breakdown`` runs it under jemalloc heap profiling."""
        make_args = with_jemalloc_prof(self._sweep_make_args) if breakdown else self._sweep_make_args
        return MemTaskData(
            source=self._sweep_source,
            specifier=commit,
            make_args=make_args,
            topology=TopologySpec.standalone(),
            note=f"[memory-sweep:{self._workload.label}] {reason}",
            requirements={},
            type=self._workload.command,
            val_sizes=[self._workload.value_size],
            has_expire=self._workload.has_expire,
            # Profiling inflates RSS about 100x (profiler metadata), so the measuring run never
            # profiles. The per-structure breakdown comes from a separate task (see
            # _queue_breakdown_if_due) that only the engines that publish it get.
            enable_profiling=breakdown,
            key_size=self._workload.key_size,
            field_size=self._workload.field_size,
            user_data_bytes=self._workload.user_data_bytes,
            # The series measures the structure's settled size: a sequentially
            # loaded collection is otherwise caught mid-rehash with two tables
            # live, and the extra table would be recorded as the data type's cost.
            settle=True,
        )

    def _is_my_task(self, task: BaseTaskData) -> bool:
        """Match only tasks created by THIS workload coordinator.

        Every field that defines the series' shape must match, and the task
        must have been measured settled: a loaded (unsettled) cell measures a
        different quantity and may not land on the settled line.
        """
        if not isinstance(task, MemTaskData) or not task.sweep_commit:
            return False
        return (
            task.source == self._sweep_source
            and task.type == self._workload.command
            and task.has_expire == self._workload.has_expire
            and task.val_sizes == [self._workload.value_size]
            and task.key_size == self._workload.key_size
            and task.field_size == self._workload.field_size
            and bool(task.settle)
            # A manual task with another item count measures another point, not this series.
            and task.item_count == self._workload.item_count
        )

    def _extract_result(self, task: BaseTaskData) -> Optional[tuple[float, float, int]]:
        """Extract bytes_per_item from output. CV=0 (memory is deterministic)."""

        output_file = CONDUCTRESS_RESULTS / "output.jsonl"
        if not output_file.exists():
            return None

        for line in reversed(output_file.read_text().strip().splitlines()):
            try:
                entry = json.loads(line)
                if entry.get("task_id") == task.task_id:
                    score = entry.get("score")
                    if score and score > 0:
                        return (score, 0.0, 1)
            except (ValueError, KeyError, TypeError):
                continue
        return None

    def _extract_memory_record(self, task: BaseTaskData) -> Optional[dict]:
        """Extract the task's memory record (identity, settle, used_memory and RSS samples)."""

        output_file = CONDUCTRESS_RESULTS / "output.jsonl"
        if not output_file.exists():
            return None

        for line in reversed(output_file.read_text().strip().splitlines()):
            try:
                entry = json.loads(line)
                if entry.get("task_id") == task.task_id:
                    results = entry.get("data", {}).get("results", [])
                    record = results[0].get("memory_record") if results else None
                    return record if isinstance(record, dict) else None
            except (ValueError, KeyError, TypeError):
                continue
        return None

    def _extract_breakdown(self, task: BaseTaskData) -> Optional[dict[str, float]]:
        """Extract per-category breakdown from the task output."""

        output_file = CONDUCTRESS_RESULTS / "output.jsonl"
        if not output_file.exists():
            return None

        for line in reversed(output_file.read_text().strip().splitlines()):
            try:
                entry = json.loads(line)
                if entry.get("task_id") == task.task_id:
                    data = entry.get("data", {})
                    results = data.get("results", [])
                    if results and results[0].get("breakdown"):
                        return results[0]["breakdown"]
            except (ValueError, KeyError, TypeError):
                continue
        return None

    def _extract_raw_stacks(self, task: BaseTaskData) -> Optional[list[list]]:
        """Extract retained resolved stacks from the task output."""

        output_file = CONDUCTRESS_RESULTS / "output.jsonl"
        if not output_file.exists():
            return None

        for line in reversed(output_file.read_text().strip().splitlines()):
            try:
                entry = json.loads(line)
                if entry.get("task_id") == task.task_id:
                    data = entry.get("data", {})
                    results = data.get("results", [])
                    if results and results[0].get("raw_stacks"):
                        return results[0]["raw_stacks"]
            except (ValueError, KeyError, TypeError):
                continue
        return None

    def on_task_completed(self, task: BaseTaskData) -> None:
        """Record the measuring task's point; a breakdown task only adds the breakdown to its point."""
        if not self._is_my_task(task):
            return
        if task.enable_profiling:  # type: ignore[attr-defined]
            self._attach_breakdown(task)
            self.queue_next_if_needed()
            return

        result = self._extract_result(task)
        if result:
            value, cv, reps = result
            self.record_result(task.sweep_commit, value, cv, reps)  # type: ignore[attr-defined]
            self._annotate_point(task)

            record = self._extract_memory_record(task)
            point = self.state.points.get(task.sweep_commit)  # type: ignore[attr-defined]
            if point is not None and record is not None:
                point.memory_record = record
                self.state.save(self.state_file)
            self._queue_breakdown_if_due(task.sweep_commit)  # type: ignore[attr-defined]
        else:
            commit = getattr(task, "sweep_commit", "?")
            logger.warning("Could not extract result for %s [%s]", commit[:8], self._workload.label)
        self.queue_next_if_needed()

    def on_task_failed(self, task: BaseTaskData) -> None:
        """A failed breakdown task must not mark the commit's measured point as failed."""
        if self._is_my_task(task) and task.enable_profiling:  # type: ignore[attr-defined]
            logger.warning("Breakdown task failed for %s [%s]", task.sweep_commit[:8], self._workload.label)  # type: ignore[attr-defined]
            return
        super().on_task_failed(task)

    def _attach_breakdown(self, task: BaseTaskData) -> None:
        """Store the breakdown and raw stacks of a breakdown task on the commit's measured point."""
        commit = task.sweep_commit  # type: ignore[attr-defined]
        point = self.state.points.get(commit)
        if point is None:
            logger.warning("Breakdown for %s [%s] has no measured point; dropped", commit[:8], self._workload.label)
            return
        breakdown = self._extract_breakdown(task)
        raw_stacks = self._extract_raw_stacks(task)
        if breakdown:
            point.breakdown = breakdown
        if raw_stacks:
            point.raw_stacks = raw_stacks
        if breakdown or raw_stacks:
            self.state.save(self.state_file)
            logger.info("Recorded breakdown for %s [%s]", commit[:8], self._workload.label)

    def _breakdown_due(self, commit: str) -> bool:
        """True when ``commit`` is a tip point of an engine that publishes a breakdown, and no
        point from the same commit date or later has one. That is at most one breakdown task per
        calendar day of tip commit dates for each series."""
        from conductress.config import should_profile_internals

        point = self.state.points.get(commit)
        if point is None or point.sample != "tip" or not should_profile_internals(self.engine):
            return False
        newest = max((p.date for p in self.state.points.values() if p.breakdown and p.date), default="")
        return point.date > newest

    def _queue_breakdown_if_due(self, commit: str) -> None:
        if not self._breakdown_due(commit):
            return
        from conductress.task_queue import TaskQueue

        task = self._make_task(commit, "heap breakdown for tip point", breakdown=True)
        task.sweep_commit = commit
        TaskQueue().submit_task(task)
        logger.info("[sweep] Queued breakdown task: %s %s", self.workload_id, commit[:8])


def create_memory_coordinators(repo_path: Path, engine: Optional[SweepEngine] = None) -> list[MemorySweepCoordinator]:
    """Factory: create one coordinator per configured workload."""
    return [MemorySweepCoordinator(repo_path, wl, engine=engine) for wl in MEMORY_WORKLOADS]
