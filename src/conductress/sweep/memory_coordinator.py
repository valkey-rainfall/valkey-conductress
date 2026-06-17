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

from conductress.config import CONDUCTRESS_RESULTS, MEMORY_STATE_DIR, PROJECT_ROOT, SweepEngine
from conductress.heap_profiler import JEMALLOC_PROF_CONFIGURE_OPTS
from conductress.sweep.coordinator import SWEEP_SOURCE, BaseSweepCoordinator
from conductress.sweep.planner import SweepTask
from conductress.task_queue import BaseTaskData
from conductress.tasks.task_mem_efficiency import MemTaskData

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

    @property
    def state_file(self) -> Path:
        return MEMORY_STATE_DIR / f"memory_state_{self.label}.json"

    def state_file_for_engine(self, engine: Optional[SweepEngine] = None) -> Path:
        prefix = f"{engine.source}-" if engine and engine.source != "valkey" else ""
        return MEMORY_STATE_DIR / f"memory_state_{prefix}{self.label}.json"


# All memory workloads to sweep. Add new entries here to extend coverage.
MEMORY_WORKLOADS: list[MemoryWorkload] = [
    MemoryWorkload(command="set", key_size=16, value_size=64, label="set-k16-v64", user_data_bytes=80),
    MemoryWorkload(
        command="set", key_size=16, value_size=64, has_expire=True, label="set-k16-v64-expire", user_data_bytes=80
    ),
    MemoryWorkload(command="zadd", key_size=0, value_size=20, label="zadd-m20", user_data_bytes=28),
    MemoryWorkload(command="sadd", key_size=0, value_size=20, label="sadd-m20", user_data_bytes=20),
    MemoryWorkload(command="hset", key_size=0, value_size=64, field_size=64, label="hset-f64-v64", user_data_bytes=128),
]


class MemorySweepCoordinator(BaseSweepCoordinator):
    """Memory efficiency sweep: tracks bytes/item overhead across history.

    Each instance handles one workload (e.g., set-64b, zadd-64b).
    Multiple instances coexist as subscribers on the same TaskRunner.
    """

    metric_unit = "bytes/item"
    lower_is_better = True

    def __init__(self, repo_path: Path, workload: MemoryWorkload, engine: Optional[SweepEngine] = None):
        self._workload = workload
        state_file = workload.state_file_for_engine(engine)
        super().__init__(repo_path, state_file, engine=engine)

    @property
    def metric_id(self) -> str:  # type: ignore[override]
        return "memory"

    @property
    def workload_id(self) -> str:  # type: ignore[override]
        prefix = f"{self.engine.source}-" if self.engine and self.engine.source != "valkey" else ""
        return f"memory-{prefix}{self._workload.label}"

    def get_urgency_score(self) -> float:
        """Memory urgency with flatness discount.

        Memory overhead is deterministic — once change-points are found,
        backfilling flat plateaus adds no information. Discount urgency
        when most adjacent-point pairs show <2% delta.
        """
        import math

        base = super().get_urgency_score()
        if base in (0.0, float("inf")):
            return base

        # Need enough points to assess flatness
        completed = sum(1 for p in self.state.points.values() if p.value is not None)
        if completed < 5:
            return base

        # Compute flatness from all adjacent completed points
        ordered = sorted(
            (
                (self.state.merge_commits.index(c), p.value)
                for c, p in self.state.points.items()
                if p.value is not None and c in set(self.state.merge_commits)
            ),
            key=lambda x: x[0],
        )
        if len(ordered) < 2:
            return base

        flat_threshold = 0.01
        flat_count = 0
        total_pairs = len(ordered) - 1
        for i in range(total_pairs):
            left_val = ordered[i][1]
            right_val = ordered[i + 1][1]
            if left_val == 0:
                continue
            delta = abs(right_val - left_val) / left_val
            if delta < flat_threshold:
                flat_count += 1

        flatness = flat_count / total_pairs if total_pairs > 0 else 0

        # Discount: if 80%+ of pairs are flat, reduce urgency by 5x
        if flatness > 0.8:
            discount = 0.2
        elif flatness > 0.5:
            discount = 0.5
        else:
            discount = 1.0

        return base * discount

    def export(self, output_path: Path, platform: str) -> int:
        """Override to pass num_keys for export-time re-categorization."""
        from conductress.sweep.exporter import export_series

        export_series(
            self.state,
            output_path,
            platform=platform,
            workload=self.workload_id,
            lower_is_better=self.lower_is_better,
            num_keys=self._workload.item_count,
        )
        return sum(1 for p in self.state.points.values() if p.value is not None)

    def _create_task(self, sweep_task: SweepTask) -> MemTaskData:
        # Combine engine make_args with jemalloc profiling flag
        engine_args = self._sweep_make_args
        make_args = f"{engine_args} {JEMALLOC_PROF_CONFIGURE_OPTS}" if engine_args else JEMALLOC_PROF_CONFIGURE_OPTS
        return MemTaskData(
            source=self._sweep_source,
            specifier=sweep_task.commit,
            make_args=make_args,
            replicas=0,
            note=f"[memory-sweep:{self._workload.label}] {sweep_task.reason}",
            requirements={},
            type=self._workload.command,
            val_sizes=[self._workload.value_size],
            has_expire=self._workload.has_expire,
            enable_profiling=True,
            key_size=self._workload.key_size,
            field_size=self._workload.field_size,
            user_data_bytes=self._workload.user_data_bytes,
        )

    def _is_my_task(self, task: BaseTaskData) -> bool:
        """Match only tasks created by THIS workload coordinator."""
        if not isinstance(task, MemTaskData) or not task.sweep_commit:
            return False
        return (
            task.source == self._sweep_source
            and task.type == self._workload.command
            and task.has_expire == self._workload.has_expire
            and task.val_sizes == [self._workload.value_size]
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
        """Override to attach breakdown and raw stacks to the recorded point."""
        if not self._is_my_task(task):
            return

        result = self._extract_result(task)
        if result:
            value, cv, reps = result
            self.record_result(task.sweep_commit, value, cv, reps)  # type: ignore[attr-defined]

            breakdown = self._extract_breakdown(task)
            raw_stacks = self._extract_raw_stacks(task)
            if task.sweep_commit in self.state.points:  # type: ignore[attr-defined]
                if breakdown:
                    self.state.points[task.sweep_commit].breakdown = breakdown  # type: ignore[attr-defined]
                if raw_stacks:
                    self.state.points[task.sweep_commit].raw_stacks = raw_stacks  # type: ignore[attr-defined]
                self.state.save(self.state_file)
                logger.info("Recorded breakdown for %s [%s]", task.sweep_commit[:8], self._workload.label)  # type: ignore[attr-defined]
        else:
            commit = getattr(task, "sweep_commit", "?")
            logger.warning("Could not extract result for %s [%s]", commit[:8], self._workload.label)
        self.queue_next_if_needed()


def create_memory_coordinators(repo_path: Path, engine: Optional[SweepEngine] = None) -> list[MemorySweepCoordinator]:
    """Factory: create one coordinator per configured workload."""
    return [MemorySweepCoordinator(repo_path, wl, engine=engine) for wl in MEMORY_WORKLOADS]
