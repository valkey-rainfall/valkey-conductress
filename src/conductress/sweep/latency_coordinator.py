"""Retired epoch-1 latency series: keeps its history published, never schedules.

This series measured GET p99 at a flat request rate with memtier_benchmark.
The epoch-3 latency series replaced it (see ``config.SWEEP_V1_RETIRED_SERIES``),
so the task type that produced its points no longer exists.  The coordinator
remains because the publisher exports every series through its coordinator:
without it the dashboard would lose the recorded history.  The base class
never asks a retired series for a task, so the scheduling hooks below are
unreachable and say so.
"""

import logging
from pathlib import Path
from typing import Optional

from conductress.config import LATENCY_STATE_FILE, LATENCY_TARGET_RPS
from conductress.sweep.coordinator import BaseSweepCoordinator
from conductress.sweep.planner import SweepTask
from conductress.task_queue import BaseTaskData

logger = logging.getLogger(__name__)


class LatencySweepCoordinator(BaseSweepCoordinator):
    """Publisher for the retired epoch-1 latency history (p99 at a fixed rate)."""

    metric_unit = "µs"
    lower_is_better = True

    def __init__(self, repo_path: Path):
        super().__init__(repo_path, LATENCY_STATE_FILE)

    @property
    def metric_id(self) -> str:  # type: ignore[override]
        return "latency"

    @property
    def workload_id(self) -> str:  # type: ignore[override]
        return "get-k16-v16"

    def schedule_key(self) -> None:
        """A retired series has nothing left to measure."""
        return None

    def _create_task(self, sweep_task: SweepTask) -> BaseTaskData:
        raise RuntimeError(f"{self.metric_id}:{self.workload_id} is retired and does not schedule tasks")

    def _is_my_task(self, task: BaseTaskData) -> bool:
        return False

    def _extract_result(self, task: BaseTaskData) -> Optional[tuple[float, float, int]]:
        return None

    def export(self, output_path: Path, platform: str) -> int:
        """Export the recorded latency history to a series JSON file. Returns point count."""
        from conductress.sweep.exporter import export_latency

        return export_latency(
            self.state,
            output_path,
            platform=platform,
            workload=self.workload_id,
            target_rps=LATENCY_TARGET_RPS,
        )
