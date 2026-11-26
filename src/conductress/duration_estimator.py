"""Expected and observed task-duration helpers for fleet status surfaces."""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from conductress.tail_reader import tail_lines

DEFAULT_TASK_DURATION_SECONDS = 300


def task_family(task_type: str) -> str:
    """Map a serialized task type to a stable calibration family."""
    return {
        "PerfTaskData": "perf",
        "CanaryPerfTaskData": "perf",
        "BoundedInsertionTaskData": "perf",
        "CachecannonTaskData": "cachecannon",
        "ScenarioTaskData": "scenario",
        "MemTaskData": "memory",
    }.get(task_type, "other")


def _document(task: Any) -> Mapping[str, Any]:
    if isinstance(task, Mapping):
        return task
    if is_dataclass(task):
        return asdict(task)  # type: ignore[arg-type]  # is_dataclass narrows runtime instances
    return vars(task)


def estimate_task_duration_seconds(task: Any, calibration: Optional[Mapping[str, float]] = None) -> int:
    """Estimate wall-clock duration from the task shape, with optional host calibration."""
    document = _document(task)
    task_type = str(document.get("task_type") or type(task).__name__)
    family = task_family(task_type)
    repetitions = max(1, int(document.get("repetitions") or 1))
    warmup = max(0, int(document.get("warmup") or 0))
    duration = max(0, int(document.get("duration") or 0))

    if task_type in {"PerfTaskData", "CanaryPerfTaskData"}:
        max_reps = max(0, int(document.get("max_reps") or 0))
        if max_reps > repetitions and float(document.get("target_cv") or 0) > 0:
            repetitions = min(max_reps, repetitions + 2)
        seconds = 60 + repetitions * (warmup + duration + 18)
        if document.get("perf_stat_enabled"):
            seconds += 45
    elif task_type == "BoundedInsertionTaskData":
        insertions = max(1, int(document.get("insertions") or 1))
        # Conservative 500K inserts/s floor plus restart/cache-drop overhead.
        fill_seconds = (insertions + 499_999) // 500_000
        seconds = 60 + repetitions * (fill_seconds + 18)
        if document.get("perf_stat_enabled"):
            seconds += 45
    elif task_type == "CachecannonTaskData":
        seconds = 90 + repetitions * (warmup + duration + 15)
    elif task_type == "ScenarioTaskData":
        seconds = 180 + repetitions * (2 * (warmup + duration) + 20)
    elif task_type == "MemTaskData":
        sizes = document.get("val_sizes") or [0]
        seconds = 180 + len(sizes) * (120 + (90 if document.get("settle") else 0))
    else:
        seconds = DEFAULT_TASK_DURATION_SECONDS

    factor = (calibration or {}).get(family, 1.0)
    return max(60, round(seconds * factor))


def load_duration_calibration(path: Path, *, max_records: int = 200) -> dict[str, float]:
    """Return median observed/expected factors by task family from recent unique tasks.

    Reads only the last ``max_records * 2`` lines of the file. Prefer
    :func:`load_duration_calibration_from_lines` with a pre-read tail for hot
    paths that also need recent results.
    """
    return load_duration_calibration_from_lines(tail_lines(path, max_records * 2), max_records=max_records)


def load_duration_calibration_from_lines(lines: list[str], *, max_records: int = 200) -> dict[str, float]:
    """Compute calibration factors from already-loaded JSONL lines.

    Same semantics as :func:`load_duration_calibration` but operates on
    an in-memory line list so the caller can share a single tail read
    between calibration and recent-results extraction.
    """
    ratios: dict[str, list[float]] = defaultdict(list)
    seen: set[str] = set()
    for line in reversed(lines[-max_records * 2 :]):
        try:
            record = json.loads(line)
            task_id = record.get("task_id")
            observed = float(record.get("observed_duration_sec") or 0)
            expected = float(record.get("expected_duration_sec") or 0)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if not task_id or task_id in seen or observed <= 0 or expected <= 0:
            continue
        seen.add(task_id)
        family = str(record.get("duration_family") or "other")
        ratios[family].append(observed / expected)

    return {
        family: min(2.0, max(0.5, statistics.median(values))) for family, values in ratios.items() if len(values) >= 3
    }
