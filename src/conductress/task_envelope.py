"""Build versioned remote task envelopes from existing task dataclasses."""

from __future__ import annotations

import getpass
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Optional

from .task_queue import BaseTaskData


def _default_submitter() -> str:
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return "unknown"


def serialize_task(task: BaseTaskData) -> dict[str, Any]:
    data = asdict(task)
    data["timestamp"] = task.timestamp.isoformat()
    return data


def build_task_envelope(
    task: BaseTaskData,
    *,
    runner_id: str,
    priority: int = 100,
    submitted_by: Optional[str] = None,
) -> dict[str, Any]:
    submitted_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": 1,
        "task_id": task.task_id,
        "runner_id": runner_id,
        "task_class": "manual",
        "priority": priority,
        "submitted_at": submitted_at,
        "submitted_by": submitted_by or _default_submitter(),
        "canary_id": None,
        "task": serialize_task(task),
    }
