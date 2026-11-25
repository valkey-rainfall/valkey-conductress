"""Per-task usage rows (``task_usage``, schema version 5).

One row says how much runner time one task used. The control service writes a
row for a remote task in the same transaction that ends it. A runner posts a
row for each task it ran from its own local queue (sweep or hand-queued), which
the control service never sees. The table is only ever added to: nothing here
changes or reads back the ``tasks`` table except to copy values out of it.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from ..duration_estimator import estimate_task_duration_seconds
from .db import parse_utc

USAGE_CLASSES = ("manual", "canary", "sweep", "local")
USAGE_OUTCOMES = ("completed", "failed", "cancelled-queued", "cancelled-running")

_INSERT_USAGE = (
    "INSERT OR IGNORE INTO task_usage(task_id, submitted_by, batch_id, note, runner_id, platform, task_class, "
    "queued_at, started_at, ended_at, expected_seconds, runner_seconds, outcome) "
    "VALUES (:task_id, :submitted_by, :batch_id, :note, :runner_id, :platform, :task_class, "
    ":queued_at, :started_at, :ended_at, :expected_seconds, :runner_seconds, :outcome)"
)


def seconds_between(started_at: Optional[str], ended_at: str) -> float:
    """Runner seconds of a task. A task that never started used none."""
    if not started_at:
        return 0.0
    return max(0.0, (parse_utc(ended_at) - parse_utc(started_at)).total_seconds())


def usage_from_task_row(
    row: sqlite3.Row, *, outcome: str, ended_at: str, platform: Optional[str] = None
) -> dict[str, Any]:
    """Build the usage record of a remote task row that ended with ``outcome``."""
    try:
        task = json.loads(row["envelope_json"]).get("task", {})
    except (json.JSONDecodeError, TypeError, AttributeError):
        task = {}
    started_at = row["accepted_at"]
    return {
        "task_id": row["task_id"],
        "submitted_by": row["submitter_login"] or row["submitted_by"],
        "batch_id": row["batch_id"],
        "note": task.get("note"),
        "runner_id": row["runner_id"],
        "platform": platform,
        "task_class": row["task_class"],
        "queued_at": row["created_at"],
        "started_at": started_at,
        "ended_at": ended_at,
        "expected_seconds": estimate_task_duration_seconds(task) if task else None,
        "runner_seconds": seconds_between(started_at, ended_at),
        "outcome": outcome,
    }


def insert_usage(connection: sqlite3.Connection, record: dict[str, Any]) -> bool:
    """Store one usage row. A task that already has a row is left alone. Returns True if stored."""
    return connection.execute(_INSERT_USAGE, record).rowcount == 1


def backfill_task_usage(connection: sqlite3.Connection) -> int:
    """Add a usage row for every task that already ended. Existing rows and tasks are not touched.

    Completed and failed tasks end at ``completed_at``. A cancelled task ends at
    ``updated_at`` and is ``cancelled-running`` only if a runner had accepted it.
    The platform stays empty here; the service fills it from the fleet manifest.
    """
    added = 0
    rows = connection.execute("SELECT * FROM tasks WHERE state IN ('completed', 'failed', 'cancelled')").fetchall()
    for row in rows:
        if row["state"] == "cancelled":
            outcome = "cancelled-running" if row["accepted_at"] else "cancelled-queued"
            ended_at = row["updated_at"]
        else:
            outcome = row["state"]
            ended_at = row["completed_at"] or row["updated_at"]
        added += insert_usage(connection, usage_from_task_row(row, outcome=outcome, ended_at=ended_at))
    return added
