"""Per-task usage rows (``task_usage``, schema version 5).

One row says how much runner time one task used. The control service writes a
row for a remote task in the same transaction that ends it. A runner posts a
row for each task it ran from its own local queue (sweep or hand-queued), which
the control service never sees. The table is only ever added to: nothing here
changes or reads back the ``tasks`` table except to copy values out of it.

It also holds the pure functions of the operator report and the public summary.
The summary groups rows by platform and class only, so no login, batch or note
can reach it; ``public_fields.py`` adds the allowlist on top.
"""

from __future__ import annotations

import json
import re
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from ..duration_estimator import estimate_task_duration_seconds
from .db import parse_utc, utc_text
from .errors import ControlError

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


# ---------------------------------------------------------------- report

REPORT_GROUPS = ("agent", "batch", "platform")
OTHERS = "others"
_RAN = ("completed", "failed", "cancelled-running")  # outcomes that used runner time
_DURATION = re.compile(r"^(\d+)([hd])$")


def parse_since(value: str, now: datetime) -> str:
    """Turn ``24h``, ``7d`` or a date (``2026-08-29``, UTC midnight) into the UTC text a row's ended_at is compared to."""
    match = _DURATION.match(value)
    if match:
        unit = timedelta(hours=1) if match.group(2) == "h" else timedelta(days=1)
        return utc_text(now - int(match.group(1)) * unit)
    try:
        return utc_text(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc))
    except ValueError as exc:
        raise ControlError("SINCE_INVALID", "since must be a duration such as 24h or 7d, or a date YYYY-MM-DD") from exc


def _group_key(row: dict[str, Any], by: str) -> str:
    value = {"agent": row["submitted_by"], "batch": row["batch_id"], "platform": row["platform"]}[by]
    return value or "-"


def _wait_seconds(row: dict[str, Any]) -> Optional[float]:
    if row["outcome"] == "cancelled-queued" or not row["started_at"] or not row["queued_at"]:
        return None
    return max(0.0, (parse_utc(row["started_at"]) - parse_utc(row["queued_at"])).total_seconds())


def _summarize(key: str, rows: list[dict[str, Any]], total_seconds: float) -> dict[str, Any]:
    seconds = sum(row["runner_seconds"] for row in rows)
    waits = [w for w in (_wait_seconds(row) for row in rows) if w is not None]
    return {
        "key": key,
        "task_count": len(rows),
        "runner_seconds": round(seconds, 1),
        "share": round(seconds / total_seconds, 4) if total_seconds else 0.0,
        "mean_wait_seconds": round(sum(waits) / len(waits), 1) if waits else None,
        "failed_count": sum(1 for row in rows if row["outcome"] == "failed"),
        "cancelled_count": sum(1 for row in rows if row["outcome"].startswith("cancelled")),
    }


def build_report(rows: list[dict[str, Any]], *, by: str, viewer_login: Optional[str]) -> list[dict[str, Any]]:
    """Group usage rows. ``viewer_login`` None is an operator and sees every group.

    Anyone else sees their own rows grouped by ``by`` and all other time in one
    row named ``others``, so another login, batch ID or platform split of
    other people's work never appears.
    """
    total = sum(row["runner_seconds"] for row in rows)
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if viewer_login is None or row["submitted_by"] == viewer_login:
            key = _group_key(row, by)
        else:
            key = OTHERS
        groups.setdefault(key, []).append(row)
    result = [_summarize(key, members, total) for key, members in groups.items()]
    return sorted(result, key=lambda item: (-item["runner_seconds"], item["key"]))


# ---------------------------------------------------------------- public summary

SUMMARY_WINDOW_DAYS = 30


def build_public_summary(rows: list[dict[str, Any]], queue_depth: dict[str, int], *, now: datetime) -> dict[str, Any]:
    """Aggregates per platform and day. Rows are grouped by platform and class only: no login, batch or note."""
    platforms: dict[str, dict[str, Any]] = {
        name: {"queue_depth": depth, "days": {}} for name, depth in queue_depth.items()
    }
    for row in rows:
        if row["outcome"] not in _RAN:
            continue
        entry = platforms.setdefault(row["platform"] or "unknown", {"queue_depth": 0, "days": {}})
        day = entry["days"].setdefault(row["ended_at"][:10], {"seconds": {}, "counts": {}, "waits": []})
        day["seconds"][row["task_class"]] = day["seconds"].get(row["task_class"], 0.0) + row["runner_seconds"]
        day["counts"][row["task_class"]] = day["counts"].get(row["task_class"], 0) + 1
        wait = _wait_seconds(row)
        if wait is not None:
            day["waits"].append(wait)
    return {
        "schema_version": 1,
        "generated_at": utc_text(now),
        "window_days": SUMMARY_WINDOW_DAYS,
        "platforms": [
            {
                "platform": name,
                "queue_depth": entry["queue_depth"],
                "days": [
                    {
                        "date": date,
                        "runner_seconds": {c: round(day["seconds"].get(c, 0.0), 1) for c in USAGE_CLASSES},
                        "task_count": {c: day["counts"].get(c, 0) for c in USAGE_CLASSES},
                        "median_wait_seconds": round(statistics.median(day["waits"]), 1) if day["waits"] else None,
                    }
                    for date, day in sorted(entry["days"].items())
                ],
            }
            for name, entry in sorted(platforms.items())
        ],
    }
