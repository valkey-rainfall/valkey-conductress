"""Archive a runner's canary history under a new runner id.

When a host's environment changes so much that later canary observations
cannot be judged against the earlier ones (intelbench, 2026-11-06: kernel,
glibc and microcode update, canary +12.3%), the operator closes the series:
every observation up to a date is renamed to an archive id such as
``intelbench-pre-2026-11-07``; the live runner keeps its id and its next
observation starts a fresh series (ordinal 1, phase ``observation``), because
the drift analyzer counts and windows prior rows by ``runner_id``.

Only the canary series tables are renamed: ``canary_observations``,
``canary_schedule`` and ``canary_calibration_reports``. Tasks, runner status,
the audit log and result blobs keep the live id; the archive id is a series
identity, not a host identity (no token, no status file). The first live
observation after the cut, if already ingested, keeps its environment evidence
but loses the statistics computed against the old history.

This is a MANUAL, operator-run step (``conductress-control archive-runner``)
with a dry run, a backup of the database first, and an audit entry. It is the
control-plane half of a series split; the runner-side half is
``conductress sweep close-generation``.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional

from .db import ControlDatabase, utc_text

_RUNNER_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
SERIES_TABLES = ("canary_observations", "canary_schedule", "canary_calibration_reports")


@dataclass
class ArchiveResult:
    runner_id: str
    archive_id: str
    until: str
    dry_run: bool
    counts: dict[str, int] = field(default_factory=dict)  # rows that move, per table
    reset_rows: int = 0  # live observations after the cut whose statistics were reset
    after: dict[str, int] = field(default_factory=dict)  # rows under the archive id afterwards
    backup: Optional[Path] = None
    manifest_entry: dict[str, Any] = field(default_factory=dict)

    def summary_lines(self) -> list[str]:
        verb = "Would" if self.dry_run else "Did"
        lines = [f"Archive {self.runner_id} up to {self.until} as {self.archive_id}:"]
        for table, count in self.counts.items():
            lines.append(f"  {verb} rename {count} row(s) in {table}")
        lines.append(f"  {verb} reset the statistics of {self.reset_rows} live observation(s) after the cut")
        if self.backup is not None:
            lines.append(f"  Backup: {self.backup}")
        if not self.dry_run and self.after:
            lines.append("  Rows now under the archive id: " + ", ".join(f"{t}={n}" for t, n in self.after.items()))
        return lines


def count_series_rows(connection: sqlite3.Connection, runner_id: str, until: Optional[str] = None) -> dict[str, int]:
    """Rows per canary series table for one runner (observations and schedule bounded by ``until``)."""
    counts: dict[str, int] = {}
    for table in SERIES_TABLES:
        if table == "canary_calibration_reports" or until is None:
            sql, params = f"SELECT COUNT(*) FROM {table} WHERE runner_id = ?", (runner_id,)
        else:
            sql, params = f"SELECT COUNT(*) FROM {table} WHERE runner_id = ? AND utc_date <= ?", (runner_id, until)  # type: ignore[assignment]
        counts[table] = int(connection.execute(sql, params).fetchone()[0])
    return counts


def backup_database(database: ControlDatabase, destination: Path) -> Path:
    """Copy the database with SQLite's online backup (safe with WAL files active)."""
    if destination.exists():
        raise FileExistsError(f"backup already exists, refusing to overwrite: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source = database.connect()
    try:
        target = sqlite3.connect(destination)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return destination


def manifest_entry(
    live_runner: dict[str, Any], archive_id: str, until: str, note: str, display_name: Optional[str] = None
) -> dict[str, Any]:
    """The fleet manifest entry that makes the archive readable but inert."""
    return {
        "runner_id": archive_id,
        "display_name": display_name or f"{live_runner.get('display_name', live_runner['runner_id'])} (until {until})",
        "platform": live_runner["platform"],
        "platform_aliases": [],
        "enabled": False,
        "archived": True,
        "archive_of": live_runner["runner_id"],
        "archived_until": until,
        "archive_note": note,
        "canary_profile": live_runner.get("canary_profile"),
        "status_ttl_seconds": live_runner.get("status_ttl_seconds", 900),
    }


def archive_runner(
    database: ControlDatabase,
    *,
    runner_id: str,
    archive_id: str,
    until: str,
    note: str,
    actor: str,
    live_runner: Optional[dict[str, Any]] = None,
    dry_run: bool = False,
    backup: Optional[Path] = None,
) -> ArchiveResult:
    """Rename a runner's canary series rows up to ``until`` (inclusive) to ``archive_id``.

    ``until`` is a UTC date (``YYYY-MM-DD``). Observations and schedule rows
    with ``utc_date <= until`` move, and so does the runner's calibration
    report (it describes the old series). Live observations with
    ``utc_date > until`` keep their id; their rolling statistics, which were
    computed against the old history, are reset so the series reads as a
    fresh start (``phase = observation``, ordinal 1, 2, ...). One audit entry
    records the operation. A ``backup`` path, when given, is written with
    SQLite's online backup BEFORE anything changes and is never overwritten.

    Nothing is deleted. ``dry_run`` only counts.
    """
    for value, name in ((runner_id, "runner_id"), (archive_id, "archive_id")):
        if not _RUNNER_ID_RE.fullmatch(value):
            raise ValueError(f"{name} must be 1-63 lowercase letters, digits or hyphens: {value!r}")
    if archive_id == runner_id:
        raise ValueError("archive_id must differ from runner_id")
    date.fromisoformat(until)  # YYYY-MM-DD or ValueError

    result = ArchiveResult(runner_id=runner_id, archive_id=archive_id, until=until, dry_run=dry_run)
    if live_runner is not None:
        result.manifest_entry = manifest_entry(live_runner, archive_id, until, note)

    connection = database.connect()
    try:
        existing = count_series_rows(connection, archive_id)
        if any(existing.values()):
            raise ValueError(f"archive id {archive_id!r} already has rows: {existing}")
        result.counts = count_series_rows(connection, runner_id, until)
        result.reset_rows = int(
            connection.execute(
                "SELECT COUNT(*) FROM canary_observations WHERE runner_id = ? AND utc_date > ? AND accepted = 1",
                (runner_id, until),
            ).fetchone()[0]
        )
    finally:
        connection.close()
    if dry_run:
        return result

    if backup is not None:
        result.backup = backup_database(database, backup)

    with database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE canary_observations SET runner_id = ? WHERE runner_id = ? AND utc_date <= ?",
            (archive_id, runner_id, until),
        )
        connection.execute(
            "UPDATE canary_schedule SET runner_id = ? WHERE runner_id = ? AND utc_date <= ?",
            (archive_id, runner_id, until),
        )
        connection.execute(
            "UPDATE canary_calibration_reports SET runner_id = ? WHERE runner_id = ?",
            (archive_id, runner_id),
        )
        # The live series restarts: the rows already ingested after the cut
        # keep their score and environment evidence, but not the statistics
        # the old history produced. Ordinals are recomputed from the row order.
        live_rows = connection.execute(
            "SELECT utc_date, task_id FROM canary_observations "
            "WHERE runner_id = ? AND utc_date > ? AND accepted = 1 ORDER BY utc_date ASC, task_id ASC",
            (runner_id, until),
        ).fetchall()
        for ordinal, row in enumerate(live_rows, start=1):
            connection.execute(
                "UPDATE canary_observations SET phase = ?, series_ordinal = ?, ref_sample_count = ?, "
                "ref_median = NULL, ref_mad = NULL, delta_pct = NULL, window_start = NULL, window_end = NULL, "
                "candidate_signal = 'insufficient-data' "
                "WHERE runner_id = ? AND utc_date = ? AND task_id = ?",
                (_phase_for_ordinal(ordinal), ordinal, ordinal - 1, runner_id, row["utc_date"], row["task_id"]),
            )
        database.insert_audit(
            connection,
            actor=actor,
            action="runner.archive",
            runner_id=runner_id,
            detail={
                "archive_id": archive_id,
                "until": until,
                "note": note,
                "renamed": result.counts,
                "reset_live_observations": len(live_rows),
                "backup": str(result.backup) if result.backup else None,
                "at": utc_text(),
            },
        )
        result.after = count_series_rows(connection, archive_id)
    return result


def _phase_for_ordinal(ordinal: int) -> str:
    from .drift_analyzer import (
        CALIBRATION_WINDOW,
        OBSERVATION_WINDOW,
        PHASE_CALIBRATING,
        PHASE_OBSERVATION,
        PHASE_READY,
    )

    if ordinal <= OBSERVATION_WINDOW:
        return PHASE_OBSERVATION
    if ordinal < CALIBRATION_WINDOW:
        return PHASE_CALIBRATING
    return PHASE_READY


def format_manifest_entry(entry: dict[str, Any]) -> str:
    return json.dumps(entry, indent=2)
