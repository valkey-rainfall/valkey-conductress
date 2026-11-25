"""Schema version 5 (task_usage): the migration is additive, safe to repeat, and checkable on a copy."""

import hashlib
import json
import sqlite3
import sys

import pytest

from conductress.control import db as db_module
from conductress.control import migration_check
from conductress.control import usage as usage_module
from conductress.control.__main__ import main as control_main
from conductress.control.db import DATABASE_SCHEMA_VERSION, ControlDatabase
from conductress.control.migration_check import check_migration, snapshot

OLD_TABLES = ("tasks", "audit_log", "runner_status", "schema_migrations")


def _envelope(task_id, note, runner_id="armbench"):
    return json.dumps(
        {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": runner_id,
            "task_class": "manual",
            "priority": 100,
            "submitted_at": "2026-08-29T00:00:00Z",
            "submitted_by": "example-agent",
            "task": {"task_type": "PerfTaskData", "source": "valkey", "specifier": "abc123", "note": note},
        },
        sort_keys=True,
    )


# task_id, state, accepted_at, completed_at, updated_at
_TASKS = [
    ("t-queued", "queued", None, None, "2026-08-29T01:00:00Z"),
    ("t-pending", "pending-approval", None, None, "2026-08-29T01:00:00Z"),
    ("t-claimed", "claimed", None, None, "2026-08-29T01:00:00Z"),
    ("t-accepted", "accepted", "2026-08-29T01:10:00Z", None, "2026-08-29T01:10:00Z"),
    ("t-requested", "cancel-requested", "2026-08-29T01:10:00Z", None, "2026-08-29T01:12:00Z"),
    ("t-done", "completed", "2026-08-29T02:00:00Z", "2026-08-29T02:10:00Z", "2026-08-29T02:10:00Z"),
    ("t-failed", "failed", "2026-08-29T03:00:00Z", "2026-08-29T03:00:30Z", "2026-08-29T03:00:30Z"),
    ("t-cancelled", "cancelled", None, None, "2026-08-29T04:00:00Z"),
]


def build_v4_database(path):
    """Create a version 4 file the way the released code did, then fill it with made-up rows."""
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
    for version, script in enumerate(
        (db_module._SCHEMA_V1, db_module._SCHEMA_V2, db_module._SCHEMA_V3, db_module._SCHEMA_V4), start=1
    ):
        connection.executescript(script)
        connection.execute("INSERT INTO schema_migrations VALUES (?, '2026-08-01T00:00:00Z')", (version,))
    for index, (task_id, state, accepted_at, completed_at, updated_at) in enumerate(_TASKS):
        connection.execute(
            "INSERT INTO tasks(task_id, runner_id, task_class, priority, state, submitted_at, submitted_by, "
            "batch_id, submitter_login, envelope_json, accepted_at, completed_at, created_at, updated_at) "
            "VALUES (?, 'armbench', 'manual', 100, ?, '2026-08-29T00:00:00Z', 'example-agent', ?, 'example-agent', "
            "?, ?, ?, '2026-08-29T00:00:00Z', ?)",
            (
                task_id,
                state,
                f"batch-{index % 2}",
                _envelope(task_id, f"note for {task_id}"),
                accepted_at,
                completed_at,
                updated_at,
            ),
        )
        connection.execute(
            "INSERT INTO audit_log(timestamp, actor, action, task_id, runner_id, new_state) "
            "VALUES ('2026-08-29T00:00:00Z', 'user:example-agent', 'task.submit', ?, 'armbench', ?)",
            (task_id, state),
        )
    connection.execute(
        "INSERT INTO runner_status(runner_id, status_json, updated_at) "
        "VALUES ('armbench', '{\"host\": \"198.51.100.7\"}', '2026-08-29T05:00:00Z')"
    )
    connection.close()
    return path


@pytest.fixture
def v4_path(tmp_path):
    return build_v4_database(tmp_path / "control.db")


def _migrate(path):
    ControlDatabase(path, path.with_suffix(".audit.jsonl")).initialize()


def _rows(path, sql):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(sql)]
    finally:
        connection.close()


def test_schema_version_is_five():
    assert DATABASE_SCHEMA_VERSION == 5


def test_v4_to_v5_keeps_every_old_row_and_adds_usage_rows(v4_path):
    before = snapshot(v4_path)
    assert before.version == 4
    assert before.integrity == "ok"

    _migrate(v4_path)
    after = snapshot(v4_path)

    assert after.version == 5
    assert after.integrity == "ok"
    assert after.foreign_key_problems == 0
    for table in ("tasks", "audit_log", "runner_status"):
        assert after.counts[table] == before.counts[table]
        assert after.digests[table] == before.digests[table], table
    assert after.counts["schema_migrations"] == before.counts["schema_migrations"] + 1
    assert "task_usage" not in before.counts
    # One row per ended task (completed, failed, cancelled), none for the others.
    assert after.counts["task_usage"] == 3


def test_backfilled_usage_rows_use_accepted_and_completed_times(v4_path):
    _migrate(v4_path)
    usage = {r["task_id"]: r for r in _rows(v4_path, "SELECT * FROM task_usage")}
    assert set(usage) == {"t-done", "t-failed", "t-cancelled"}
    done = usage["t-done"]
    assert (done["outcome"], done["runner_seconds"], done["started_at"]) == ("completed", 600.0, "2026-08-29T02:00:00Z")
    assert done["submitted_by"] == "example-agent"
    assert done["note"] == "note for t-done"
    assert done["platform"] is None
    assert (usage["t-failed"]["outcome"], usage["t-failed"]["runner_seconds"]) == ("failed", 30.0)
    assert (usage["t-cancelled"]["outcome"], usage["t-cancelled"]["runner_seconds"]) == ("cancelled-queued", 0.0)


def test_second_migration_run_changes_nothing(v4_path):
    _migrate(v4_path)
    first = snapshot(v4_path)
    _migrate(v4_path)
    second = snapshot(v4_path)
    assert second.counts == first.counts
    assert second.digests == first.digests


def test_failed_migration_leaves_a_version_4_database(v4_path, monkeypatch):
    before = snapshot(v4_path)

    def broken(_connection):
        raise RuntimeError("stop in the middle")

    monkeypatch.setattr(usage_module, "backfill_task_usage", broken)
    with pytest.raises(RuntimeError, match="stop in the middle"):
        _migrate(v4_path)
    after = snapshot(v4_path)
    assert after.version == 4
    assert "task_usage" not in after.counts
    assert after.digests == before.digests


def test_removing_the_version_row_lets_the_migration_run_again_without_duplicates(v4_path):
    _migrate(v4_path)
    connection = sqlite3.connect(v4_path)
    connection.execute("DELETE FROM schema_migrations WHERE version = 5")
    connection.commit()
    connection.close()
    _migrate(v4_path)
    snap = snapshot(v4_path)
    assert (snap.version, snap.counts["task_usage"]) == (5, 3)


def test_new_database_gets_the_usage_table(tmp_path):
    path = tmp_path / "fresh.db"
    _migrate(path)
    snap = snapshot(path)
    assert snap.version == 5
    assert snap.counts["task_usage"] == 0


def test_check_migration_passes_and_never_writes_the_source(v4_path, tmp_path):
    digest = hashlib.sha256(v4_path.read_bytes()).hexdigest()
    report = check_migration(v4_path, work_dir=tmp_path)
    assert report.ok, report.lines()
    assert (report.before.version, report.after.version) == (4, 5)
    assert report.second_run_changed == []
    assert report.before.counts["tasks"] == report.after.counts["tasks"] == len(_TASKS)
    assert hashlib.sha256(v4_path.read_bytes()).hexdigest() == digest
    assert sorted(p.name for p in tmp_path.iterdir()) == ["control.db"]
    text = "\n".join(report.lines())
    assert "RESULT: ok" in text
    assert "198.51.100.7" not in text
    assert "note for" not in text


def test_check_migration_reports_a_row_that_a_migration_changed(v4_path, monkeypatch):
    original = migration_check._migrate

    def migrate_and_touch(path):
        original(path)
        connection = sqlite3.connect(path)
        connection.execute("UPDATE tasks SET priority = 7 WHERE task_id = 't-done'")
        connection.commit()
        connection.close()

    monkeypatch.setattr(migration_check, "_migrate", migrate_and_touch)
    report = check_migration(v4_path)
    assert not report.ok
    assert any("existing rows changed in: tasks" in problem for problem in report.problems)


def test_check_migration_refuses_a_database_newer_than_the_code(tmp_path):
    path = tmp_path / "newer.db"
    _migrate(path)
    connection = sqlite3.connect(path)
    connection.execute("INSERT INTO schema_migrations VALUES (99, '2026-08-01T00:00:00Z')")
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="newer than supported"):
        check_migration(path)


def test_check_migration_command_prints_a_report(v4_path, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["conductress-control", "check-migration", str(v4_path)])
    control_main()
    out = capsys.readouterr().out
    assert "schema version: 4 -> 5" in out
    assert "task_usage" in out
    assert "RESULT: ok" in out


def test_check_migration_command_fails_for_a_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["conductress-control", "check-migration", str(tmp_path / "missing.db")])
    with pytest.raises(SystemExit) as exit_info:
        control_main()
    assert exit_info.value.code == 2
