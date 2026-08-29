import json
import sqlite3
import stat

import pytest

from conductress.control.db import ControlDatabase


def test_database_initializes_wal_schema_and_reopens(control_env):
    database = control_env["database"]
    with database.read() as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 1
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert {"tasks", "runner_status", "audit_log", "schema_migrations"} <= tables
    assert stat.S_IMODE(database.path.stat().st_mode) == 0o600

    ControlDatabase(database.path, database.audit_jsonl_path).initialize()


def test_transaction_rolls_back(control_env):
    database = control_env["database"]
    with pytest.raises(RuntimeError):
        with database.transaction(immediate=True) as connection:
            connection.execute(
                "INSERT INTO runner_status(runner_id, status_json, updated_at) VALUES ('x', '{}', 'now')"
            )
            raise RuntimeError("rollback")

    with database.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM runner_status").fetchone()[0] == 0


def test_audit_is_stored_and_mirrored(control_env):
    database = control_env["database"]
    with database.transaction(immediate=True) as connection:
        record = database.insert_audit(
            connection,
            actor="operator:test",
            action="test.action",
            task_id="task-1",
            new_state="queued",
        )
    database.append_audit_jsonl(record)

    with database.read() as connection:
        row = connection.execute("SELECT * FROM audit_log").fetchone()
    assert row["actor"] == "operator:test"
    assert row["new_state"] == "queued"
    assert json.loads(database.audit_jsonl_path.read_text(encoding="utf-8"))["action"] == "test.action"


def test_read_context_closes_connection(control_env):
    database = control_env["database"]
    with database.read() as connection:
        assert connection.execute("SELECT 1").fetchone()[0] == 1

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connection.execute("SELECT 1")
