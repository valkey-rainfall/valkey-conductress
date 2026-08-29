"""SQLite persistence for the fleet control service."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)
DATABASE_SCHEMA_VERSION = 1

_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    runner_id TEXT NOT NULL,
    task_class TEXT NOT NULL CHECK (task_class IN ('manual', 'canary', 'sweep')),
    priority INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('queued', 'claimed', 'accepted', 'completed', 'failed', 'cancelled')
    ),
    submitted_at TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    canary_id TEXT,
    envelope_json TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    claimed_at TEXT,
    lease_expires TEXT,
    claim_token TEXT,
    accepted_at TEXT,
    completed_at TEXT,
    outcome_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_runner_state_priority
    ON tasks(runner_id, state, priority DESC, submitted_at ASC);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);

CREATE TABLE IF NOT EXISTS runner_status (
    runner_id TEXT PRIMARY KEY,
    status_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    task_id TEXT,
    runner_id TEXT,
    old_state TEXT,
    new_state TEXT,
    detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_task ON audit_log(task_id);
CREATE INDEX IF NOT EXISTS idx_audit_runner ON audit_log(runner_id);
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: Optional[datetime] = None) -> str:
    return (value or utc_now()).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ControlDatabase:
    def __init__(self, path: Path, audit_jsonl_path: Path):
        self.path = path
        self.audit_jsonl_path = audit_jsonl_path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        os.chmod(self.path, 0o600)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def initialize(self) -> None:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            row = connection.execute("SELECT MAX(version) AS version FROM schema_migrations").fetchone()
            version = row["version"] or 0
            if version > DATABASE_SCHEMA_VERSION:
                raise RuntimeError(f"database schema {version} is newer than supported {DATABASE_SCHEMA_VERSION}")
            if version < 1:
                connection.executescript(_SCHEMA_V1)
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (1, utc_text()),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def insert_audit(
        self,
        connection: sqlite3.Connection,
        *,
        actor: str,
        action: str,
        task_id: Optional[str] = None,
        runner_id: Optional[str] = None,
        old_state: Optional[str] = None,
        new_state: Optional[str] = None,
        detail: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        record = {
            "timestamp": utc_text(),
            "actor": actor,
            "action": action,
            "task_id": task_id,
            "runner_id": runner_id,
            "old_state": old_state,
            "new_state": new_state,
            "detail": detail,
        }
        connection.execute(
            "INSERT INTO audit_log(timestamp, actor, action, task_id, runner_id, old_state, new_state, detail_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                record["timestamp"],
                actor,
                action,
                task_id,
                runner_id,
                old_state,
                new_state,
                json.dumps(detail, sort_keys=True) if detail is not None else None,
            ),
        )
        return record

    def append_audit_jsonl(self, record: dict[str, Any]) -> None:
        """Best-effort mirror; SQLite audit_log remains authoritative."""
        try:
            self.audit_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with self.audit_jsonl_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, sort_keys=True) + "\n")
        except OSError:
            logger.warning("unable to append audit JSONL", exc_info=True)
