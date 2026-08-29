"""Domain service for fleet tasks, runner lifecycle, and status."""

from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import timedelta
from typing import Any, Optional

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from .db import ControlDatabase, parse_utc, utc_now, utc_text
from .errors import ConflictError, ControlError, NotFoundError
from .fleet_registry import FleetRegistry
from .schema import load_schema


def _validator(name: str) -> Draft202012Validator:
    return Draft202012Validator(load_schema(name), format_checker=FormatChecker())


class ControlService:
    def __init__(self, database: ControlDatabase, registry: FleetRegistry, claim_lease_seconds: int = 300):
        self.database = database
        self.registry = registry
        self.claim_lease_seconds = claim_lease_seconds
        self.task_validator = _validator("task-envelope.schema.json")
        self.status_validator = _validator("runner-status.schema.json")
        self.outcome_validator = _validator("task-outcome.schema.json")

    @staticmethod
    def _validate(validator: Draft202012Validator, document: dict[str, Any], kind: str) -> None:
        try:
            validator.validate(document)
        except ValidationError as exc:
            path = ".".join(str(part) for part in exc.absolute_path)
            location = f" at {path}" if path else ""
            raise ControlError("SCHEMA_INVALID", f"invalid {kind}{location}: {exc.message}") from exc

    @staticmethod
    def _canonical(document: dict[str, Any]) -> str:
        return json.dumps(document, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _validate_datetime(value: str, field: str) -> None:
        try:
            parsed = parse_utc(value)
        except (TypeError, ValueError) as exc:
            raise ControlError("SCHEMA_INVALID", f"invalid date-time at {field}") from exc
        if parsed.tzinfo is None:
            raise ControlError("SCHEMA_INVALID", f"date-time at {field} must include a timezone")

    @staticmethod
    def _task_from_row(row: sqlite3.Row, *, include_envelope: bool = True) -> dict[str, Any]:
        task = {
            "task_id": row["task_id"],
            "runner_id": row["runner_id"],
            "task_class": row["task_class"],
            "priority": row["priority"],
            "state": row["state"],
            "submitted_at": row["submitted_at"],
            "submitted_by": row["submitted_by"],
            "canary_id": row["canary_id"],
            "claimed_at": row["claimed_at"],
            "lease_expires": row["lease_expires"],
            "accepted_at": row["accepted_at"],
            "completed_at": row["completed_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        if include_envelope:
            task["envelope"] = json.loads(row["envelope_json"])
            task["outcome"] = json.loads(row["outcome_json"]) if row["outcome_json"] else None
        return task

    def submit_task(
        self,
        envelope: dict[str, Any],
        *,
        actor: str,
        idempotency_key: Optional[str] = None,
    ) -> tuple[dict[str, Any], bool]:
        self._validate(self.task_validator, envelope, "task envelope")
        self._validate_datetime(envelope["submitted_at"], "submitted_at")
        self.registry.get_runner(envelope["runner_id"])
        canonical = self._canonical(envelope)
        audits = []
        with self.database.transaction(immediate=True) as connection:
            existing = None
            if idempotency_key:
                existing = connection.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
            if existing is None:
                existing = connection.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (envelope["task_id"],)
                ).fetchone()
            if existing is not None:
                if existing["envelope_json"] != canonical:
                    raise ConflictError(
                        "IDEMPOTENCY_CONFLICT",
                        "task ID or idempotency key already exists with a different payload",
                    )
                return self._task_from_row(existing), False

            now = utc_text()
            connection.execute(
                "INSERT INTO tasks(task_id, runner_id, task_class, priority, state, submitted_at, "
                "submitted_by, canary_id, envelope_json, idempotency_key, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
                (
                    envelope["task_id"],
                    envelope["runner_id"],
                    envelope["task_class"],
                    envelope["priority"],
                    envelope["submitted_at"],
                    envelope["submitted_by"],
                    envelope.get("canary_id"),
                    canonical,
                    idempotency_key,
                    now,
                    now,
                ),
            )
            audits.append(
                self.database.insert_audit(
                    connection,
                    actor=actor,
                    action="task.submit",
                    task_id=envelope["task_id"],
                    runner_id=envelope["runner_id"],
                    new_state="queued",
                )
            )
            row = connection.execute("SELECT * FROM tasks WHERE task_id = ?", (envelope["task_id"],)).fetchone()
        for audit in audits:
            self.database.append_audit_jsonl(audit)
        return self._task_from_row(row), True

    def get_task(self, task_id: str) -> dict[str, Any]:
        with self.database.read() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if row is None:
            raise NotFoundError("TASK_NOT_FOUND", f"unknown task: {task_id}")
        return self._task_from_row(row)

    def list_tasks(
        self,
        *,
        runner_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if limit < 1 or limit > 250 or offset < 0:
            raise ControlError("PAGINATION_INVALID", "limit must be 1-250 and offset must be non-negative")
        clauses = []
        params: list[Any] = []
        if runner_id:
            clauses.append("runner_id = ?")
            params.append(runner_id)
        if state:
            clauses.append("state = ?")
            params.append(state)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        with self.database.read() as connection:
            rows = connection.execute(
                f"SELECT * FROM tasks{where} ORDER BY created_at DESC LIMIT ? OFFSET ?", params
            ).fetchall()
        return [self._task_from_row(row, include_envelope=False) for row in rows]

    def cancel_task(self, task_id: str, *, actor: str) -> tuple[dict[str, Any], bool]:
        audit = None
        with self.database.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("TASK_NOT_FOUND", f"unknown task: {task_id}")
            if row["state"] == "cancelled":
                return self._task_from_row(row), False
            if row["state"] != "queued":
                raise ConflictError("TASK_NOT_CANCELLABLE", "only queued tasks can be cancelled")
            now = utc_text()
            connection.execute(
                "UPDATE tasks SET state='cancelled', updated_at=? WHERE task_id=? AND state='queued'",
                (now, task_id),
            )
            audit = self.database.insert_audit(
                connection,
                actor=actor,
                action="task.cancel",
                task_id=task_id,
                runner_id=row["runner_id"],
                old_state="queued",
                new_state="cancelled",
            )
            updated = connection.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if audit:
            self.database.append_audit_jsonl(audit)
        return self._task_from_row(updated), True

    def _expire_stale_claims_in_transaction(
        self, connection: sqlite3.Connection, *, actor: str, now_text: str
    ) -> list[dict[str, Any]]:
        stale = connection.execute(
            "SELECT * FROM tasks WHERE state='claimed' AND lease_expires <= ?", (now_text,)
        ).fetchall()
        audits = []
        for row in stale:
            connection.execute(
                "UPDATE tasks SET state='queued', claimed_at=NULL, lease_expires=NULL, claim_token=NULL, "
                "updated_at=? WHERE task_id=? AND state='claimed'",
                (now_text, row["task_id"]),
            )
            audits.append(
                self.database.insert_audit(
                    connection,
                    actor=actor,
                    action="task.claim_expired",
                    task_id=row["task_id"],
                    runner_id=row["runner_id"],
                    old_state="claimed",
                    new_state="queued",
                )
            )
        return audits

    def expire_stale_claims(self, *, actor: str = "system") -> int:
        now_text = utc_text()
        with self.database.transaction(immediate=True) as connection:
            audits = self._expire_stale_claims_in_transaction(connection, actor=actor, now_text=now_text)
        for audit in audits:
            self.database.append_audit_jsonl(audit)
        return len(audits)

    def claim_task(self, runner_id: str, *, actor: str) -> Optional[dict[str, Any]]:
        self.registry.get_runner(runner_id)
        now = utc_now()
        now_text = utc_text(now)
        lease_expires = utc_text(now + timedelta(seconds=self.claim_lease_seconds))
        audits = []
        result = None
        with self.database.transaction(immediate=True) as connection:
            audits.extend(self._expire_stale_claims_in_transaction(connection, actor="system", now_text=now_text))
            accepted = connection.execute(
                "SELECT task_id FROM tasks WHERE runner_id=? AND state='accepted' LIMIT 1",
                (runner_id,),
            ).fetchone()
            if accepted:
                raise ConflictError(
                    "RUNNER_HAS_ACCEPTED_TASK",
                    f"runner already owns accepted task {accepted['task_id']}",
                )
            existing = connection.execute(
                "SELECT * FROM tasks WHERE runner_id=? AND state='claimed' ORDER BY claimed_at LIMIT 1",
                (runner_id,),
            ).fetchone()
            if existing:
                result = self._claim_response(existing)
            else:
                row = connection.execute(
                    "SELECT * FROM tasks WHERE runner_id=? AND state='queued' "
                    "ORDER BY priority DESC, submitted_at ASC, task_id ASC LIMIT 1",
                    (runner_id,),
                ).fetchone()
                if row:
                    claim_token = secrets.token_urlsafe(32)
                    updated = connection.execute(
                        "UPDATE tasks SET state='claimed', claimed_at=?, lease_expires=?, claim_token=?, updated_at=? "
                        "WHERE task_id=? AND state='queued'",
                        (now_text, lease_expires, claim_token, now_text, row["task_id"]),
                    )
                    if updated.rowcount != 1:
                        raise ConflictError("CLAIM_RACE", "task was claimed concurrently")
                    audits.append(
                        self.database.insert_audit(
                            connection,
                            actor=actor,
                            action="task.claim",
                            task_id=row["task_id"],
                            runner_id=runner_id,
                            old_state="queued",
                            new_state="claimed",
                        )
                    )
                    claimed = connection.execute("SELECT * FROM tasks WHERE task_id = ?", (row["task_id"],)).fetchone()
                    result = self._claim_response(claimed)
        for audit in audits:
            self.database.append_audit_jsonl(audit)
        return result

    @staticmethod
    def _claim_response(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "task": ControlService._task_from_row(row),
            "claim_token": row["claim_token"],
            "lease_expires": row["lease_expires"],
        }

    def accept_task(self, runner_id: str, task_id: str, claim_token: str, *, actor: str) -> tuple[dict[str, Any], bool]:
        audit = None
        with self.database.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("TASK_NOT_FOUND", f"unknown task: {task_id}")
            self._verify_task_runner(row, runner_id)
            if row["state"] in {"accepted", "completed", "failed"}:
                if row["claim_token"] != claim_token:
                    raise ConflictError("CLAIM_TOKEN_INVALID", "claim token does not match")
                return self._task_from_row(row), False
            if row["state"] != "claimed":
                raise ConflictError("TASK_NOT_CLAIMED", "task is not currently claimed")
            if row["claim_token"] != claim_token:
                raise ConflictError("CLAIM_TOKEN_INVALID", "claim token does not match")
            if parse_utc(row["lease_expires"]) <= utc_now():
                raise ConflictError("CLAIM_EXPIRED", "claim lease expired before acceptance")
            now = utc_text()
            connection.execute(
                "UPDATE tasks SET state='accepted', accepted_at=?, lease_expires=NULL, updated_at=? "
                "WHERE task_id=? AND state='claimed'",
                (now, now, task_id),
            )
            audit = self.database.insert_audit(
                connection,
                actor=actor,
                action="task.accept",
                task_id=task_id,
                runner_id=runner_id,
                old_state="claimed",
                new_state="accepted",
            )
            updated = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if audit:
            self.database.append_audit_jsonl(audit)
        return self._task_from_row(updated), True

    def record_outcome(
        self, runner_id: str, task_id: str, outcome: dict[str, Any], *, actor: str
    ) -> tuple[dict[str, Any], bool]:
        self._validate(self.outcome_validator, outcome, "task outcome")
        self._validate_datetime(outcome["completed_at"], "completed_at")
        if outcome["task_id"] != task_id or outcome["runner_id"] != runner_id:
            raise ControlError("OUTCOME_IDENTITY_MISMATCH", "outcome task_id/runner_id does not match request")
        terminal = outcome["state"]
        canonical = self._canonical(outcome)
        audit = None
        with self.database.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("TASK_NOT_FOUND", f"unknown task: {task_id}")
            self._verify_task_runner(row, runner_id)
            if row["state"] in {"completed", "failed"}:
                if row["state"] == terminal and row["outcome_json"] == canonical:
                    return self._task_from_row(row), False
                raise ConflictError("OUTCOME_CONFLICT", "task already has a different terminal outcome")
            if row["state"] != "accepted":
                raise ConflictError("TASK_NOT_ACCEPTED", "task must be accepted before reporting outcome")
            now = outcome["completed_at"]
            connection.execute(
                "UPDATE tasks SET state=?, completed_at=?, outcome_json=?, updated_at=? WHERE task_id=?",
                (terminal, now, canonical, utc_text(), task_id),
            )
            audit = self.database.insert_audit(
                connection,
                actor=actor,
                action=f"task.{terminal}",
                task_id=task_id,
                runner_id=runner_id,
                old_state=row["state"],
                new_state=terminal,
            )
            updated = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if audit:
            self.database.append_audit_jsonl(audit)
        return self._task_from_row(updated), True

    @staticmethod
    def _verify_task_runner(row: sqlite3.Row, runner_id: str) -> None:
        if row["runner_id"] != runner_id:
            raise ConflictError("WRONG_RUNNER", "task is assigned to a different runner")

    def push_status(self, runner_id: str, status: dict[str, Any], *, actor: str) -> None:
        self.registry.get_runner(runner_id)
        self._validate(self.status_validator, status, "runner status")
        self._validate_datetime(status["timestamp"], "timestamp")
        if status["runner_id"] != runner_id:
            raise ControlError("STATUS_IDENTITY_MISMATCH", "status runner_id does not match request")
        now = utc_text()
        with self.database.transaction(immediate=True) as connection:
            connection.execute(
                "INSERT INTO runner_status(runner_id, status_json, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(runner_id) DO UPDATE SET status_json=excluded.status_json, updated_at=excluded.updated_at",
                (runner_id, self._canonical(status), now),
            )
            audit = self.database.insert_audit(
                connection,
                actor=actor,
                action="runner.status",
                runner_id=runner_id,
                detail={"status_timestamp": status["timestamp"]},
            )
        self.database.append_audit_jsonl(audit)

    def fleet_status(self, runner_id: Optional[str] = None) -> list[dict[str, Any]]:
        runners = (
            [self.registry.get_runner(runner_id, require_enabled=False)] if runner_id else self.registry.list_runners()
        )
        result = []
        with self.database.read() as connection:
            for runner in runners:
                status_row = connection.execute(
                    "SELECT status_json, updated_at FROM runner_status WHERE runner_id=?",
                    (runner["runner_id"],),
                ).fetchone()
                counts = connection.execute(
                    "SELECT state, COUNT(*) AS count FROM tasks WHERE runner_id=? GROUP BY state",
                    (runner["runner_id"],),
                ).fetchall()
                result.append(
                    {
                        **runner,
                        "status": json.loads(status_row["status_json"]) if status_row else None,
                        "status_updated_at": status_row["updated_at"] if status_row else None,
                        "task_counts": {row["state"]: row["count"] for row in counts},
                    }
                )
        return result
