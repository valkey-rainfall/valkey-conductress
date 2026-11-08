"""Domain service for fleet tasks, runner lifecycle, and status."""

from __future__ import annotations

import json
import logging
import secrets
import sqlite3
from datetime import timedelta
from typing import Any, Optional

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from ..duration_estimator import estimate_task_duration_seconds
from .canary_profiles import CanaryProfileRegistry
from .db import ControlDatabase, parse_utc, utc_now, utc_text
from .drift_analyzer import DriftAnalyzer
from .errors import AuthorizationError, ConflictError, ControlError, NotFoundError
from .fleet_registry import FleetRegistry
from .provenance import ProvenanceGate, ProvenanceVerdict
from .schema import load_schema
from .users import User, UserDirectory

logger = logging.getLogger(__name__)

PUBLIC_DASHBOARD_TASK_LIMIT = 50


def _validator(name: str) -> Draft202012Validator:
    return Draft202012Validator(load_schema(name), format_checker=FormatChecker())


def _default_notifier(request: Any) -> None:
    """Send the approval webhook request with a short timeout."""
    import urllib.request  # local import keeps module import light

    with urllib.request.urlopen(request, timeout=5):  # nosec - control-plane egress to a configured URL
        return


def _fork_repos(user: User) -> list[str]:
    """Return the fork repositories a user's own account is trusted to supply.

    A user's fork of the project lives under their GitHub account
    (``{github}/valkey``). Each declared source that is already in
    ``owner/repo`` form is trusted as written; bare source names name the
    project fork under the user's account.
    """
    repos: list[str] = []
    for source in user.sources:
        if "/" in source:
            repos.append(source)
        else:
            repos.append(f"{user.github}/valkey")
    if not repos:
        repos.append(f"{user.github}/valkey")
    return list(dict.fromkeys(repos))


class ControlService:
    def __init__(
        self,
        database: ControlDatabase,
        registry: FleetRegistry,
        claim_lease_seconds: int = 300,
        *,
        canary_profiles: Optional[CanaryProfileRegistry] = None,
        user_directory: Optional[UserDirectory] = None,
        provenance_gate: Optional[ProvenanceGate] = None,
        notification_url: Optional[str] = None,
        notifier: Optional[Any] = None,
        published_tasks_dir: Optional[Any] = None,
        results_jsonl_path: Optional[Any] = None,
    ):
        self.database = database
        self.registry = registry
        self.claim_lease_seconds = claim_lease_seconds
        self.user_directory = user_directory
        self.provenance_gate = provenance_gate
        self.notification_url = notification_url
        self.published_tasks_dir = published_tasks_dir
        self.results_jsonl_path = results_jsonl_path
        # Injectable HTTP delivery for the approval webhook; defaults to urlopen.
        self._notifier = notifier or _default_notifier
        self.task_validator = _validator("task-envelope.schema.json")
        self.status_validator = _validator("runner-status.schema.json")
        self.outcome_validator = _validator("task-outcome.schema.json")
        self._canary_profiles = canary_profiles
        self.drift_analyzer = DriftAnalyzer(
            database,
            canary_profiles=canary_profiles,
            fleet_registry=registry,
        )

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
            "batch_id": row["batch_id"],
            "submitter_login": row["submitter_login"],
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

    def _resolve_submitter(self, envelope: dict[str, Any], identity_login: Optional[str]) -> Optional[User]:
        """Return the directory user this submission is charged to, if known.

        Prefers the authenticated token login; falls back to the envelope's
        submitter login. Returns None when no directory is configured or the
        login is not present in it.
        """
        if self.user_directory is None:
            return None
        login = identity_login
        if login is None:
            submitter = envelope.get("submitter")
            if isinstance(submitter, dict):
                login = submitter.get("login")
        if not login:
            return None
        return self.user_directory.get(login)

    def _evaluate_provenance(
        self,
        envelope: dict[str, Any],
        *,
        submitter: Optional[User],
        owner_bypass: bool,
    ) -> ProvenanceVerdict:
        """Run the provenance gate for a submission and return its verdict.

        When no gate is configured the submission is accepted unchanged so a
        deployment without GitHub reachability keeps working. A submitter's
        declared sources extend the trusted repository allowlist so their own
        forks are accepted.
        """
        if self.provenance_gate is None:
            return ProvenanceVerdict(True, "provenance gate not configured")
        gate = self.provenance_gate
        if submitter is not None:
            gate = gate.with_extra_repos(tuple(_fork_repos(submitter)))
        allow_bypass = owner_bypass and submitter is not None and submitter.is_owner
        return gate.evaluate(envelope.get("provenance"), owner_bypass=allow_bypass)

    def _approval_decision(
        self, envelope: dict[str, Any], submitter: Optional[User], identity_login: Optional[str]
    ) -> tuple[str, Optional[str]]:
        """Return the initial task state and an optional reason.

        An operator token (no login presented) is trusted and queues directly.
        A directory user queues unless it is over its daily quota. A submission
        whose presented login is not in the directory is held for an approver.
        Canary and sweep classes are system-generated and never held.
        """
        if envelope.get("task_class") in {"canary", "sweep"}:
            return "queued", None
        # No user directory configured: preserve trusted-operator behaviour.
        if self.user_directory is None:
            return "queued", None
        # An operator/runner token presents no login and is trusted.
        presented_login = identity_login
        if presented_login is None:
            submitter_block = envelope.get("submitter")
            if isinstance(submitter_block, dict):
                presented_login = submitter_block.get("login")
        if presented_login is None:
            return "queued", None
        if submitter is None:
            # A login was presented but is not in the directory.
            return "pending-approval", "submitter is not a known user"
        estimated = estimate_task_duration_seconds(envelope.get("task", {}))
        if self._over_quota(submitter, additional_seconds=estimated):
            return "pending-approval", "submitter is over the daily runner-minute quota"
        return "queued", None

    def _over_quota(self, submitter: User, *, additional_seconds: float) -> bool:
        """Return True if adding a task would exceed the account's daily quota.

        Usage is charged to the submitter's GitHub account (an agent draws on
        its sponsor's account). A quota of zero means unlimited.
        """
        if self.user_directory is None:
            return False
        quota_minutes = self.user_directory.quota_minutes(submitter.login)
        if quota_minutes <= 0:
            return False
        account = self.user_directory.quota_account(submitter.login)
        logins = [
            u.login for u in self.user_directory.users.values() if self.user_directory.quota_account(u.login) == account
        ]
        day_start = utc_now().strftime("%Y-%m-%d")
        used_seconds = 0.0
        placeholders = ",".join("?" for _ in logins)
        with self.database.read() as connection:
            rows = connection.execute(
                f"SELECT envelope_json FROM tasks WHERE submitter_login IN ({placeholders}) "
                "AND state NOT IN ('cancelled', 'failed') AND substr(created_at, 1, 10) = ?",
                (*logins, day_start),
            ).fetchall()
        for row in rows:
            try:
                task_body = json.loads(row["envelope_json"]).get("task", {})
            except (json.JSONDecodeError, TypeError):
                continue
            used_seconds += estimate_task_duration_seconds(task_body)
        return (used_seconds + additional_seconds) > quota_minutes * 60

    def _notify_pending_approval(self, task: dict[str, Any], reason: Optional[str]) -> None:
        """POST a small JSON notice to the configured approval webhook, if any.

        Best-effort: a delivery failure is logged and never blocks submission.
        The payload is documented in docs/control-service.md.
        """
        if not self.notification_url:
            return
        payload = {
            "schema_version": 1,
            "event": "task.pending_approval",
            "task_id": task["task_id"],
            "batch_id": task.get("batch_id"),
            "runner_id": task["runner_id"],
            "submitter_login": task.get("submitter_login"),
            "submitted_by": task.get("submitted_by"),
            "reason": reason,
        }
        try:
            import urllib.request  # local import keeps module import light

            request = urllib.request.Request(
                self.notification_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            self._notifier(request)
        except Exception:  # pragma: no cover - defensive, never blocks submit
            logger.warning("approval notification failed for task %s", task["task_id"], exc_info=True)

    def submit_task(
        self,
        envelope: dict[str, Any],
        *,
        actor: str,
        idempotency_key: Optional[str] = None,
        identity_login: Optional[str] = None,
        owner_bypass: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        self._validate(self.task_validator, envelope, "task envelope")
        self._validate_datetime(envelope["submitted_at"], "submitted_at")
        self.registry.get_runner(envelope["runner_id"])
        submitter = self._resolve_submitter(envelope, identity_login)
        verdict = self._evaluate_provenance(envelope, submitter=submitter, owner_bypass=owner_bypass)
        if not verdict.accepted:
            raise AuthorizationError("PROVENANCE_REJECTED", verdict.reason)
        # Store the resolved pull-request block on the envelope so downstream
        # consumers see the verified PR identity rather than a claimed one.
        if verdict.pull_request is not None:
            provenance = dict(envelope.get("provenance") or {})
            provenance["pr"] = verdict.pull_request
            envelope = {**envelope, "provenance": provenance}
        initial_state, approval_reason = self._approval_decision(envelope, submitter, identity_login)
        batch_id = envelope.get("batch_id")
        submitter_login = submitter.login if submitter is not None else identity_login
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
                "submitted_by, canary_id, batch_id, submitter_login, envelope_json, idempotency_key, "
                "created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    envelope["task_id"],
                    envelope["runner_id"],
                    envelope["task_class"],
                    envelope["priority"],
                    initial_state,
                    envelope["submitted_at"],
                    envelope["submitted_by"],
                    envelope.get("canary_id"),
                    batch_id,
                    submitter_login,
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
                    new_state=initial_state,
                    detail={"reason": approval_reason} if approval_reason else None,
                )
            )
            row = connection.execute("SELECT * FROM tasks WHERE task_id = ?", (envelope["task_id"],)).fetchone()
        for audit in audits:
            self.database.append_audit_jsonl(audit)
        if initial_state == "pending-approval":
            self._notify_pending_approval(self._task_from_row(row), approval_reason)
        return self._task_from_row(row), True

    def get_task(self, task_id: str) -> dict[str, Any]:
        with self.database.read() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if row is None:
            raise NotFoundError("TASK_NOT_FOUND", f"unknown task: {task_id}")
        return self._task_from_row(row)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        """Return the tasks that share a batch ID, most recent first."""
        with self.database.read() as connection:
            rows = connection.execute(
                "SELECT * FROM tasks WHERE batch_id = ? ORDER BY created_at DESC", (batch_id,)
            ).fetchall()
        if not rows:
            raise NotFoundError("BATCH_NOT_FOUND", f"unknown batch: {batch_id}")
        return {"batch_id": batch_id, "tasks": [self._task_from_row(row) for row in rows]}

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

    def dashboard_status(self) -> dict[str, Any]:
        """Return the sanitized, read-only mailbox summary used by the public dashboard."""
        runners = []
        with self.database.read() as connection:
            for runner in self.registry.list_runners(enabled_only=True):
                total_count = connection.execute(
                    "SELECT COUNT(*) FROM tasks WHERE runner_id=? " "AND state IN ('queued', 'claimed', 'accepted')",
                    (runner["runner_id"],),
                ).fetchone()[0]
                rows = connection.execute(
                    "SELECT task_id, task_class, priority, state, submitted_at, envelope_json "
                    "FROM tasks WHERE runner_id=? AND state IN ('queued', 'claimed', 'accepted') "
                    "ORDER BY CASE task_class WHEN 'manual' THEN 3 WHEN 'canary' THEN 2 ELSE 1 END DESC, "
                    "priority DESC, submitted_at ASC, task_id ASC LIMIT ?",
                    (runner["runner_id"], PUBLIC_DASHBOARD_TASK_LIMIT),
                ).fetchall()
                remote_tasks = []
                for row in rows:
                    task = json.loads(row["envelope_json"])["task"]
                    remote_tasks.append(
                        {
                            "id": row["task_id"],
                            "type": task.get("task_type"),
                            "note": task.get("note"),
                            "source": task.get("source"),
                            "specifier": task.get("specifier"),
                            "state": row["state"],
                            "task_class": row["task_class"],
                            "priority": row["priority"],
                            "submitted_at": row["submitted_at"],
                            "expected_duration_sec": estimate_task_duration_seconds(task),
                        }
                    )
                runners.append(
                    {
                        "runner_id": runner["runner_id"],
                        "total_count": total_count,
                        "returned_count": len(remote_tasks),
                        "truncated": total_count > len(remote_tasks),
                        "expected_duration_complete": total_count == len(remote_tasks),
                        "expected_duration_sec": sum(task["expected_duration_sec"] for task in remote_tasks),
                        "remote_tasks": remote_tasks,
                    }
                )
        return {"generated_at": utc_text(), "runners": runners}

    # States a caller may still cancel outright (nothing has been handed to a
    # runner yet). Others require the softer cancel-requested path.
    _UNCLAIMED_CANCELLABLE = ("queued", "pending-approval")

    def cancel_task(
        self,
        selector: str,
        *,
        actor: str,
        identity_login: Optional[str] = None,
        is_owner: bool = True,
    ) -> tuple[list[dict[str, Any]], int]:
        """Cancel a caller's tasks by task ID or batch ID.

        A caller cancels their own tasks; an owner (or an operator, the default)
        may cancel anyone's. An unclaimed task (queued or pending-approval) is
        cancelled outright; a task already handed to a runner (claimed,
        accepted) is marked cancel-requested so the runner stops at its next
        boundary. Terminal tasks are left unchanged. Returns the affected task
        rows and a count of state changes.
        """
        audits = []
        changed = 0
        with self.database.transaction(immediate=True) as connection:
            rows = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ? OR batch_id = ?", (selector, selector)
            ).fetchall()
            if not rows:
                raise NotFoundError("TASK_NOT_FOUND", f"unknown task or batch: {selector}")
            affected: list[str] = []
            now = utc_text()
            for row in rows:
                if not is_owner and identity_login is not None and row["submitter_login"] != identity_login:
                    # A non-owner may only cancel their own tasks; skip others in a batch.
                    continue
                state = row["state"]
                if state in self._UNCLAIMED_CANCELLABLE:
                    new_state = "cancelled"
                elif state in {"claimed", "accepted"}:
                    new_state = "cancel-requested"
                else:
                    # cancelled, cancel-requested, completed, failed: nothing to do.
                    continue
                connection.execute(
                    "UPDATE tasks SET state=?, updated_at=? WHERE task_id=? AND state=?",
                    (new_state, now, row["task_id"], state),
                )
                audits.append(
                    self.database.insert_audit(
                        connection,
                        actor=actor,
                        action="task.cancel" if new_state == "cancelled" else "task.cancel_requested",
                        task_id=row["task_id"],
                        runner_id=row["runner_id"],
                        old_state=state,
                        new_state=new_state,
                    )
                )
                affected.append(row["task_id"])
                changed += 1
            placeholders = ",".join("?" for _ in {row["task_id"] for row in rows})
            updated_rows = connection.execute(
                f"SELECT * FROM tasks WHERE task_id IN ({placeholders})",
                tuple({row["task_id"] for row in rows}),
            ).fetchall()
        for audit in audits:
            self.database.append_audit_jsonl(audit)
        return [self._task_from_row(row) for row in updated_rows], changed

    def list_mine(self, login: str, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return the tasks a login submitted, most recent first."""
        if limit < 1 or limit > 250:
            raise ControlError("PAGINATION_INVALID", "limit must be 1-250")
        with self.database.read() as connection:
            rows = connection.execute(
                "SELECT * FROM tasks WHERE submitter_login = ? ORDER BY created_at DESC LIMIT ?",
                (login, limit),
            ).fetchall()
        return [self._task_from_row(row, include_envelope=False) for row in rows]

    def pending_tasks(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return tasks awaiting approval, oldest first."""
        with self.database.read() as connection:
            rows = connection.execute(
                "SELECT * FROM tasks WHERE state='pending-approval' ORDER BY created_at ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._task_from_row(row) for row in rows]

    def approve_tasks(self, selector: str, *, actor: str) -> tuple[list[dict[str, Any]], int]:
        """Move pending-approval tasks (by task or batch ID) into the queue."""
        return self._resolve_approval(selector, actor=actor, decision="approve")

    def reject_tasks(self, selector: str, *, actor: str) -> tuple[list[dict[str, Any]], int]:
        """Reject pending-approval tasks (by task or batch ID)."""
        return self._resolve_approval(selector, actor=actor, decision="reject")

    def _resolve_approval(self, selector: str, *, actor: str, decision: str) -> tuple[list[dict[str, Any]], int]:
        new_state = "queued" if decision == "approve" else "cancelled"
        action = "task.approve" if decision == "approve" else "task.reject"
        audits = []
        changed = 0
        with self.database.transaction(immediate=True) as connection:
            rows = connection.execute(
                "SELECT * FROM tasks WHERE (task_id = ? OR batch_id = ?) AND state='pending-approval'",
                (selector, selector),
            ).fetchall()
            if not rows:
                raise NotFoundError("PENDING_NOT_FOUND", f"no pending-approval task or batch: {selector}")
            now = utc_text()
            ids = []
            for row in rows:
                connection.execute(
                    "UPDATE tasks SET state=?, updated_at=? WHERE task_id=? AND state='pending-approval'",
                    (new_state, now, row["task_id"]),
                )
                audits.append(
                    self.database.insert_audit(
                        connection,
                        actor=actor,
                        action=action,
                        task_id=row["task_id"],
                        runner_id=row["runner_id"],
                        old_state="pending-approval",
                        new_state=new_state,
                    )
                )
                ids.append(row["task_id"])
                changed += 1
            placeholders = ",".join("?" for _ in ids)
            updated_rows = connection.execute(
                f"SELECT * FROM tasks WHERE task_id IN ({placeholders})", tuple(ids)
            ).fetchall()
        for audit in audits:
            self.database.append_audit_jsonl(audit)
        return [self._task_from_row(row) for row in updated_rows], changed

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
                    "ORDER BY CASE task_class WHEN 'manual' THEN 3 WHEN 'canary' THEN 2 ELSE 1 END DESC, "
                    "priority DESC, submitted_at ASC, task_id ASC LIMIT 1",
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
        canary_metadata = None
        with self.database.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("TASK_NOT_FOUND", f"unknown task: {task_id}")
            self._verify_task_runner(row, runner_id)
            if row["state"] in {"completed", "failed"}:
                if row["state"] == terminal and row["outcome_json"] == canonical:
                    return self._task_from_row(row), False
                raise ConflictError("OUTCOME_CONFLICT", "task already has a different terminal outcome")
            if row["state"] not in {"accepted", "cancel-requested"}:
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
            # Capture canary metadata for post-transaction ingestion
            if row["task_class"] == "canary" and terminal == "completed" and row["canary_id"]:
                canary_metadata = {
                    "canary_id": row["canary_id"],
                    "envelope_json": row["envelope_json"],
                }
            updated = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if audit:
            self.database.append_audit_jsonl(audit)
        # Ingest canary observation outside the main transaction
        if canary_metadata:
            self._ingest_canary_observation(task_id, runner_id, outcome, canary_metadata)
        # Mirror the outcome to the results JSONL and publish a static task JSON
        # for the dashboard and agents. Both are best effort and never fail the
        # outcome path.
        self._publish_outcome(self._task_from_row(updated))
        return self._task_from_row(updated), True

    def _publish_outcome(self, task: dict[str, Any]) -> None:
        """Persist the completed task as JSONL and a static per-task JSON file.

        The control-plane SQLite row is authoritative; this adds an append-only
        results mirror and a public static file under the published tasks
        directory (served without auth, since results are public). Failures are
        logged and never propagate.
        """
        record = {
            "task_id": task["task_id"],
            "runner_id": task["runner_id"],
            "batch_id": task.get("batch_id"),
            "submitter_login": task.get("submitter_login"),
            "state": task["state"],
            "completed_at": task.get("completed_at"),
            "envelope": task.get("envelope"),
            "outcome": task.get("outcome"),
        }
        if self.results_jsonl_path is not None:
            try:
                self.results_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
                with self.results_jsonl_path.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(record, sort_keys=True) + "\n")
            except OSError:
                logger.warning("unable to append results JSONL for task %s", task["task_id"], exc_info=True)
        if self.published_tasks_dir is not None:
            try:
                tasks_dir = self.published_tasks_dir / "tasks"
                tasks_dir.mkdir(parents=True, exist_ok=True)
                target = tasks_dir / f"{task['task_id']}.json"
                temporary = target.with_suffix(".json.tmp")
                temporary.write_text(json.dumps(record, sort_keys=True), encoding="utf-8")
                temporary.replace(target)
            except OSError:
                logger.warning("unable to publish task JSON for task %s", task["task_id"], exc_info=True)

    def _ingest_canary_observation(
        self,
        task_id: str,
        runner_id: str,
        outcome: dict[str, Any],
        canary_metadata: dict[str, Any],
    ) -> None:
        """Best-effort canary drift analysis ingestion after outcome recording.

        Failures here are logged but do not affect the task completion path.
        """
        try:
            canary_id = canary_metadata["canary_id"]
            # canary_id format: "profile_id:utc_date"
            parts = canary_id.rsplit(":", 1)
            if len(parts) != 2:
                logger.warning("malformed canary_id %r on task %s: expected 'profile:date'", canary_id, task_id)
                return
            profile_id, utc_date = parts

            # Validate non-empty after split
            if not profile_id:
                logger.warning("empty profile_id from canary_id %r on task %s", canary_id, task_id)
                return
            if not utc_date:
                logger.warning("empty utc_date from canary_id %r on task %s", canary_id, task_id)
                return

            # Resolve profile_version structurally from the CanaryProfileRegistry.
            # Fall back to regex on the note field for legacy/test compatibility.
            profile_version = 1  # final fallback default
            if self._canary_profiles is not None:
                profile = self._canary_profiles.get(profile_id)
                if profile is not None:
                    profile_version = profile.profile_version
                else:
                    # Profile not in registry; try regex on note as legacy fallback
                    profile_version = self._parse_version_from_note(canary_metadata)
            else:
                # No registry available; use regex fallback
                profile_version = self._parse_version_from_note(canary_metadata)

            # Extract environment fingerprint from outcome result
            result = outcome.get("result") or {}
            environment = result.get("environment")

            # Build provenance-enriched environment including available outcome fields
            enriched_env = dict(environment) if environment else {}
            provenance_sv = result.get("provenance_schema_version")
            if provenance_sv is not None:
                enriched_env["provenance_schema_version"] = provenance_sv
            enriched_env["runner_id"] = result.get("runner_id", runner_id)
            # The host the server ran on, when it is not the runner host
            # (g4net): its kernel, glibc, microcode and boot id are what the
            # score depends on, so they join the fingerprint as server_* keys.
            server_env = result.get("server_environment")
            if isinstance(server_env, dict) and not server_env.get("same_host", False):
                for key in ("kernel_release", "glibc_version", "cpu_microcode", "boot_id", "host_fingerprint"):
                    if key in server_env:
                        enriched_env[f"server_{key}"] = server_env[key]
            # Prefer result provenance; fall back to the fleet manifest.
            platform_id = result.get("platform")
            if not platform_id:
                try:
                    runner = self.registry.get_runner(runner_id, require_enabled=False)
                    platform_id = runner.get("platform")
                except (KeyError, ValueError):
                    platform_id = None
            if platform_id:
                enriched_env["platform"] = platform_id

            self.drift_analyzer.ingest_outcome(
                task_id=task_id,
                runner_id=runner_id,
                outcome=outcome,
                profile_id=profile_id,
                profile_version=profile_version,
                utc_date=utc_date,
                environment=enriched_env if enriched_env else None,
            )
        except Exception:
            logger.warning(
                "canary drift analysis ingestion failed for task %s",
                task_id,
                exc_info=True,
            )

    @staticmethod
    def _parse_version_from_note(canary_metadata: dict[str, Any]) -> int:
        """Regex fallback: extract version from note field for tests/legacy."""
        envelope = json.loads(canary_metadata.get("envelope_json", "{}"))
        note = envelope.get("task", {}).get("note", "")
        # Note format: "canary {profile_id} v{version} ({date})"
        if " v" in note:
            try:
                version_str = note.split(" v")[1].split(" ")[0].split(")")[0]
                return int(version_str)
            except (IndexError, ValueError):
                pass
        return 1

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

    # ------------------------------------------------------------------
    # Canary status (read-only, no mutations)
    # ------------------------------------------------------------------

    @staticmethod
    def _project_latest_observation(raw: dict[str, Any]) -> dict[str, Any]:
        """Project a raw DB observation row into a stable API shape.

        Only the explicitly listed fields are exposed — no raw DB columns leak.
        ``environment_json`` is parsed with a corrupt-guard (log + None + parse
        error flag).  ``actionable`` is always cast to ``bool``.
        """
        # Parse environment safely
        env_json = raw.get("environment_json")
        environment = None
        env_parse_error = False
        if env_json:
            try:
                environment = json.loads(env_json)
            except (json.JSONDecodeError, TypeError):
                logger.warning("corrupt environment_json for task %s", raw.get("task_id"))
                env_parse_error = True

        return {
            "utc_date": raw.get("utc_date"),
            "task_id": raw.get("task_id"),
            "score": raw.get("score"),
            "completed_at": raw.get("completed_at"),
            "phase": raw.get("phase"),
            "ref_median": raw.get("ref_median"),
            "ref_mad": raw.get("ref_mad"),
            "delta_pct": raw.get("delta_pct"),
            "series_ordinal": raw.get("series_ordinal"),
            "ref_sample_count": raw.get("ref_sample_count"),
            "candidate_warning_pct": raw.get("candidate_warning_pct"),
            "candidate_alarm_pct": raw.get("candidate_alarm_pct"),
            "candidate_signal": raw.get("candidate_signal"),
            "actionable": bool(raw.get("actionable")),
            "window_start": raw.get("window_start"),
            "window_end": raw.get("window_end"),
            "environment": environment,
            "environment_parse_error": env_parse_error,
            "env_change_annotation": raw.get("env_change_annotation"),
            "provenance_schema_version": raw.get("provenance_schema_version"),
        }

    def canary_status(self, runner_id: Optional[str] = None) -> dict[str, Any]:
        """Return read-only canary status for all enabled runners (or one).

        Composes manifest/profile info, schedule state, drift series summary,
        latest accepted observation, and calibration report when available.
        No mutations, no scheduler ticks, no runner traffic.
        """
        if runner_id:
            runners = [self.registry.get_runner(runner_id, require_enabled=False)]
        else:
            # Enabled runners first, then every archive (a closed series of a
            # live runner stays readable, it just never gets new observations).
            runners = self.registry.list_runners(enabled_only=True)
            runners += [r for r in self.registry.list_runners() if r.get("archived")]

        result = []
        for runner in runners:
            rid = runner["runner_id"]
            profile_id = runner.get("canary_profile")

            entry: dict[str, Any] = {
                "runner_id": rid,
                "display_name": runner.get("display_name"),
                "platform": runner.get("platform"),
                "enabled": runner.get("enabled", False),
                "canary_profile": profile_id,
                "archived": bool(runner.get("archived")),
            }
            if runner.get("archived"):
                entry["archive_of"] = runner.get("archive_of")
                entry["archived_until"] = runner.get("archived_until")
                entry["archive_note"] = runner.get("archive_note")
            else:
                entry["archives"] = [
                    {
                        "runner_id": a["runner_id"],
                        "archived_until": a.get("archived_until"),
                        "archive_note": a.get("archive_note"),
                    }
                    for a in self.registry.archives_of(rid)
                ]

            if not profile_id:
                entry["profile"] = None
                entry["schedule"] = None
                entry["series"] = None
                entry["latest_observation"] = None
                entry["calibration_report"] = None
                entry["semantics"] = "no-profile"
                result.append(entry)
                continue

            # Resolve profile metadata
            profile = self._canary_profiles.get(profile_id) if self._canary_profiles is not None else None
            if profile is None:
                entry["profile"] = {"profile_id": profile_id, "status": "unknown"}
                entry["schedule"] = None
                entry["series"] = None
                entry["latest_observation"] = None
                entry["calibration_report"] = None
                entry["semantics"] = "unknown-profile"
                result.append(entry)
                continue

            entry["profile"] = {
                "profile_id": profile.profile_id,
                "profile_version": profile.profile_version,
                "description": profile.data.get("description"),
                "source": profile.source,
                "pinned_commit": profile.pinned_commit,
                "status": "active",
            }

            # Recent schedule entries (bounded)
            schedule_entries = self._canary_schedule_entries(rid, profile.profile_id)
            entry["schedule"] = schedule_entries

            # Drift series summary
            series = self.drift_analyzer.get_series_summary(
                rid,
                profile.profile_id,
                profile.profile_version,
            )
            entry["series"] = series

            # Latest accepted observation — stable projection, no raw DB columns
            raw_latest = series.get("latest_observation")
            if raw_latest is not None:
                entry["latest_observation"] = self._project_latest_observation(raw_latest)
            else:
                entry["latest_observation"] = None

            # Calibration report
            cal_report = self.drift_analyzer.get_calibration_report(
                rid,
                profile.profile_id,
                profile.profile_version,
            )
            if cal_report is not None:
                # Return the structured report, not the raw row
                entry["calibration_report"] = cal_report.get("report")
            else:
                entry["calibration_report"] = None

            # Determine human-readable semantics
            phase = series.get("phase", "no-data")
            if phase == "no-data":
                entry["semantics"] = "observation-only"
            elif phase == "observation":
                entry["semantics"] = "observation-only"
            elif phase == "calibrating":
                entry["semantics"] = "calibrating"
            elif phase == "ready":
                if cal_report and cal_report.get("report", {}).get("status") == "ready-for-review":
                    entry["semantics"] = "ready-for-review"
                else:
                    entry["semantics"] = "non-actionable"
            else:
                entry["semantics"] = "non-actionable"

            result.append(entry)

        return {"generated_at": utc_text(), "runners": result}

    def _canary_schedule_entries(self, runner_id: str, profile_id: str, *, limit: int = 7) -> list[dict[str, Any]]:
        """Return recent canary_schedule rows with task state annotation."""
        with self.database.read() as conn:
            rows = conn.execute(
                "SELECT cs.runner_id, cs.profile_id, cs.utc_date, cs.state, "
                "cs.task_id, cs.created_at, cs.updated_at, "
                "t.state AS task_state "
                "FROM canary_schedule cs "
                "LEFT JOIN tasks t ON cs.task_id = t.task_id "
                "WHERE cs.runner_id = ? AND cs.profile_id = ? "
                "ORDER BY cs.utc_date DESC LIMIT ?",
                (runner_id, profile_id, limit),
            ).fetchall()
        entries = []
        for row in rows:
            entries.append(
                {
                    "utc_date": row["utc_date"],
                    "state": row["state"],
                    "task_id": row["task_id"],
                    "task_state": row["task_state"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )
        return entries

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
