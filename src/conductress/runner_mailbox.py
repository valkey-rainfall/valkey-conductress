"""Runner-side pull mailbox, active only at task boundaries."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from .config import CONDUCTRESS_FAILED_LOG, CONDUCTRESS_OUTPUT
from .delivery_journal import DeliveryJournal
from .fleet_client import FleetClient, FleetClientError
from .runner_identity import get_runner_config
from .task_queue import BaseTaskData, TaskQueue


def _utc_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class RunnerMailbox:
    """Own at most one remotely accepted task; never contacts control during execution."""

    def __init__(
        self,
        journal_path: Path,
        *,
        mode: str = "off",
        client_factory: Callable[[], FleetClient] = FleetClient.from_runner_env,
    ):
        if mode not in {"off", "shadow", "live"}:
            raise ValueError(f"invalid fleet mode: {mode}")
        self.mode = mode
        self.runner_id = get_runner_config().runner_id
        self.journal = DeliveryJournal(journal_path)
        self._client_factory = client_factory

    @property
    def enabled(self) -> bool:
        return self.mode != "off"

    def _contact(self, action: Callable[[FleetClient], Any]) -> Any:
        started = time.monotonic()
        try:
            result = action(self._client_factory())
        except FleetClientError as exc:
            stats = self.journal.stats
            self.journal.update_stats(
                control_reachable=False,
                last_error=f"{exc.code}: {exc.message}",
                claim_failures_consecutive=int(stats.get("claim_failures_consecutive", 0)) + 1,
                last_control_latency_ms=round((time.monotonic() - started) * 1000, 1),
            )
            raise
        self.journal.update_stats(
            control_reachable=True,
            last_control_contact_utc=_utc_text(),
            last_control_latency_ms=round((time.monotonic() - started) * 1000, 1),
            last_error=None,
            claim_failures_consecutive=0,
        )
        return result

    def owns(self, task_id: str) -> bool:
        active = self.journal.active
        return active is not None and active["task_id"] == task_id

    def blocks_execution(self) -> bool:
        active = self.journal.active
        return active is not None and active["stage"] in {"claimed", "imported"}

    def reconcile(self, queue: TaskQueue) -> Optional[BaseTaskData]:
        """Recover any claimed/imported/accepted task before requesting another."""
        active = self.journal.active
        if active is None:
            return None
        task_id = active["task_id"]
        stage = active["stage"]

        if stage == "outcome_pending":
            self.flush_pending_outcome()
            return None

        task_document = active["envelope"]["task"]
        task = BaseTaskData.from_dict(task_document)
        if task.task_id != task_id:
            raise ValueError(f"journal task ID mismatch: {task_id} != {task.task_id}")

        result = self._find_result(task_id)
        if result is not None:
            self.stage_success(task, result=result)
            queue.remove_task(task_id)
            self.flush_pending_outcome()
            return None
        failure = self._find_failure(task_id)
        if failure is not None:
            self.stage_failure(task, failure["error"])
            queue.remove_task(task_id)
            self.flush_pending_outcome()
            return None

        if stage == "claimed":
            if not queue.has_task(task_id):
                queue.import_task(task_document)
            self.journal.update_active(stage="imported", imported_at=_utc_text())
            self.journal.record_import(task_id)
            stage = "imported"

        if stage in {"claimed", "imported"}:
            if not queue.has_task(task_id):
                raise RuntimeError(f"claimed task is missing from local queue: {task_id}")
            try:
                self._contact(lambda client: client.accept_task(task_id, active["claim_token"]))
            except FleetClientError as exc:
                if exc.code in {
                    "CLAIM_EXPIRED",
                    "CLAIM_TOKEN_INVALID",
                    "TASK_NOT_CLAIMED",
                }:
                    queue.remove_task(task_id)
                    self.journal.clear_active()
                return None
            self.journal.update_active(stage="accepted", accepted_at=_utc_text())

        if not queue.has_task(task_id):
            raise RuntimeError(f"accepted task is missing from local queue and results: {task_id}")
        return task

    def poll(self, queue: TaskQueue) -> Optional[BaseTaskData]:
        """Claim at most one task when local work is empty."""
        recovered = self.reconcile(queue)
        if recovered is not None or self.journal.active is not None:
            return recovered
        if self.mode != "live" or queue.get_queue_length() > 0:
            return None

        self.journal.update_stats(last_poll_utc=_utc_text())
        try:
            document = self._contact(lambda client: client.claim_task(self.runner_id))
        except FleetClientError:
            self.journal.update_stats(last_poll_result="error")
            return None
        if document is None:
            self.journal.update_stats(last_poll_result="no_tasks")
            return None

        claim = document["claim"]
        task_detail = claim["task"]
        envelope = task_detail["envelope"]
        task_id = task_detail["task_id"]
        if envelope["task_id"] != task_id or envelope["runner_id"] != self.runner_id:
            raise ValueError("claimed envelope identity does not match runner/task")
        active = {
            "task_id": task_id,
            "runner_id": self.runner_id,
            "stage": "claimed",
            "claim_token": claim["claim_token"],
            "lease_expires": claim["lease_expires"],
            "envelope": envelope,
            "claimed_at": _utc_text(),
        }
        self.journal.set_active(active)
        self.journal.update_stats(last_poll_result="claimed")
        return self.reconcile(queue)

    def stage_success(self, task: BaseTaskData, *, result: Optional[dict[str, Any]] = None) -> None:
        self._require_active(task.task_id)
        result = result or self._find_result(task.task_id) or {"task_id": task.task_id}
        summary = {
            key: result.get(key)
            for key in ("task_id", "method", "score", "commit_hash", "end_time", "note")
            if result.get(key) is not None
        }
        outcome = {
            "schema_version": 1,
            "task_id": task.task_id,
            "runner_id": self.runner_id,
            "state": "completed",
            "completed_at": _utc_text(),
            "result": summary,
            "error": None,
        }
        self.journal.update_active(stage="outcome_pending", outcome=outcome)

    def stage_failure(self, task: BaseTaskData, error: str) -> None:
        self._require_active(task.task_id)
        outcome = {
            "schema_version": 1,
            "task_id": task.task_id,
            "runner_id": self.runner_id,
            "state": "failed",
            "completed_at": _utc_text(),
            "result": None,
            "error": error,
        }
        self.journal.update_active(stage="outcome_pending", outcome=outcome)

    def flush_pending_outcome(self) -> bool:
        active = self.journal.active
        if active is None or active["stage"] != "outcome_pending":
            return True
        try:
            self._contact(lambda client: client.report_outcome(active["task_id"], active["outcome"]))
        except FleetClientError:
            return False
        self.journal.clear_active()
        return True

    def push_status(self, status: dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        try:
            self._contact(lambda client: client.push_status(self.runner_id, status))
        except FleetClientError:
            return False
        return True

    def status(self) -> dict[str, Any]:
        active = self.journal.active
        stats = self.journal.stats
        return {
            "mode": self.mode,
            "control_reachable": stats.get("control_reachable"),
            "last_control_contact_utc": stats.get("last_control_contact_utc"),
            "last_control_latency_ms": stats.get("last_control_latency_ms"),
            "last_error": stats.get("last_error"),
            "last_claim_result": stats.get("last_poll_result"),
            "last_poll_utc": stats.get("last_poll_utc"),
            "claim_failures_consecutive": stats.get("claim_failures_consecutive", 0),
            "accepted_task_id": active["task_id"] if active else None,
            "active_stage": active["stage"] if active else None,
            "pending_outcomes_count": 1 if active and active["stage"] == "outcome_pending" else 0,
            "delivery_journal_depth": 1 if active else 0,
            "imported_count_total": stats.get("imported_count_total", 0),
            "last_imported_task_id": stats.get("last_imported_task_id"),
        }

    def _require_active(self, task_id: str) -> dict[str, Any]:
        active = self.journal.active
        if active is None or active["task_id"] != task_id:
            raise ValueError(f"task is not the active remote delivery: {task_id}")
        return active

    @staticmethod
    def _recent_lines(path: Path, max_bytes: int = 1024 * 1024) -> list[str]:
        if not path.exists():
            return []
        with path.open("rb") as stream:
            stream.seek(0, 2)
            end = stream.tell()
            start = max(0, end - max_bytes)
            stream.seek(start)
            data = stream.read()
        lines = data.splitlines()
        if start > 0 and lines:
            lines = lines[1:]
        return [line.decode("utf-8", errors="replace") for line in lines]

    @staticmethod
    def _find_result(task_id: str) -> Optional[dict[str, Any]]:
        if not CONDUCTRESS_OUTPUT.exists():
            return None
        for line in reversed(RunnerMailbox._recent_lines(CONDUCTRESS_OUTPUT)):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("task_id") == task_id:
                return record
        return None

    @staticmethod
    def _find_failure(task_id: str) -> Optional[dict[str, Any]]:
        if not CONDUCTRESS_FAILED_LOG.exists():
            return None
        for line in reversed(RunnerMailbox._recent_lines(CONDUCTRESS_FAILED_LOG)):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("task_id") == task_id:
                return record
        return None
