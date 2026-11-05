"""Runner-side pull mailbox, active only at task boundaries."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from .config import CONDUCTRESS_FAILED_LOG, CONDUCTRESS_OUTPUT
from .delivery_journal import DeliveryJournal
from .fleet_client import FleetClient, FleetClientError
from .outcome_size import DEFAULT_MAX_OUTCOME_BYTES, is_permanent_rejection, trim_outcome, wire_bytes
from .runner_identity import get_runner_config
from .tail_reader import find_record_by_task_id
from .task_queue import BaseTaskData, TaskQueue

logger = logging.getLogger(__name__)

# Environment override for the client-side outcome budget (bytes).
MAX_OUTCOME_BYTES_ENV = "CONDUCTRESS_MAX_OUTCOME_BYTES"

# Error prefix of an outcome downgraded to ``failed`` because the control
# service refused the real outcome. Also what tells the ladder that the
# failed report itself was the last step.
OUTCOME_REJECTED_PREFIX = "outcome_rejected:"


def _utc_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _max_outcome_bytes_from_env() -> int:
    raw = os.environ.get(MAX_OUTCOME_BYTES_ENV)
    if not raw:
        return DEFAULT_MAX_OUTCOME_BYTES
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{MAX_OUTCOME_BYTES_ENV} must be an integer number of bytes") from exc
    if value < 1024:
        raise ValueError(f"{MAX_OUTCOME_BYTES_ENV} must be at least 1024")
    return value


class RunnerMailbox:
    """Own at most one remotely accepted task; never contacts control during execution.

    Outcome size: an outcome is measured before it is sent and trimmed to
    ``max_outcome_bytes`` (bulk result keys dropped, biggest first). A
    permanent rejection by the control service (HTTP 413, 400, 422) is never
    retried with the same bytes; see :meth:`flush_pending_outcome` for the
    ladder. The full result row always stays on disk.
    """

    def __init__(
        self,
        journal_path: Path,
        *,
        mode: str = "off",
        client_factory: Callable[[], FleetClient] = FleetClient.from_runner_env,
        max_outcome_bytes: Optional[int] = None,
    ):
        if mode not in {"off", "shadow", "live"}:
            raise ValueError(f"invalid fleet mode: {mode}")
        self.mode = mode
        self.runner_id = get_runner_config().runner_id
        self.journal = DeliveryJournal(journal_path)
        self._client_factory = client_factory
        self.max_outcome_bytes = max_outcome_bytes if max_outcome_bytes is not None else _max_outcome_bytes_from_env()
        if self.max_outcome_bytes < 1024:
            raise ValueError("max_outcome_bytes must be at least 1024")

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
        # Supply the trusted envelope task_id so that the deserialized task
        # carries the authoritative identity (e.g. deterministic canary IDs)
        # rather than the timestamp-derived default from the inner document.
        try:
            task = BaseTaskData.from_dict(task_document, envelope_task_id=task_id)
        except (ValueError, TypeError) as exc:
            # The envelope body cannot be turned into a runnable task on this
            # deployment (unknown task type, or a field this code cannot honour).
            # Left unhandled the mailbox would raise on every reconcile and wedge
            # the runner. Report the task as failed, drop it from the local queue,
            # and clear the active slot so the runner can move on.
            self._fail_unloadable_task(task_id, str(exc))
            queue.remove_task(task_id)
            return None
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
                queue.import_task(task_document, envelope_task_id=task_id)
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

    # Result fields dropped before the outcome is pushed to the control plane.
    # These are the unbounded stack arrays a result can carry: the jemalloc
    # per-frame ``raw_stacks`` (hundreds to thousands of frames) and the
    # collapsed CPU flamegraph stacks. Every scalar, aggregate, the memory
    # scalars and the categorized ``breakdown`` stay. Dropped both at the top
    # level and inside each per-run entry under ``data.results``. The full
    # stacks remain on the runner and in the published artifacts; the control
    # plane keeps the bounded record.
    _RESULT_OMIT_KEYS = ("raw_stacks", "cpu_stacks_main", "cpu_stacks_io")

    @classmethod
    def _summarize_result(cls, result: dict[str, Any]) -> dict[str, Any]:
        """Return the full result record with only the unbounded stacks removed.

        The control plane is the durable home of the result, so the whole record
        travels (scalars, per-rep aggregates, the memory scalars and the
        jemalloc ``breakdown``). Only the unbounded stack arrays -- the jemalloc
        ``raw_stacks`` and the collapsed CPU flamegraph stacks -- are stripped,
        top level and within each ``data.results`` entry.
        """
        summary = {key: value for key, value in result.items() if key not in cls._RESULT_OMIT_KEYS}
        raw_data = summary.get("data")
        if isinstance(raw_data, dict):
            summary["data"] = cls._strip_raw_stacks(raw_data)
        return summary

    @classmethod
    def _strip_raw_stacks(cls, data: dict[str, Any]) -> dict[str, Any]:
        cleaned = {key: value for key, value in data.items() if key not in cls._RESULT_OMIT_KEYS}
        results = cleaned.get("results")
        if isinstance(results, list):
            cleaned["results"] = [
                (
                    {key: value for key, value in entry.items() if key not in cls._RESULT_OMIT_KEYS}
                    if isinstance(entry, dict)
                    else entry
                )
                for entry in results
            ]
        return cleaned

    def stage_success(self, task: BaseTaskData, *, result: Optional[dict[str, Any]] = None) -> None:
        self._require_active(task.task_id)
        result = result or self._find_result(task.task_id) or {"task_id": task.task_id}
        summary = self._summarize_result(result)
        outcome = {
            "schema_version": 1,
            "task_id": task.task_id,
            "runner_id": self.runner_id,
            "state": "completed",
            "completed_at": _utc_text(),
            "result": summary,
            "error": None,
        }
        outcome = self._preflight_size(outcome)
        self.journal.update_active(stage="outcome_pending", outcome=outcome, rejections=[])

    def _preflight_size(self, outcome: dict[str, Any]) -> dict[str, Any]:
        """Trim an outcome that would exceed the client-side budget before the first send."""
        size = wire_bytes(outcome)
        if size <= self.max_outcome_bytes:
            return outcome
        reason = f"preflight: {size} bytes over max_outcome_bytes {self.max_outcome_bytes}"
        trimmed, dropped = trim_outcome(
            outcome, self.max_outcome_bytes, reason=reason, full_record_path=str(CONDUCTRESS_OUTPUT)
        )
        if not dropped:
            logger.warning(
                "outcome for %s is %d bytes (budget %d) and has nothing to trim; sending as is",
                outcome["task_id"],
                size,
                self.max_outcome_bytes,
            )
            return outcome
        self._record_downgrade(outcome["task_id"], "trimmed", reason, dropped, size, wire_bytes(trimmed))
        return trimmed

    def _record_downgrade(
        self, task_id: str, stage: str, reason: str, dropped: list[str], original_bytes: int, sent_bytes: Optional[int]
    ) -> None:
        """Log and persist why an outcome was not delivered in full (shown by ``fleet status``)."""
        logger.warning(
            "outcome for %s downgraded (%s): %s; dropped %s; %d -> %s bytes",
            task_id,
            stage,
            reason,
            ", ".join(dropped) if dropped else "nothing",
            original_bytes,
            sent_bytes if sent_bytes is not None else "none",
        )
        self.journal.update_stats(
            last_outcome_downgrade={
                "task_id": task_id,
                "stage": stage,
                "reason": reason,
                "dropped": list(dropped),
                "original_bytes": original_bytes,
                "sent_bytes": sent_bytes,
                "at": _utc_text(),
            }
        )

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

    def _fail_unloadable_task(self, task_id: str, error: str) -> None:
        """Report a task whose envelope cannot be deserialized as failed.

        Used when the envelope body cannot be turned into a runnable task, so no
        ``BaseTaskData`` instance exists to pass to :meth:`stage_failure`. Stages
        the failure outcome directly against the active task and attempts an
        immediate flush; if the control plane is unreachable the outcome stays
        pending in the journal and flushes on a later boundary.
        """
        self._require_active(task_id)
        outcome = {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": self.runner_id,
            "state": "failed",
            "completed_at": _utc_text(),
            "result": None,
            "error": error,
        }
        self.journal.update_active(stage="outcome_pending", outcome=outcome)
        self.flush_pending_outcome()

    def flush_pending_outcome(self) -> bool:
        """Deliver the staged outcome. True when nothing is pending afterwards.

        A transient error (5xx, unreachable, auth, control-plane state) keeps
        the outcome in the journal for the next boundary, as before. A
        permanent rejection of the payload (HTTP 413, 400, 422) can never
        succeed with the same bytes, so it steps down a ladder instead of
        wedging the runner, each step sent at once:

        1. full outcome -> rejected -> reduced outcome (every bulk result key
           dropped, ``outcome_delivery`` says what and where the full row is);
        2. reduced outcome -> rejected -> ``state: failed`` with an
           ``outcome_rejected:`` error naming the code, the size and the local
           file that keeps the full result;
        3. that failure report -> rejected -> journal cleared, the downgrade
           record is the only trace.

        Each step is persisted before it is sent, so a restart resumes at the
        same step. The full result row stays on disk at every step.
        """
        while True:
            active = self.journal.active
            if active is None or active["stage"] != "outcome_pending":
                return True
            try:
                self._contact(lambda client: client.report_outcome(active["task_id"], active["outcome"]))
            except FleetClientError as exc:
                if not is_permanent_rejection(exc.status):
                    return False
                self._downgrade_rejected_outcome(active, exc)
                continue
            self.journal.clear_active()
            return True

    def _downgrade_rejected_outcome(self, active: dict[str, Any], exc: FleetClientError) -> None:
        """Re-stage the next ladder step for an outcome the control service refused."""
        task_id = active["task_id"]
        outcome = active["outcome"]
        size = wire_bytes(outcome)
        rejection = f"{exc.code} (HTTP {exc.status}), {size} bytes"
        rejections = list(active.get("rejections") or []) + [
            {"code": exc.code, "status": exc.status, "message": exc.message, "bytes": size, "at": _utc_text()}
        ]
        completed = outcome.get("state") == "completed"
        record_path = str(CONDUCTRESS_OUTPUT if completed else CONDUCTRESS_FAILED_LOG)

        if completed and len(rejections) == 1:
            reduced, dropped = trim_outcome(outcome, 0, reason=f"rejected: {rejection}", full_record_path=record_path)
            if dropped:
                self._record_downgrade(task_id, "reduced", f"rejected: {rejection}", dropped, size, wire_bytes(reduced))
                self.journal.update_active(outcome=reduced, rejections=rejections)
                return

        error = str(outcome.get("error") or "")
        if completed or not error.startswith(OUTCOME_REJECTED_PREFIX):
            message = f"{OUTCOME_REJECTED_PREFIX} {rejection}; full result kept at {record_path}"
            if not completed:
                message = f"{OUTCOME_REJECTED_PREFIX} {rejection}; original error: {error[:200]}; record kept at {record_path}"
            failed = {
                "schema_version": 1,
                "task_id": task_id,
                "runner_id": self.runner_id,
                "state": "failed",
                "completed_at": _utc_text(),
                "result": None,
                "error": message,
            }
            self._record_downgrade(task_id, "failed", message, [], size, wire_bytes(failed))
            self.journal.update_active(outcome=failed, rejections=rejections)
            return

        # The failure report was refused as well. Nothing smaller exists;
        # clear the slot so the runner can claim again. The row is on disk.
        reason = f"dropped: {rejection}; record kept at {record_path}"
        logger.error("outcome for %s could not be delivered in any form: %s", task_id, reason)
        self._record_downgrade(task_id, "dropped", reason, [], size, None)
        self.journal.clear_active()

    def push_status(self, status: dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        try:
            self._contact(lambda client: client.push_status(self.runner_id, status))
        except FleetClientError:
            return False
        return True

    def cancel_requested(self, task_id: str) -> bool:
        """Return True if the control plane has asked this task to stop.

        A claimed or accepted task can be marked ``cancel-requested`` on the
        control plane while the runner owns it. The runner calls this at a
        rep/cell boundary and, when it is True, stops the task and reports the
        partial outcome. Any control-plane error is treated as "not requested"
        so a transient outage never aborts a running benchmark; the check is
        re-evaluated at the next boundary, so it is restart-safe.
        """
        if not self.enabled or not self.owns(task_id):
            return False
        try:
            document = self._contact(lambda client: client.task(task_id))
        except FleetClientError:
            return False
        task = (document or {}).get("task") or {}
        return task.get("state") == "cancel-requested"

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
            "max_outcome_bytes": self.max_outcome_bytes,
            # Why the last outcome was not delivered in full (trimmed, reduced,
            # failed, dropped); None when every outcome went out whole.
            "last_outcome_downgrade": stats.get("last_outcome_downgrade"),
        }

    def _require_active(self, task_id: str) -> dict[str, Any]:
        active = self.journal.active
        if active is None or active["task_id"] != task_id:
            raise ValueError(f"task is not the active remote delivery: {task_id}")
        return active

    @staticmethod
    def _find_result(task_id: str) -> Optional[dict[str, Any]]:
        return find_record_by_task_id(CONDUCTRESS_OUTPUT, task_id)

    @staticmethod
    def _find_failure(task_id: str) -> Optional[dict[str, Any]]:
        return find_record_by_task_id(CONDUCTRESS_FAILED_LOG, task_id)
