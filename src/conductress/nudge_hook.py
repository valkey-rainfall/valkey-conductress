"""HTTP webhook subscriber that sends push notifications to an external dashboard."""

import json
import logging
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from .config import CONDUCTRESS_OUTPUT
from .task_queue import BaseTaskData

logger = logging.getLogger(__name__)

# Attributes available on PerfTaskData that are useful to include in nudges
_TASK_ATTRS = (
    "test",
    "val_size",
    "io_threads",
    "pipelining",
    "warmup",
    "duration",
    "replicas",
    "note",
)


class NudgeHook:
    """HTTP webhook subscriber that sends push notifications to an external dashboard.

    When a benchmark task completes or fails, this hook reads the latest result from
    the output log and POSTs a JSON payload to the configured endpoint URL. This is
    useful for integrating with AI dashboards (e.g. OpenMesh) that need to be
    notified of new data without polling.

    Args:
        endpoint_url: HTTP(S) endpoint to POST nudge payloads to.
        events: Set of event type strings that trigger nudges.
            Supported values: "completed", "failed", "empty".
            Defaults to all three.
    """

    def __init__(self, endpoint_url: str, events: Optional[set[str]] = None) -> None:
        self._endpoint_url = endpoint_url
        self._events: set[str] = events if events is not None else {"completed", "failed", "empty"}
        # Tracks whether an "empty" nudge has already been fired for the current
        # idle period. Reset to False whenever a task completes/failed, so the
        # next on_queue_empty call represents a genuine non-empty -> empty
        # transition.
        self._empty_nudged: bool = False
        logger.info(
            "NudgeHook initialized: endpoint=%s, events=%s",
            endpoint_url,
            self._events,
        )

    def on_task_completed(self, task: BaseTaskData) -> None:
        """Send an HTTP POST with results when a task completes successfully."""
        # A task just ran — the queue was non-empty. Clear the empty-nudge flag
        # unconditionally (even if "completed" is not in the subscribed events set)
        # so that a subsequent on_queue_empty call can fire once.
        self._empty_nudged = False
        if "completed" in self._events:
            payload = self._build_task_payload("completed", task)
            self._send(payload)

    def on_task_failed(self, task: BaseTaskData) -> None:
        """Send an HTTP POST with task info when a task fails."""
        # A task just ran — the queue was non-empty. Clear the empty-nudge flag
        # unconditionally (even if "failed" is not in the subscribed events set)
        # so that a subsequent on_queue_empty call can fire once.
        self._empty_nudged = False
        if "failed" in self._events:
            payload = self._build_task_payload("failed", task)
            self._send(payload)

    def on_queue_empty(self) -> None:
        """Send a lightweight HTTP POST when the task queue is empty.

        Fires at most once per transition from non-empty to empty. During idle
        polling (every QUEUE_POLL_INTERVAL) the runner repeatedly calls
        on_queue_empty; without dedupe this would spam the endpoint with
        duplicate "empty" nudges every few seconds.
        """
        if "empty" not in self._events:
            return
        if self._empty_nudged:
            logger.debug("Queue still empty — skipping duplicate 'empty' nudge")
            return
        self._empty_nudged = True
        payload = {
            "event": "empty",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._send(payload)

    def _build_task_payload(self, event_type: str, task: BaseTaskData) -> dict:
        """Construct the JSON payload for a task-completion nudge."""
        payload: dict = {
            "event": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "task_id": task.task_id,
            "source": task.source,
            "specifier": task.specifier,
            "note": task.note,
            "task_type": task.task_type,
        }
        # Include PerfTaskData-specific attributes if present
        for attr in _TASK_ATTRS:
            if hasattr(task, attr):
                payload[attr] = getattr(task, attr)
        # Read latest result from the output log
        result = self._read_latest_result(task.task_id)
        if result:
            payload["score"] = result.get("score")
            payload["commit_hash"] = result.get("commit_hash")
            if result.get("data"):
                payload["data"] = result["data"]
        return payload

    @staticmethod
    def _read_latest_result(task_id: str) -> Optional[dict]:
        """Read the latest result record matching *task_id* from the output log.

        Performs a full scan of the output log. This is acceptable at current
        scale: the log is appended to once per benchmark run (≈ every 30-60 s)
        and typically holds fewer than a few thousand lines. If the log grows
        significantly in the future, consider indexing or caching.
        """
        try:
            latest_match: Optional[dict] = None
            with open(CONDUCTRESS_OUTPUT, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        if record.get("task_id") == task_id:
                            latest_match = record
                    except (json.JSONDecodeError, KeyError):
                        continue
            return latest_match
        except (FileNotFoundError, PermissionError):
            return None

    def _send(self, payload: dict) -> None:
        """POST *payload* as JSON to the configured endpoint URL.

        Delegates the actual HTTP request to a daemon thread so that a slow or
        unresponsive endpoint cannot stall the benchmark runner. The inner
        request has a short timeout so the thread does not linger indefinitely.
        """
        thread = threading.Thread(target=self._do_send, args=(payload,), daemon=True)
        thread.start()

    def _do_send(self, payload: dict) -> None:
        """Perform the actual HTTP POST (called from a background thread)."""
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self._endpoint_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=3) as response:
                logger.debug(
                    "Nudge sent to %s: HTTP %s",
                    self._endpoint_url,
                    response.status,
                )
        except urllib.error.URLError as e:
            logger.warning("Nudge HTTP error to %s: %s", self._endpoint_url, e)
        except Exception as e:
            logger.warning("Failed to send nudge to %s: %s", self._endpoint_url, e)
