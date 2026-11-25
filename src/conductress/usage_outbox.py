"""Usage records a runner has not yet delivered to the control service.

A runner appends one record after each task it ran from its own queue and
tries to deliver at the next boundary. A failed delivery keeps the record for
the next try, so an outage never blocks a task and never loses a record
(until the cap below). The file is only a delivery queue: a record that was
delivered twice is stored once by the control service.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

# shortcut: the oldest record is dropped past this many, which only happens after
# days without a reachable control service; raise it if usage gaps matter more.
MAX_PENDING = 500


class UsageOutbox:
    def __init__(self, path: Path, *, max_pending: int = MAX_PENDING):
        self.path = path
        self.max_pending = max_pending

    def pending(self) -> list[dict[str, Any]]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except (OSError, json.JSONDecodeError):
            logger.warning("usage outbox %s is unreadable; starting it again", self.path)
            return []
        return data if isinstance(data, list) else []

    def add(self, record: dict[str, Any]) -> None:
        records = self.pending() + [record]
        if len(records) > self.max_pending:
            logger.warning("usage outbox is full; dropping %d oldest record(s)", len(records) - self.max_pending)
            records = records[-self.max_pending :]
        self._write(records)

    def flush(self, send: Callable[[dict[str, Any]], Any]) -> int:
        """Send pending records in order. Stop at the first failure and keep the rest. Returns the count sent."""
        records = self.pending()
        sent = 0
        for record in records:
            try:
                send(record)
            except Exception as exc:  # best effort: any failure keeps the record for the next boundary
                logger.warning("usage record for %s not delivered: %s", record.get("task_id"), exc)
                break
            sent += 1
        if sent:
            self._write(records[sent:])
        return sent

    def _write(self, records: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=self.path.name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(records, handle)
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
