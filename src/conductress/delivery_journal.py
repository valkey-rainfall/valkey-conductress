"""Durable state for one accepted remote task and mailbox health."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

JOURNAL_SCHEMA_VERSION = 1


def _default_state() -> dict[str, Any]:
    return {
        "schema_version": JOURNAL_SCHEMA_VERSION,
        "active": None,
        "stats": {
            "imported_count_total": 0,
            "last_imported_task_id": None,
            "last_poll_utc": None,
            "last_poll_result": None,
            "claim_failures_consecutive": 0,
            "last_control_contact_utc": None,
            "last_control_latency_ms": None,
            "control_reachable": None,
            "last_error": None,
        },
    }


class DeliveryJournal:
    def __init__(self, path: Path):
        self.path = path
        self._state = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return _default_state()
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("schema_version") != JOURNAL_SCHEMA_VERSION:
            raise ValueError(f"unsupported delivery journal: {self.path}")
        if not isinstance(data.get("stats"), dict) or "active" not in data:
            raise ValueError(f"invalid delivery journal: {self.path}")
        return data

    @property
    def active(self) -> Optional[dict[str, Any]]:
        active = self._state["active"]
        return dict(active) if active is not None else None

    @property
    def stats(self) -> dict[str, Any]:
        return dict(self._state["stats"])

    def set_active(self, active: dict[str, Any]) -> None:
        self._state["active"] = active
        self._write()

    def update_active(self, **changes: Any) -> None:
        if self._state["active"] is None:
            raise ValueError("delivery journal has no active task")
        self._state["active"].update(changes)
        self._write()

    def clear_active(self) -> None:
        self._state["active"] = None
        self._write()

    def update_stats(self, **changes: Any) -> None:
        self._state["stats"].update(changes)
        self._write()

    def record_import(self, task_id: str) -> None:
        stats = self._state["stats"]
        stats["imported_count_total"] = int(stats.get("imported_count_total", 0)) + 1
        stats["last_imported_task_id"] = task_id
        self._write()

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                json.dump(self._state, temporary, indent=2, sort_keys=True)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.replace(temporary_path, self.path)
            os.chmod(self.path, 0o600)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
