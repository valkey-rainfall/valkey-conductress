"""Build versioned remote task envelopes from existing task dataclasses."""

from __future__ import annotations

import getpass
from datetime import datetime, timezone
from typing import Any, Optional

from .task_queue import BaseTaskData

# Queue priority of a task submitted without an explicit value. The control
# service clamps every non-operator submission to this value.
DEFAULT_PRIORITY = 100


def _default_submitter() -> str:
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return "unknown"


def serialize_task(task: BaseTaskData) -> dict[str, Any]:
    data = task.to_document()
    data["timestamp"] = task.timestamp.isoformat()
    return data


def build_submitter(login: str, kind: str, sponsor: Optional[str] = None) -> dict[str, Any]:
    """Build the envelope ``submitter`` block.

    ``kind`` is ``human`` or ``agent``; an agent names the human account it acts
    for in ``sponsor`` so quota and provenance resolve to that account.
    """
    if kind not in {"human", "agent"}:
        raise ValueError("submitter kind must be 'human' or 'agent'")
    return {"login": login, "kind": kind, "sponsor": sponsor}


def build_provenance(
    repo: str,
    sha: str,
    *,
    recipe: Optional[str] = None,
    pr: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build the envelope ``provenance`` block.

    ``pr`` is either ``None`` or ``{"repo", "number", "head_sha"}`` identifying
    the open pull request whose head this ``sha`` matches.
    """
    return {"repo": repo, "sha": sha, "recipe": recipe, "pr": pr}


def build_task_envelope(
    task: BaseTaskData,
    *,
    runner_id: str,
    priority: int = DEFAULT_PRIORITY,
    submitted_by: Optional[str] = None,
    submitter: Optional[dict[str, Any]] = None,
    provenance: Optional[dict[str, Any]] = None,
    batch_id: Optional[str] = None,
) -> dict[str, Any]:
    submitted_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    envelope: dict[str, Any] = {
        "schema_version": 1,
        "task_id": task.task_id,
        "runner_id": runner_id,
        "task_class": "manual",
        "priority": priority,
        "submitted_at": submitted_at,
        "submitted_by": submitted_by or _default_submitter(),
        "canary_id": None,
        "batch_id": batch_id,
        "submitter": submitter,
        "provenance": provenance,
        "task": serialize_task(task),
    }
    return envelope
