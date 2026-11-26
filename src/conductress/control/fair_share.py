"""Fair-share choice of the next task for one runner (multi-agent queue note, section 4.4).

Class and priority decide first; this module only chooses between logins whose next
task has the same class and priority. Each login is charged the runner time it used
recently (decayed with a half-life) plus the expected duration of its next task. The
lowest charge goes first. Everything here is pure: the claim path reads the rows and
passes them in, so the maths is tested without a database.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

# A usage row older than this many half-lives weighs under 0.4 percent and is ignored.
CUTOFF_HALF_LIVES = 8


@dataclass(frozen=True)
class Head:
    """The first-in-line queued task of one login."""

    agent: str
    task_id: str
    submitted_at: str
    expected_seconds: float


def decayed_seconds(seconds: float, ended_at: datetime, now: datetime, half_life_seconds: float) -> float:
    """Runner seconds of one finished task, weighted by how long ago it ended. A future end counts in full."""
    age = max(0.0, (now - ended_at).total_seconds())
    if age > CUTOFF_HALF_LIVES * half_life_seconds:
        return 0.0
    return seconds * math.pow(2.0, -age / half_life_seconds)


def recent_usage(
    rows: Iterable[tuple[str, float, datetime]], now: datetime, half_life_seconds: float
) -> dict[str, float]:
    """Decayed runner seconds per login from ``(login, runner_seconds, ended_at)`` rows."""
    usage: dict[str, float] = {}
    for agent, seconds, ended_at in rows:
        usage[agent] = usage.get(agent, 0.0) + decayed_seconds(seconds, ended_at, now, half_life_seconds)
    return usage


def choose_head(heads: list[Head], usage: dict[str, float]) -> Optional[Head]:
    """The head to claim next, or None when there is nothing to choose between.

    Score is ``usage + expected duration``; the lowest wins, then the older submit time,
    then the task ID, so the choice is deterministic. With no recorded usage for any
    candidate the plain first-in-first-out order is kept: ``heads`` must be in that order.
    """
    if not heads:
        return None
    if not any(usage.get(head.agent, 0.0) > 0.0 for head in heads):
        return heads[0]
    return min(
        heads, key=lambda head: (usage.get(head.agent, 0.0) + head.expected_seconds, head.submitted_at, head.task_id)
    )
