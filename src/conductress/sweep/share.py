"""Reserved share of runner time for the sweep while it has work that must not wait (note, section 4.5).

The runner keeps two decayed totals of the seconds it spent on tasks: sweep tasks and everything
else. ``fraction`` is the sweep's part. While the sweep has need and its part is under the
configured share, the runner starts a sweep task before it claims remote work. Everything here is
pure (time is passed in), so the rule is tested without a runner. The share is 0 by default,
which means the rule never fires and the order is the one of earlier versions.
"""

from __future__ import annotations

# Half-life of the window, the same as the fair-share window of the control service.
HALF_LIFE_SECONDS = 24 * 3600.0


class ShareWindow:
    """Decayed runner seconds of sweep tasks and of all other tasks on this runner.

    shortcut: the totals live in memory and start at zero when the runner starts. Seed them from
    the result rows (``end_time``, ``observed_duration_sec``, and the ``-sweep`` marker in
    ``note``) if a runner restarts so often that a fresh window matters.
    """

    def __init__(self, half_life_seconds: float = HALF_LIFE_SECONDS) -> None:
        self._half_life = half_life_seconds
        self._sweep = 0.0
        self._other = 0.0
        self._at = 0.0

    def _decay_to(self, now: float) -> None:
        if now > self._at:
            factor = 0.5 ** ((now - self._at) / self._half_life)
            self._sweep *= factor
            self._other *= factor
            self._at = now

    def add(self, seconds: float, *, is_sweep: bool, now: float) -> None:
        """Count a task that just ended and used ``seconds`` of runner time."""
        self._decay_to(now)
        if is_sweep:
            self._sweep += max(0.0, seconds)
        else:
            self._other += max(0.0, seconds)

    def fraction(self, now: float) -> float:
        """The sweep's part of the recent runner time, 0.0 when nothing was recorded."""
        self._decay_to(now)
        total = self._sweep + self._other
        return self._sweep / total if total > 0 else 0.0


def sweep_goes_first(share_percent: float, has_need: bool, fraction: float) -> bool:
    """True when the sweep is owed its turn: a share is set, there is need, and the sweep is under its share."""
    return share_percent > 0 and has_need and fraction < share_percent / 100.0
