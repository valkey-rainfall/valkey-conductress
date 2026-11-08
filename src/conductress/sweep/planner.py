"""Sweep planner: adaptive hierarchical bisection for performance tracking.

The planner maintains a priority queue of segments to investigate and decides
which commit to benchmark next based on the expected information gain.

Priority order:
1. New HEAD on unstable (nightly tracking)
2. Release commits not yet benchmarked (mandatory landmarks)
3. Active bisection (narrowing a detected regression/improvement)
4. Largest unresolved historical segment (backfill)

A planner built with ``tracks_history=False`` stops after step 2: it measures
the release landmarks it is given and the tip, the tip at most once per
``tip_interval_seconds``, and never bisects or backfills.  That is the shape of
a comparison engine whose history nobody reads.
"""

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Optional


class PointStatus(Enum):
    """Status of a benchmarked commit."""

    COMPLETED = auto()
    BUILD_FAILED = auto()
    PENDING = auto()


class TaskPriority(Enum):
    """Priority levels for sweep tasks, lower value = higher priority."""

    NIGHTLY = 1
    LANDMARK = 2
    BISECTION = 3
    BACKFILL = 4


@dataclass
class BenchmarkPoint:
    """A single benchmarked commit with its result."""

    commit: str
    date: str  # ISO format YYYY-MM-DD
    value: Optional[float] = None  # metric value (rps, bytes_per_key, etc.)
    cv: Optional[float] = None
    reps: int = 3
    # Min/max of the per-rep score series behind ``value``.  Populated for
    # cachecannon v3 points so the dashboard can show the between-restart
    # spread the score was drawn from; None on every other series and on
    # points recorded before the fields existed.
    score_min: Optional[float] = None
    score_max: Optional[float] = None
    # Load-generator (client) CPU utilization behind ``value``.  Populated for
    # cachecannon v3 points from the result row's ``client_cpu`` block so the
    # published series can say whether a point measured the server or the load
    # generator; None on every other series and on points recorded before the
    # fields existed.  ``client_saturated`` is true when the generator ran at or
    # above CLIENT_CPU_SATURATION_THRESHOLD of its allocated cores, i.e. the
    # measured throughput reflects the client's capacity, not the server's.
    client_cores_busy: Optional[float] = None
    client_allocated_cores: Optional[int] = None
    client_utilization: Optional[float] = None
    client_saturated: Optional[bool] = None
    pr: Optional[int] = None
    pr_title: Optional[str] = None
    perf_counters: Optional[dict[str, int]] = None  # raw perf stat counters (process-wide)
    perf_counters_main: Optional[dict[str, int]] = None  # raw perf stat counters, main thread only
    perf_counters_io: Optional[dict[str, int]] = None  # raw perf stat counters, IO threads summed
    perf_duration_seconds: Optional[float] = None  # perf stat measurement window (one rep)
    perf_rps: Optional[float] = None  # throughput during perf stat collection
    perf_rep_count: Optional[int] = None  # reps whose raw counters were summed into perf_counters*
    # Privilege scope of the perf stat counters ("user+kernel", "user", ...).
    # None means the point predates scope recording and was counted user-only
    # (perf stat ran unprivileged before PR #196); absolute counts and IPC are
    # not comparable across scopes, so the exporter tags every point with it.
    perf_counters_scope: Optional[str] = None
    breakdown: Optional[dict[str, float]] = None  # per-category memory breakdown (bytes/key)
    raw_stacks: Optional[list[list]] = None  # retained resolved stacks for re-categorization
    latency_data: Optional[dict] = None  # full latency results (p50, p99.9, histogram, rps)
    cpu_stacks_main: Optional[list[list]] = None  # per-thread collapsed stacks [[stack, count], ...]
    cpu_stacks_io: Optional[list[list]] = None  # IO thread collapsed stacks [[stack, count], ...]
    # Why this commit was measured: "release" (a landmark), "tip" (HEAD when
    # measured) or "history" (bisection / backfill).  None on points recorded
    # before the field existed.
    sample: Optional[str] = None
    # The task that produced ``value`` (its result row's task_id). Lets the
    # exporter honour the task exclusion list (sweep/generations.py). None on
    # points recorded before the field existed.
    task_id: Optional[str] = None
    # The reduced environment of the result row (runner_identity.environment_for_period)
    # and the row's end time, so the exporter can draw environment period
    # markers. None on points recorded before the fields existed.
    environment: Optional[dict[str, Any]] = None
    measured_at: Optional[str] = None
    status: PointStatus = PointStatus.PENDING

    @property
    def is_complete(self) -> bool:
        return self.status == PointStatus.COMPLETED and self.value is not None


@dataclass
class Landmark:
    """A release commit that must always be benchmarked."""

    commit: str
    date: str
    label: str  # e.g. "8.0.0"


@dataclass
class Segment:
    """A range between two benchmarked commits that may contain a performance change."""

    left_commit: str
    right_commit: str
    left_value: float
    right_value: float
    commit_count: int  # number of merge commits between left and right (exclusive)
    left_cv: float = 0.0
    right_cv: float = 0.0
    left_reps: int = 3
    right_reps: int = 3

    @property
    def delta(self) -> float:
        """Relative performance change from left to right."""
        if self.left_value == 0:
            return 0.0
        return (self.right_value - self.left_value) / self.left_value

    @property
    def abs_delta(self) -> float:
        return abs(self.delta)

    @property
    def noise_floor(self) -> float:
        """Minimum detectable change given the CV of both endpoints (as fraction)."""
        return max(self.left_cv, self.right_cv) / 100.0

    @property
    def is_significant(self) -> bool:
        """Check if the delta is statistically significant using a Welch's t-test approximation.

        Returns True if the difference between endpoints is significant at p<0.05,
        or if we lack enough information to compute significance (conservative fallback).
        """
        if self.left_reps < 2 or self.right_reps < 2:
            # Can't compute variance with <2 reps; fall back to threshold-based check
            return self.abs_delta >= max(0.02, self.noise_floor)

        # Standard error from CV: SE = mean * CV% / 100 / sqrt(n)
        left_se = self.left_value * (self.left_cv / 100.0) / math.sqrt(self.left_reps)
        right_se = self.right_value * (self.right_cv / 100.0) / math.sqrt(self.right_reps)

        pooled_se = math.sqrt(left_se**2 + right_se**2)
        if pooled_se == 0:
            return self.abs_delta >= 0.02  # Both CV=0; use threshold only

        t_stat = abs(self.right_value - self.left_value) / pooled_se

        # Welch-Satterthwaite degrees of freedom
        df: float
        if left_se == 0 or right_se == 0:
            df = max(self.left_reps, self.right_reps) - 1
        else:
            df = (left_se**2 + right_se**2) ** 2 / (
                left_se**4 / (self.left_reps - 1) + right_se**4 / (self.right_reps - 1)
            )

        # Approximate t critical value for p<0.05 two-tailed
        # For df >= 3: use 2.5 as a conservative approximation (exact ranges from 3.18 at df=3 to 1.96 at df=∞)
        t_crit = (
            3.18
            if df <= 3
            else 2.78 if df <= 5 else 2.45 if df <= 7 else 2.23 if df <= 10 else 2.09 if df <= 20 else 2.0
        )  # noqa: E501

        return t_stat > t_crit


@dataclass
class SweepTask:
    """A task to benchmark a specific commit."""

    commit: str
    date: str
    priority: TaskPriority
    reason: str  # human-readable explanation
    pr: Optional[int] = None


@dataclass
class SweepState:
    """Complete state of the sweep planner, persisted to disk."""

    points: dict[str, BenchmarkPoint] = field(default_factory=dict)
    landmarks: list[Landmark] = field(default_factory=list)
    # Ordered list of all merge commits (oldest first)
    merge_commits: list[str] = field(default_factory=list)
    # Commit -> date mapping for all known commits
    commit_dates: dict[str, str] = field(default_factory=dict)
    # Commit -> PR number (from squash merge subject)
    commit_prs: dict[str, int] = field(default_factory=dict)
    # Commit -> PR title / commit subject
    commit_titles: dict[str, str] = field(default_factory=dict)
    # Last known HEAD that was benchmarked
    last_benchmarked_head: Optional[str] = None
    # Unix time the last tip measurement was recorded (spacing for release-and-tip engines)
    last_benchmarked_head_at: Optional[float] = None
    # Threshold for bisection (relative, e.g. 0.01 = 1%)
    threshold: float = 0.02

    def save(self, path: Path) -> None:
        """Serialize sweep state to a JSON file."""
        import json

        data = {
            "threshold": self.threshold,
            "last_benchmarked_head": self.last_benchmarked_head,
            "last_benchmarked_head_at": self.last_benchmarked_head_at,
            "merge_commits": self.merge_commits,
            "commit_dates": self.commit_dates,
            "commit_prs": self.commit_prs,
            "commit_titles": self.commit_titles,
            "landmarks": [{"commit": lm.commit, "date": lm.date, "label": lm.label} for lm in self.landmarks],
            "points": {
                commit: {
                    "commit": p.commit,
                    "date": p.date,
                    "value": p.value,
                    "cv": p.cv,
                    "reps": p.reps,
                    "score_min": p.score_min,
                    "score_max": p.score_max,
                    "client_cores_busy": p.client_cores_busy,
                    "client_allocated_cores": p.client_allocated_cores,
                    "client_utilization": p.client_utilization,
                    "client_saturated": p.client_saturated,
                    "pr": p.pr,
                    "pr_title": p.pr_title,
                    "perf_counters": p.perf_counters,
                    "perf_counters_main": p.perf_counters_main,
                    "perf_counters_io": p.perf_counters_io,
                    "perf_duration_seconds": p.perf_duration_seconds,
                    "perf_rps": p.perf_rps,
                    "perf_rep_count": p.perf_rep_count,
                    "perf_counters_scope": p.perf_counters_scope,
                    "breakdown": p.breakdown,
                    "raw_stacks": p.raw_stacks,
                    "latency_data": p.latency_data,
                    "cpu_stacks_main": p.cpu_stacks_main,
                    "cpu_stacks_io": p.cpu_stacks_io,
                    "sample": p.sample,
                    "task_id": p.task_id,
                    "environment": p.environment,
                    "measured_at": p.measured_at,
                    "status": p.status.name,
                }
                for commit, p in self.points.items()
            },
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2))

    @classmethod
    def load(cls, path: Path) -> "SweepState":
        """Deserialize sweep state from a JSON file. Returns empty state if missing."""
        import json

        if not path.exists():
            return cls()

        data = json.loads(path.read_text())
        state = cls(
            threshold=data.get("threshold", 0.02),
            last_benchmarked_head=data.get("last_benchmarked_head"),
            last_benchmarked_head_at=data.get("last_benchmarked_head_at"),
            merge_commits=data.get("merge_commits", []),
            commit_dates=data.get("commit_dates", {}),
            commit_prs=data.get("commit_prs", {}),
            commit_titles=data.get("commit_titles", {}),
        )

        for lm_data in data.get("landmarks", []):
            state.landmarks.append(
                Landmark(
                    commit=lm_data["commit"],
                    date=lm_data["date"],
                    label=lm_data["label"],
                )
            )

        for commit, p_data in data.get("points", {}).items():
            state.points[commit] = BenchmarkPoint(
                commit=p_data["commit"],
                date=p_data.get("date", ""),
                value=p_data.get("value", p_data.get("rps")),
                cv=p_data.get("cv"),
                reps=p_data.get("reps", 3),
                score_min=p_data.get("score_min"),
                score_max=p_data.get("score_max"),
                client_cores_busy=p_data.get("client_cores_busy"),
                client_allocated_cores=p_data.get("client_allocated_cores"),
                client_utilization=p_data.get("client_utilization"),
                client_saturated=p_data.get("client_saturated"),
                pr=p_data.get("pr"),
                pr_title=p_data.get("pr_title"),
                perf_counters=p_data.get("perf_counters"),
                perf_counters_main=p_data.get("perf_counters_main"),
                perf_counters_io=p_data.get("perf_counters_io"),
                perf_duration_seconds=p_data.get("perf_duration_seconds"),
                perf_rps=p_data.get("perf_rps"),
                perf_rep_count=p_data.get("perf_rep_count"),
                perf_counters_scope=p_data.get("perf_counters_scope"),
                breakdown=p_data.get("breakdown"),
                raw_stacks=p_data.get("raw_stacks"),
                latency_data=p_data.get("latency_data"),
                cpu_stacks_main=p_data.get("cpu_stacks_main"),
                cpu_stacks_io=p_data.get("cpu_stacks_io"),
                sample=p_data.get("sample"),
                task_id=p_data.get("task_id"),
                environment=p_data.get("environment"),
                measured_at=p_data.get("measured_at"),
                status=PointStatus[p_data.get("status", "PENDING")],
            )

        return state


class SweepPlanner:
    """Decides which commit to benchmark next using adaptive hierarchical bisection.

    The planner is pure logic — it takes state in and produces tasks out.
    It does not perform I/O, git operations, or benchmarking itself.
    """

    def __init__(
        self,
        state: SweepState,
        tracks_history: bool = True,
        tip_interval_seconds: float = 0.0,
        clock: Callable[[], float] = time.time,
    ):
        self.state = state
        self.tracks_history = tracks_history
        self.tip_interval_seconds = tip_interval_seconds
        self._clock = clock
        # Build index for fast commit position lookup
        self._commit_index: dict[str, int] = {c: i for i, c in enumerate(state.merge_commits)}

    def get_next_task(self, current_head: Optional[str] = None) -> Optional[SweepTask]:
        """Determine the highest-priority task to execute next.

        Args:
            current_head: The current HEAD of unstable (for nightly tracking).
                         Pass None to skip nightly check.

        Returns:
            The next SweepTask to execute, or None if all work is done.
        """
        # Priority 1: Nightly tracking — benchmark new HEAD
        task = self._check_nightly(current_head)
        if task:
            return task

        # Priority 2: Landmarks — benchmark release commits (skeleton for bisection)
        task = self._check_landmarks()
        if task:
            return task

        # A release-and-tip planner stops here: nobody reads this engine's
        # history, so there is nothing to bisect or backfill.
        if not self.tracks_history:
            return None

        # Priority 3: Bisection — narrow the largest unresolved segment
        task = self._check_bisection()
        if task:
            return task

        # Priority 4: Backfill — skeleton pass for unsampled regions
        task = self._check_backfill()
        if task:
            return task

        return None

    def record_result(
        self,
        commit: str,
        value: float,
        cv: float,
        reps: int = 3,
        pr: Optional[int] = None,
        pr_title: Optional[str] = None,
        sample: Optional[str] = None,
    ) -> None:
        """Record a benchmark result for a commit.

        ``sample`` says why the commit was measured ("release", "tip",
        "history"); it is stored on the point so the export can tell a
        comparison reader which points are release and tip samples.
        """
        if commit not in self.state.points:
            date = self.state.commit_dates.get(commit, "")
            self.state.points[commit] = BenchmarkPoint(
                commit=commit,
                date=date,
                value=value,
                cv=cv,
                reps=reps,
                pr=pr,
                pr_title=pr_title,
                sample=sample,
                status=PointStatus.COMPLETED,
            )
        else:
            point = self.state.points[commit]
            point.value = value
            point.cv = cv
            point.reps = reps
            point.status = PointStatus.COMPLETED
            if pr is not None:
                point.pr = pr
            if pr_title is not None:
                point.pr_title = pr_title
            if sample is not None:
                point.sample = sample

    def record_build_failure(self, commit: str) -> None:
        """Mark a commit as having a build failure."""
        date = self.state.commit_dates.get(commit, "")
        if commit not in self.state.points:
            self.state.points[commit] = BenchmarkPoint(
                commit=commit,
                date=date,
                status=PointStatus.BUILD_FAILED,
            )
        else:
            self.state.points[commit].status = PointStatus.BUILD_FAILED

    def get_segments(self) -> list[Segment]:
        """Get all segments between adjacent benchmarked points, sorted by |delta| descending."""
        completed = self._get_ordered_completed_points()
        if len(completed) < 2:
            return []

        segments = []
        for i in range(len(completed) - 1):
            left = completed[i]
            right = completed[i + 1]
            commit_count = self._commits_between(left.commit, right.commit)
            if commit_count > 0:  # Only include segments with commits to bisect
                seg = Segment(
                    left_commit=left.commit,
                    right_commit=right.commit,
                    left_value=left.value,  # type: ignore[arg-type]
                    right_value=right.value,  # type: ignore[arg-type]
                    commit_count=commit_count,
                    left_cv=left.cv or 0.0,
                    right_cv=right.cv or 0.0,
                    left_reps=left.reps,
                    right_reps=right.reps,
                )
                segments.append(seg)

        segments.sort(key=lambda s: s.abs_delta, reverse=True)
        return segments

    def get_unresolved_segments(self) -> list[Segment]:
        """Get segments that are statistically significant and have commits to bisect."""
        return [
            s
            for s in self.get_segments()
            if s.abs_delta >= self.state.threshold and s.is_significant and s.commit_count > 0
        ]

    def tip_due(self, current_head: Optional[str]) -> bool:
        """Whether the current tip should be measured now.

        A tip is due when it has not been measured yet and, for a planner with a
        tip interval, the previous tip measurement is at least that old.  The
        interval is what keeps a busy upstream (several merges a day) from
        turning every merge into a full cell set for an engine whose tip only
        needs to be recent, not complete.
        """
        if current_head is None:
            return False
        if current_head == self.state.last_benchmarked_head:
            return False
        if current_head in self.state.points:
            return False
        last_at = self.state.last_benchmarked_head_at
        if self.tip_interval_seconds > 0 and last_at is not None:
            if self._clock() - last_at < self.tip_interval_seconds:
                return False
        return True

    def _check_nightly(self, current_head: Optional[str]) -> Optional[SweepTask]:
        """Check if HEAD needs benchmarking."""
        if not self.tip_due(current_head):
            return None
        assert current_head is not None
        date = self.state.commit_dates.get(current_head, "")
        return SweepTask(
            commit=current_head,
            date=date,
            priority=TaskPriority.NIGHTLY,
            reason="New HEAD on unstable",
        )

    def _check_bisection(self) -> Optional[SweepTask]:
        """Find the largest unresolved segment and pick its midpoint."""
        segments = self.get_unresolved_segments()
        if not segments:
            return None

        # Pick the segment with the largest absolute delta
        segment = segments[0]
        midpoint = self._find_midpoint(segment.left_commit, segment.right_commit)
        if midpoint is None:
            return None

        direction = "regression" if segment.delta < 0 else "improvement"
        return SweepTask(
            commit=midpoint,
            date=self.state.commit_dates.get(midpoint, ""),
            priority=TaskPriority.BISECTION,
            reason=f"Bisecting {abs(segment.delta)*100:.1f}% {direction} "
            f"between {segment.left_commit[:8]} and {segment.right_commit[:8]}",
        )

    def _check_landmarks(self) -> Optional[SweepTask]:
        """Check if any release commits need benchmarking."""
        for landmark in self.state.landmarks:
            if landmark.commit not in self.state.points:
                if landmark.commit in self._commit_index:
                    return SweepTask(
                        commit=landmark.commit,
                        date=landmark.date,
                        priority=TaskPriority.LANDMARK,
                        reason=f"Release {landmark.label}",
                    )
        return None

    def _check_backfill(self) -> Optional[SweepTask]:
        """Find the largest gap in coverage and sample its midpoint."""
        if not self.state.merge_commits:
            return None

        completed = self._get_ordered_completed_points()

        # If we have fewer than 2 points, do the skeleton pass
        if len(completed) < 2:
            return self._skeleton_task()

        # Find the largest gap (by commit count) that hasn't been bisected
        # This includes segments below threshold — we still want coverage
        largest_gap = 0
        best_left = None
        best_right = None

        for i in range(len(completed) - 1):
            left = completed[i]
            right = completed[i + 1]
            gap = self._commits_between(left.commit, right.commit)
            if gap > largest_gap:
                largest_gap = gap
                best_left = left.commit
                best_right = right.commit

        # Also check edges: before first point and after last point
        if completed:
            first_commit = completed[0].commit
            last_commit = completed[-1].commit
            # Count edge commits excluding BUILD_FAILED (consistent with _commits_between)
            first_idx = self._commit_index.get(first_commit, 0)
            edge_before = self._count_unattempted_in_range(0, first_idx)
            if edge_before > largest_gap:
                largest_gap = edge_before
                best_left = None
                best_right = first_commit
            last_idx = self._commit_index.get(last_commit, 0)
            edge_after = self._count_unattempted_in_range(last_idx + 1, len(self.state.merge_commits))
            if edge_after > largest_gap:
                largest_gap = edge_after
                best_left = last_commit
                best_right = None

        if largest_gap == 0:
            return None

        midpoint = self._find_midpoint_by_index(best_left, best_right)
        if midpoint is None:
            return None

        return SweepTask(
            commit=midpoint,
            date=self.state.commit_dates.get(midpoint, ""),
            priority=TaskPriority.BACKFILL,
            reason=f"Backfill: {largest_gap} commits in gap",
        )

    def _skeleton_task(self) -> Optional[SweepTask]:
        """Pick the next skeleton point (evenly spaced across history)."""
        if not self.state.merge_commits:
            return None

        # Find the first and last commits that aren't benchmarked
        total = len(self.state.merge_commits)
        # Target ~16 skeleton points
        step = max(1, total // 16)

        for i in range(0, total, step):
            commit = self.state.merge_commits[i]
            if commit not in self.state.points:
                return SweepTask(
                    commit=commit,
                    date=self.state.commit_dates.get(commit, ""),
                    priority=TaskPriority.BACKFILL,
                    reason="Skeleton pass",
                )

        # Also check the last commit
        last = self.state.merge_commits[-1]
        if last not in self.state.points:
            return SweepTask(
                commit=last,
                date=self.state.commit_dates.get(last, ""),
                priority=TaskPriority.BACKFILL,
                reason="Skeleton pass (last commit)",
            )

        return None

    def _get_ordered_completed_points(self) -> list[BenchmarkPoint]:
        """Get all completed points ordered by their position in merge_commits.

        A point whose task is on the exclusion list (sweep/generations.py) is
        left out here, so neither the exporters nor the bisection ever see it.
        """
        from conductress.sweep.generations import excluded_task_ids  # local: generations imports SweepState

        excluded = excluded_task_ids()
        completed = [
            p
            for p in self.state.points.values()
            if p.is_complete and p.commit in self._commit_index and not (p.task_id and p.task_id in excluded)
        ]
        completed.sort(key=lambda p: self._commit_index[p.commit])
        return completed

    def _commits_between(self, left: str, right: str) -> int:
        """Count merge commits strictly between left and right (exclusive)."""
        left_idx = self._commit_index.get(left)
        right_idx = self._commit_index.get(right)
        if left_idx is None or right_idx is None:
            return 0
        # Exclude build-failed commits from the count
        count = 0
        for i in range(left_idx + 1, right_idx):
            commit = self.state.merge_commits[i]
            point = self.state.points.get(commit)
            if point is None or point.status != PointStatus.BUILD_FAILED:
                count += 1
        return count

    def _count_unattempted_in_range(self, start_idx: int, end_idx: int) -> int:
        """Count commits in [start_idx, end_idx) that have not been attempted (point is None)."""
        count = 0
        for i in range(start_idx, end_idx):
            commit = self.state.merge_commits[i]
            if self.state.points.get(commit) is None:
                count += 1
        return count

    def _find_midpoint(self, left: str, right: str) -> Optional[str]:
        """Find the middle commit between left and right, skipping build failures."""
        return self._find_midpoint_by_index(left, right)

    def _find_midpoint_by_index(self, left: Optional[str], right: Optional[str]) -> Optional[str]:
        """Find midpoint between two commits (either can be None for edges)."""
        left_idx = self._commit_index[left] if left else -1
        right_idx = self._commit_index[right] if right else len(self.state.merge_commits)

        # Collect valid candidates (not already benchmarked, not build-failed)
        candidates = []
        for i in range(left_idx + 1, right_idx):
            commit = self.state.merge_commits[i]
            point = self.state.points.get(commit)
            if point is None:  # Not yet attempted
                candidates.append(i)

        if not candidates:
            return None

        # Pick the middle candidate
        mid_idx = candidates[len(candidates) // 2]
        return self.state.merge_commits[mid_idx]
