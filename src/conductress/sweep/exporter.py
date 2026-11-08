"""Exporter: generates series.json for the dashboard from sweep state."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

from conductress.config import ANNOTATION_THRESHOLD
from conductress.heap_profiler import recategorize_from_stacks
from conductress.sweep.planner import BenchmarkPoint, PointStatus, Segment, SweepPlanner, SweepState

if TYPE_CHECKING:
    from conductress.config import SweepEngine

# =============================================================================
# Perf metric definitions and normalization
# =============================================================================

PERF_METRICS: dict[str, dict[str, Any]] = {
    "ipc": {
        "label": "IPC",
        "unit": "instructions/cycle",
        "compute": lambda c, **_: c["instructions"] / c["cycles"] if c.get("cycles") else None,
    },
    "instructions-per-req": {
        "label": "Instructions per Request",
        "unit": "instructions/request",
        # Raw counters are SUMMED across all collected reps (see task_perf_benchmark
        # run loop), but rps/duration describe a single rep, so the summed instruction
        # total must be divided by the rep count to recover per-request instructions.
        # Omitting this caused a bimodal artifact (value scaled linearly with rep
        # count: 3x for fixed 3-rep runs, up to 10x for adaptive runs). Unlike ratio
        # metrics (IPC, MPKI, stall%), the rep factor does not cancel here.
        "compute": lambda c, rps=0, duration=0, reps=1, **_: (
            c["instructions"] / (rps * duration * (reps or 1)) if rps and duration and c.get("instructions") else None
        ),
    },
    "cycles-per-req": {
        "label": "Cycles per Request",
        "unit": "cycles/request",
        # Same summed-across-reps correction as instructions-per-req. Read the two
        # together: a commit that changes cycles/request while instructions/request
        # stays flat changed how the same work is fetched or scheduled (code layout,
        # cache placement), not how much work is done.
        "compute": lambda c, rps=0, duration=0, reps=1, **_: (
            c["cycles"] / (rps * duration * (reps or 1)) if rps and duration and c.get("cycles") else None
        ),
    },
    "icache-mpki": {
        "label": "I-Cache MPKI",
        "unit": "misses/1Ki",
        "compute": lambda c, **_: (
            c["L1-icache-load-misses"] / c["instructions"] * 1000
            if c.get("instructions") and c.get("L1-icache-load-misses")
            else None
        ),
    },
    "branch-mpki": {
        "label": "Branch MPKI",
        "unit": "misses/1Ki",
        "compute": lambda c, **_: (
            c["branch-misses"] / c["instructions"] * 1000 if c.get("instructions") and c.get("branch-misses") else None
        ),
    },
    "frontend-stall-pct": {
        "label": "Frontend Stall %",
        "unit": "%",
        "compute": lambda c, **_: (
            c["stalled-cycles-frontend"] / c["cycles"] * 100
            if c.get("cycles") and c.get("stalled-cycles-frontend")
            else None
        ),
    },
    "backend-stall-pct": {
        "label": "Backend Stall %",
        "unit": "%",
        "compute": lambda c, **_: (
            c["stalled-cycles-backend"] / c["cycles"] * 100
            if c.get("cycles") and c.get("stalled-cycles-backend")
            else None
        ),
    },
    "llc-mpki": {
        "label": "LLC MPKI",
        "unit": "misses/1Ki",
        "compute": lambda c, **_: (
            c["LLC-load-misses"] / c["instructions"] * 1000
            if c.get("instructions") and c.get("LLC-load-misses")
            else None
        ),
    },
    "tma-retiring-pct": {
        "label": "Retiring %",
        "unit": "%",
        "compute": lambda c, **_: (
            c["topdown-retiring"] / c["slots"] * 100 if c.get("slots") and c.get("topdown-retiring") else None
        ),
    },
    "tma-fe-bound-pct": {
        "label": "Frontend Bound %",
        "unit": "%",
        "compute": lambda c, **_: (
            c["topdown-fe-bound"] / c["slots"] * 100 if c.get("slots") and c.get("topdown-fe-bound") else None
        ),
    },
    "tma-be-bound-pct": {
        "label": "Backend Bound %",
        "unit": "%",
        "compute": lambda c, **_: (
            c["topdown-be-bound"] / c["slots"] * 100 if c.get("slots") and c.get("topdown-be-bound") else None
        ),
    },
    "tma-bad-spec-pct": {
        "label": "Bad Speculation %",
        "unit": "%",
        "compute": lambda c, **_: (
            c["topdown-bad-spec"] / c["slots"] * 100 if c.get("slots") and c.get("topdown-bad-spec") else None
        ),
    },
    # Kernel interaction. Both events are software/tracepoint counters that only
    # count under a privileged perf stat (PR #196); rows counted user-only carry
    # 0 for them and are skipped by the falsy guard rather than plotted as zero.
    "syscalls-per-req": {
        "label": "Syscalls per Request",
        "unit": "syscalls/request",
        # Same summed-across-reps correction as instructions-per-req.
        "compute": lambda c, rps=0, duration=0, reps=1, **_: (
            c["raw_syscalls:sys_enter"] / (rps * duration * (reps or 1))
            if rps and duration and c.get("raw_syscalls:sys_enter")
            else None
        ),
    },
    "context-switches-per-sec": {
        "label": "Context Switches per Second",
        "unit": "switches/s",
        "compute": lambda c, duration=0, reps=1, **_: (
            c["context-switches"] / (duration * (reps or 1)) if duration and c.get("context-switches") else None
        ),
    },
}

PERF_GROUPS: list[dict[str, Any]] = [
    {
        "id": "efficiency",
        "title": "Execution Efficiency",
        "series": ["ipc", "instructions-per-req", "cycles-per-req"],
        "y_axes": [
            {"id": "left", "label": "IPC", "series": ["ipc"]},
            {"id": "right", "label": "per request", "series": ["instructions-per-req", "cycles-per-req"]},
        ],
    },
    {
        "id": "cache",
        "title": "Cache Pressure",
        "series": ["icache-mpki", "llc-mpki"],
        "y_axes": [{"id": "left", "label": "misses per 1K instructions", "series": ["icache-mpki", "llc-mpki"]}],
    },
    {
        "id": "pipeline",
        "title": "Pipeline Stalls",
        "series": ["frontend-stall-pct", "backend-stall-pct"],
        "y_axes": [
            {"id": "left", "label": "% of cycles stalled", "series": ["frontend-stall-pct", "backend-stall-pct"]}
        ],
    },
    {
        "id": "tma",
        "title": "Pipeline Breakdown (TMA)",
        "series": ["tma-retiring-pct", "tma-fe-bound-pct", "tma-be-bound-pct", "tma-bad-spec-pct"],
        "y_axes": [
            {
                "id": "left",
                "label": "% of pipeline slots",
                "series": ["tma-retiring-pct", "tma-fe-bound-pct", "tma-be-bound-pct", "tma-bad-spec-pct"],
            }
        ],
    },
    {
        "id": "branching",
        "title": "Branch Prediction",
        "series": ["branch-mpki"],
        "y_axes": [{"id": "left", "label": "misses per 1K instructions", "series": ["branch-mpki"]}],
    },
    # Kernel interaction, split into two single-metric groups so that each pairs
    # cleanly main-vs-io side by side. Syscalls/request is per-request (cancels the
    # rep factor, comparable across commits); context-switches/s is a per-second
    # rate on a different scale — bundling the two on one dual-axis chart broke the
    # visual main/io pairing, so they are now separate groups.
    {
        "id": "syscalls",
        "title": "Syscalls",
        "series": ["syscalls-per-req"],
        "y_axes": [{"id": "left", "label": "syscalls/request", "series": ["syscalls-per-req"]}],
    },
    {
        "id": "context-switches",
        "title": "Context Switches",
        "series": ["context-switches-per-sec"],
        "y_axes": [{"id": "left", "label": "context switches/s", "series": ["context-switches-per-sec"]}],
    },
    {
        "id": "cpu-main",
        "title": "CPU Profile — Main Thread",
        "series": ["cpu-main"],
        "stacked": True,
        "y_axes": [{"id": "left", "label": "% of samples", "series": ["cpu-main"]}],
    },
    {
        "id": "cpu-io",
        "title": "CPU Profile — IO Threads",
        "series": ["cpu-io"],
        "stacked": True,
        "y_axes": [{"id": "left", "label": "% of samples", "series": ["cpu-io"]}],
    },
]

# Counter-based groups that can be split by thread (everything except the
# cpu-main/cpu-io flamegraph groups, which are already thread-specific).
_PER_THREAD_BASE_GROUP_IDS = ("efficiency", "cache", "pipeline", "tma", "branching", "syscalls", "context-switches")
_PER_THREAD_SUFFIXES = (("-main", "Main Thread"), ("-io", "IO Threads"))


def _build_per_thread_groups() -> list[dict[str, Any]]:
    """Derive per-thread (main/io) variants of the counter-based perf groups.

    For each splittable base group (e.g. ``efficiency``) this produces
    ``efficiency-main`` and ``efficiency-io`` groups whose series/axes reference
    the per-thread metric variants (``ipc-main`` / ``ipc-io`` etc.), mirroring the
    cpu-main/cpu-io split. The original process-wide groups are left untouched.
    """
    base_by_id: dict[str, dict[str, Any]] = {g["id"]: g for g in PERF_GROUPS}
    out: list[dict[str, Any]] = []
    for base_id in _PER_THREAD_BASE_GROUP_IDS:
        base = base_by_id.get(base_id)
        if base is None:
            continue
        for suffix, label in _PER_THREAD_SUFFIXES:
            axes: list[dict[str, Any]] = base["y_axes"]
            group: dict[str, Any] = {
                "id": f"{base_id}{suffix}",
                "title": f"{base['title']} — {label}",
                "series": [f"{s}{suffix}" for s in base["series"]],
                "y_axes": [
                    {
                        "id": axis["id"],
                        "label": axis["label"],
                        "series": [f"{s}{suffix}" for s in axis["series"]],
                    }
                    for axis in axes
                ],
                "per_thread": suffix.lstrip("-"),  # "main" | "io" — dashboard hint
            }
            out.append(group)
    return out


PER_THREAD_PERF_GROUPS = _build_per_thread_groups()


def export_series(
    state: SweepState,
    output_path: Path,
    platform: str = "arm64/c7g.metal/graviton3",
    workload: str = "",
    lower_is_better: bool = False,
    num_keys: int = 0,
    repo: str = "valkey-io/valkey",
    branch: str = "unstable",
    include_breakdown: bool = True,
    engine: Optional["SweepEngine"] = None,
    connections: Optional[int] = None,
    client_threads: Optional[int] = None,
    tls: Optional[bool] = None,
    settled: Optional[bool] = None,
) -> None:
    """Export sweep state to dashboard-ready series.json.

    Args:
        state: The current sweep state with benchmark results.
        output_path: Where to write series.json.
        platform: Platform identifier string.
        workload: Workload identifier string.
        num_keys: If provided and point has raw_stacks, recompute breakdown at export time.
        include_breakdown: When False, omit the jemalloc memory breakdown entirely (engines that
            opt out of internal profiling, e.g. Redis, keep total memory but not the decomposition).
        engine: The engine this series measures; names it and its scope in ``metadata`` so a
            reader can tell a full-history series from a release-and-tip one.  None is the
            default Valkey sweep.
        connections: Total client connections the series was measured at; recorded in
            ``metadata`` when provided so a reader knows the client budget the line was
            produced under.  None omits the key (legacy callers).
        client_threads: Client worker threads the series was measured at; recorded in
            ``metadata`` when provided.  The P1 GET series runs 16, the rest 8, and a
            series' client budget is part of its identity -- publishing it lets a reader
            tell the two client shapes apart.  None omits the key.
        tls: Whether the series measured the encrypted (TLS) transport; recorded in
            ``metadata`` when provided so a reader can tell the TLS GET line apart from
            the plaintext GET line of the same shape.  None omits the key (legacy
            callers and every non-TLS series).
        settled: Whether the series' memory points were sampled after any in-progress
            rehash had finished; recorded in ``metadata`` when provided so a reader can
            tell a settled series from one measured as loaded.  None omits the key.
    """
    if not workload:
        from conductress.config import SWEEP_IO_THREADS, SWEEP_PIPELINING, SWEEP_TEST, SWEEP_VAL_SIZE

        workload = f"{SWEEP_TEST.upper()}_{SWEEP_VAL_SIZE}B_t{SWEEP_IO_THREADS}_p{SWEEP_PIPELINING}"

    planner = SweepPlanner(state)
    commit_index = planner._commit_index

    # Build ordered points list
    points: list[dict[str, Any]] = []
    has_breakdown = False
    for point in planner._get_ordered_completed_points():
        entry: dict[str, Any] = {
            "commit": point.commit,
            "date": point.date,
            "commit_index": commit_index.get(point.commit, 0),
            "results": {
                workload: {
                    "rps": point.value,
                    "cv": point.cv,
                    "reps": point.reps,
                }
            },
        }
        # v3 points carry the min/max of the per-rep score series; publish them
        # beside rps/cv/reps so the dashboard can show the between-restart
        # spread. Absent on v1 series (always None), so the keys are omitted
        # rather than exported as null.
        if point.score_min is not None or point.score_max is not None:
            entry["results"][workload]["score_min"] = point.score_min
            entry["results"][workload]["score_max"] = point.score_max
        # Load-generator CPU utilization behind the point.  Published so a reader
        # can tell a server measurement from one that was bound by the client;
        # a saturated point measured the generator, not the server.  Absent on
        # v1 series (always None), so each key is omitted rather than exported
        # as null.
        if point.client_utilization is not None:
            entry["results"][workload]["client_utilization"] = point.client_utilization
        if point.client_saturated is not None:
            entry["results"][workload]["client_saturated"] = point.client_saturated
        if point.client_cores_busy is not None:
            entry["results"][workload]["client_cores_busy"] = point.client_cores_busy
        if point.client_allocated_cores is not None:
            entry["results"][workload]["client_allocated_cores"] = point.client_allocated_cores
        pr = state.commit_prs.get(point.commit) or point.pr
        pr_title = state.commit_titles.get(point.commit)
        if pr is not None:
            entry["pr"] = pr
        if pr_title is not None:
            entry["pr_title"] = pr_title
        if point.sample is not None:
            entry["sample"] = point.sample
        if point.measured_at is not None:
            entry["measured_at"] = point.measured_at
        if point.environment is not None:
            entry["env"] = point.environment
        if include_breakdown and point.raw_stacks and num_keys > 0:
            entry["breakdown"] = recategorize_from_stacks(point.raw_stacks, num_keys)
            has_breakdown = True
        elif include_breakdown and point.breakdown:
            entry["breakdown"] = point.breakdown
            has_breakdown = True
        points.append(entry)

    # Environment periods: runs of consecutive points measured under one
    # environment, with a boundary (and its label) wherever the recorded
    # provenance changed. Markers only: the series is never split here.
    periods = _build_periods(points, load_environment_events())

    # Build landmarks list
    landmarks = [
        {
            "commit": lm.commit,
            "date": lm.date,
            "label": lm.label,
            "type": "release",
            "commit_index": commit_index.get(lm.commit, 0),
        }
        for lm in state.landmarks
    ]

    # Build annotations from segments that have been bisected down to 1 commit
    annotations = _build_annotations(state, planner, workload, lower_is_better)
    period_of = {p["commit"]: p["period"] for p in points}
    for annotation in annotations:
        left = annotation.pop("_left_commit", None)
        if left is not None and period_of.get(left) != period_of.get(annotation["commit"]):
            # The two points sit in different environment periods: the delta
            # may be the environment, not the commit.
            annotation["env_period_crossed"] = True

    series: dict[str, Any] = {
        "metadata": {
            "repo": repo,
            "branch": branch,
            "platform": platform,
            "workload": workload,
            "engine": engine.source if engine else "valkey",
            "scope": engine.scope if engine else "history",
            "generated": datetime.now(timezone.utc).isoformat(),
            "total_commits": len(state.merge_commits),
        },
        "workloads": [workload],
        "points": points,
        "landmarks": landmarks,
        "annotations": annotations,
        "periods": periods,
    }

    # Add category metadata if any points have breakdown data
    if has_breakdown:
        from conductress.heap_profiler import CATEGORY_NAMES

        series["metadata"]["categories"] = CATEGORY_NAMES

    # Record the client budget the series was measured at, when the caller knows
    # it. A series' connections and client-thread count are part of its identity
    # (the P1 GET series runs 16 threads, the rest 8), so publishing them lets a
    # reader tell two client shapes apart on the dashboard.
    if connections is not None:
        series["metadata"]["connections"] = connections
    if client_threads is not None:
        series["metadata"]["client_threads"] = client_threads
    # The transport the series measured.  A TLS GET series and a plaintext GET
    # series of the same shape publish under different workload labels, so this
    # tells them apart on the dashboard.  None omits the key (every non-TLS
    # series and legacy callers).
    if tls is not None:
        series["metadata"]["tls"] = tls
    # Whether a memory series' points were taken after the data structures had
    # finished any in-progress rehash (see the memory task's ``settle``).  A
    # series that carries no key was measured as loaded, and the two are not
    # comparable on one line.  None omits the key (throughput series).
    if settled is not None:
        series["metadata"]["settled"] = settled

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(series, indent=2))


def _event_bucket(measured_at: Optional[str], events: list[dict[str, Any]]) -> Optional[int]:
    """How many manual environment events precede a measurement (None when its time is unknown)."""
    if not measured_at:
        return None
    try:
        from conductress.sweep.generations import parse_task_time

        moment = parse_task_time(measured_at)
    except ValueError:
        return None
    return sum(1 for e in events if e["at"] <= moment)


def _build_periods(points: list[dict[str, Any]], events: Optional[list[dict[str, Any]]] = None) -> list[dict[str, Any]]:
    """Group ordered point entries into environment periods; set each entry's ``period``.

    Consecutive points whose recorded environments differ (as the canary's
    change annotation judges them, ``environment_changes``) start a new period,
    labelled with the change. A change of boot id alone is a ``boot_only``
    period (a reboot: a possible change, drawn lighter). Manual events from
    ``environment_events.json`` (``at`` UTC, ``label``) also open a period for
    points measured at or after them. A point without a recorded environment
    joins the period of the previous point, so a deploy that starts recording
    draws no boundary by itself.
    """
    from conductress.control.drift_analyzer import boot_only, change_label, environment_changes

    events = sorted(events or [], key=lambda e: e["at"])
    periods: list[dict[str, Any]] = []
    current: Optional[dict[str, Any]] = None
    prev_env: Optional[dict[str, Any]] = None
    prev_bucket: Optional[int] = None
    for entry in points:
        env = entry.get("env")
        bucket = _event_bucket(entry.get("measured_at"), events)
        changes: list[str] = []
        if current is not None and env is not None and prev_env is not None:
            changes = environment_changes(prev_env, env)
            if bucket is not None and prev_bucket is not None and bucket != prev_bucket:
                for event in events[prev_bucket:bucket]:
                    changes.append(f"event: {event.get('label', 'environment event')}")
        if current is None or changes:
            current = {
                "id": len(periods),
                "start_commit_index": entry["commit_index"],
                "end_commit_index": entry["commit_index"],
                "start_date": entry["date"],
                "end_date": entry["date"],
                "env": env,
                "boot_only": boot_only(changes) if changes else False,
                "label": change_label([c for c in changes if not c.startswith("event: ")]) if changes else "",
            }
            event_labels = [c[len("event: ") :] for c in changes if c.startswith("event: ")]
            if event_labels:
                current["label"] = "; ".join(filter(None, [current["label"], *event_labels]))
                current["boot_only"] = False
            periods.append(current)
        else:
            last = periods[-1]
            last["end_commit_index"] = entry["commit_index"]
            last["end_date"] = entry["date"]
            if last["env"] is None and env is not None:
                last["env"] = env
        if env is not None:
            prev_env = env
        if bucket is not None:
            prev_bucket = bucket
        entry["period"] = periods[-1]["id"]
    return periods


def load_environment_events(path: Optional[Path] = None) -> list[dict[str, Any]]:
    """Manual environment events (``sweep_data/environment_events.json``): ``[{at, label}]``.

    ``at`` is an ISO 8601 UTC instant. Written by hand next to generations.json
    for changes the provenance fields cannot see (a known hardware event, a
    BIOS update). Missing file: no events.
    """
    from datetime import datetime, timezone

    from conductress.config import SWEEP_STATE_DIR

    path = path or (SWEEP_STATE_DIR / "environment_events.json")
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    events = data.get("events", []) if isinstance(data, dict) else data
    parsed = []
    for event in events:
        at = datetime.fromisoformat(str(event["at"]).replace("Z", "+00:00"))
        at = at.replace(tzinfo=timezone.utc) if at.tzinfo is None else at.astimezone(timezone.utc)
        parsed.append({"at": at, "label": str(event.get("label", ""))})
    return parsed


def _build_annotations(
    state: SweepState, planner: SweepPlanner, workload: str, lower_is_better: bool = False
) -> list[dict[str, Any]]:
    """Build annotations for commits where a change was pinpointed."""
    annotations: list[dict[str, Any]] = []

    # Find segments of exactly 1 commit (fully bisected)
    completed = planner._get_ordered_completed_points()
    commit_index = planner._commit_index

    for i in range(len(completed) - 1):
        left = completed[i]
        right = completed[i + 1]
        left_idx = commit_index.get(left.commit, 0)
        right_idx = commit_index.get(right.commit, 0)

        # Adjacent commits — this is a pinpointed change
        if right_idx - left_idx == 1:
            if left.value is None or right.value is None:
                continue
            delta = (right.value - left.value) / left.value
            if abs(delta) >= ANNOTATION_THRESHOLD:
                # Gate on statistical significance (Welch's t-test via Segment).
                # With noisy points (e.g. bimodal Intel, CV 3-7%) adjacent-commit
                # deltas exceed the threshold from noise alone; require the
                # difference to also be significant given each point's CV and
                # rep count. Legacy points without CV fall back to the
                # threshold-only check inside is_significant.
                segment = Segment(
                    left_commit=left.commit,
                    right_commit=right.commit,
                    left_value=left.value,
                    right_value=right.value,
                    commit_count=0,
                    left_cv=left.cv or 0.0,
                    right_cv=right.cv or 0.0,
                    left_reps=left.reps,
                    right_reps=right.reps,
                )
                if not segment.is_significant:
                    continue
                annotation: dict[str, Any] = {
                    "commit": right.commit,
                    "date": right.date,
                    "delta": round(delta, 4),
                    "workload": workload,
                    "type": "increase" if delta > 0 else "decrease",
                    "good": (delta < 0) if lower_is_better else (delta > 0),
                }
                pr = state.commit_prs.get(right.commit) or right.pr
                pr_title = state.commit_titles.get(right.commit) or right.pr_title
                if pr is not None:
                    annotation["pr"] = pr
                if pr_title is not None:
                    annotation["pr_title"] = pr_title
                annotation["_left_commit"] = left.commit  # consumed by export_series
                annotations.append(annotation)

    return annotations


@dataclass
class NotableSource:
    """One sweep series feeding the combined notable-changes export.

    Attributes:
        state: Sweep state holding the series' completed points.
        workload: Workload identifier (as shown on the dashboard).
        metric: Metric identifier the series tracks ("throughput" or "memory").
        lower_is_better: Whether a decrease is an improvement (True for memory).
    """

    state: SweepState
    workload: str
    metric: str
    lower_is_better: bool = False


def export_notable(sources: list[NotableSource], output_path: Path, platform: str) -> None:
    """Export a combined notable-changes feed across workloads and metrics.

    Aggregates pinpointed annotations (bisected to a single commit, |delta| >=
    ANNOTATION_THRESHOLD) from every source series into one platform-level file,
    each entry tagged with its workload and metric. Entries are ordered newest
    first by commit date so the dashboard can render a recency-ordered feed and
    merge feeds from multiple platforms by commit.
    """
    annotations: list[dict[str, Any]] = []
    for source in sources:
        planner = SweepPlanner(source.state)
        for annotation in _build_annotations(source.state, planner, source.workload, source.lower_is_better):
            annotation["metric"] = source.metric
            annotations.append(annotation)

    # Newest first; dates are YYYY-MM-DD strings so lexicographic order is chronological.
    annotations.sort(key=lambda a: (a.get("date") or "", a["commit"]), reverse=True)

    payload: dict[str, Any] = {
        "metadata": {
            "platform": platform,
            "generated": datetime.now(timezone.utc).isoformat(),
        },
        "annotations": annotations,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))


# =============================================================================
# Perf metric series export
# =============================================================================


def _effective_perf_reps(point: BenchmarkPoint) -> int:
    """Number of reps whose raw counters were summed into this point's perf data.

    Raw perf counters are summed across reps, so absolute per-request metrics must
    divide by this count. ``perf_rep_count`` is the exact summed-rep count recorded
    going forward; historical points predate that field and fall back to ``reps``
    (which equalled the perf-rep count for all collected sweep data — verified).
    """
    return point.perf_rep_count or point.reps or 1


def _compute_metric_from_counters(
    counters: Optional[dict], metric_id: str, rps: float, duration: float, reps: int = 1
) -> Any:
    """Compute a normalized metric value from a raw perf-counter dict."""
    if not counters:
        return None
    metric_def = PERF_METRICS.get(metric_id)
    if not metric_def:
        return None
    try:
        return metric_def["compute"](counters, rps=rps, duration=duration, reps=reps)
    except (ZeroDivisionError, KeyError, TypeError):
        return None


def _compute_metric(point: BenchmarkPoint, metric_id: str) -> Any:
    """Compute a normalized metric value from a point's process-wide perf counters."""
    return _compute_metric_from_counters(
        point.perf_counters,
        metric_id,
        point.perf_rps or 0,
        point.perf_duration_seconds or 0,
        _effective_perf_reps(point),
    )


# Scope assigned to perf points recorded before the scope field existed. perf
# stat ran unprivileged until PR #196, so every such point is user-space only.
LEGACY_PERF_SCOPE = "user"


def perf_scope(point: BenchmarkPoint) -> str:
    """Privilege scope a point's perf counters were counted under."""
    return point.perf_counters_scope or LEGACY_PERF_SCOPE


def export_perf_metrics(
    state: SweepState,
    output_dir: Path,
    platform: str,
    workload: str = "",
    repo: str = "valkey-io/valkey",
    branch: str = "unstable",
) -> dict[str, int]:
    """Export perf stat metrics as individual series files.

    Returns a dict of metric_id -> point_count for metrics that had data.
    """
    if not workload:
        from conductress.config import SWEEP_IO_THREADS, SWEEP_PIPELINING, SWEEP_TEST, SWEEP_VAL_SIZE

        workload = f"{SWEEP_TEST.lower()}{SWEEP_VAL_SIZE}b-t{SWEEP_IO_THREADS}-p{SWEEP_PIPELINING}"

    planner = SweepPlanner(state)
    commit_index = planner._commit_index
    completed = planner._get_ordered_completed_points()

    # Filter to points that have perf counters
    perf_points = [p for p in completed if p.perf_counters]
    if not perf_points:
        return {}

    # Build landmarks (shared across all metric files)
    landmarks = [
        {
            "commit": lm.commit,
            "date": lm.date,
            "label": lm.label,
            "type": "release",
            "commit_index": commit_index.get(lm.commit, 0),
        }
        for lm in state.landmarks
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    exported: dict[str, int] = {}

    # Each metric is emitted in up to three variants: process-wide (no suffix),
    # main-thread ("-main"), and IO-threads ("-io"). The per-thread variants read
    # the point's per-thread counter dicts (sparse/forward-only — only points that
    # collected per-thread data contribute). All variants reuse the same compute
    # lambdas via _compute_metric_from_counters.
    variants: list[tuple[str, str]] = [
        ("", "perf_counters"),
        ("-main", "perf_counters_main"),
        ("-io", "perf_counters_io"),
    ]

    for metric_id, metric_def in PERF_METRICS.items():
        for suffix, counter_attr in variants:
            points: list[dict[str, Any]] = []
            for point in perf_points:
                counters = getattr(point, counter_attr, None)
                if not counters:
                    continue
                value = _compute_metric_from_counters(
                    counters,
                    metric_id,
                    point.perf_rps or 0,
                    point.perf_duration_seconds or 0,
                    _effective_perf_reps(point),
                )
                if value is None:
                    continue
                entry: dict[str, Any] = {
                    "commit": point.commit,
                    "commit_index": commit_index.get(point.commit, 0),
                    "date": point.date,
                    "value": round(value, 6),
                    # Counts from different privilege scopes are not comparable
                    # (kernel work was ~20% of IO-thread cycles at P16 with four
                    # I/O threads, more at depth 1), so every point says which it
                    # is. The dashboard draws scopes as separate runs instead of
                    # one line with a false step at the deploy date.
                    "scope": perf_scope(point),
                }
                pr = state.commit_prs.get(point.commit) or point.pr
                pr_title = state.commit_titles.get(point.commit)
                if pr is not None:
                    entry["pr"] = pr
                if pr_title is not None:
                    entry["pr_title"] = pr_title
                points.append(entry)

            if not points:
                continue

            metric_key = f"{metric_id}{suffix}"
            series: dict[str, Any] = {
                "metadata": {
                    "repo": "valkey-io/valkey",
                    "branch": "unstable",
                    "platform": platform,
                    "workload": workload,
                    "metric": metric_key,
                    "unit": metric_def["unit"],
                    "generated": datetime.now(timezone.utc).isoformat(),
                    "total_commits": len(state.merge_commits),
                    # Scopes present in this series, in first-seen (commit) order.
                    "scopes": list(dict.fromkeys(p["scope"] for p in points)),
                },
                "points": points,
                "landmarks": landmarks,
            }

            filename = f"series-{platform}-{workload}-{metric_key}.json"
            (output_dir / filename).write_text(json.dumps(series, indent=2))
            exported[metric_key] = len(points)

    return exported


def export_latency(
    state: SweepState,
    output_path: Path,
    platform: str,
    workload: str,
    target_rps: int = 100_000,
    tool_version: str = "d52544b1",
    repo: str = "valkey-io/valkey",
    branch: str = "unstable",
    tool: str = "memtier_benchmark",
    connections: Optional[int] = None,
    client_threads: Optional[int] = None,
) -> int:
    """Export latency sweep data to a dashboard-ready series file.

    Reads extended latency data (all percentiles, and a histogram when the
    generator produced one) stored on each point, since the state only stores
    p99 as the primary value.  ``tool`` names the load generator so a reader of
    the series knows which generator's latency it is looking at.

    Returns the number of exported points.
    """

    planner = SweepPlanner(state)
    commit_index = planner._commit_index
    completed = planner._get_ordered_completed_points()

    if not completed:
        return 0

    # Build points
    points: list[dict[str, Any]] = []
    for point in completed:
        entry: dict[str, Any] = {
            "commit": point.commit,
            "commit_index": commit_index.get(point.commit, 0),
            "date": point.date,
            "p99_us": point.value,  # primary metric stored in state
        }
        # Include full latency data if available
        if point.latency_data:
            entry["p50_us"] = point.latency_data.get("p50_us")
            entry["p99_9_us"] = point.latency_data.get("p99_9_us")
            entry["p100_us"] = point.latency_data.get("p100_us")
            entry["target_rps"] = point.latency_data.get("target_rps")
            entry["actual_rps"] = point.latency_data.get("actual_rps")
            if point.latency_data.get("histogram") is not None:
                entry["histogram"] = point.latency_data["histogram"]
        # Load-generator CPU utilization behind the point, same as the v3
        # throughput series.  A rate-held latency cell runs the client well
        # under its budget, but the schema stays uniform across every v3 series;
        # keys are omitted when None (v1 / legacy points), never exported null.
        if point.client_utilization is not None:
            entry["client_utilization"] = point.client_utilization
        if point.client_saturated is not None:
            entry["client_saturated"] = point.client_saturated
        if point.client_cores_busy is not None:
            entry["client_cores_busy"] = point.client_cores_busy
        if point.client_allocated_cores is not None:
            entry["client_allocated_cores"] = point.client_allocated_cores
        pr = state.commit_prs.get(point.commit) or point.pr
        pr_title = state.commit_titles.get(point.commit) or point.pr_title
        if pr is not None:
            entry["pr"] = pr
        if pr_title is not None:
            entry["pr_title"] = pr_title
        points.append(entry)

    # Build landmarks
    landmarks = [
        {
            "commit": lm.commit,
            "date": lm.date,
            "label": lm.label,
            "type": "release",
            "commit_index": commit_index.get(lm.commit, 0),
        }
        for lm in state.landmarks
    ]

    # Build annotations (same logic as throughput but with lower_is_better=True)
    annotations = _build_annotations(state, planner, workload, lower_is_better=True)

    series: dict[str, Any] = {
        "metadata": {
            "repo": repo,
            "branch": branch,
            "platform": platform,
            "workload": workload,
            "metric": "latency",
            "unit": "µs",
            "load_fraction": None,
            "target_rps": target_rps,
            "pipeline": 1,
            "tool": tool,
            "tool_version": tool_version,
            "generated": datetime.now(timezone.utc).isoformat(),
            "total_commits": len(state.merge_commits),
        },
        "points": points,
        "landmarks": landmarks,
        "annotations": annotations,
    }

    # Record the client budget the latency series was measured at, when known.
    # The latency series runs the default 8 client threads, but publishing the
    # budget keeps every v3 series' metadata uniform and self-describing.
    if connections is not None:
        series["metadata"]["connections"] = connections
    if client_threads is not None:
        series["metadata"]["client_threads"] = client_threads

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(series, indent=2))
    return len(points)


def export_cpu_profile(
    state: SweepState,
    output_dir: Path,
    platform: str,
    workload: str,
    repo: str = "valkey-io/valkey",
    branch: str = "unstable",
) -> dict[str, int]:
    """Export per-thread CPU profile breakdowns as stacked-area series files.

    Produces two files per workload:
      series-{platform}-{workload}-cpu-main.json  (main thread categories)
      series-{platform}-{workload}-cpu-io.json    (IO thread categories)

    Each point's ``breakdown`` is a category -> percentage map (sums to ~100,
    including an ``idle`` band). Forward-only: only points that actually have
    collapsed stacks contribute, so the series is sparse until sweep tasks
    accumulate CPU data. Returns {metric_id: point_count} for non-empty series.
    """
    from conductress.cpu_profiler import (
        CPU_CATEGORIES_IO,
        CPU_CATEGORIES_MAIN,
        CPU_CATEGORY_NAMES_IO,
        CPU_CATEGORY_NAMES_MAIN,
        categorize_cpu_stacks,
    )

    planner = SweepPlanner(state)
    commit_index = planner._commit_index
    completed = planner._get_ordered_completed_points()

    landmarks = [
        {
            "commit": lm.commit,
            "date": lm.date,
            "label": lm.label,
            "type": "release",
            "commit_index": commit_index.get(lm.commit, 0),
        }
        for lm in state.landmarks
    ]

    variants = [
        ("cpu-main", "cpu_stacks_main", CPU_CATEGORIES_MAIN, CPU_CATEGORY_NAMES_MAIN, "Main thread"),
        ("cpu-io", "cpu_stacks_io", CPU_CATEGORIES_IO, CPU_CATEGORY_NAMES_IO, "IO threads"),
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    exported: dict[str, int] = {}

    for metric_id, attr, categories, category_names, thread_label in variants:
        points: list[dict[str, Any]] = []
        for point in completed:
            stacks = getattr(point, attr, None)
            if not stacks:
                continue
            breakdown = categorize_cpu_stacks(stacks, categories)
            # Round for compact JSON; drop near-zero noise categories.
            breakdown = {cat: round(pct, 4) for cat, pct in breakdown.items() if pct >= 0.05}
            entry: dict[str, Any] = {
                "commit": point.commit,
                "commit_index": commit_index.get(point.commit, 0),
                "date": point.date,
                "breakdown": breakdown,
            }
            pr = state.commit_prs.get(point.commit) or point.pr
            pr_title = state.commit_titles.get(point.commit) or point.pr_title
            if pr is not None:
                entry["pr"] = pr
            if pr_title is not None:
                entry["pr_title"] = pr_title
            points.append(entry)

        if not points:
            continue

        series: dict[str, Any] = {
            "metadata": {
                "repo": repo,
                "branch": branch,
                "platform": platform,
                "workload": workload,
                "metric": metric_id,
                "thread": thread_label,
                "unit": "%",
                "categories": category_names,
                "generated": datetime.now(timezone.utc).isoformat(),
                "total_commits": len(state.merge_commits),
            },
            "points": points,
            "landmarks": landmarks,
        }

        filename = f"series-{platform}-{workload}-{metric_id}.json"
        (output_dir / filename).write_text(json.dumps(series, indent=2))
        exported[metric_id] = len(points)

    return exported


def export_cpu_stacks_raw(
    state: SweepState,
    output_dir: Path,
    platform: str,
    workload: str,
    repo: str = "valkey-io/valkey",
    branch: str = "unstable",
    already_published: Optional[Callable[[str], bool]] = None,
) -> dict[str, int]:
    """Export raw per-commit CPU collapsed stacks for future flamegraph drill-down.

    For each completed point that carries collapsed stacks, writes one file:
      series-{platform}-{workload}-cpu-stacks-{commit}.json

    containing the raw ``cpu_stacks_main`` / ``cpu_stacks_io`` arrays
    (shape ``[[stack_string, sample_count], ...]``) preserved exactly, plus
    minimal metadata. These files are intentionally separate from the small
    ``cpu-main`` / ``cpu-io`` percentage series so the dashboard load stays fast;
    a future flamegraph page fetches a single raw-stacks file lazily by commit.

    A lightweight per-workload index is also written:
      series-{platform}-{workload}-cpu-stacks-index.json

    listing ``[{commit, commit_index, date}, ...]`` for every commit that has a
    raw-stacks file, so a future page can discover available stacks without a
    directory listing on the static host.

    Forward-only/sparse: points without stacks are skipped. Idempotent: per-commit
    files that already exist are not rewritten (raw IO arrays are multi-MB, so
    re-serializing the full history every publish would be wasteful). A caller
    that exports into a fresh staging directory passes ``already_published``,
    a predicate on the file name that says whether the final destination
    already holds the file; without it a staged export would regenerate every
    file every time, since the staging directory is always empty. The index is
    always rebuilt from the points that have stacks.

    Returns ``{"files_written": n, "indexed": m}``.
    """
    planner = SweepPlanner(state)
    commit_index = planner._commit_index
    completed = planner._get_ordered_completed_points()

    output_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0
    index_entries: list[dict[str, Any]] = []

    for point in completed:
        if not point.cpu_stacks_main and not point.cpu_stacks_io:
            continue

        idx = commit_index.get(point.commit, 0)
        index_entries.append({"commit": point.commit, "commit_index": idx, "date": point.date})

        filename = f"series-{platform}-{workload}-cpu-stacks-{point.commit}.json"
        file_path = output_dir / filename
        # Idempotent: raw IO arrays are multi-MB; never rewrite an existing file,
        # here or at the final destination the caller publishes from.
        if file_path.exists():
            continue
        if already_published is not None and already_published(filename):
            continue

        metadata: dict[str, Any] = {
            "repo": repo,
            "branch": branch,
            "platform": platform,
            "workload": workload,
            "metric": "cpu-stacks",
            "commit": point.commit,
            "commit_index": idx,
            "date": point.date,
            "generated": datetime.now(timezone.utc).isoformat(),
        }
        pr = state.commit_prs.get(point.commit) or point.pr
        pr_title = state.commit_titles.get(point.commit) or point.pr_title
        if pr is not None:
            metadata["pr"] = pr
        if pr_title is not None:
            metadata["pr_title"] = pr_title

        payload: dict[str, Any] = {
            "metadata": metadata,
            "cpu_stacks_main": point.cpu_stacks_main or [],
            "cpu_stacks_io": point.cpu_stacks_io or [],
        }
        file_path.write_text(json.dumps(payload))
        files_written += 1

    if not index_entries:
        return {"files_written": 0, "indexed": 0}

    index = {
        "metadata": {
            "repo": repo,
            "branch": branch,
            "platform": platform,
            "workload": workload,
            "metric": "cpu-stacks-index",
            "generated": datetime.now(timezone.utc).isoformat(),
        },
        "commits": index_entries,
    }
    index_path = output_dir / f"series-{platform}-{workload}-cpu-stacks-index.json"
    index_path.write_text(json.dumps(index, indent=2))

    return {"files_written": files_written, "indexed": len(index_entries)}


def export_manifest(
    output_dir: Path,
    platforms: list[str],
    workloads: list[tuple[str, str]],
    epoch_id: str = "v1",
    epochs: Optional[list[dict[str, str]]] = None,
    generations: Optional[list[dict[str, Any]]] = None,
    generation: Optional[dict[str, Any]] = None,
) -> None:
    """Write a per-platform manifest for one measurement epoch.

    The v1 filename remains unchanged for backward compatibility.  v2+
    manifests receive `.epoch-<id>` and every manifest advertises the epochs
    available in the same publication batch.

    ``generations`` lists this platform's closed series generations (see
    ``sweep/generations.py``) so the dashboard can offer their read-only
    history; a live manifest with none carries an empty list.  ``generation``
    marks the manifest of one closed generation itself: the file gets a
    ``.gen-<id>`` suffix and the entry is stored under ``generation``.
    """
    from conductress.publisher import detect_platform

    platform_id, _ = detect_platform()
    throughput = [w for w, m in workloads if m == "throughput"]
    memory = [w for w, m in workloads if m == "memory"]
    latency = [w for w, m in workloads if m == "latency"]
    manifest: dict[str, Any] = {
        "version": 4,
        "platform": platform_id,
        "epoch": epoch_id,
        "epochs": epochs or [{"id": "v1", "label": "Legacy v1 (stock generator)", "generator": "stock"}],
        "throughput_workloads": throughput,
        "memory_workloads": memory,
        "latency_workloads": latency,
        "groups": [
            {
                "id": "throughput",
                "title": "Throughput",
                "series": ["throughput"],
                "y_axes": [{"id": "left", "label": "requests/sec", "series": ["throughput"]}],
            },
            {
                "id": "memory",
                "title": "Memory Overhead",
                "series": ["memory"],
                "y_axes": [{"id": "left", "label": "bytes/key", "series": ["memory"]}],
            },
        ]
        + PERF_GROUPS
        + PER_THREAD_PERF_GROUPS,
        "generations": list(generations or []),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"manifest-{platform_id}.json" if epoch_id == "v1" else f"manifest-{platform_id}.epoch-{epoch_id}.json"
    if generation is not None:
        from conductress.sweep.generations import generation_path

        manifest["generation"] = dict(generation)
        filename = generation_path(Path(filename), generation["id"]).name
    (output_dir / filename).write_text(json.dumps(manifest, indent=2))
