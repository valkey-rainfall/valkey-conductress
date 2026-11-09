"""Confirmation sweep: re-measure a few old sweep points after a host change.

A confirmation run measures the NEW environment of this runner on (commit,
workload) points that the live v3 generation measured BEFORE an environment
event, and compares the new values with the old ones pair by pair. It answers
"does the new host reproduce the old numbers?" with a paired test, faster than
the daily canary can.

Rules (design section 5, with the owner's decisions):

- OFF by default (``CONDUCTRESS_CONFIRM_SWEEP``). Off: no coordinator is
  built, no task is queued, no file is published.
- Triggers: (a) automatic, the first time this runner records a result row
  whose HOST fields differ from the previous row (kernel_release,
  cpu_microcode, glibc_version, packages.*, cachecannon_commit, instance_id,
  host_fingerprint, and their ``server_`` twins on a two-host runner). A
  harness revision change or a reboot alone never triggers. (b) an operator,
  for any event, with ``conductress sweep confirm``. At most one open run per
  runner.
- Pairs: throughput points of the live v3 generation measured before the
  event time with cv <= 1.0%, reps >= 5, a client that was not saturated,
  and a task that is not excluded. One per workload first, then by release
  points first and lowest cv; at most 2 per workload. Budget 8 tasks by
  default (6-12).
- Priority: the coordinator sits in the last scheduling tier, so it queues a
  cell only when no nightly, landmark, bisection or backfill task (memory
  backfill included) is due. It is runner-side on purpose: a fleet task would
  run before the sweep scheduler.
- Stop rules after each task, on the median log ratio of the pairs done:
  confirmed (>= 3 pairs, 95% bootstrap CI excludes 0 and |effect| >= floor),
  cleared (>= 5 pairs, CI inside +/- floor); else stop at the budget with the
  status "insufficient-data".
- It never bisects, never records into a series and never moves a
  generation: its tasks carry no ``sweep_commit`` and a note of their own.
  Result rows carry ``sample: "confirm"`` and the run id.

Files: ``sweep_data/confirm_runs.json`` (state, private) and
``confirm-<platform>.epoch-v3.json`` in the publish dir (public, allowlisted
fields only).
"""

from __future__ import annotations

import json
import logging
import math
import os
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from conductress import config
from conductress.public_fields import check_public_text, public_change_label, public_text

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
SAMPLE = "confirm"
NOTE_PREFIX = "[confirm-v3:"
# Scheduling tier: below the memory series' deferred backfill (tier 1).
CONFIRM_TIER = 2

BUDGET_DEFAULT = 8
BUDGET_MIN = 6
BUDGET_MAX = 12
MAX_PER_WORKLOAD = 2
MAX_CV_PCT = 1.0
MIN_REPS = 5
MIN_PAIRS_CONFIRM = 3
MIN_PAIRS_CLEAR = 5

STATUS_REQUESTED = "requested"
STATUS_RUNNING = "running"
STATUS_CONFIRMED = "confirmed"
STATUS_CLEARED = "cleared"
STATUS_INSUFFICIENT = "insufficient-data"
STATUS_NO_PAIRS = "no-pairs"
OPEN_STATUSES = frozenset({STATUS_REQUESTED, STATUS_RUNNING})

TRIGGER_AUTOMATIC = "automatic"
TRIGGER_OPERATOR = "operator"

# Host fields of a result row's ``environment``. Not here on purpose:
# conductress_revision (harness only) and boot_id (reboot only), by decision.
HOST_FIELDS = (
    "kernel_release",
    "cpu_microcode",
    "glibc_version",
    "cachecannon_commit",
    "instance_id",
    "host_fingerprint",
)
# The same for ``server_environment`` on a runner whose server is another host.
SERVER_HOST_FIELDS = ("kernel_release", "cpu_microcode", "glibc_version", "instance_id", "host_fingerprint")

_TASK_TIME_FORMAT = "%Y.%m.%d_%H.%M.%S.%f"


def default_state_path() -> Path:
    return config.SWEEP_STATE_DIR / "confirm_runs.json"


def default_results_file() -> Path:
    return config.CONDUCTRESS_RESULTS / "output.jsonl"


# --------------------------------------------------------------------------- times


def _stamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(text: Any) -> Optional[datetime]:
    """An ISO 8601 instant or a task-id time (``2026.11.06_16.19.23.794391``) as UTC; None for bad input."""
    if not isinstance(text, str) or not text:
        return None
    try:
        return datetime.strptime(text, _TASK_TIME_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


# --------------------------------------------------------------------------- triggers


def host_signature(row: dict[str, Any]) -> dict[str, str]:
    """The host fields of one result row, flat (``packages.<name>``, ``server_<field>``)."""
    signature: dict[str, str] = {}
    environment = row.get("environment") or {}
    if isinstance(environment, dict):
        _add_host_fields(signature, environment, HOST_FIELDS, "")
    server = row.get("server_environment")
    if isinstance(server, dict) and not server.get("same_host", False):
        _add_host_fields(signature, server, SERVER_HOST_FIELDS, "server_")
    return signature


def _add_host_fields(
    signature: dict[str, str], environment: dict[str, Any], fields: Sequence[str], prefix: str
) -> None:
    for key in fields:
        if environment.get(key) is not None:
            signature[f"{prefix}{key}"] = str(environment[key])
    packages = environment.get("packages")
    if isinstance(packages, dict):
        for name, version in packages.items():
            if version is not None:
                signature[f"{prefix}packages.{name}"] = str(version)


def host_changes(previous: dict[str, str], current: dict[str, str]) -> list[str]:
    """``key: 'old' -> 'new'`` for every host field both rows recorded with different values.

    A field only one row has is not a change: an older row schema did not
    record it.
    """
    return [
        f"{key}: '{previous[key]}' -> '{current[key]}'"
        for key in sorted(set(previous) & set(current))
        if previous[key] != current[key]
    ]


def host_change_around(results_file: Path, at: datetime) -> Optional[list[str]]:
    """Host changes between the last result row that ended at or before ``at`` and the first one after it.

    None when there is no row on one side, so nothing can be said yet.
    """
    before: Optional[dict[str, Any]] = None
    after: Optional[dict[str, Any]] = None
    if not results_file.exists():
        return None
    with results_file.open(encoding="utf-8") as handle:
        for line in handle:
            row = _parse_row(line)
            end = parse_time(row.get("end_time")) if row else None
            if row is None or end is None:
                continue
            if end <= at:
                before = row
            elif after is None:
                after = row
    if before is None or after is None:
        return None
    return host_changes(host_signature(before), host_signature(after))


def _parse_row(line: str) -> Optional[dict[str, Any]]:
    try:
        row = json.loads(line)
    except ValueError:
        return None
    return row if isinstance(row, dict) else None


def find_row(results_file: Path, task_id: str) -> Optional[dict[str, Any]]:
    """The result row of ``task_id`` (the newest one), or None."""
    if not results_file.exists():
        return None
    for line in reversed(results_file.read_text(encoding="utf-8").splitlines()):
        if task_id not in line:
            continue
        row = _parse_row(line)
        if row is not None and row.get("task_id") == task_id:
            return row
    return None


# --------------------------------------------------------------------------- pairs


@dataclass(frozen=True)
class Candidate:
    """One old point a confirmation run may measure again."""

    workload: str
    metric: str
    commit: str
    value: float
    cv: float
    release: bool
    measured_at: datetime


def eligible_points(sources: Iterable[Any], event_at: datetime, excluded: frozenset) -> list[Candidate]:
    """The points of the given series that pass the pair rules (see the module docstring)."""
    out = []
    for source in sources:
        for point in source.state.points.values():
            measured = parse_time(point.measured_at)
            if (
                not point.is_complete
                or measured is None
                or measured >= event_at
                or point.cv is None
                or point.cv > MAX_CV_PCT
                or point.reps < MIN_REPS
                or point.client_saturated is True
                or (point.task_id and point.task_id in excluded)
            ):
                continue
            out.append(
                Candidate(
                    workload=source.workload_id,
                    metric=source.metric_id,
                    commit=point.commit,
                    value=float(point.value),
                    cv=float(point.cv),
                    release=point.sample == "release",
                    measured_at=measured,
                )
            )
    return out


def pick_pairs(candidates: Sequence[Candidate], budget: int) -> list[Candidate]:
    """One pair per workload first (lowest cv first), then the rest; at most 2 per workload."""

    def key(c: Candidate) -> tuple:
        return (not c.release, c.cv, c.workload, c.commit)

    by_workload: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        by_workload.setdefault(candidate.workload, []).append(candidate)
    for items in by_workload.values():
        items.sort(key=key)
    firsts = sorted((items[0] for items in by_workload.values()), key=key)
    rest = sorted((c for items in by_workload.values() for c in items[1:MAX_PER_WORKLOAD]), key=key)
    return (firsts + rest)[: max(budget, 0)]


# --------------------------------------------------------------------------- stop rules


def evaluate(before: Sequence[float], after: Sequence[float], floor: float, key: str) -> dict[str, Any]:
    """Effect, 95% CI and status of the pairs done so far (status None: keep measuring).

    ``floor`` is a fraction (0.005 for 0.5%). The test is the canary findings'
    paired test: median log ratio, bootstrap CI over pairs.
    """
    import numpy as np

    from conductress.control.findings import FindingsConfig, paired_test

    n = min(len(before), len(after))
    result: dict[str, Any] = {"pairs": n, "effect": None, "ci_lo": None, "ci_hi": None, "status": None}
    if n == 1:
        result["effect"] = after[0] / before[0] - 1.0
    if n < 2:
        return result
    cfg = FindingsConfig()
    rng = np.random.default_rng((cfg.seed, zlib.crc32(key.encode("utf-8"))))
    test = paired_test(list(before[:n]), list(after[:n]), rng, cfg)
    effect, lo, hi = test["effect"], test["ci_lo"], test["ci_hi"]
    result.update(effect=effect, ci_lo=lo, ci_hi=hi)
    if n >= MIN_PAIRS_CONFIRM and (lo > 0 or hi < 0) and abs(effect) >= floor:
        result["status"] = STATUS_CONFIRMED
    elif n >= MIN_PAIRS_CLEAR and -floor <= lo and hi <= floor:
        result["status"] = STATUS_CLEARED
    return result


# --------------------------------------------------------------------------- state file


def load_state(path: Optional[Path] = None) -> dict[str, Any]:
    """``{schema_version, last_host, runs}``; an empty state when the file is missing."""
    path = path or default_state_path()
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "last_host": None, "runs": []}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        "schema_version": data.get("schema_version", SCHEMA_VERSION),
        "last_host": data.get("last_host"),
        "runs": list(data.get("runs") or []),
    }


def save_state(state: dict[str, Any], path: Optional[Path] = None) -> None:
    path = path or default_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def open_run(state: dict[str, Any]) -> Optional[dict[str, Any]]:
    return next((r for r in state["runs"] if r.get("status") in OPEN_STATUSES), None)


def new_run(
    state: dict[str, Any], *, event_at: datetime, label: str, trigger: str, budget: int, now: datetime
) -> dict[str, Any]:
    """Append a requested run to ``state``. Refuses a second open run and a budget outside 6-12."""
    current = open_run(state)
    if current is not None:
        raise ValueError(f"run {current['run_id']} is still open; one open run per runner")
    if not BUDGET_MIN <= budget <= BUDGET_MAX:
        raise ValueError(f"budget must be {BUDGET_MIN}-{BUDGET_MAX} tasks, got {budget}")
    run_id = f"confirm-{now.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    if any(r.get("run_id") == run_id for r in state["runs"]):
        raise ValueError(f"a run with id {run_id} already exists; retry in a second")
    run = {
        "run_id": run_id,
        "event_at": _stamp(event_at),
        "label": label,
        "trigger": trigger,
        "budget": budget,
        "floor_pct": None,
        "status": STATUS_REQUESTED,
        "requested_at": _stamp(now),
        "started_at": None,
        "ended_at": None,
        "effect": None,
        "ci": None,
        "pairs": [],
    }
    state["runs"].append(run)
    return run


def request_run(
    *,
    at: datetime,
    label: str,
    budget: int = BUDGET_DEFAULT,
    path: Optional[Path] = None,
    now: Optional[datetime] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """The operator command: record a requested run for the event at ``at``. The label is public."""
    clean = check_public_text(label, "label")
    state = load_state(path)
    run = new_run(
        state,
        event_at=at,
        label=clean,
        trigger=TRIGGER_OPERATOR,
        budget=budget,
        now=now or datetime.now(timezone.utc),
    )
    # shortcut: read-modify-write without a lock; the runner rewrites this file
    # between tasks, so a request written in that same instant can be lost.
    # Upgrade to a lock file if operators request runs often.
    if not dry_run:
        save_state(state, path)
    return run


# --------------------------------------------------------------------------- coordinator


class ConfirmationCoordinator:
    """Runner subscriber that runs confirmation sweeps. Built only when the switch is on.

    ``sources`` are the live v3 throughput coordinators; their state is the
    live generation the pairs are read from, and their ``_create_task`` builds
    a cell of exactly the series' shape.
    """

    retired = False

    def __init__(
        self,
        sources: Sequence[Any],
        state_path: Optional[Path] = None,
        results_file: Optional[Path] = None,
        floor_pct: Optional[float] = None,
        queue: Optional[Any] = None,
        clock: Any = None,
    ) -> None:
        self.sources = list(sources)
        self.state_path = state_path or default_state_path()
        self.results_file = results_file or default_results_file()
        self.floor_pct = config.CONFIRM_FLOOR_PCT if floor_pct is None else floor_pct
        self._queue = queue
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    # --- scheduler interface (duck-typed, see TaskRunner._schedule_next)

    def schedule_tier(self) -> int:
        return CONFIRM_TIER

    def get_urgency_score(self) -> float:
        return 0.0

    def has_nightly_task(self) -> bool:
        return False

    def _task_queue(self) -> Any:
        if self._queue is not None:
            return self._queue
        from conductress.task_queue import TaskQueue

        return TaskQueue()

    def _source(self, workload: str) -> Optional[Any]:
        return next((s for s in self.sources if s.workload_id == workload), None)

    def on_queue_empty(self) -> None:
        """Queue the next cell of the open run, if there is one and none is queued."""
        state = load_state(self.state_path)
        run = open_run(state)
        if run is None:
            return
        if run["status"] == STATUS_REQUESTED:
            self._start(run)
            save_state(state, self.state_path)
            if run["status"] != STATUS_RUNNING:
                return
        queue = self._task_queue()
        if any(getattr(t, "confirm_run_id", "") == run["run_id"] for t in queue.get_all_tasks()):
            return
        pending = [p for p in run["pairs"] if p["state"] == "pending"]
        if not pending:
            self._finish(run, None)
            save_state(state, self.state_path)
            return
        pair = pending[0]
        source = self._source(pair["workload"])
        if source is None:
            pair["state"] = "failed"
            save_state(state, self.state_path)
            return
        from conductress.sweep.planner import SweepTask, TaskPriority

        index = run["pairs"].index(pair) + 1
        task = source._create_task(  # pylint: disable=protected-access
            SweepTask(
                commit=pair["commit"],
                date="",
                priority=TaskPriority.CONFIRM,
                reason=f"Confirmation run {run['run_id']}: pair {index} of {len(run['pairs'])}",
            )
        )
        task.note = (
            f"{NOTE_PREFIX}{source._sweep_source}/{source.workload_id}] "  # pylint: disable=protected-access
            f"Confirmation run {run['run_id']}: pair {index} of {len(run['pairs'])}"
        )
        task.confirm_run_id = run["run_id"]
        queue.submit_task(task)
        pair["state"] = "queued"
        pair["task_id"] = task.task_id
        save_state(state, self.state_path)
        logger.info("[confirm] Queued %s %s for %s", source.workload_id, pair["commit"][:8], run["run_id"])

    def _start(self, run: dict[str, Any]) -> None:
        from conductress.sweep.generations import excluded_task_ids

        event_at = parse_time(run["event_at"])
        assert event_at is not None
        picked = pick_pairs(eligible_points(self.sources, event_at, excluded_task_ids()), run["budget"])
        run["floor_pct"] = self.floor_pct
        run["started_at"] = _stamp(self._clock())
        run["pairs"] = [
            {
                "workload": c.workload,
                "metric": c.metric,
                "commit": c.commit,
                "before": c.value,
                "before_cv": c.cv,
                "before_measured_at": _stamp(c.measured_at),
                "after": None,
                "task_id": None,
                "state": "pending",
            }
            for c in picked
        ]
        if picked:
            run["status"] = STATUS_RUNNING
        else:
            run["status"] = STATUS_NO_PAIRS
            run["ended_at"] = run["started_at"]
            logger.warning("[confirm] %s: no old point passes the pair rules", run["run_id"])

    def _finish(self, run: dict[str, Any], status: Optional[str]) -> None:
        run["status"] = status or STATUS_INSUFFICIENT
        run["ended_at"] = _stamp(self._clock())
        logger.info("[confirm] %s ended: %s", run["run_id"], run["status"])

    def _evaluate(self, run: dict[str, Any]) -> None:
        done = [p for p in run["pairs"] if p["state"] == "done"]
        result = evaluate(
            [p["before"] for p in done], [p["after"] for p in done], run["floor_pct"] / 100.0, run["run_id"]
        )
        run["effect"] = result["effect"]
        run["ci"] = None if result["ci_lo"] is None else [result["ci_lo"], result["ci_hi"]]
        if result["status"] is not None:
            self._finish(run, result["status"])
        elif not any(p["state"] in ("pending", "queued") for p in run["pairs"]):
            self._finish(run, None)

    def on_task_completed(self, task: Any) -> None:
        state = load_state(self.state_path)
        row = find_row(self.results_file, task.task_id)
        if row is not None:
            self._check_trigger(state, row)
        run_id = getattr(task, "confirm_run_id", "")
        run = open_run(state)
        if run_id and run is not None and run["run_id"] == run_id:
            pair = next((p for p in run["pairs"] if p.get("task_id") == task.task_id), None)
            from conductress.sweep.generations import is_excluded

            score = row.get("score") if row else None
            if pair is not None:
                if isinstance(score, (int, float)) and score > 0 and not is_excluded(task.task_id):
                    pair["after"] = float(score)
                    pair["state"] = "done"
                else:
                    pair["state"] = "failed"
                self._evaluate(run)
        save_state(state, self.state_path)

    def on_task_failed(self, task: Any) -> None:
        run_id = getattr(task, "confirm_run_id", "")
        if not run_id:
            return
        state = load_state(self.state_path)
        run = open_run(state)
        if run is None or run["run_id"] != run_id:
            return
        for pair in run["pairs"]:
            if pair.get("task_id") == task.task_id:
                pair["state"] = "failed"
        self._evaluate(run)
        save_state(state, self.state_path)

    def _check_trigger(self, state: dict[str, Any], row: dict[str, Any]) -> None:
        """Open an automatic run when this row's host fields differ from the previous row's."""
        signature = host_signature(row)
        end = parse_time(row.get("end_time"))
        previous = state.get("last_host")
        state["last_host"] = {"signature": signature, "at": _stamp(end) if end else None}
        if not previous or not previous.get("at"):
            return
        changes = host_changes(previous.get("signature") or {}, signature)
        if not changes:
            return
        if open_run(state) is not None:
            logger.info("[confirm] host change seen, but a run is already open: %s", changes)
            return
        # The event time is the start of the first task in the new environment
        # (never before the previous row ended): every point measured before it
        # ran in the old environment, and it falls in the canary observation
        # gap that holds the change.
        previous_end = parse_time(previous["at"])
        assert previous_end is not None
        from conductress.sweep.generations import row_start_time

        start = row_start_time(row)
        event_at = max(t for t in (start, previous_end + timedelta(seconds=1)) if t is not None)
        label = public_text(public_change_label(changes)) or "host change"
        run = new_run(
            state, event_at=event_at, label=label, trigger=TRIGGER_AUTOMATIC, budget=BUDGET_DEFAULT, now=self._clock()
        )
        logger.info("[confirm] host change opened %s: %s", run["run_id"], label)

    # --- export

    def export(self, output_path: Path, platform: str) -> int:
        """Write the public confirm file; returns the number of runs."""
        state = load_state(self.state_path)
        document = public_document(state, platform, self._clock())
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
        return len(document["runs"])


def _pct(value: Optional[float]) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return round(100.0 * value, 3)


def public_run(run: dict[str, Any]) -> dict[str, Any]:
    """The allowlisted fields of one run: no task ids, no harness revisions, no host identity."""
    ci = run.get("ci")
    pairs = [
        {
            "workload": str(p["workload"]),
            "metric": str(p["metric"]),
            "commit": str(p["commit"]),
            "before": p.get("before"),
            "after": p.get("after"),
        }
        for p in run.get("pairs") or []
    ]
    return {
        "run_id": run.get("run_id"),
        "event_at": run.get("event_at"),
        "label": public_text(run.get("label")) or "",
        "trigger": run.get("trigger"),
        "budget": run.get("budget"),
        "status": run.get("status"),
        "floor_pct": run.get("floor_pct"),
        "pairs_done": sum(1 for p in run.get("pairs") or [] if p.get("state") == "done"),
        "effect_pct": _pct(run.get("effect")),
        "ci_pct": [_pct(ci[0]), _pct(ci[1])] if ci else None,
        "started_at": run.get("started_at"),
        "ended_at": run.get("ended_at"),
        "pairs": pairs,
    }


def public_document(state: dict[str, Any], platform: str, now: datetime) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "metadata": {"epoch": config.SWEEP_V3_EPOCH_ID, "platform": platform, "generated_at": _stamp(now)},
        "runs": [public_run(r) for r in state["runs"]],
    }


def create_confirmation_coordinator(subscribers: Sequence[Any]) -> Optional[ConfirmationCoordinator]:
    """The coordinator over the live v3 throughput series, or None when the switch is off.

    shortcut: only the default engine's series are paired; a comparison
    engine's points are not. Add them if a comparison engine needs it.
    """
    if not config.CONFIRM_SWEEP:
        return None
    from conductress.sweep.coordinator_v3 import BaseCachecannonSweepCoordinatorV3

    sources = [
        s
        for s in subscribers
        if isinstance(s, BaseCachecannonSweepCoordinatorV3)
        and s.metric_id == "throughput"
        and (s.engine is None or s.engine.source == "valkey")
    ]
    return ConfirmationCoordinator(sources)
