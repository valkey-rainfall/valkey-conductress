"""Series generations: close a platform's sweep history at a manual cut.

A measurement epoch (``v1``, ``v3``) names the protocol and is global. A
GENERATION is per platform: when the host's environment changed enough that
later points cannot share a chart line with earlier ones (kernel, glibc and
microcode update on intelbench, 2026-11-06), an operator closes the live
generation at a cut time. The points measured before the cut move to a
read-only, archived generation; the live series restart from the cut.

Rules (decided with the operator, see docs/configuration.md "Series
generations"):

- Closing is MANUAL. Nothing here runs from the service.
- A point belongs to the bucket of the task that produced it, by the task's
  START time (``end_time - observed_duration_sec`` of its result row): a task
  that started before the cut measured the old environment for its whole run.
- Tasks that started between ``cut`` and ``resume`` (the host was being
  updated or rebooted) are DISCARDED: dropped from both generations and listed
  in ``excluded_tasks.json`` with the reason. Their raw result rows are not
  touched here.
- Memory series measure bytes per key and are not affected by these
  environment changes, so only the v3 throughput and latency series (the
  files under the v3 state dir) are split.

Files, all under ``SWEEP_STATE_DIR`` (``sweep_data/``):

- ``generations.json``: the closed generations of this platform.
- ``excluded_tasks.json``: task ids the coordinators and exporters must skip.
- ``v3/generations/<id>/state_cachecannon-v3_<label>.json``: the archived
  state files, read once by ``conductress sweep export-archived``.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from conductress import config
from conductress.sweep.coordinator_v3 import V3_STATE_DIR
from conductress.sweep.planner import SweepState
from conductress.sweep.reset_series import _utc_stamp, service_is_active

GENERATIONS_FILE = config.SWEEP_STATE_DIR / "generations.json"
EXCLUDED_TASKS_FILE = config.SWEEP_STATE_DIR / "excluded_tasks.json"
GENERATIONS_SCHEMA_VERSION = 1

V3_STATE_PREFIX = "state_cachecannon-v3_"
_NOTE_RE = re.compile(r"\[cachecannon-sweep-v3:([^/\]]+)/([^\]]+)\]")
_END_TIME_RE = re.compile(r'"end_time":\s*"(\d{4}\.\d{2}\.\d{2}_\d{2}\.\d{2}\.\d{2}\.\d{6})"')
_TASK_TIME_FORMAT = "%Y.%m.%d_%H.%M.%S.%f"


# --------------------------------------------------------------------------- files


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_generations(path: Path = GENERATIONS_FILE) -> list[dict[str, Any]]:
    """The closed generations recorded on this runner, oldest first."""
    data = _read_json(path, {"schema_version": GENERATIONS_SCHEMA_VERSION, "closed": []})
    if not isinstance(data, dict) or data.get("schema_version") != GENERATIONS_SCHEMA_VERSION:
        raise ValueError(f"unsupported generations file: {path}")
    closed = data.get("closed", [])
    return list(closed) if isinstance(closed, list) else []


def save_generations(closed: list[dict[str, Any]], path: Path = GENERATIONS_FILE) -> None:
    _write_json(path, {"schema_version": GENERATIONS_SCHEMA_VERSION, "closed": list(closed)})


def load_excluded_tasks(path: Path = EXCLUDED_TASKS_FILE) -> dict[str, dict[str, Any]]:
    """Excluded task ids, each with the entry that explains the exclusion."""
    data = _read_json(path, {"schema_version": GENERATIONS_SCHEMA_VERSION, "excluded": []})
    if not isinstance(data, dict) or data.get("schema_version") != GENERATIONS_SCHEMA_VERSION:
        raise ValueError(f"unsupported excluded tasks file: {path}")
    entries = data.get("excluded", [])
    return {e["task_id"]: e for e in entries if isinstance(e, dict) and isinstance(e.get("task_id"), str)}


def append_excluded_tasks(entries: Iterable[dict[str, Any]], path: Path = EXCLUDED_TASKS_FILE) -> None:
    """Add exclusion entries; an id already listed keeps its first entry."""
    existing = load_excluded_tasks(path)
    for entry in entries:
        existing.setdefault(entry["task_id"], entry)
    _write_json(path, {"schema_version": GENERATIONS_SCHEMA_VERSION, "excluded": list(existing.values())})


_excluded_cache: tuple[Optional[Path], Optional[float], frozenset] = (None, None, frozenset())


def excluded_task_ids(path: Path = EXCLUDED_TASKS_FILE) -> frozenset:
    """The exclusion list as a set, re-read only when the file's mtime changes.

    Read on every task completion and every planner pass, so the file is
    parsed only when it moved. A missing file means nothing is excluded.
    """
    global _excluded_cache  # pylint: disable=global-statement
    try:
        mtime: Optional[float] = path.stat().st_mtime
    except OSError:
        mtime = None
    cached_path, cached_mtime, ids = _excluded_cache
    if cached_path != path or cached_mtime != mtime:
        ids = frozenset(load_excluded_tasks(path)) if mtime is not None else frozenset()
        _excluded_cache = (path, mtime, ids)
    return ids


def is_excluded(task_id: Optional[str], path: Path = EXCLUDED_TASKS_FILE) -> bool:
    """Whether a task id is on the exclusion list."""
    return bool(task_id) and task_id in excluded_task_ids(path)


# --------------------------------------------------------------------------- naming


def generation_dir(generation_id: str, state_dir: Path = V3_STATE_DIR) -> Path:
    """Where a closed generation's state files live."""
    return state_dir / "generations" / generation_id


def generation_path(path: Path, generation_id: Optional[str]) -> Path:
    """``series-x.epoch-v3.json`` -> ``series-x.epoch-v3.gen-<id>.json`` (None: unchanged)."""
    if not generation_id:
        return path
    return path.with_name(f"{path.stem}.gen-{generation_id}{path.suffix}")


def label_from_state_file(path: Path) -> str:
    name = path.name
    if not name.startswith(V3_STATE_PREFIX) or not name.endswith(".json"):
        raise ValueError(f"not a v3 state file: {path}")
    return name[len(V3_STATE_PREFIX) : -len(".json")]


def rehome(coordinator: Any, state_dir: Path) -> bool:
    """Point a coordinator at the copy of its state file under ``state_dir``.

    Used to export a closed generation: the roster is built as usual, then
    each coordinator reads the archived state instead of the live one. Returns
    False (and leaves the coordinator alone) when no archived file exists.
    """
    archived = state_dir / coordinator.state_file.name
    if not archived.exists():
        return False
    coordinator.state_file = archived
    coordinator.state = SweepState.load(archived)
    coordinator.planner = coordinator._new_planner()  # pylint: disable=protected-access
    return True


# --------------------------------------------------------------------------- result rows


def parse_task_time(text: str) -> datetime:
    """A Conductress task id / end_time (``2026.11.06_16.19.23.794391``) as a UTC datetime."""
    return datetime.strptime(text, _TASK_TIME_FORMAT).replace(tzinfo=timezone.utc)


def row_start_time(row: dict[str, Any]) -> Optional[datetime]:
    """When the task behind a result row started: end_time minus the measured duration."""
    end_text = row.get("end_time")
    duration = row.get("observed_duration_sec")
    if not isinstance(end_text, str) or not isinstance(duration, (int, float)):
        return None
    try:
        end = parse_task_time(end_text)
    except ValueError:
        return None
    return end - timedelta(seconds=float(duration))


@dataclass(frozen=True)
class RowRef:
    """The result row that produced a series point."""

    task_id: str
    end_time: datetime
    start_time: Optional[datetime]
    commit: str


def latest_rows(results_file: Path, labels: Iterable[str], since: datetime) -> dict[tuple[str, str], RowRef]:
    """The latest result row per (series label, commit) with ``end_time >= since``.

    One streaming pass over ``output.jsonl``. Only lines whose end_time (found
    with a regex, no JSON parse) is at or after ``since`` are decoded, so a
    multi-gigabyte results file costs one read and a few hundred parses. A
    commit absent from the result is older than ``since`` on this series.
    """
    wanted = set(labels)
    since_text = since.astimezone(timezone.utc).strftime(_TASK_TIME_FORMAT)
    found: dict[tuple[str, str], RowRef] = {}
    if not results_file.exists():
        return found
    with results_file.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = _END_TIME_RE.search(line)
            if match is None or match.group(1) < since_text:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            note = row.get("note")
            series = _NOTE_RE.match(note) if isinstance(note, str) else None
            if series is None or series.group(2) not in wanted:
                continue
            commit = row.get("commit_hash")
            task_id = row.get("task_id")
            if not isinstance(commit, str) or not isinstance(task_id, str):
                continue
            key = (series.group(2), commit)
            ref = RowRef(
                task_id=task_id, end_time=parse_task_time(match.group(1)), start_time=row_start_time(row), commit=commit
            )
            if key not in found or ref.end_time > found[key].end_time:
                found[key] = ref
    return found


def _find_row(rows: dict[tuple[str, str], RowRef], label: str, commit: str) -> Optional[RowRef]:
    ref = rows.get((label, commit))
    if ref is not None:
        return ref
    # A result row may carry a short hash while the state holds the full one.
    for (row_label, row_commit), candidate in rows.items():
        if row_label == label and (commit.startswith(row_commit) or row_commit.startswith(commit)):
            return candidate
    return None


# --------------------------------------------------------------------------- close


@dataclass
class SeriesSplit:
    label: str
    state_file: Path
    before: list[str] = field(default_factory=list)  # commits kept in the closed generation
    after: list[str] = field(default_factory=list)  # commits kept live
    discarded: list[dict[str, Any]] = field(default_factory=list)  # exclusion entries
    relocated_tasks: list[Path] = field(default_factory=list)
    state_backup: Optional[Path] = None


@dataclass
class CloseGenerationResult:
    generation_id: str
    cut: datetime
    resume: datetime
    dry_run: bool
    generation_dir: Path
    series: list[SeriesSplit] = field(default_factory=list)
    refused_reason: Optional[str] = None

    @property
    def discarded_count(self) -> int:
        return sum(len(s.discarded) for s in self.series)

    def summary_lines(self) -> list[str]:
        if self.refused_reason:
            return [f"Refused: {self.refused_reason}"]
        verb = "Would" if self.dry_run else "Did"
        lines = [
            f"Generation {self.generation_id}: cut {self.cut.isoformat()}"
            + (f", live resumes {self.resume.isoformat()}" if self.resume != self.cut else ""),
            f"  {verb} write archived state files to {self.generation_dir}",
        ]
        for s in self.series:
            lines.append(
                f"  {s.label}: {len(s.before)} point(s) archived, {len(s.after)} kept live, {len(s.discarded)} discarded"
            )
            for entry in s.discarded:
                lines.append(
                    f"    discard {entry['task_id']} (commit {entry['commit'][:8]}, started {entry['started_at']})"
                )
            for task_file in s.relocated_tasks:
                lines.append(f"    {verb.lower()} relocate queued cell {task_file.name}")
        lines.append(f"  {verb} record {self.discarded_count} excluded task(s) in excluded_tasks.json")
        return lines


def _bucket(start: Optional[datetime], cut: datetime, resume: datetime) -> str:
    if start is None or start < cut:
        return "before"
    if start < resume:
        return "discard"
    return "after"


def _copy_state_without_points(state: SweepState) -> SweepState:
    copy = SweepState(
        threshold=state.threshold,
        last_benchmarked_head=state.last_benchmarked_head,
        last_benchmarked_head_at=state.last_benchmarked_head_at,
        merge_commits=list(state.merge_commits),
        commit_dates=dict(state.commit_dates),
        commit_prs=dict(state.commit_prs),
        commit_titles=dict(state.commit_titles),
    )
    copy.landmarks = list(state.landmarks)
    return copy


def close_generation(
    *,
    cut: datetime,
    resume: Optional[datetime] = None,
    label: str,
    reason: str,
    recorded_by: str = "operator",
    workloads: Optional[Iterable[str]] = None,
    dry_run: bool = False,
    force: bool = False,
    state_dir: Path = V3_STATE_DIR,
    generations_file: Path = GENERATIONS_FILE,
    excluded_file: Path = EXCLUDED_TASKS_FILE,
    results_file: Optional[Path] = None,
    queue_dir: Optional[Path] = None,
    service_active: Optional[bool] = None,
    now: Optional[datetime] = None,
) -> CloseGenerationResult:
    """Close the live v3 generation of this platform at ``cut``.

    Every v3 throughput and latency series under ``state_dir`` (or only the
    labels in ``workloads``) is split by the start time of the task behind each
    completed point: before ``cut`` -> archived generation; at or after
    ``resume`` (default: ``cut``) -> stays live; in between -> discarded and
    listed in the exclusion file. Pending and build-failed points stay live.
    Queued cells of the split series are relocated like ``reset-series`` does.

    Pure of argparse; everything is a path or value so it is unit-testable.
    """
    if cut.tzinfo is None or (resume is not None and resume.tzinfo is None):
        raise ValueError("cut and resume must be timezone-aware (UTC)")
    resume = resume or cut
    if resume < cut:
        raise ValueError("resume must not be before cut")
    generation_id = cut.strftime("%Y-%m-%d")
    gen_dir = generation_dir(generation_id, state_dir)
    result = CloseGenerationResult(
        generation_id=generation_id, cut=cut, resume=resume, dry_run=dry_run, generation_dir=gen_dir
    )

    if not force:
        active = service_is_active() if service_active is None else service_active
        if active:
            result.refused_reason = (
                "conductress.service is active; stop it before closing a generation, or pass --force"
            )
            return result
    if any(g.get("id") == generation_id for g in load_generations(generations_file)):
        result.refused_reason = f"generation {generation_id} is already closed"
        return result

    results_file = results_file or config.CONDUCTRESS_OUTPUT
    queue_dir = Path(queue_dir) if queue_dir is not None else config.CONDUCTRESS_QUEUE
    wanted = set(workloads) if workloads is not None else None
    state_files = sorted(p for p in state_dir.glob(f"{V3_STATE_PREFIX}*.json") if "generations" not in p.parts)
    splits: list[SeriesSplit] = []
    for state_file in state_files:
        series_label = label_from_state_file(state_file)
        if wanted is not None and series_label not in wanted:
            continue
        splits.append(SeriesSplit(label=series_label, state_file=state_file))
    if not splits:
        result.refused_reason = f"no v3 state files to split under {state_dir}"
        return result

    since = datetime(cut.year, cut.month, cut.day, tzinfo=timezone.utc)
    rows = latest_rows(results_file, [s.label for s in splits], since)
    stamp = _utc_stamp()
    recorded_at = (now or datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z")
    archived_states: dict[Path, SweepState] = {}
    live_states: dict[Path, SweepState] = {}

    for split in splits:
        state = SweepState.load(split.state_file)
        archived = _copy_state_without_points(state)
        live = _copy_state_without_points(state)
        for commit, point in state.points.items():
            if point.value is None:
                live.points[commit] = point  # pending / build-failed: not a measurement
                continue
            ref = _find_row(rows, split.label, commit)
            bucket = "before" if ref is None else _bucket(ref.start_time, cut, resume)
            if bucket == "before":
                archived.points[commit] = point
                split.before.append(commit)
            elif bucket == "after":
                live.points[commit] = point
                split.after.append(commit)
            else:
                assert ref is not None
                split.discarded.append(
                    {
                        "task_id": ref.task_id,
                        "series": f"cachecannon-sweep-v3/{split.label}",
                        "commit": commit,
                        "started_at": ref.start_time.isoformat().replace("+00:00", "Z") if ref.start_time else None,
                        "reason": f"started between the cut {cut.isoformat()} and the resume {resume.isoformat()}: {reason}",
                        "recorded_at": recorded_at,
                        "recorded_by": recorded_by,
                    }
                )
        archived_states[split.state_file] = archived
        live_states[split.state_file] = live
        split.state_backup = split.state_file.with_name(f"{split.state_file.name}.bak-{stamp}")
        if queue_dir.exists():
            prefix = "[cachecannon-sweep-v3:"
            for task_file in sorted(queue_dir.glob("task_*.json")):
                try:
                    doc = json.loads(task_file.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                note = doc.get("note")
                match = _NOTE_RE.match(note) if isinstance(note, str) and note.startswith(prefix) else None
                if match is not None and match.group(2) == split.label:
                    split.relocated_tasks.append(task_file)

    result.series = splits
    if dry_run:
        return result

    gen_dir.mkdir(parents=True, exist_ok=True)
    relocate_dir = queue_dir / f"reset-{stamp}"
    for split in splits:
        assert split.state_backup is not None
        shutil.copy2(split.state_file, split.state_backup)
        archived_states[split.state_file].save(gen_dir / split.state_file.name)
        live_states[split.state_file].save(split.state_file)
        for task_file in split.relocated_tasks:
            relocate_dir.mkdir(parents=True, exist_ok=True)
            task_file.rename(relocate_dir / task_file.name)
    entries = [entry for split in splits for entry in split.discarded]
    if entries:
        append_excluded_tasks(entries, excluded_file)
    # A copy travels with the archived state so its export is self-contained.
    if excluded_file.exists():
        shutil.copy2(excluded_file, gen_dir / excluded_file.name)
    closed = load_generations(generations_file)
    closed.append(
        {
            "id": generation_id,
            "epoch": "v3",
            "until": cut.isoformat().replace("+00:00", "Z"),
            "live_from": resume.isoformat().replace("+00:00", "Z"),
            "label": label,
            "reason": reason,
            "workloads": [s.label for s in splits],
            "excluded_tasks": len(entries),
            "closed_at": recorded_at,
            "closed_by": recorded_by,
        }
    )
    save_generations(closed, generations_file)
    return result
