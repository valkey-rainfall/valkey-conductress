"""Gather the inputs of the canary findings (see ``findings.py``) on the data host.

Two sources:

- the control database: every accepted canary observation of a live runner
  and of its archives (an archive holds the runner's history before a manual
  series cut, so the two are stitched back together for event analysis);
- the published sweep directory, where the runners' publishers put their
  manifests and series files: manual environment events, operator
  explanations, and the paired evidence of each closed series generation
  (the same commit and workload measured before and after the cut), and
  the runners' confirmation runs (``confirm-<label>.epoch-<epoch>.json``).

Everything here only reads. A missing or unreadable file is skipped with a
log line; it never stops the analysis.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .db import ControlDatabase
from .findings import ConfirmationRun, Explanation, ManualEvent, Observation, PairedSeries, RunnerInput
from .fleet_registry import FleetRegistry

logger = logging.getLogger(__name__)

_LATENCY_ID = re.compile(r"-r\d+k?$")
_MANIFEST = re.compile(r"^manifest-(?P<label>[a-z0-9]+)\.epoch-(?P<epoch>[a-z0-9]+)\.json$")


def parse_time(value: Any) -> Optional[datetime]:
    """An ISO 8601 instant as an aware UTC datetime; None for missing or bad input."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def load_observations(database: ControlDatabase, runner_ids: list[str], profile_id: str) -> list[Observation]:
    """Accepted observations of the given runner ids (a live runner and its archives), oldest first.

    The profile version is added to the environment, so a profile change is an
    event like any other change of the measurement.
    """
    if not runner_ids:
        return []
    marks = ",".join("?" for _ in runner_ids)
    with database.read() as connection:
        rows = connection.execute(
            "SELECT utc_date, score, completed_at, environment_json, profile_version FROM canary_observations "
            f"WHERE runner_id IN ({marks}) AND profile_id = ? AND accepted = 1 AND score IS NOT NULL "
            "ORDER BY utc_date, completed_at",
            (*runner_ids, profile_id),
        ).fetchall()
    by_date: dict[str, Observation] = {}
    for row in rows:
        try:
            environment = json.loads(row["environment_json"] or "{}")
        except (TypeError, ValueError):
            environment = {}
        if not isinstance(environment, dict):
            environment = {}
        environment["profile_version"] = row["profile_version"]
        # One observation per date; an archive and the live series never share a date, but if
        # they did, the later completion wins.
        by_date[row["utc_date"]] = Observation(
            row["utc_date"], float(row["score"]), environment, parse_time(row["completed_at"])
        )
    return [by_date[d] for d in sorted(by_date)]


def _read_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        logger.warning("unable to read %s for canary findings", path, exc_info=True)
        return None


def _series_platform(sweep_dir: Path, label: str, epoch: str, manifest: dict[str, Any]) -> Optional[str]:
    """The full platform string of a sweep platform label, read from one of its live series files."""
    for workload in manifest.get("throughput_workloads") or []:
        data = _read_json(sweep_dir / f"series-{label}-{workload}-throughput.epoch-{epoch}.json")
        if isinstance(data, dict):
            platform = (data.get("metadata") or {}).get("platform")
            if platform:
                return str(platform)
    return None


def _value(point: dict[str, Any], workload: str, metric: str) -> Optional[float]:
    if metric == "latency":
        value = point.get("p99_us")
    else:
        value = ((point.get("results") or {}).get(workload) or {}).get("rps")
    if isinstance(value, (int, float)) and value > 0:
        return float(value)
    return None


def _paired_for_generation(sweep_dir: Path, label: str, epoch: str, generation: dict[str, Any]) -> list[PairedSeries]:
    cut = parse_time(generation.get("until"))
    gen_id = generation.get("id")
    if cut is None or not gen_id:
        return []
    out = []
    for workload in generation.get("workloads") or []:
        metric = "latency" if _LATENCY_ID.search(str(workload)) else "throughput"
        live = _read_json(sweep_dir / f"series-{label}-{workload}-{metric}.epoch-{epoch}.json")
        old = _read_json(sweep_dir / f"series-{label}-{workload}-{metric}.epoch-{epoch}.gen-{gen_id}.json")
        if not isinstance(live, dict) or not isinstance(old, dict):
            continue
        key = (live.get("metadata") or {}).get("workload") or workload
        before = {p.get("commit"): _value(p, key, metric) for p in old.get("points") or []}
        after = {p.get("commit"): _value(p, key, metric) for p in live.get("points") or []}
        commits = sorted(c for c in set(before) & set(after) if c and before[c] and after[c])
        if not commits:
            continue
        out.append(
            PairedSeries(
                series=f"{metric} {workload}",
                cut=cut,
                before=tuple(before[c] for c in commits),  # type: ignore[misc]
                after=tuple(after[c] for c in commits),  # type: ignore[misc]
                lower_is_better=metric == "latency",
            )
        )
    return out


CONFIRMATION_SERIES = "confirmation run"


def _confirmation_inputs(document: dict[str, Any]) -> tuple[list[PairedSeries], list[ConfirmationRun]]:
    """Paired evidence (cut = event time) and a summary per run of one confirm file."""
    paired, runs = [], []
    for run in document.get("runs") or []:
        if not isinstance(run, dict):
            continue
        event_at = parse_time(run.get("event_at"))
        if event_at is None:
            continue
        done = [
            p
            for p in run.get("pairs") or []
            if isinstance(p, dict)
            and isinstance(p.get("before"), (int, float))
            and isinstance(p.get("after"), (int, float))
            and p["before"] > 0
            and p["after"] > 0
        ]
        if done:
            paired.append(
                PairedSeries(
                    series=CONFIRMATION_SERIES,
                    cut=event_at,
                    before=tuple(float(p["before"]) for p in done),
                    after=tuple(float(p["after"]) for p in done),
                )
            )
        when = parse_time(run.get("ended_at")) or parse_time(run.get("started_at")) or event_at
        runs.append(ConfirmationRun(when.date().isoformat(), len(done), str(run.get("status") or "")))
    return paired, runs


def load_sweep_inputs(sweep_dir: Optional[Path]) -> dict[str, dict[str, Any]]:
    """Per full platform string: manual events, explanations and paired series from the live manifests."""
    result: dict[str, dict[str, Any]] = {}
    if sweep_dir is None or not sweep_dir.is_dir():
        return result
    for path in sorted(sweep_dir.glob("manifest-*.epoch-*.json")):
        match = _MANIFEST.match(path.name)
        if match is None:
            continue  # a closed generation's own manifest (.gen-<id>) or an unrelated file
        manifest = _read_json(path)
        if not isinstance(manifest, dict):
            continue
        label, epoch = match.group("label"), match.group("epoch")
        platform = _series_platform(sweep_dir, label, epoch, manifest)
        if platform is None:
            continue
        entry = result.setdefault(
            platform, {"manual_events": {}, "explanations": {}, "paired": [], "confirmation_runs": []}
        )
        for raw in manifest.get("environment_events") or []:
            at = parse_time(raw.get("at")) if isinstance(raw, dict) else None
            if at is None:
                continue
            fields = tuple(str(f) for f in raw.get("fields") or [] if isinstance(f, str))
            event = ManualEvent(at, str(raw.get("label") or ""), fields, parse_time(raw.get("until")))
            entry["manual_events"][(event.at, event.label)] = event
        for raw in manifest.get("explanations") or []:
            if isinstance(raw, dict) and raw.get("event_date") and raw.get("text"):
                entry["explanations"][str(raw["event_date"])] = Explanation(str(raw["event_date"]), str(raw["text"]))
        for generation in manifest.get("generations") or []:
            if isinstance(generation, dict) and generation.get("epoch", epoch) == epoch:
                entry["paired"].extend(_paired_for_generation(sweep_dir, label, epoch, generation))
        confirm = _read_json(sweep_dir / f"confirm-{label}.epoch-{epoch}.json")
        if isinstance(confirm, dict):
            paired, runs = _confirmation_inputs(confirm)
            entry["paired"].extend(paired)
            entry["confirmation_runs"].extend(runs)
    return result


def load_runner_inputs(
    database: ControlDatabase, registry: FleetRegistry, sweep_dir: Optional[Path]
) -> list[RunnerInput]:
    """One input per live runner with a canary profile, its archives stitched in."""
    sweep = load_sweep_inputs(sweep_dir)
    inputs = []
    for runner in registry.list_runners(include_archived=False):
        profile_id = runner.get("canary_profile")
        if not profile_id:
            continue
        ids = [runner["runner_id"]] + [a["runner_id"] for a in registry.archives_of(runner["runner_id"])]
        observations = load_observations(database, ids, profile_id)
        if not observations:
            continue
        extra = sweep.get(runner.get("platform", ""), {})
        inputs.append(
            RunnerInput(
                runner_id=runner["runner_id"],
                display_name=runner.get("display_name") or runner["runner_id"],
                observations=observations,
                manual_events=list((extra.get("manual_events") or {}).values()),
                explanations=list((extra.get("explanations") or {}).values()),
                paired=list(extra.get("paired") or []),
                confirmation_runs=list(extra.get("confirmation_runs") or []),
            )
        )
    return inputs
