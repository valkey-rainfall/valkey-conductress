"""Operator notes about this runner's environment: manual events and explanations.

File: ``sweep_data/environment_events.json``, next to ``generations.json``.
Written only by ``conductress sweep add-event`` and ``conductress sweep
explain-event`` (or by hand), never by the service. Two lists:

- ``events``: changes the provenance fields cannot see, or see only in part
  (a package release that keeps the library version string, a BIOS update):
  ``{at, until?, label, fields?}``. The sweep exporter uses ``at`` for the
  environment periods; the canary findings on the data host add each event
  to the observation gap that contains it.
- ``explanations``: ``{event_date, text}``. An explanation moves the canary
  event of that date on this runner to the state "explained".

The live manifest publishes both lists without the recorder and recording
time (:func:`manifest_notes`). Labels and texts are public and must pass
:func:`conductress.public_fields.check_public_text`.

The older form of the file, a bare list of ``{at, label}``, is still read.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from conductress.public_fields import check_public_text, public_text

SCHEMA_VERSION = 1
_FIELD_RE = re.compile(r"^[a-z][a-z0-9_. -]{0,39}$")


def default_path() -> Path:
    from conductress.config import SWEEP_STATE_DIR

    return SWEEP_STATE_DIR / "environment_events.json"


def parse_utc(text: str) -> datetime:
    """An ISO 8601 instant as an aware UTC datetime (a naive value is UTC)."""
    value = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _stamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_document(path: Optional[Path] = None) -> dict[str, Any]:
    """The whole file as ``{schema_version, events, explanations}``; empty lists when missing."""
    path = path or default_path()
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "events": [], "explanations": []}
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {"schema_version": SCHEMA_VERSION, "events": list(data), "explanations": []}
    return {
        "schema_version": data.get("schema_version", SCHEMA_VERSION),
        "events": list(data.get("events") or []),
        "explanations": list(data.get("explanations") or []),
    }


def _write(document: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def add_event(
    *,
    at: datetime,
    label: str,
    fields: Iterable[str] = (),
    until: Optional[datetime] = None,
    recorded_by: str = "operator",
    path: Optional[Path] = None,
    dry_run: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Append a manual event and return it. Refuses private-looking text and a reversed interval."""
    path = path or default_path()
    clean_label = check_public_text(label, "label")
    clean_fields = []
    for name in fields:
        value = " ".join(str(name).split()).lower()
        if not value:
            continue
        if not _FIELD_RE.match(value):
            raise ValueError(f"field {name!r} must be 1-40 lowercase letters, digits, spaces, '.', '_' or '-'")
        clean_fields.append(value)
    if until is not None and until < at:
        raise ValueError("--until must not be before --at")
    entry: dict[str, Any] = {"at": _stamp(at), "label": clean_label}
    if until is not None:
        entry["until"] = _stamp(until)
    if clean_fields:
        entry["fields"] = clean_fields
    document = load_document(path)
    if any(e.get("at") == entry["at"] and e.get("label") == clean_label for e in document["events"]):
        raise ValueError("the same event (time and label) is already recorded")
    recorded = dict(entry, recorded_at=_stamp(now or datetime.now(timezone.utc)), recorded_by=recorded_by)
    if not dry_run:
        document["events"].append(recorded)
        document["events"].sort(key=lambda e: str(e.get("at")))
        _write(document, path)
    return entry


def add_explanation(
    *,
    event_date: str,
    text: str,
    recorded_by: str = "operator",
    path: Optional[Path] = None,
    dry_run: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Record (or replace) the explanation for the event of ``event_date`` (YYYY-MM-DD)."""
    path = path or default_path()
    try:
        day = date.fromisoformat(event_date).isoformat()
    except ValueError as exc:
        raise ValueError(f"event date must be YYYY-MM-DD, got {event_date!r}") from exc
    entry = {"event_date": day, "text": check_public_text(text, "explanation")}
    if not dry_run:
        document = load_document(path)
        document["explanations"] = [e for e in document["explanations"] if e.get("event_date") != day]
        document["explanations"].append(
            dict(entry, recorded_at=_stamp(now or datetime.now(timezone.utc)), recorded_by=recorded_by)
        )
        document["explanations"].sort(key=lambda e: str(e.get("event_date")))
        _write(document, path)
    return entry


def manifest_notes(path: Optional[Path] = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``(events, explanations)`` for the public manifest: allowlisted keys, checked text, no recorder."""
    try:
        document = load_document(path)
    except (OSError, ValueError):
        return [], []
    events = []
    for raw in document["events"]:
        if not isinstance(raw, dict):
            continue
        try:
            at = _stamp(parse_utc(raw["at"]))
        except (KeyError, TypeError, ValueError):
            continue
        entry: dict[str, Any] = {"at": at, "label": public_text(raw.get("label")) or ""}
        if raw.get("until"):
            try:
                entry["until"] = _stamp(parse_utc(raw["until"]))
            except (TypeError, ValueError):
                pass
        fields = [f for f in (public_text(str(x), max_chars=40) for x in raw.get("fields") or []) if f]
        if fields:
            entry["fields"] = fields
        events.append(entry)
    explanations = []
    for raw in document["explanations"]:
        if isinstance(raw, dict) and raw.get("event_date") and public_text(raw.get("text")):
            explanations.append({"event_date": str(raw["event_date"]), "text": public_text(raw.get("text"))})
    return events, explanations
