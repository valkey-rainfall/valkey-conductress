"""What the public dashboard files may say about a runner's environment.

The control service publishes ``canary/<runner>.json`` and ``canary/fleet.json``
and the runners publish sweep series and manifests, all without auth. A
recorded environment holds values that must not be public: the harness
revision (a private-fork commit), the load generator commit, instance ids, boot
ids, host fingerprints and operator names. This module is the one place that
decides what a public file may carry:

- ``public_environment`` keeps only allowlisted environment keys (versions of
  public packages, CPU model, platform words);
- ``public_change_label`` turns a change list (``key: 'old' -> 'new'``) into a
  public sentence: version fields keep their values, private fields become the
  TYPE of change ("harness revision changed", "reboot", "host replaced");
- ``public_value_key`` / ``change_type_label`` classify one field the same
  way, for documents that list changes field by field (the canary findings);
- ``public_text`` / ``check_public_text`` guard operator-written text (labels,
  explanations): text that looks like a revision id, an IP address, an
  absolute path, an e-mail address or an instance id is refused by the
  writers and withheld by the publishers;
- ``public_canary_entry`` / ``public_canary_status`` are the allowlist
  serializers for the canary status files. A key outside the allowlist is
  dropped, never copied; ``unknown_canary_keys`` lists such keys so a test can
  fail when a new field appears and force a decision.

Allowlist, not denylist: a field nobody has looked at is private by default.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

# Environment keys whose VALUES are public: distribution package versions, the
# CPU-reported microcode revision, CPU model and platform words.
PUBLIC_ENVIRONMENT_KEYS = frozenset(
    {
        "provenance_schema_version",
        "kernel_release",
        "glibc_version",
        "cpu_microcode",
        "cpu_model",
        "machine",
        "platform",
        "cachecannon_dirty",
        "packages",
        "same_host",
    }
)
# The ``server_`` twins on two-host runners.
PUBLIC_ENVIRONMENT_KEYS = PUBLIC_ENVIRONMENT_KEYS | frozenset(
    f"server_{key}" for key in ("kernel_release", "glibc_version", "cpu_microcode", "cpu_model", "machine", "packages")
)

# Fields whose change is public only as a TYPE of event, never as a value.
_CHANGE_TYPE_LABELS = {
    "boot_id": "reboot",
    "server_boot_id": "server reboot",
    "conductress_revision": "harness revision changed",
    "cachecannon_commit": "load generator changed",
    "cachecannon_sha256": "load generator changed",
    "server_conductress_revision": "server harness revision changed",
    "instance_id": "host replaced",
    "host_fingerprint": "host replaced",
    "server_instance_id": "server host replaced",
    "server_host_fingerprint": "server host replaced",
    "mitigations_hash": "CPU mitigations changed",
    "server_mitigations_hash": "server CPU mitigations changed",
}

EVENT_PREFIX = "event: "


def public_value_key(key: str) -> bool:
    """Whether the VALUE of environment field ``key`` may be published (``packages.<name>`` included)."""
    return key in PUBLIC_ENVIRONMENT_KEYS or key.startswith("packages.") or key.startswith("server_packages.")


def change_type_label(key: str) -> str:
    """The public words for a change of ``key`` when its value is not public ("reboot", "host replaced")."""
    return _CHANGE_TYPE_LABELS.get(key, f"{key} changed")


def public_environment(environment: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The allowlisted keys of a recorded environment (None stays None)."""
    if environment is None:
        return None
    public: Dict[str, Any] = {}
    for key in sorted(environment):
        if key not in PUBLIC_ENVIRONMENT_KEYS:
            continue
        value = environment[key]
        if key.endswith("packages") and isinstance(value, dict):
            value = {str(k): v for k, v in sorted(value.items())}
        public[key] = value
    return public


def public_change_label(changes: Iterable[str], max_value_chars: int = 24) -> str:
    """A public label for a list of ``key: 'old' -> 'new'`` changes.

    Version fields keep ``key: old -> new``. Private fields become the type of
    change (see ``_CHANGE_TYPE_LABELS``); any other field becomes ``key
    changed``. Manual events (``event: <label>``) pass through: the operator
    wrote them for a public reader. Repeated phrases appear once.
    """
    parts: List[str] = []
    for change in changes:
        if change.startswith(EVENT_PREFIX):
            phrase = change[len(EVENT_PREFIX) :].strip()
        else:
            key, _, rest = change.partition(": ")
            if key in _CHANGE_TYPE_LABELS or not public_value_key(key):
                phrase = change_type_label(key)
            else:
                old_v, _, new_v = rest.partition(" -> ")
                phrase = f"{key}: {_clip(old_v, max_value_chars)} -> {_clip(new_v, max_value_chars)}"
        if phrase and phrase not in parts:
            parts.append(phrase)
    return "; ".join(parts)


def public_annotation(annotation: Optional[str]) -> Optional[str]:
    """``public_change_label`` for a stored ``field: 'old' -> 'new'; ...`` annotation string."""
    if not annotation:
        return annotation
    return public_change_label(part.strip() for part in annotation.split("; ") if part.strip())


def _clip(value: str, max_chars: int) -> str:
    return value.strip().strip("'\"")[:max_chars]


# ------------------------------------------------------------- sweep manifests

# A closed generation entry (sweep/generations.py) minus who closed it.
PUBLIC_GENERATION_KEYS = frozenset(
    {"id", "epoch", "until", "live_from", "label", "reason", "workloads", "excluded_tasks", "closed_at"}
)


def public_generation(generation: Dict[str, Any]) -> Dict[str, Any]:
    """A manifest's generation entry without ``closed_by`` (an operator's name), its free text checked."""
    public = {key: generation[key] for key in sorted(generation) if key in PUBLIC_GENERATION_KEYS}
    for key in ("label", "reason"):
        if isinstance(public.get(key), str):
            public[key] = public_text(public[key]) or ""
    return public


# ------------------------------------------------------------- operator text

WITHHELD = "(text withheld: it failed the public text check)"

_TEXT_PATTERNS = (
    ("a 40-character revision id", re.compile(r"\b[0-9a-f]{40}\b", re.IGNORECASE)),
    ("an IPv4 address", re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")),
    (
        "an absolute system or home path",
        re.compile(r"(?:^|[\s'\"(=])/(?:home|etc|opt|var|root|tmp|usr|local)/", re.IGNORECASE),
    ),
    ("an e-mail address", re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")),
    ("a cloud instance id", re.compile(r"\bi-[0-9a-f]{8,17}\b")),
)


def private_detail(text: str) -> Optional[str]:
    """What private detail ``text`` seems to hold, or None when it holds none."""
    for what, pattern in _TEXT_PATTERNS:
        if pattern.search(text):
            return what
    return None


def public_text(text: Optional[str], *, max_chars: int = 300) -> Optional[str]:
    """``text`` (whitespace normalised, cut to ``max_chars``) when safe, :data:`WITHHELD` when not, None for none."""
    if text is None:
        return None
    value = " ".join(str(text).split())
    if not value:
        return None
    if private_detail(value) is not None:
        return WITHHELD
    return value[:max_chars]


def check_public_text(text: str, what: str, *, max_chars: int = 300) -> str:
    """The normalised text, or ValueError naming the problem (for the writing commands)."""
    value = " ".join(str(text).split())
    if not value:
        raise ValueError(f"{what} must not be empty")
    if len(value) > max_chars:
        raise ValueError(f"{what} is longer than {max_chars} characters")
    detail = private_detail(value)
    if detail is not None:
        raise ValueError(f"{what} looks like it holds {detail}; it is published, so write it for a public reader")
    return value


# ----------------------------------------------------------------- canary files

CANARY_ENTRY_KEYS = frozenset(
    {
        "runner_id",
        "display_name",
        "platform",
        "enabled",
        "canary_profile",
        "archived",
        "archive_of",
        "archived_until",
        "archive_note",
        "archives",
        "profile",
        "schedule",
        "series",
        "latest_observation",
        "calibration_report",
        "semantics",
    }
)
CANARY_ARCHIVE_KEYS = frozenset({"runner_id", "archived_until", "archive_note"})
CANARY_PROFILE_KEYS = frozenset({"profile_id", "profile_version", "description", "source", "pinned_commit", "status"})
CANARY_SCHEDULE_KEYS = frozenset({"utc_date", "state", "task_id", "task_state", "created_at", "updated_at"})
CANARY_SERIES_KEYS = frozenset(
    {
        "runner_id",
        "profile_id",
        "profile_version",
        "phase",
        "progress",
        "accepted_count",
        "rejected_count",
        "calibration_status",
        "calibration_samples_required",
        "observation_samples_required",
        "latest_observation",
    }
)
# Observation fields copied as they are. ``environment`` and
# ``env_change_annotation`` are transformed; ``environment_json`` is dropped.
CANARY_OBSERVATION_KEYS = frozenset(
    {
        "runner_id",
        "profile_id",
        "profile_version",
        "task_id",
        "utc_date",
        "score",
        "ref_median",
        "ref_mad",
        "ref_sample_count",
        "delta_pct",
        "candidate_signal",
        "candidate_warning_pct",
        "candidate_alarm_pct",
        "phase",
        "accepted",
        "actionable",
        "rejection_reason",
        "series_ordinal",
        "window_start",
        "window_end",
        "completed_at",
        "created_at",
        "provenance_schema_version",
        "environment_parse_error",
    }
)
CANARY_OBSERVATION_TRANSFORMED_KEYS = frozenset({"environment", "env_change_annotation"})
CANARY_OBSERVATION_DROPPED_KEYS = frozenset({"environment_json"})
CANARY_REPORT_KEYS = frozenset(
    {
        "runner_id",
        "profile_id",
        "profile_version",
        "status",
        "sample_count",
        "date_range",
        "median_score",
        "mad",
        "min_score",
        "max_score",
        "spread_pct",
        "robust_sigma",
        "variability_floor_pct",
        "candidate_warning_pct",
        "candidate_alarm_pct",
        "recommended_warning_pct",
        "recommended_alarm_pct",
        "zero_median_warning",
        "note",
    }
)
CANARY_STATUS_KEYS = frozenset({"generated_at", "runners"})


def _pick(source: Optional[Dict[str, Any]], keys: frozenset) -> Optional[Dict[str, Any]]:
    if source is None:
        return None
    return {key: source[key] for key in sorted(source) if key in keys}


def public_observation(observation: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The public projection of a canary observation (status entry or series row)."""
    if observation is None:
        return None
    public = _pick(observation, CANARY_OBSERVATION_KEYS) or {}
    if "env_change_annotation" in observation:
        public["env_change_annotation"] = public_annotation(observation.get("env_change_annotation"))
    if "environment" in observation:
        public["environment"] = public_environment(observation.get("environment"))
    return public


def public_canary_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """The public projection of one runner entry of ``GET /api/v1/canary/status``."""
    public = _pick(entry, CANARY_ENTRY_KEYS) or {}
    if entry.get("archives") is not None:
        public["archives"] = [_pick(a, CANARY_ARCHIVE_KEYS) for a in entry["archives"]]
    if "profile" in entry:
        public["profile"] = _pick(entry.get("profile"), CANARY_PROFILE_KEYS)
    if "schedule" in entry:
        schedule = entry.get("schedule")
        public["schedule"] = None if schedule is None else [_pick(s, CANARY_SCHEDULE_KEYS) for s in schedule]
    if "series" in entry:
        series = _pick(entry.get("series"), CANARY_SERIES_KEYS)
        if series is not None and "latest_observation" in series:
            series["latest_observation"] = public_observation(series["latest_observation"])
        public["series"] = series
    if "latest_observation" in entry:
        public["latest_observation"] = public_observation(entry.get("latest_observation"))
    if "calibration_report" in entry:
        public["calibration_report"] = _pick(entry.get("calibration_report"), CANARY_REPORT_KEYS)
    return public


def public_canary_status(status: Dict[str, Any]) -> Dict[str, Any]:
    """The public projection of the whole canary status document (``canary/fleet.json``)."""
    public = _pick(status, CANARY_STATUS_KEYS) or {}
    public["runners"] = [public_canary_entry(e) for e in status.get("runners", [])]
    return public


def unknown_canary_keys(status: Dict[str, Any]) -> List[str]:
    """Keys in a canary status document that the allowlist has not decided on.

    Each is ``<path>.<key>``. A test fails on any entry, so a field added to the
    status API is classified (public, transformed or dropped) before it can be
    published by accident.
    """
    unknown: List[str] = []

    def check(obj: Optional[Dict[str, Any]], keys: frozenset, path: str) -> None:
        if not obj:
            return
        for key in obj:
            if key not in keys:
                unknown.append(f"{path}.{key}")

    def check_observation(obs: Optional[Dict[str, Any]], path: str) -> None:
        check(
            obs,
            CANARY_OBSERVATION_KEYS | CANARY_OBSERVATION_TRANSFORMED_KEYS | CANARY_OBSERVATION_DROPPED_KEYS,
            path,
        )

    check(status, CANARY_STATUS_KEYS, "status")
    for entry in status.get("runners", []):
        path = f"runners[{entry.get('runner_id')}]"
        check(entry, CANARY_ENTRY_KEYS, path)
        for archive in entry.get("archives") or []:
            check(archive, CANARY_ARCHIVE_KEYS, f"{path}.archives[]")
        check(entry.get("profile"), CANARY_PROFILE_KEYS, f"{path}.profile")
        for item in entry.get("schedule") or []:
            check(item, CANARY_SCHEDULE_KEYS, f"{path}.schedule[]")
        series = entry.get("series")
        check(series, CANARY_SERIES_KEYS, f"{path}.series")
        if series:
            check_observation(series.get("latest_observation"), f"{path}.series.latest_observation")
        check_observation(entry.get("latest_observation"), f"{path}.latest_observation")
        check(entry.get("calibration_report"), CANARY_REPORT_KEYS, f"{path}.calibration_report")
    return unknown
