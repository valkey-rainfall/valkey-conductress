"""Canary findings: environment events, their statistics, ranking and consistency.

The daily canary measures one pinned commit on every runner, so a step in its
series is a change of the runner, not of the code. This module turns the
accepted canary observations of each runner, plus operator input, into
EVENTS (one per observation gap in which the recorded environment changed),
tests each event, ranks the events and describes how consistent each runner
stayed. It is pure: the control service gathers the inputs and publishes the
two documents :func:`build_findings` returns.

Rules (also in ``docs/canary-drift-analysis.md``, "Findings"):

- Event: every field change between two consecutive accepted observations of
  one runner, plus every manual event whose time falls in that gap, is ONE
  event. No fixed-commit measurement lies between them, so no data can
  separate them. The effect is attributed to the event as a whole, never to
  one field.
- Step location: robust binary segmentation restricted to event positions.
  A candidate is accepted as a step (a "cut") when its split gives the best
  L1 cost reduction, p < 0.01, ``|effect| >=`` the runner floor, at least 7
  days before and 3 after. Windows of later tests are bounded by accepted
  cuts only, so a harness deploy every few days does not leave 1-day windows.
- Test per (event, series): effect = median(after) / median(before) - 1 with
  a bootstrap CI, two-sided Mann-Whitney by permutation of the rank sum.
  Paired series (the same commit and workload measured before and after):
  median log ratio, bootstrap CI over pairs, Wilcoxon signed-rank test by
  sign flips.
- Benjamini-Hochberg over every test in one analysis.
- Floor per runner: ``max(0.5%, 3 x robust sd of the canary before window)``.
- States: ``confirmed``, ``suspected``, ``insufficient-data``, ``cleared``
  (``below_floor_shift`` when significant but inside the floor),
  ``explained`` (an operator explanation exists) and, for one-day fleet-wide
  moves after a fleet harness deploy, ``suspected-fleet`` (low rank, never
  open, never an alarm).

Published values: nothing in the documents may be private. The changed fields
go through the allowlist of ``conductress.public_fields``: a field keeps its
values only when ``public_value_key`` allows it (package and CPU-reported
versions); harness and load generator revisions, boot ids, instance ids, host
fingerprints and mitigation hashes appear only as the type of change
(``change_type_label``: "harness revision changed", "reboot", "host
replaced"). Free text from operators passes ``public_text`` first.
"""

from __future__ import annotations

import math
import zlib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional, Sequence

import numpy as np

from ..public_fields import change_type_label, public_text, public_value_key
from .drift_analyzer import MAD_SIGMA_SCALE

FINDINGS_SCHEMA_VERSION = 1
ANALYSIS_VERSION = "findings-1"

STATE_CONFIRMED = "confirmed"
STATE_SUSPECTED = "suspected"
STATE_INSUFFICIENT = "insufficient-data"
STATE_CLEARED = "cleared"
STATE_EXPLAINED = "explained"
STATE_SUSPECTED_FLEET = "suspected-fleet"
OPEN_STATES = (STATE_CONFIRMED, STATE_SUSPECTED)
# Page order: open findings first, one-day fleet moves low, cleared last.
STATE_ORDER = {
    STATE_CONFIRMED: 0,
    STATE_SUSPECTED: 1,
    STATE_INSUFFICIENT: 2,
    STATE_SUSPECTED_FLEET: 3,
    STATE_EXPLAINED: 4,
    STATE_CLEARED: 5,
}

KIND_HOST = "host replaced"
KIND_KERNEL = "kernel or firmware"
KIND_LIBRARY = "system package"
KIND_GENERATOR = "load generator"
KIND_PROFILE = "canary profile"
KIND_REBOOT = "reboot only"
KIND_HARNESS = "harness revision"
KIND_MANUAL = "manual"
KIND_OTHER = "other change"

# Fields that never describe the environment (identity and record metadata).
_IGNORED_FIELDS = frozenset(
    {
        "runner_id",
        "platform",
        "provenance_schema_version",
        "same_host",
        "environment_parse_error",
        "server_same_host",
    }
)
# Private fields whose change is published as its type only (change_type_label).
_KNOWN_PRIVATE_FIELDS = frozenset(
    prefix + key
    for prefix in ("", "server_")
    for key in (
        "boot_id",
        "conductress_revision",
        "cachecannon_commit",
        "cachecannon_sha256",
        "instance_id",
        "host_fingerprint",
        "mitigations_hash",
    )
)
# Readable names for fields whose values are public.
_LABELS = {
    "kernel_release": "kernel",
    "glibc_version": "glibc",
    "cpu_microcode": "microcode",
    "cpu_model": "CPU model",
    "machine": "machine",
    "profile_version": "canary profile version",
}
_KIND_RULES: tuple[tuple[str, frozenset], ...] = (
    (KIND_HOST, frozenset({"instance_id", "host_fingerprint", "cpu_model", "machine"})),
    (KIND_KERNEL, frozenset({"kernel_release", "cpu_microcode", "mitigations_hash"})),
    (KIND_LIBRARY, frozenset({"glibc_version"})),
    (KIND_GENERATOR, frozenset({"cachecannon_commit", "cachecannon_dirty", "cachecannon_sha256"})),
    (KIND_PROFILE, frozenset({"profile_version"})),
)
# Words an operator may use in a manual event's ``fields``, mapped to recorded field names.
_MANUAL_ALIASES = {
    "kernel": "kernel_release",
    "glibc": "glibc_version",
    "microcode": "cpu_microcode",
    "microcode_ctl": "packages.microcode_ctl",
    "linux-firmware": "packages.linux-firmware",
    "tuned": "packages.tuned",
    "systemd": "packages.systemd",
    "reboot": "boot_id",
    "load generator": "cachecannon_commit",
    "harness": "conductress_revision",
}
_PACKAGE_KIND = {"kernel": KIND_KERNEL, "microcode_ctl": KIND_KERNEL, "linux-firmware": KIND_KERNEL}


# --------------------------------------------------------------------------- inputs


@dataclass(frozen=True)
class Observation:
    """One accepted canary observation of one runner (archives included)."""

    utc_date: str
    score: float
    environment: dict[str, Any]
    measured_at: Optional[datetime] = None  # completion time; None: utc_date 06:00 UTC is assumed

    @property
    def when(self) -> datetime:
        if self.measured_at is not None:
            return self.measured_at
        return datetime.fromisoformat(self.utc_date).replace(hour=6, tzinfo=timezone.utc)


@dataclass(frozen=True)
class ManualEvent:
    """An operator-recorded change (``environment_events.json`` on the runner)."""

    at: datetime
    label: str = ""
    fields: tuple[str, ...] = ()
    until: Optional[datetime] = None


@dataclass(frozen=True)
class Explanation:
    """An operator explanation for the event dated ``event_date`` on one runner."""

    event_date: str
    text: str


@dataclass(frozen=True)
class PairedSeries:
    """The same (commit, workload) measured before and after an environment change.

    ``cut`` is the time after which a measurement belongs to the new
    environment; it assigns the evidence to the runner event whose
    observation gap contains it.
    """

    series: str
    cut: datetime
    before: tuple[float, ...]
    after: tuple[float, ...]
    lower_is_better: bool = False


@dataclass
class RunnerInput:
    runner_id: str
    display_name: str
    observations: list[Observation]
    manual_events: list[ManualEvent] = field(default_factory=list)
    explanations: list[Explanation] = field(default_factory=list)
    paired: list[PairedSeries] = field(default_factory=list)


@dataclass(frozen=True)
class FindingsConfig:
    floor_min: float = 0.005
    floor_sd: float = 3.0
    window: int = 28
    min_before: int = 7
    min_after: int = 3
    min_pairs: int = 5
    alpha: float = 0.05
    locate_p: float = 0.01
    alternative_gain: float = 0.9
    fleet_min_runners: int = 3
    fleet_window_hours: int = 48
    fleet_z: float = 3.0
    bootstrap: int = 10000
    permutations: int = 20000
    seed: int = 20261109


# --------------------------------------------------------------------------- statistics


def robust_sd(values: Sequence[float]) -> float:
    """``1.4826 x MAD``: a consistent estimate of sigma for normal data."""
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return float("nan")
    return float(MAD_SIGMA_SCALE * np.median(np.abs(arr - np.median(arr))))


def _rng(cfg: FindingsConfig, key: str) -> np.random.Generator:
    """A generator seeded from the test's identity: the published numbers never change on re-analysis."""
    return np.random.default_rng((cfg.seed, zlib.crc32(key.encode("utf-8"))))


def l1_cost(values: Sequence[float]) -> float:
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return 0.0
    return float(np.abs(arr - np.median(arr)).sum())


def median_ratio_ci(
    before: Sequence[float], after: Sequence[float], rng: np.random.Generator, resamples: int
) -> tuple[float, float]:
    """95% percentile bootstrap CI of median(after) / median(before) - 1 (both windows resampled)."""
    b = np.asarray(before, dtype=float)
    a = np.asarray(after, dtype=float)
    mb = np.median(b[rng.integers(0, b.size, (resamples, b.size))], axis=1)
    ma = np.median(a[rng.integers(0, a.size, (resamples, a.size))], axis=1)
    ratios = ma / mb - 1.0
    lo, hi = np.quantile(ratios, [0.025, 0.975])
    return float(lo), float(hi)


def mann_whitney_p(
    before: Sequence[float], after: Sequence[float], rng: np.random.Generator, permutations: int
) -> float:
    """Two-sided Mann-Whitney p by permutation of the rank sum (mid ranks for ties).

    ``(hits + 1) / (n + 1)``: never zero, and stable for a fixed generator.
    """
    from scipy.stats import rankdata

    pool = np.concatenate([np.asarray(before, dtype=float), np.asarray(after, dtype=float)])
    ranks = rankdata(pool)
    k = len(after)
    expected = k * (pool.size + 1) / 2.0
    observed = abs(ranks[len(before) :].sum() - expected)
    order = rng.random((permutations, pool.size)).argsort(axis=1)[:, :k]
    sums = ranks[order].sum(axis=1)
    hits = int((np.abs(sums - expected) >= observed - 1e-9).sum())
    return (hits + 1) / (permutations + 1)


def paired_test(
    before: Sequence[float], after: Sequence[float], rng: np.random.Generator, cfg: FindingsConfig
) -> dict[str, float]:
    """Effect, CI, signed-rank p and same-direction share of per-pair log ratios."""
    r = np.log(np.asarray(after, dtype=float) / np.asarray(before, dtype=float))
    effect = float(math.expm1(float(np.median(r))))
    boots = np.median(r[rng.integers(0, r.size, (cfg.bootstrap, r.size))], axis=1)
    lo, hi = np.quantile(np.expm1(boots), [0.025, 0.975])
    # Wilcoxon signed-rank by sign flips: robust, and exact-like even when every ratio is equal.
    from scipy.stats import rankdata

    signed_ranks = np.sign(r) * rankdata(np.abs(r))
    observed = abs(float(signed_ranks.sum()))
    signs = rng.choice(np.array([-1.0, 1.0]), size=(cfg.permutations, r.size))
    hits = int((np.abs((signs * np.abs(signed_ranks)).sum(axis=1)) >= observed - 1e-9).sum())
    positive = int((r > 0).sum())
    negative = int((r < 0).sum())
    same = max(positive, negative) / r.size
    return {
        "effect": effect,
        "ci_lo": float(lo),
        "ci_hi": float(hi),
        "p": (hits + 1) / (cfg.permutations + 1),
        "same_direction": float(same),
    }


def benjamini_hochberg(p_values: Sequence[float]) -> list[float]:
    """BH q-values, in the input order."""
    n = len(p_values)
    order = sorted(range(n), key=lambda i: p_values[i])
    q = [1.0] * n
    running = 1.0
    for rank in range(n, 0, -1):
        i = order[rank - 1]
        running = min(running, p_values[i] * n / rank)
        q[i] = min(1.0, running)
    return q


# --------------------------------------------------------------------------- events


def _flatten(environment: dict[str, Any]) -> dict[str, Any]:
    """One level of nesting (``packages: {glibc: ...}`` -> ``packages.glibc``)."""
    flat: dict[str, Any] = {}
    for key, value in (environment or {}).items():
        if isinstance(value, dict):
            for sub, sub_value in value.items():
                flat[f"{key}.{sub}"] = sub_value
        else:
            flat[key] = value
    return flat


def field_changes(prev: dict[str, Any], cur: dict[str, Any]) -> list[tuple[str, Any, Any]]:
    """``(field, old, new)`` for each environment field that changed, sorted by field.

    A field the OLD record did not have while the new record's provenance
    schema is newer is a new field, not a change (same rule as the canary
    annotation).
    """
    prev_schema = prev.get("provenance_schema_version") or 0
    cur_schema = cur.get("provenance_schema_version") or 0
    a, b = _flatten(prev), _flatten(cur)
    out = []
    for key in sorted(set(a) | set(b)):
        base = key[len("server_") :] if key.startswith("server_") else key
        if key in _IGNORED_FIELDS or base in _IGNORED_FIELDS:
            continue
        if a.get(key) == b.get(key):
            continue
        if key not in a and cur_schema > prev_schema:
            continue
        out.append((key, a.get(key), b.get(key)))
    return out


def event_kind(fields: Iterable[str], manual: bool) -> str:
    names = set()
    for name in fields:
        base = name[len("server_") :] if name.startswith("server_") else name
        names.add(base)
    for kind, members in _KIND_RULES:
        if names & members:
            return kind
    packages = {n.split(".", 1)[1] for n in names if n.startswith("packages.")}
    if packages:
        kinds = [_PACKAGE_KIND.get(p, KIND_LIBRARY) for p in packages]
        return KIND_KERNEL if KIND_KERNEL in kinds else KIND_LIBRARY
    if names and names <= {"boot_id"}:
        return KIND_REBOOT
    if names and names <= {"conductress_revision"}:
        return KIND_HARNESS
    if not names or (manual and names <= {"manual"}):
        return KIND_MANUAL
    return KIND_OTHER


@dataclass
class Event:
    runner_id: str
    position: int  # index of the first observation after the change; len(obs) when none yet
    date: str
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    changes: list[tuple[str, Any, Any]]
    manual: list[ManualEvent]

    @property
    def event_id(self) -> str:
        return f"{self.runner_id}:{self.date}:0"

    @property
    def field_names(self) -> list[str]:
        return [name for name, _, _ in self.changes]

    @property
    def kind(self) -> str:
        manual = [_MANUAL_ALIASES.get(f.strip().lower(), "manual") for m in self.manual for f in m.fields]
        if self.manual and not manual:
            manual = ["manual"]
        return event_kind(self.field_names + manual, manual=bool(self.manual))

    @property
    def harness_only(self) -> bool:
        return self.kind == KIND_HARNESS and not self.manual


def build_events(runner: RunnerInput) -> list[Event]:
    """One event per observation gap with a recorded or manual change, in time order."""
    obs = runner.observations
    events: dict[int, Event] = {}
    for k in range(1, len(obs)):
        changes = field_changes(obs[k - 1].environment, obs[k].environment)
        if changes:
            events[k] = Event(runner.runner_id, k, obs[k].utc_date, obs[k - 1].when, obs[k].when, changes, [])
    for manual in sorted(runner.manual_events, key=lambda m: m.at):
        k = next((i for i, o in enumerate(obs) if o.when > manual.at), len(obs))
        if k == 0:
            continue  # before the first observation: nothing to compare with
        if k not in events:
            if k < len(obs):
                day = obs[k].utc_date
            else:
                day = (manual.at.astimezone(timezone.utc).date() + timedelta(days=1)).isoformat()
            events[k] = Event(runner.runner_id, k, day, obs[k - 1].when, obs[k].when if k < len(obs) else None, [], [])
        events[k].manual.append(manual)
    return [events[k] for k in sorted(events)]


# --------------------------------------------------------------------------- segmentation


def _segment_bounds(n: int, pos: int, cuts: Sequence[int]) -> tuple[int, int]:
    lo = max([c for c in cuts if c < pos] + [0])
    hi = min([c for c in cuts if c > pos] + [n])
    return lo, hi


def windows(x: Sequence[float], pos: int, cuts: Sequence[int], cfg: FindingsConfig) -> tuple[list[float], list[float]]:
    lo, hi = _segment_bounds(len(x), pos, cuts)
    return list(x[max(lo, pos - cfg.window) : pos]), list(x[pos : min(hi, pos + cfg.window)])


def split_gain(x: Sequence[float], pos: int, cuts: Sequence[int]) -> float:
    lo, hi = _segment_bounds(len(x), pos, cuts)
    return l1_cost(x[lo:hi]) - l1_cost(x[lo:pos]) - l1_cost(x[pos:hi])


def runner_floor(x: Sequence[float], pos: int, cuts: Sequence[int], cfg: FindingsConfig) -> tuple[float, float]:
    """``(floor, robust sd)`` as fractions, from the before window (in-segment, else the prior days)."""
    before, _ = windows(x, pos, cuts, cfg)
    if len(before) < cfg.min_before:
        before = list(x[max(0, pos - cfg.window) : pos])
    if len(before) < cfg.min_before:
        return cfg.floor_min, float("nan")
    sd = robust_sd(before) / float(np.median(before))
    return max(cfg.floor_min, cfg.floor_sd * sd), sd


@dataclass
class Step:
    position: int
    effect: float
    p: float
    alternatives: list[int]


def locate_steps(runner_id: str, x: Sequence[float], events: Sequence[Event], cfg: FindingsConfig) -> list[Step]:
    """Greedy robust segmentation restricted to event positions (see the module rules)."""
    cuts: list[int] = []
    steps: list[Step] = []
    positions = sorted({e.position for e in events if 0 < e.position < len(x)})
    while True:
        candidates = [p for p in positions if p not in cuts]
        if not candidates:
            break
        best = max(candidates, key=lambda p: split_gain(x, p, cuts))
        before, after = windows(x, best, cuts, cfg)
        if len(before) < cfg.min_before or len(after) < cfg.min_after:
            break
        effect = float(np.median(after) / np.median(before) - 1.0)
        p = mann_whitney_p(before, after, _rng(cfg, f"locate|{runner_id}|{best}|{len(cuts)}"), cfg.permutations)
        floor, _ = runner_floor(x, best, cuts, cfg)
        if p >= cfg.locate_p or abs(effect) < floor:
            break
        gain = split_gain(x, best, cuts)
        alternatives = [c for c in candidates if c != best and split_gain(x, c, cuts) >= cfg.alternative_gain * gain]
        cuts.append(best)
        steps.append(Step(best, effect, p, alternatives))
    return steps


# --------------------------------------------------------------------------- tests and states


def _state(test: dict[str, Any], cfg: FindingsConfig) -> str:
    lo, hi = test["ci_lo"], test["ci_hi"]
    if not test["enough"]:
        return STATE_INSUFFICIENT
    floor = test["floor"]
    if test["q"] < cfg.alpha and (lo > 0 or hi < 0) and abs(test["effect"]) >= floor:
        return STATE_CONFIRMED
    if -floor <= lo and hi <= floor:
        return STATE_CLEARED
    if test["p"] < cfg.alpha or abs(test["effect"]) >= 2 * floor:
        return STATE_SUSPECTED
    return STATE_INSUFFICIENT  # no signal, but the interval is still too wide to clear


def _severity(test: dict[str, Any]) -> float:
    lo, hi = test["ci_lo"], test["ci_hi"]
    if lo is None or hi is None or math.isnan(lo) or math.isnan(hi) or lo <= 0 <= hi:
        return 0.0
    return 100.0 * min(abs(lo), abs(hi)) * (1.0 - test["q"])


def _pct(value: Optional[float], digits: int = 3) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return round(100.0 * value, digits)


def _num(value: Optional[float], digits: int = 6) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return round(float(value), digits)


@dataclass
class _RunnerAnalysis:
    runner: RunnerInput
    scores: list[float]
    events: list[Event]
    steps: list[Step]
    cuts: list[int]
    tests: list[dict[str, Any]] = field(default_factory=list)


def _canary_test(analysis: _RunnerAnalysis, event: Event, cfg: FindingsConfig) -> dict[str, Any]:
    x = analysis.scores
    pos = event.position
    before, after = windows(x, pos, analysis.cuts, cfg)
    later = [e.position for e in analysis.events if e.position > pos]
    strict_after = list(x[pos : min(later + [len(x)])])
    floor, sd = runner_floor(x, pos, analysis.cuts, cfg)
    test: dict[str, Any] = {
        "event_id": event.event_id,
        "series": "canary",
        "kind": "canary",
        "n_before": len(before),
        "n_after": len(after),
        "n_after_strict": len(strict_after),
        "floor": floor,
        "sd": sd,
        "effect": float("nan"),
        "effect_strict": float("nan"),
        "ci_lo": float("nan"),
        "ci_hi": float("nan"),
        "p": 1.0,
        "enough": len(before) >= cfg.min_before and len(after) >= cfg.min_after,
        "cut": pos in analysis.cuts,
    }
    if before and after:
        mb = float(np.median(before))
        test["effect"] = float(np.median(after)) / mb - 1.0
        test["effect_strict"] = float(np.median(strict_after)) / mb - 1.0 if strict_after else float("nan")
    if len(before) >= 2 and after:
        rng = _rng(cfg, f"canary|{event.event_id}")
        test["ci_lo"], test["ci_hi"] = median_ratio_ci(before, after, rng, cfg.bootstrap)
        test["p"] = mann_whitney_p(before, after, rng, cfg.permutations)
    return test


def _paired_tests(analysis: _RunnerAnalysis, cfg: FindingsConfig) -> list[dict[str, Any]]:
    obs = analysis.runner.observations
    out = []
    for paired in analysis.runner.paired:
        # The event whose observation gap holds the cut owns the evidence.
        owner = None
        for event in analysis.events:
            prev_when = obs[event.position - 1].when
            next_when = obs[event.position].when if event.position < len(obs) else None
            if prev_when <= paired.cut and (next_when is None or paired.cut < next_when):
                owner = event
                break
        if owner is None or not paired.before:
            continue
        floor, sd = runner_floor(analysis.scores, owner.position, analysis.cuts, cfg)
        n = min(len(paired.before), len(paired.after))
        test: dict[str, Any] = {
            "event_id": owner.event_id,
            "series": paired.series,
            "kind": "paired",
            "n_before": n,
            "n_after": n,
            "floor": floor,
            "sd": sd,
            "enough": n >= cfg.min_pairs,
            "lower_is_better": paired.lower_is_better,
            "effect": float("nan"),
            "ci_lo": float("nan"),
            "ci_hi": float("nan"),
            "p": 1.0,
            "same_direction": None,
        }
        before, after = list(paired.before[:n]), list(paired.after[:n])
        if n >= 2:
            result = paired_test(before, after, _rng(cfg, f"paired|{owner.event_id}|{paired.series}"), cfg)
            test.update(
                effect=result["effect"],
                ci_lo=result["ci_lo"],
                ci_hi=result["ci_hi"],
                p=result["p"],
                same_direction=result["same_direction"],
            )
        elif n == 1:
            test["effect"] = after[0] / before[0] - 1.0
            test["same_direction"] = 1.0
        out.append(test)
    return out


def _cross_runner(
    analyses: Sequence[_RunnerAnalysis], analysis: _RunnerAnalysis, event: Event, cfg: FindingsConfig
) -> list[dict[str, Any]]:
    """Effect on every other runner over the dates of this event's after window, in robust sd."""
    obs = analysis.runner.observations
    _, after = windows(analysis.scores, event.position, analysis.cuts, cfg)
    if not after:
        return []
    first = obs[event.position].utc_date
    last = obs[event.position + len(after) - 1].utc_date
    out = []
    for other in analyses:
        if other is analysis:
            continue
        dates = [o.utc_date for o in other.runner.observations]
        idx = [i for i, d in enumerate(dates) if first <= d <= last]
        if not idx:
            continue
        start = idx[0]
        before = other.scores[max(0, start - cfg.window) : start]
        if len(before) < cfg.min_before:
            continue
        values = [other.scores[i] for i in idx]
        mb = float(np.median(before))
        effect = float(np.median(values)) / mb - 1.0
        sd = robust_sd(before) / mb
        out.append(
            {
                "runner_id": other.runner.runner_id,
                "effect_pct": _pct(effect),
                "z": _num(effect / sd, 2) if sd > 0 else None,
                "n_days": len(values),
            }
        )
    return out


def _public_changes(event: Event) -> list[dict[str, Any]]:
    """The changed fields in publishable form, through the shared public allowlist."""
    out: list[dict[str, Any]] = []
    seen_words: set[str] = set()
    for name, old, new in event.changes:
        if public_value_key(name) or name == "profile_version":
            base = name[len("server_") :] if name.startswith("server_") else name
            prefix = "server " if name.startswith("server_") else ""
            if base.startswith("packages."):
                label = f"{base.split('.', 1)[1]} package"
            else:
                label = _LABELS.get(base, base)
            out.append(
                {
                    "field": name,
                    "label": prefix + label,
                    "old": public_text(None if old is None else str(old), max_chars=64),
                    "new": public_text(None if new is None else str(new), max_chars=64),
                }
            )
            continue
        word = change_type_label(name)
        if name not in _KNOWN_PRIVATE_FIELDS:
            word = "other recorded field changed"  # a field nobody has classified: no name, no value
        if word not in seen_words:
            seen_words.add(word)
            out.append({"field": None, "label": word, "old": None, "new": None})
    for manual in event.manual:
        for name in manual.fields:
            clean = public_text(name, max_chars=40)
            if clean and clean not in seen_words:
                seen_words.add(clean)
                out.append({"field": None, "label": clean, "old": None, "new": None})
    return out


def _plot(analysis: _RunnerAnalysis, event: Event, cfg: FindingsConfig) -> dict[str, Any]:
    obs = analysis.runner.observations
    lo, hi = _segment_bounds(len(obs), event.position, analysis.cuts)
    start = max(lo, event.position - cfg.window)
    end = min(hi, event.position + cfg.window)
    return {
        "dates": [o.utc_date for o in obs[start:end]],
        "scores": [round(o.score, 1) for o in obs[start:end]],
        "event_index": event.position - start,
        "cut_dates": [obs[c].utc_date for c in analysis.cuts if start <= c < end],
    }


def _public_test(test: dict[str, Any]) -> dict[str, Any]:
    out = {
        "series": test["series"],
        "kind": test["kind"],
        "n_before": test["n_before"],
        "n_after": test["n_after"],
        "effect_pct": _pct(test["effect"]),
        "ci_pct": [_pct(test["ci_lo"]), _pct(test["ci_hi"])],
        "p": _num(test["p"], 6),
        "q": _num(test["q"], 6),
        "floor_pct": _pct(test["floor"]),
        "state": test["state"],
        "below_floor_shift": test.get("below_floor_shift", False),
        "severity": _num(test["severity"], 3),
    }
    if test["kind"] == "canary":
        out["n_after_strict"] = test["n_after_strict"]
        out["effect_strict_pct"] = _pct(test["effect_strict"])
        out["sd_pct"] = _pct(test["sd"])
        out["located_step"] = test["cut"]
    if test["kind"] == "paired":
        out["same_direction"] = _num(test["same_direction"], 3)
        out["lower_is_better"] = test["lower_is_better"]
    return out


def _event_state(tests: Sequence[dict[str, Any]]) -> str:
    primary = [t for t in tests if t["kind"] == "canary" or (t["kind"] == "paired" and t["enough"])]
    states = [t["state"] for t in primary]
    if STATE_CONFIRMED in states:
        return STATE_CONFIRMED
    if STATE_SUSPECTED in states:
        return STATE_SUSPECTED
    canary = next((t for t in tests if t["kind"] == "canary"), None)
    if canary is not None and canary["state"] == STATE_CLEARED:
        return STATE_CLEARED
    return STATE_INSUFFICIENT


# --------------------------------------------------------------------------- fleet


def _fleet_events(analyses: Sequence[_RunnerAnalysis], cfg: FindingsConfig) -> list[dict[str, Any]]:
    """Harness deploys seen on several runners within the fleet window: one pooled row each.

    Z = sum(z_i) / sqrt(k) over runners, z_i = strict effect / robust sd of
    the runner's before window. A move with ``|Z| >=`` the threshold, most
    runners in one direction and fewer than ``min_after`` days on some runner
    is ``suspected-fleet``: a one-day fleet-wide move, low rank, never open.
    """
    harness = []
    for analysis in analyses:
        for event in analysis.events:
            if event.harness_only and event.position < len(analysis.scores):
                harness.append((event.first_seen or event.last_seen, analysis, event))
    harness.sort(key=lambda item: (item[0] or datetime.min.replace(tzinfo=timezone.utc)))
    groups: list[list[tuple[Any, _RunnerAnalysis, Event]]] = []
    for item in harness:
        if groups and item[0] is not None and groups[-1][0][0] is not None:
            if item[0] - groups[-1][0][0] <= timedelta(hours=cfg.fleet_window_hours):
                if all(other[1] is not item[1] for other in groups[-1]):
                    groups[-1].append(item)
                    continue
        groups.append([item])
    rows = []
    for group in groups:
        if len(group) < cfg.fleet_min_runners:
            continue
        members = []
        zs = []
        for _, analysis, event in group:
            test = next(t for t in analysis.tests if t["event_id"] == event.event_id and t["kind"] == "canary")
            x = analysis.scores
            pos = event.position
            before, _ = windows(x, pos, analysis.cuts, cfg)
            if len(before) < cfg.min_before:
                before = list(x[max(0, pos - cfg.window) : pos])
            entry = {
                "runner_id": analysis.runner.runner_id,
                "event_id": event.event_id,
                "effect_pct": _pct(test["effect_strict"]),
                "n_after": test["n_after_strict"],
                "z": None,
            }
            if len(before) >= cfg.min_before and not math.isnan(test["effect_strict"]):
                sd = robust_sd(before) / float(np.median(before))
                if sd > 0:
                    z = test["effect_strict"] / sd
                    zs.append(z)
                    entry["z"] = round(z, 2)
            members.append(entry)
        date_text = min(e.date for _, _, e in group)
        row: dict[str, Any] = {
            "event_id": f"fleet:{date_text}:0",
            "date": date_text,
            "kind": KIND_HARNESS,
            "runners": members,
            "z": None,
            "same_direction": None,
            "state": STATE_INSUFFICIENT,
        }
        if len(zs) >= cfg.fleet_min_runners:
            z_pooled = sum(zs) / math.sqrt(len(zs))
            same = max(sum(z > 0 for z in zs), sum(z < 0 for z in zs))
            row["z"] = round(z_pooled, 2)
            row["same_direction"] = f"{same}/{len(zs)}"
            short = any(m["n_after"] < cfg.min_after for m in members)
            if abs(z_pooled) >= cfg.fleet_z and same >= max(cfg.fleet_min_runners, math.ceil(0.75 * len(zs))):
                row["state"] = STATE_SUSPECTED_FLEET if short else STATE_SUSPECTED
            elif not short:
                row["state"] = STATE_CLEARED
        rows.append(row)
    return rows


# --------------------------------------------------------------------------- consistency


def _consistency(analysis: _RunnerAnalysis, cfg: FindingsConfig, today: date) -> dict[str, Any]:
    x = analysis.scores
    obs = analysis.runner.observations
    streak, reason = 0, "out-of-band day"
    for k in range(len(x) - 1, 0, -1):
        lo = max([c for c in analysis.cuts if c <= k] + [0])
        if lo == k:
            reason = "located step"
            break
        ref = x[max(lo, k - cfg.window) : k]
        if len(ref) < cfg.min_before:
            reason = "start of history" if lo == 0 else "not enough data since the last step"
            break
        if abs(x[k] - float(np.median(ref))) <= 3 * robust_sd(ref):
            streak += 1
        else:
            break
    else:
        reason = "start of history"
    last = x[-cfg.window :]
    first = x[: cfg.window]
    seen = {o.utc_date for o in obs}

    def missing(days: int) -> list[str]:
        start = max(today - timedelta(days=days - 1), date.fromisoformat(obs[0].utc_date)) if obs else today
        out = []
        day = start
        while day <= today:
            if day.isoformat() not in seen:
                out.append(day.isoformat())
            day += timedelta(days=1)
        return out

    horizon = (today - timedelta(days=90)).isoformat()
    recent = [t for t in analysis.tests if t["kind"] == "canary" and t["event_id"].split(":")[1] >= horizon]
    return {
        "runner_id": analysis.runner.runner_id,
        "display_name": analysis.runner.display_name,
        "accepted_days": len(x),
        "first_date": obs[0].utc_date if obs else None,
        "last_date": obs[-1].utc_date if obs else None,
        "streak_days": streak if len(x) > cfg.min_before else None,
        "streak_end_reason": reason if len(x) > cfg.min_before else "not enough data",
        "sd_recent_pct": _pct(robust_sd(last) / float(np.median(last))) if len(last) >= cfg.min_before else None,
        "sd_first_pct": _pct(robust_sd(first) / float(np.median(first))) if len(first) >= cfg.min_before else None,
        "events_tested_90d": len(recent),
        "events_cleared_90d": sum(1 for t in recent if t["state"] == STATE_CLEARED),
        "events_insufficient_90d": sum(1 for t in recent if t["state"] == STATE_INSUFFICIENT),
        "missing_days_30d": missing(30),
        "missing_days_90d": len(missing(90)),
        "last_confirmation_run": None,
    }


# --------------------------------------------------------------------------- document


def _proposal(event: Event, display: str) -> Optional[str]:
    """Text for the page: the generation close a person may perform (Rain: never automatic)."""
    start = min((m.at for m in event.manual), default=event.first_seen)
    end = max((m.until for m in event.manual if m.until is not None), default=event.last_seen)
    if start is None:
        return None

    def stamp(moment: datetime) -> str:
        return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    resume = f" --resume {stamp(end)}" if end is not None else ""
    return (
        f"A person may close the sweep generation of {display} at this event. Check the change times in the host "
        f"log first, then on the runner: conductress sweep close-generation --epoch v3 --cut {stamp(start)}{resume} "
        "--label <text> --reason <text>. Nothing does this automatically."
    )


def build_findings(
    runners: Sequence[RunnerInput], *, now: datetime, cfg: Optional[FindingsConfig] = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The public findings and consistency documents for the given runners."""
    cfg = cfg or FindingsConfig()
    analyses: list[_RunnerAnalysis] = []
    for runner in runners:
        obs = sorted(runner.observations, key=lambda o: (o.utc_date, o.when))
        runner = RunnerInput(
            runner.runner_id, runner.display_name, obs, runner.manual_events, runner.explanations, runner.paired
        )
        scores = [float(o.score) for o in obs]
        events = build_events(runner)
        steps = locate_steps(runner.runner_id, scores, events, cfg)
        analysis = _RunnerAnalysis(runner, scores, events, steps, sorted(s.position for s in steps))
        analysis.tests = [_canary_test(analysis, e, cfg) for e in events if 0 < e.position] + _paired_tests(
            analysis, cfg
        )
        analyses.append(analysis)

    all_tests = [t for a in analyses for t in a.tests]
    for test, q in zip(all_tests, benjamini_hochberg([t["p"] for t in all_tests])):
        test["q"] = q
        test["state"] = _state(test, cfg)
        test["below_floor_shift"] = test["state"] == STATE_CLEARED and q < cfg.alpha
        test["severity"] = _severity(test)

    fleet_rows = _fleet_events(analyses, cfg)
    events_out = []
    for analysis in analyses:
        steps_by_pos = {s.position: s for s in analysis.steps}
        explanations = {e.event_date: e for e in analysis.runner.explanations}
        for event in analysis.events:
            tests = [t for t in analysis.tests if t["event_id"] == event.event_id]
            if not tests:
                continue
            state = _event_state(tests)
            primary = [t for t in tests if t["kind"] == "canary" or (t["kind"] == "paired" and t["enough"])]
            canary = next(t for t in tests if t["kind"] == "canary")
            # The fixed-commit canary leads when it has enough data; else the strongest paired series.
            paired_ok = [t for t in primary if t["kind"] == "paired"]
            lead = canary if canary["enough"] or not paired_ok else max(paired_ok, key=lambda t: t["severity"])
            explanation = explanations.get(event.date)
            shown_state = STATE_EXPLAINED if explanation is not None else state
            step = steps_by_pos.get(event.position)
            fields = _public_changes(event)
            n_fields = len({f["label"] for f in fields})
            proposal = _proposal(event, analysis.runner.display_name) if state == STATE_CONFIRMED else None
            events_out.append(
                {
                    "event_id": event.event_id,
                    "runner_id": analysis.runner.runner_id,
                    "display_name": analysis.runner.display_name,
                    "date": event.date,
                    "first_seen": event.first_seen.isoformat() if event.first_seen else None,
                    "last_seen": event.last_seen.isoformat() if event.last_seen else None,
                    "kind": event.kind,
                    "fields": fields,
                    "attribution_note": (
                        f"{n_fields} fields changed together. The effect is attributed to the event as a whole; "
                        "this data cannot tell which field caused it."
                        if n_fields > 1
                        else None
                    ),
                    "manual_labels": [t for t in (public_text(m.label) for m in event.manual) if t],
                    "state": shown_state,
                    "state_before_explanation": state if explanation is not None else None,
                    "below_floor_shift": bool(canary.get("below_floor_shift")),
                    "severity": round(lead["severity"], 3),
                    "confidence": round(1.0 - lead["q"], 4),
                    "effect_pct": _pct(lead["effect"]),
                    "ci_pct": [_pct(lead["ci_lo"]), _pct(lead["ci_hi"])],
                    "q": _num(lead["q"]),
                    "floor_pct": _pct(canary["floor"]),
                    "located_step": step is not None,
                    "alternative_dates": (
                        [analysis.runner.observations[p].utc_date for p in step.alternatives] if step else []
                    ),
                    "explanation": (
                        {"text": public_text(explanation.text), "event_date": explanation.event_date}
                        if explanation is not None
                        else None
                    ),
                    "proposal": proposal,
                    "tests": [_public_test(t) for t in tests],
                    "cross_runner": _cross_runner(analyses, analysis, event, cfg),
                    "plot": _plot(analysis, event, cfg),
                }
            )
    latest_fleet = max(fleet_rows, key=lambda r: r["date"], default=None)
    for row in fleet_rows:
        # A pooled row that says nothing (too little data, no signal) is listed only for the latest deploy.
        if row["state"] == STATE_INSUFFICIENT and row is not latest_fleet:
            continue
        events_out.append(
            {
                "event_id": row["event_id"],
                "runner_id": None,
                "display_name": "Fleet",
                "date": row["date"],
                "kind": row["kind"],
                "state": row["state"],
                "severity": 0.0,
                "fleet": {"z": row["z"], "same_direction": row["same_direction"], "runners": row["runners"]},
            }
        )
    events_out.sort(key=lambda e: (STATE_ORDER[e["state"]], -e["severity"], _neg_date(e["date"]), e["event_id"]))
    open_count = sum(1 for e in events_out if e["state"] in OPEN_STATES)
    generated = now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    findings = {
        "schema_version": FINDINGS_SCHEMA_VERSION,
        "analysis_version": ANALYSIS_VERSION,
        "generated_at": generated,
        "open_count": open_count,
        "state_counts": _counts(e["state"] for e in events_out),
        "rules": {
            "floor": f"max({100 * cfg.floor_min:g}%, {cfg.floor_sd:g} x canary robust sd) per runner",
            "min_before_days": cfg.min_before,
            "min_after_days": cfg.min_after,
            "min_pairs": cfg.min_pairs,
            "alpha": cfg.alpha,
        },
        "events": events_out,
    }
    today = now.astimezone(timezone.utc).date()
    consistency = {
        "schema_version": FINDINGS_SCHEMA_VERSION,
        "generated_at": generated,
        "runners": [_consistency(a, cfg, today) for a in analyses],
        "fleet": (
            {
                "latest_deploy_date": latest_fleet["date"],
                "z": latest_fleet["z"],
                "same_direction": latest_fleet["same_direction"],
                "state": latest_fleet["state"],
            }
            if latest_fleet
            else None
        ),
    }
    return findings, consistency


def _counts(states: Iterable[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for state in states:
        out[state] = out.get(state, 0) + 1
    return dict(sorted(out.items()))


def _neg_date(day: str) -> int:
    """Sort key: newest date first."""
    return -date.fromisoformat(day).toordinal()
