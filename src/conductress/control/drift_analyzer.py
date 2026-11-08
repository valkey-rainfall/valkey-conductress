"""Canary drift analysis: ingestion, rolling median/MAD, phase tracking.

Ingests completed canary outcomes idempotently, persists immutable daily
observations, computes deterministic rolling statistics from prior
accepted observations, and tracks observation/calibration/ready phases.

Design invariants:
- A new sample is NEVER included in its own reference window.
- MAD=0 is handled explicitly (all prior samples identical).
- Duplicate/replayed outcomes produce the same observation (idempotent).
- Profile version changes start a fresh series.
- Missed days create gaps -- no interpolation or backfill.
- Out-of-order completions are accepted and slotted by UTC date.
- At most one accepted observation per (runner, profile, version, UTC date);
  a different task for an already-accepted date returns the existing one.
- No notifications, no runner traffic, no threshold mutation.

Statistical formulas:
- Median: average of two middle values for even-length lists.
- Robust sigma estimate: 1.4826 * MAD (consistent estimator of σ for normal).
- Variability floor: 3 * robust_sigma / |median| * 100  (≈99.7% of normal).
- Recommended warning: max(candidate_warning, variability_floor).
- Recommended alarm: max(candidate_alarm, 2 * variability_floor, recommended_warning).
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from .db import ControlDatabase, utc_text

logger = logging.getLogger(__name__)

# Phase constants
PHASE_OBSERVATION = "observation"
PHASE_CALIBRATING = "calibrating"
PHASE_READY = "ready"

# Window sizes
OBSERVATION_WINDOW = 14
CALIBRATION_WINDOW = 28
ROLLING_WINDOW = 28

# Minimum samples for any statistical computation
MIN_SAMPLES_FOR_STATS = 1

# MAD-to-sigma consistency constant for normal distributions
MAD_SIGMA_SCALE = 1.4826

# Keys whose change means only "the host rebooted" (a possible performance
# change, marked more lightly than a package or kernel change).
BOOT_KEYS = ("boot_id", "server_boot_id")
# Keys that describe the record, not the environment; never a change.
_META_KEYS = ("provenance_schema_version", "same_host")


def environment_changes(prev_env: Dict[str, Any], environment: Dict[str, Any]) -> List[str]:
    """``key: old -> new`` for every environment field that differs, sorted by key.

    Shared by the canary annotation and the sweep environment periods, so both
    describe a change in the same words. A key the OLD record did not carry
    because its provenance schema predates the field (the old side is absent,
    the old record's ``provenance_schema_version`` is lower) is not a change:
    a deploy that adds fields must not read as an environment event. A key
    present on both sides with a real ``None`` (microcode on ARM) that gains a
    value is a change.
    """
    prev_schema = prev_env.get("provenance_schema_version") or 0
    cur_schema = environment.get("provenance_schema_version") or 0
    changes: List[str] = []
    for key in sorted(set(prev_env) | set(environment)):
        if key in _META_KEYS:
            continue
        old_v = prev_env.get(key)
        new_v = environment.get(key)
        if old_v == new_v:
            continue
        if key not in prev_env and cur_schema > prev_schema:
            continue  # new field, not a changed environment
        changes.append(f"{key}: {old_v!r} -> {new_v!r}")
    return changes


def boot_only(changes: List[str]) -> bool:
    """Whether every change in the list is a boot id (a reboot and nothing else)."""
    return bool(changes) and all(c.split(":", 1)[0] in BOOT_KEYS for c in changes)


def change_label(changes: List[str], max_value_chars: int = 24) -> str:
    """A short label for a boundary marker: ``reboot`` or ``kernel_release: a -> b; ...``."""
    if not changes:
        return ""
    if boot_only(changes):
        return "server reboot" if all(c.startswith("server_") for c in changes) else "reboot"
    parts = []
    for change in changes:
        key, _, rest = change.partition(": ")
        if key in BOOT_KEYS:
            parts.append("server reboot" if key.startswith("server_") else "reboot")
            continue
        old_v, _, new_v = rest.partition(" -> ")
        parts.append(f"{key}: {old_v.strip(chr(39))[:max_value_chars]} -> {new_v.strip(chr(39))[:max_value_chars]}")
    return "; ".join(parts)


def _median(values: List[float]) -> float:
    """Deterministic median: average of two middle values for even counts."""
    s = sorted(values)
    n = len(s)
    if n == 0:
        raise ValueError("median of empty list")
    mid = (n - 1) // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid] + s[mid + 1]) / 2.0


def _mad(values: List[float]) -> float:
    """Median absolute deviation from the median."""
    if len(values) < 1:
        raise ValueError("MAD of empty list")
    med = _median(values)
    deviations = sorted(abs(v - med) for v in values)
    return _median(deviations) if deviations else 0.0


def _robust_sigma(mad_value: float) -> float:
    """Convert MAD to a robust estimate of standard deviation (σ).

    For normally distributed data, σ ≈ 1.4826 * MAD.
    """
    return MAD_SIGMA_SCALE * mad_value


def _variability_floor_pct(mad_value: float, median_value: float) -> float:
    """3-sigma variability floor as a percentage of |median|.

    variability_floor = 3 * 1.4826 * MAD / |median| * 100

    Returns 0.0 when median is zero (handled as review-required upstream).
    """
    if median_value == 0.0:
        return 0.0
    return 3.0 * _robust_sigma(mad_value) / abs(median_value) * 100.0


def _is_finite(value: Any) -> bool:
    """Check if a value is a finite number."""
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


def _candidate_signal(
    delta_pct: Optional[float],
    candidate_warning_pct: Optional[float],
    candidate_alarm_pct: Optional[float],
) -> str:
    """Classify observation against candidate thresholds (informational only).

    Returns one of: 'insufficient-data', 'within', 'warning', 'alarm'.
    """
    if delta_pct is None or candidate_warning_pct is None or candidate_alarm_pct is None:
        return "insufficient-data"
    abs_delta = abs(delta_pct)
    if abs_delta >= candidate_alarm_pct:
        return "alarm"
    if abs_delta >= candidate_warning_pct:
        return "warning"
    return "within"


class DriftAnalyzer:
    """Stateless analyzer that operates on the control DB.

    Call :meth:`ingest_outcome` from the task-completion path.
    Call :meth:`get_observation` / :meth:`get_observations` /
    :meth:`get_calibration_report` from query/CLI paths.
    """

    def __init__(
        self,
        database: ControlDatabase,
        *,
        canary_profiles: Any = None,
        fleet_registry: Any = None,
    ) -> None:
        self.database = database
        self._canary_profiles = canary_profiles  # CanaryProfileRegistry or None
        self._fleet_registry = fleet_registry  # FleetRegistry or None

    def _resolve_candidate_thresholds(
        self,
        profile_id: str,
        profile_version: int,
        runner_id: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        """Resolve candidate warning/alarm thresholds from profile + runner platform.

        Returns (candidate_warning_pct, candidate_alarm_pct) or (None, None).
        """
        if self._canary_profiles is None or self._fleet_registry is None:
            return None, None

        profile = self._canary_profiles.get(profile_id)
        if profile is None:
            return None, None

        # Structural version match
        if profile.profile_version != profile_version:
            return None, None

        # Resolve runner platform from fleet registry
        try:
            runner = self._fleet_registry.get_runner(runner_id, require_enabled=False)
        except Exception:
            return None, None

        # Prefer the canonical platform id, then aliases.
        platforms_thresholds = profile.thresholds.get("platforms", {})
        platform_ids = [runner.get("platform"), *runner.get("platform_aliases", [])]
        for platform_id in platform_ids:
            if platform_id in platforms_thresholds:
                thresholds = platforms_thresholds[platform_id]
                return thresholds.get("warning_pct"), thresholds.get("alarm_pct")

        return None, None

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def ingest_outcome(
        self,
        task_id: str,
        runner_id: str,
        outcome: Dict[str, Any],
        *,
        profile_id: str,
        profile_version: int,
        utc_date: str,
        environment: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Ingest a completed canary outcome into the observations table.

        Returns the observation dict if accepted, None if rejected.
        Idempotent: replaying the same (runner, profile, version, date, task_id)
        returns the existing observation unchanged.

        At most one accepted observation per (runner, profile, version, UTC date).
        A different task_id for an already-accepted date returns the existing
        accepted observation without alteration.
        """
        # Validate profile_id and utc_date are non-empty
        if not profile_id or not isinstance(profile_id, str):
            logger.warning("canary outcome %s has empty/invalid profile_id; skipping", task_id)
            return None
        if not utc_date or not isinstance(utc_date, str):
            logger.warning("canary outcome %s has empty/invalid utc_date; skipping", task_id)
            return None

        # Extract and validate score
        result = outcome.get("result") or {}
        score = result.get("score")

        if not _is_finite(score):
            logger.warning(
                "canary outcome %s has non-finite score %r; recording rejection",
                task_id,
                score,
            )
            self._record_rejected_observation(
                task_id=task_id,
                runner_id=runner_id,
                profile_id=profile_id,
                profile_version=profile_version,
                utc_date=utc_date,
                reason=f"non-finite score: {score!r}",
                environment=environment,
            )
            return None

        if not isinstance(score, (int, float)) or score <= 0:
            logger.warning(
                "canary outcome %s has non-positive score %r; recording rejection",
                task_id,
                score,
            )
            self._record_rejected_observation(
                task_id=task_id,
                runner_id=runner_id,
                profile_id=profile_id,
                profile_version=profile_version,
                utc_date=utc_date,
                reason=f"non-positive score: {score!r}",
                environment=environment,
            )
            return None

        score_val = float(score)
        completed_at = outcome.get("completed_at", utc_text())

        # Resolve candidate thresholds from profile + runner platform
        candidate_warning_pct, candidate_alarm_pct = self._resolve_candidate_thresholds(
            profile_id, profile_version, runner_id
        )

        with self.database.transaction(immediate=True) as conn:
            # Idempotency: check for existing observation with same exact key
            existing = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND utc_date = ? AND task_id = ?",
                (runner_id, profile_id, profile_version, utc_date, task_id),
            ).fetchone()
            if existing is not None:
                return dict(existing)

            # At-most-one accepted per (runner, profile, version, date):
            # if a different task already claimed this date, return it unchanged
            existing_accepted = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND utc_date = ? AND accepted = 1",
                (runner_id, profile_id, profile_version, utc_date),
            ).fetchone()
            if existing_accepted is not None:
                return dict(existing_accepted)

            # Compute rolling statistics from PRIOR accepted observations only.
            # "Prior" means strictly earlier UTC dates, regardless of arrival order.
            prior_rows = conn.execute(
                "SELECT score, utc_date FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND utc_date < ? AND accepted = 1 "
                "ORDER BY utc_date ASC, task_id ASC",
                (runner_id, profile_id, profile_version, utc_date),
            ).fetchall()
            prior_scores = [row["score"] for row in prior_rows]
            prior_dates = [row["utc_date"] for row in prior_rows]

            # Count total accepted in the ENTIRE series (before this insert)
            # for ordinal/phase computation — independent of current utc_date
            total_accepted = conn.execute(
                "SELECT COUNT(*) FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND accepted = 1",
                (runner_id, profile_id, profile_version),
            ).fetchone()[0]

            # This will be the (total_accepted + 1)-th observation
            ordinal = total_accepted + 1

            if ordinal <= OBSERVATION_WINDOW:
                phase = PHASE_OBSERVATION
            elif ordinal < CALIBRATION_WINDOW:
                phase = PHASE_CALIBRATING
            else:
                phase = PHASE_READY

            # Compute reference statistics from prior window
            ref_median = None
            ref_mad = None
            delta_pct = None
            window_start = None
            window_end = None
            ref_sample_count = len(prior_scores)

            if ref_sample_count >= MIN_SAMPLES_FOR_STATS:
                # Use at most ROLLING_WINDOW most recent prior observations
                window_scores = prior_scores[-ROLLING_WINDOW:]
                ref_median = _median(window_scores)
                ref_mad = _mad(window_scores)

                if ref_median != 0.0:
                    delta_pct = ((score_val - ref_median) / abs(ref_median)) * 100.0
                elif score_val != 0.0:
                    # Median is zero but score is not
                    delta_pct = float("inf") if score_val > 0 else float("-inf")
                else:
                    delta_pct = 0.0

                # Clamp infinite delta_pct for storage
                if delta_pct is not None and not math.isfinite(delta_pct):
                    delta_pct = 999999.0 if delta_pct > 0 else -999999.0

                # Window date range from prior observations
                recent_dates = prior_dates[-ROLLING_WINDOW:]
                if recent_dates:
                    window_start = recent_dates[0]
                    window_end = recent_dates[-1]

            # Compute candidate signal (informational only)
            cand_signal = _candidate_signal(delta_pct, candidate_warning_pct, candidate_alarm_pct)

            # Environment fingerprint and change annotation
            env_json = json.dumps(environment, sort_keys=True) if environment else None
            env_change_annotation = None
            if environment is not None:
                prev_env_row = conn.execute(
                    "SELECT environment_json FROM canary_observations "
                    "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                    "AND utc_date < ? AND accepted = 1 "
                    "ORDER BY utc_date DESC, task_id DESC LIMIT 1",
                    (runner_id, profile_id, profile_version, utc_date),
                ).fetchone()
                if prev_env_row is not None and prev_env_row["environment_json"] is not None:
                    prev_env = json.loads(prev_env_row["environment_json"])
                    changes = environment_changes(prev_env, environment)
                    env_change_annotation = "; ".join(changes) if changes else None

            # Extract provenance fields from outcome
            result_obj = outcome.get("result") or {}
            provenance_schema_version = result_obj.get("provenance_schema_version")

            now = utc_text()

            conn.execute(
                "INSERT INTO canary_observations "
                "(runner_id, profile_id, profile_version, utc_date, task_id, "
                "score, completed_at, phase, accepted, rejection_reason, "
                "ref_median, ref_mad, delta_pct, "
                "series_ordinal, ref_sample_count, "
                "candidate_warning_pct, candidate_alarm_pct, candidate_signal, actionable, "
                "window_start, window_end, "
                "environment_json, env_change_annotation, "
                "provenance_schema_version, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)",
                (
                    runner_id,
                    profile_id,
                    profile_version,
                    utc_date,
                    task_id,
                    score_val,
                    completed_at,
                    phase,
                    ref_median,
                    ref_mad,
                    delta_pct,
                    ordinal,
                    ref_sample_count,
                    candidate_warning_pct,
                    candidate_alarm_pct,
                    cand_signal,
                    window_start,
                    window_end,
                    env_json,
                    env_change_annotation,
                    provenance_schema_version,
                    now,
                ),
            )

            self.database.insert_audit(
                conn,
                actor="drift-analyzer",
                action="canary.observation",
                task_id=task_id,
                runner_id=runner_id,
                detail={
                    "profile_id": profile_id,
                    "profile_version": profile_version,
                    "utc_date": utc_date,
                    "score": score_val,
                    "phase": phase,
                    "series_ordinal": ordinal,
                    "ref_sample_count": ref_sample_count,
                    "ref_median": ref_median,
                    "ref_mad": ref_mad,
                    "delta_pct": delta_pct,
                    "candidate_signal": cand_signal,
                },
            )

            obs = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND utc_date = ? AND task_id = ?",
                (runner_id, profile_id, profile_version, utc_date, task_id),
            ).fetchone()

            # Check if we should generate a calibration report.
            # Use the total accepted count AFTER this insert for the trigger.
            if ordinal == CALIBRATION_WINDOW and phase == PHASE_READY:
                self._generate_calibration_report(
                    conn,
                    runner_id,
                    profile_id,
                    profile_version,
                    candidate_warning_pct=candidate_warning_pct,
                    candidate_alarm_pct=candidate_alarm_pct,
                )

        return dict(obs) if obs else None

    def _record_rejected_observation(
        self,
        *,
        task_id: str,
        runner_id: str,
        profile_id: str,
        profile_version: int,
        utc_date: str,
        reason: str,
        environment: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a rejected (malformed/non-finite) observation."""
        with self.database.transaction(immediate=True) as conn:
            existing = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND utc_date = ? AND task_id = ?",
                (runner_id, profile_id, profile_version, utc_date, task_id),
            ).fetchone()
            if existing is not None:
                return None

            now = utc_text()
            env_json = json.dumps(environment, sort_keys=True) if environment else None
            conn.execute(
                "INSERT INTO canary_observations "
                "(runner_id, profile_id, profile_version, utc_date, task_id, "
                "score, completed_at, phase, accepted, rejection_reason, "
                "ref_median, ref_mad, delta_pct, "
                "series_ordinal, ref_sample_count, "
                "candidate_warning_pct, candidate_alarm_pct, candidate_signal, actionable, "
                "window_start, window_end, "
                "environment_json, env_change_annotation, "
                "provenance_schema_version, created_at) "
                "VALUES (?, ?, ?, ?, ?, NULL, ?, 'observation', 0, ?, "
                "NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'insufficient-data', 0, "
                "NULL, NULL, ?, NULL, NULL, ?)",
                (
                    runner_id,
                    profile_id,
                    profile_version,
                    utc_date,
                    task_id,
                    now,
                    reason,
                    env_json,
                    now,
                ),
            )
            self.database.insert_audit(
                conn,
                actor="drift-analyzer",
                action="canary.rejected",
                task_id=task_id,
                runner_id=runner_id,
                detail={
                    "profile_id": profile_id,
                    "profile_version": profile_version,
                    "utc_date": utc_date,
                    "reason": reason,
                },
            )
        return None

    # ------------------------------------------------------------------
    # Calibration report
    # ------------------------------------------------------------------

    def _generate_calibration_report(
        self,
        conn: sqlite3.Connection,
        runner_id: str,
        profile_id: str,
        profile_version: int,
        *,
        candidate_warning_pct: Optional[float] = None,
        candidate_alarm_pct: Optional[float] = None,
    ) -> None:
        """Generate and store a calibration report at the 28-sample mark.

        Uses exactly the first 28 accepted observations sorted deterministically
        by (utc_date, task_id).
        """
        # Check if report already exists
        existing = conn.execute(
            "SELECT report_id FROM canary_calibration_reports "
            "WHERE runner_id = ? AND profile_id = ? AND profile_version = ?",
            (runner_id, profile_id, profile_version),
        ).fetchone()
        if existing:
            return  # Already generated

        # Collect exactly the first 28 accepted observations, deterministically ordered
        rows = conn.execute(
            "SELECT score, utc_date FROM canary_observations "
            "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
            "AND accepted = 1 ORDER BY utc_date ASC, task_id ASC LIMIT ?",
            (runner_id, profile_id, profile_version, CALIBRATION_WINDOW),
        ).fetchall()

        scores = [row["score"] for row in rows]
        dates = [row["utc_date"] for row in rows]

        if len(scores) < CALIBRATION_WINDOW:
            return  # Not enough data

        med = _median(scores)
        mad = _mad(scores)
        r_sigma = _robust_sigma(mad)

        # Variability floor: 3 * robust_sigma / |median| * 100
        # Handle zero median explicitly
        if med == 0.0:
            variability_floor = 0.0
            zero_median_warning = True
        else:
            variability_floor = _variability_floor_pct(mad, med)
            zero_median_warning = False

        # Recommended thresholds: merge candidate + variability floor
        if candidate_warning_pct is not None:
            rec_warning = max(candidate_warning_pct, variability_floor)
        else:
            rec_warning = variability_floor

        if candidate_alarm_pct is not None:
            rec_alarm = max(candidate_alarm_pct, 2.0 * variability_floor, rec_warning)
        else:
            rec_alarm = max(2.0 * variability_floor, rec_warning)

        report = {
            "runner_id": runner_id,
            "profile_id": profile_id,
            "profile_version": profile_version,
            "sample_count": len(scores),
            "date_range": {"start": dates[0], "end": dates[-1]},
            "median_score": med,
            "mad": mad,
            "robust_sigma": round(r_sigma, 4),
            "variability_floor_pct": round(variability_floor, 4),
            "min_score": min(scores),
            "max_score": max(scores),
            "spread_pct": round(((max(scores) - min(scores)) / abs(med) * 100.0) if med != 0 else 0.0, 4),
            "candidate_warning_pct": candidate_warning_pct,
            "candidate_alarm_pct": candidate_alarm_pct,
            "recommended_warning_pct": round(rec_warning, 4),
            "recommended_alarm_pct": round(rec_alarm, 4),
            "status": "ready-for-review",
            "zero_median_warning": zero_median_warning,
            "note": (
                "Calibration report generated from first 28 observations. "
                "Variability floor derived from 3 * 1.4826 * MAD / |median| (robust 3-sigma). "
                "Recommended thresholds are max(candidate, variability_floor). "
                "Review and accept before enabling drift alerts."
                + (
                    " WARNING: median is zero; variability floor is 0% by definition — manual review required."
                    if zero_median_warning
                    else ""
                )
            ),
        }

        now = utc_text()
        conn.execute(
            "INSERT INTO canary_calibration_reports "
            "(runner_id, profile_id, profile_version, sample_count, "
            "date_range_start, date_range_end, median_score, mad, "
            "robust_sigma, variability_floor_pct, "
            "candidate_warning_pct, candidate_alarm_pct, "
            "recommended_warning_pct, recommended_alarm_pct, "
            "status, report_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                runner_id,
                profile_id,
                profile_version,
                len(scores),
                dates[0],
                dates[-1],
                med,
                mad,
                round(r_sigma, 4),
                round(variability_floor, 4),
                candidate_warning_pct,
                candidate_alarm_pct,
                round(rec_warning, 4),
                round(rec_alarm, 4),
                "ready-for-review",
                json.dumps(report, sort_keys=True),
                now,
            ),
        )

        self.database.insert_audit(
            conn,
            actor="drift-analyzer",
            action="canary.calibration_report",
            runner_id=runner_id,
            detail={
                "profile_id": profile_id,
                "profile_version": profile_version,
                "sample_count": len(scores),
                "robust_sigma": round(r_sigma, 4),
                "variability_floor_pct": round(variability_floor, 4),
            },
        )

        logger.info(
            "calibration report generated for %s/%s v%d: robust_sigma=%.4f, " "variability_floor=%.4f%%",
            runner_id,
            profile_id,
            profile_version,
            r_sigma,
            variability_floor,
        )

    # ------------------------------------------------------------------
    # Query helpers (for PR3 CLI consumption)
    # ------------------------------------------------------------------

    def get_observation(
        self,
        runner_id: str,
        profile_id: str,
        profile_version: int,
        utc_date: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a single accepted observation, or None. Deterministic: accepted-only."""
        with self.database.read() as conn:
            row = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND utc_date = ? AND accepted = 1",
                (runner_id, profile_id, profile_version, utc_date),
            ).fetchone()
        return dict(row) if row else None

    def get_observations(
        self,
        runner_id: str,
        profile_id: str,
        profile_version: int,
        *,
        limit: int = 60,
    ) -> List[Dict[str, Any]]:
        """Get accepted observations for a series, newest first."""
        with self.database.read() as conn:
            rows = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND accepted = 1 ORDER BY utc_date DESC LIMIT ?",
                (runner_id, profile_id, profile_version, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_all_observations(
        self,
        runner_id: str,
        profile_id: str,
        profile_version: int,
        *,
        limit: int = 60,
    ) -> List[Dict[str, Any]]:
        """Get all observations (including rejected), newest first."""
        with self.database.read() as conn:
            rows = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "ORDER BY utc_date DESC LIMIT ?",
                (runner_id, profile_id, profile_version, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_calibration_report(
        self,
        runner_id: str,
        profile_id: str,
        profile_version: int,
    ) -> Optional[Dict[str, Any]]:
        """Get the calibration report, or None if not yet generated."""
        with self.database.read() as conn:
            row = conn.execute(
                "SELECT * FROM canary_calibration_reports "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ?",
                (runner_id, profile_id, profile_version),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["report"] = json.loads(result["report_json"])
        return result

    def get_series_summary(
        self,
        runner_id: str,
        profile_id: str,
        profile_version: int,
    ) -> Dict[str, Any]:
        """Summary for CLI status display.

        Phase boundaries match per-observation ingestion and docs:
        - accepted 1–14 → observation
        - accepted 15–27 → calibrating  (ordinal <= OBSERVATION_WINDOW is observation)
        - accepted 28+ → ready

        Returns ``observation_samples_required`` and
        ``calibration_samples_required`` so CLI callers can display
        progress without importing control-internal constants.
        """
        with self.database.read() as conn:
            accepted_count = conn.execute(
                "SELECT COUNT(*) FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND accepted = 1",
                (runner_id, profile_id, profile_version),
            ).fetchone()[0]

            rejected_count = conn.execute(
                "SELECT COUNT(*) FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND accepted = 0",
                (runner_id, profile_id, profile_version),
            ).fetchone()[0]

            latest = conn.execute(
                "SELECT * FROM canary_observations "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ? "
                "AND accepted = 1 ORDER BY utc_date DESC LIMIT 1",
                (runner_id, profile_id, profile_version),
            ).fetchone()

            cal = conn.execute(
                "SELECT status FROM canary_calibration_reports "
                "WHERE runner_id = ? AND profile_id = ? AND profile_version = ?",
                (runner_id, profile_id, profile_version),
            ).fetchone()

        # Determine overall phase — consistent with per-observation ordinal logic:
        #   ordinal <= OBSERVATION_WINDOW  → observation   (1..14)
        #   ordinal <  CALIBRATION_WINDOW  → calibrating   (15..27)
        #   ordinal >= CALIBRATION_WINDOW  → ready         (28+)
        progress: Optional[str] = None
        if accepted_count == 0:
            phase = "no-data"
        elif accepted_count <= OBSERVATION_WINDOW:
            phase = PHASE_OBSERVATION
            progress = f"{accepted_count}/{OBSERVATION_WINDOW}"
        elif accepted_count < CALIBRATION_WINDOW:
            phase = PHASE_CALIBRATING
            progress = f"{accepted_count}/{CALIBRATION_WINDOW}"
        else:
            phase = PHASE_READY
            progress = f"{accepted_count} samples"

        return {
            "runner_id": runner_id,
            "profile_id": profile_id,
            "profile_version": profile_version,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "phase": phase,
            "progress": progress,
            "observation_samples_required": OBSERVATION_WINDOW,
            "calibration_samples_required": CALIBRATION_WINDOW,
            "latest_observation": dict(latest) if latest else None,
            "calibration_status": cal["status"] if cal else None,
        }
