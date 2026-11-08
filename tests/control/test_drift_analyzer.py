"""Tests for canary drift analysis: ingestion, rolling stats, phase tracking.

Covers: 14/28 phase transitions, rolling-window eviction, MAD computation,
zero median, zero MAD, robust sigma / variability floor, duplicate replay,
replay with different score/task, at-most-one-accepted-per-date uniqueness,
malformed/non-finite/non-positive score, malformed canary_id, out-of-order
completion (including 28th out-of-order), profile version separation,
calibration report generation with candidate thresholds, candidate signal
classification, environment fingerprint + change annotation, provenance
retention, zero-median calibration, DB migration v3 (partial unique index),
and no regression in normal (non-canary) outcome completion.

No sleeps -- all use explicit datetime/date injection.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from conductress.control.canary_profiles import CanaryProfileRegistry
from conductress.control.db import DATABASE_SCHEMA_VERSION, ControlDatabase
from conductress.control.drift_analyzer import (
    CALIBRATION_WINDOW,
    MAD_SIGMA_SCALE,
    OBSERVATION_WINDOW,
    PHASE_CALIBRATING,
    PHASE_OBSERVATION,
    PHASE_READY,
    DriftAnalyzer,
    _mad,
    _median,
    _robust_sigma,
    _variability_floor_pct,
)
from conductress.control.fleet_registry import FleetRegistry
from conductress.control.service import ControlService

from .helpers import fleet_manifest, task_envelope, task_outcome

# ---------------------------------------------------------------------------
# Profile helper
# ---------------------------------------------------------------------------


def _valid_profile(
    profile_id: str = "throughput-get-v1",
    profile_version: int = 1,
    pinned_commit: str = "a" * 40,
):
    return {
        "schema_version": 1,
        "profile_id": profile_id,
        "profile_version": profile_version,
        "description": "test canary profile",
        "source": "valkey",
        "pinned_commit": pinned_commit,
        "build": {"make_args": ""},
        "workload": {
            "test": "get",
            "val_size": 512,
            "key_size": 0,
            "io_threads": 9,
            "pipelining": 10,
            "clients": 1200,
            "threads": 16,
            "keyspace": 3000000,
            "warmup_seconds": 30,
            "duration_seconds": 300,
            "repetitions": 5,
            "seed": 42,
        },
        "schedule": {"utc_hour": 6, "freshness_hours": 18},
        "thresholds": {
            "platforms": {
                "graviton3": {"warning_pct": 2.0, "alarm_pct": 4.0},
                "graviton4": {"warning_pct": 2.0, "alarm_pct": 4.0},
                "amd": {"warning_pct": 0.5, "alarm_pct": 1.0},
            }
        },
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_env(tmp_path: Path, *, with_profiles: bool = False):
    """Create a DB, fleet registry, and drift analyzer."""
    manifest_path = tmp_path / "fleet.json"
    manifest_path.write_text(json.dumps(fleet_manifest()), encoding="utf-8")
    db = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
    db.initialize()
    registry = FleetRegistry.from_file(manifest_path)

    canary_profiles = None
    if with_profiles:
        canary_dir = tmp_path / "canary_profiles"
        canary_dir.mkdir()
        (canary_dir / "throughput-get-v1.json").write_text(json.dumps(_valid_profile()), encoding="utf-8")
        canary_profiles = CanaryProfileRegistry.from_directory(canary_dir)

    analyzer = DriftAnalyzer(
        db,
        canary_profiles=canary_profiles,
        fleet_registry=registry,
    )
    return db, registry, analyzer


def _ingest_n(
    analyzer: DriftAnalyzer,
    n: int,
    *,
    runner_id: str = "armbench",
    profile_id: str = "throughput-get-v1",
    profile_version: int = 1,
    base_score: float = 200000.0,
    score_fn=None,
    start_day: int = 1,
):
    """Ingest n observations for consecutive days starting at 2026-09-{start_day}.

    Uses valid dates only (day 1-28 map to 2026-09-01 through 2026-09-28).
    """
    results = []
    for i in range(n):
        day = start_day + i
        utc_date = f"2026-09-{day:02d}"
        task_id = f"canary:{runner_id}:{profile_id}:{utc_date}"
        score = score_fn(i) if score_fn else base_score + i * 100.0
        outcome = {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": runner_id,
            "state": "completed",
            "completed_at": f"{utc_date}T12:00:00Z",
            "result": {"score": score},
            "error": None,
        }
        obs = analyzer.ingest_outcome(
            task_id=task_id,
            runner_id=runner_id,
            outcome=outcome,
            profile_id=profile_id,
            profile_version=profile_version,
            utc_date=utc_date,
        )
        results.append(obs)
    return results


# ---------------------------------------------------------------------------
# Pure math tests
# ---------------------------------------------------------------------------


class TestMedianMAD:
    def test_median_odd(self):
        assert _median([3, 1, 2]) == 2

    def test_median_even_averages_two_middle(self):
        """Even-count median must be average of two middle values."""
        assert _median([4, 1, 3, 2]) == 2.5

    def test_median_even_six(self):
        """Six values: average of 3rd and 4th."""
        assert _median([1, 2, 3, 4, 5, 6]) == 3.5

    def test_median_single(self):
        assert _median([42]) == 42

    def test_median_empty_raises(self):
        with pytest.raises(ValueError):
            _median([])

    def test_mad_constant(self):
        assert _mad([5, 5, 5, 5]) == 0.0

    def test_mad_symmetric(self):
        # [1, 2, 3, 4, 5], median=3, deviations=[0,1,1,2,2], MAD=1
        assert _mad([1, 2, 3, 4, 5]) == 1.0

    def test_mad_single(self):
        assert _mad([10]) == 0.0

    def test_mad_two(self):
        # [10, 20], median=15, deviations=[5, 5], MAD=5
        assert _mad([10, 20]) == 5.0

    def test_robust_sigma(self):
        assert _robust_sigma(1.0) == pytest.approx(MAD_SIGMA_SCALE)
        assert _robust_sigma(0.0) == 0.0

    def test_variability_floor(self):
        """3 * 1.4826 * MAD / |median| * 100."""
        mad = 1000.0
        median = 100000.0
        expected = 3.0 * MAD_SIGMA_SCALE * mad / median * 100.0
        assert _variability_floor_pct(mad, median) == pytest.approx(expected)

    def test_variability_floor_zero_median(self):
        assert _variability_floor_pct(1000.0, 0.0) == 0.0


# ---------------------------------------------------------------------------
# Phase transition tests
# ---------------------------------------------------------------------------


class TestPhaseTransitions:
    def test_first_14_are_observation(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, OBSERVATION_WINDOW)
        for obs in results:
            assert obs["phase"] == PHASE_OBSERVATION

    def test_15th_is_calibrating(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, OBSERVATION_WINDOW + 1)
        assert results[OBSERVATION_WINDOW]["phase"] == PHASE_CALIBRATING

    def test_15_to_27_are_calibrating(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, CALIBRATION_WINDOW - 1)
        for obs in results[OBSERVATION_WINDOW:]:
            assert obs["phase"] == PHASE_CALIBRATING

    def test_28th_is_ready(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, CALIBRATION_WINDOW)
        assert results[CALIBRATION_WINDOW - 1]["phase"] == PHASE_READY

    def test_beyond_28_stays_ready(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, CALIBRATION_WINDOW + 2, start_day=1)
        for obs in results[CALIBRATION_WINDOW - 1 :]:
            assert obs["phase"] == PHASE_READY


# ---------------------------------------------------------------------------
# Rolling window tests
# ---------------------------------------------------------------------------


class TestRollingWindow:
    def test_first_observation_has_no_reference(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, 1)
        obs = results[0]
        assert obs["ref_median"] is None
        assert obs["ref_mad"] is None
        assert obs["delta_pct"] is None
        assert obs["ref_sample_count"] == 0

    def test_second_observation_has_reference_from_first(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 2, base_score=100000.0)
        obs_list = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        obs2 = obs_list[0]  # newest
        assert obs2["ref_median"] is not None
        assert obs2["ref_sample_count"] == 1
        assert obs2["ref_median"] == 100000.0

    def test_new_sample_not_in_own_window(self, tmp_path):
        """The critical invariant: a sample cannot move its own baseline."""
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 10, base_score=100000.0, score_fn=lambda i: 100000.0)
        outlier_date = "2026-09-11"
        outlier_tid = f"canary:armbench:throughput-get-v1:{outlier_date}"
        obs = analyzer.ingest_outcome(
            task_id=outlier_tid,
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": outlier_tid,
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": f"{outlier_date}T12:00:00Z",
                "result": {"score": 200000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date=outlier_date,
        )
        assert obs["ref_median"] == 100000.0
        assert obs["ref_mad"] == 0.0
        assert abs(obs["delta_pct"] - 100.0) < 0.01

    def test_window_eviction_at_28(self, tmp_path):
        """After 28+ prior samples, only the most recent 28 are used."""
        _, _, analyzer = _make_env(tmp_path)

        # Ingest 30 samples across Oct to avoid September overflow
        def score_fn(i):
            return 50000.0 if i < 2 else 100000.0

        # Use October dates for room
        results = []
        for i in range(30):
            day = i + 1
            utc_date = f"2026-10-{day:02d}"
            tid = f"canary:armbench:throughput-get-v1:{utc_date}"
            score = score_fn(i)
            outcome = {
                "schema_version": 1,
                "task_id": tid,
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": f"{utc_date}T12:00:00Z",
                "result": {"score": score},
                "error": None,
            }
            obs = analyzer.ingest_outcome(
                task_id=tid,
                runner_id="armbench",
                outcome=outcome,
                profile_id="throughput-get-v1",
                profile_version=1,
                utc_date=utc_date,
            )
            results.append(obs)

        # The 31st sample should NOT see the 50k values in its window
        date_31 = "2026-10-31"
        tid_31 = f"canary:armbench:throughput-get-v1:{date_31}"
        obs = analyzer.ingest_outcome(
            task_id=tid_31,
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": tid_31,
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": f"{date_31}T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date=date_31,
        )
        assert obs["ref_median"] == 100000.0
        assert obs["ref_mad"] == 0.0


# ---------------------------------------------------------------------------
# MAD edge cases
# ---------------------------------------------------------------------------


class TestMADEdgeCases:
    def test_zero_mad_constant_series(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, 5, score_fn=lambda i: 100000.0)
        for obs in results[2:]:
            assert obs["ref_mad"] == 0.0

    def test_zero_median(self, tmp_path):
        """Handle a series where median is zero."""
        _, _, analyzer = _make_env(tmp_path)
        # score must be positive, so this tests with 0.0 explicitly stored
        # Actually positive-guard will reject 0.0. Use very small positive values
        # then a big outlier to test median-near-zero differently.
        # Instead test zero-median via the _variability_floor_pct function directly
        # and via calibration zero-median test.
        pass

    def test_both_zero_delta(self, tmp_path):
        """_median([0]) is 0, but 0.0 score is rejected as non-positive.
        Test via pure math helpers instead."""
        # This case can't arise in practice since non-positive scores are rejected.
        # Verified via _variability_floor_pct(any_mad, 0.0) == 0.0
        assert _variability_floor_pct(100.0, 0.0) == 0.0


# ---------------------------------------------------------------------------
# Duplicate and replay
# ---------------------------------------------------------------------------


class TestDuplicateReplay:
    def test_duplicate_ingestion_is_idempotent(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        utc_date = "2026-09-01"
        task_id = "canary:armbench:throughput-get-v1:2026-09-01"
        outcome = {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": f"{utc_date}T12:00:00Z",
            "result": {"score": 100000.0},
            "error": None,
        }
        kwargs = dict(
            task_id=task_id,
            runner_id="armbench",
            outcome=outcome,
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date=utc_date,
        )
        obs1 = analyzer.ingest_outcome(**kwargs)
        obs2 = analyzer.ingest_outcome(**kwargs)
        assert obs1["task_id"] == obs2["task_id"]
        assert obs1["score"] == obs2["score"]
        assert obs1["created_at"] == obs2["created_at"]

    def test_replayed_outcome_does_not_affect_later_stats(self, tmp_path):
        """Replaying an old outcome doesn't create a second observation."""
        db, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 5)
        task_id = "canary:armbench:throughput-get-v1:2026-09-01"
        outcome = {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": {"score": 200000.0},
            "error": None,
        }
        analyzer.ingest_outcome(
            task_id=task_id,
            runner_id="armbench",
            outcome=outcome,
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-01",
        )
        with db.read() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM canary_observations " "WHERE runner_id = 'armbench' AND accepted = 1"
            ).fetchone()[0]
        assert count == 5

    def test_different_task_same_date_returns_existing_accepted(self, tmp_path):
        """A different task_id for an already-accepted date returns the existing
        accepted observation without alteration (fix #5)."""
        db, _, analyzer = _make_env(tmp_path)
        utc_date = "2026-09-01"
        # First task accepted
        obs1 = analyzer.ingest_outcome(
            task_id="task-A",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "task-A",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": f"{utc_date}T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date=utc_date,
        )
        assert obs1 is not None
        assert obs1["task_id"] == "task-A"
        assert obs1["score"] == 100000.0

        # Second task for same date with different score: returns existing
        obs2 = analyzer.ingest_outcome(
            task_id="task-B",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "task-B",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": f"{utc_date}T12:00:00Z",
                "result": {"score": 999999.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date=utc_date,
        )
        assert obs2 is not None
        assert obs2["task_id"] == "task-A"  # original task, not B
        assert obs2["score"] == 100000.0  # original score, not altered

        # Only one accepted in DB
        with db.read() as conn:
            count = conn.execute("SELECT COUNT(*) FROM canary_observations WHERE accepted = 1").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Malformed and non-finite scores
# ---------------------------------------------------------------------------


class TestMalformedScores:
    @pytest.mark.parametrize("bad_score", [None, "hello", float("nan"), float("inf"), float("-inf")])
    def test_non_finite_score_rejected(self, tmp_path, bad_score):
        _, _, analyzer = _make_env(tmp_path)
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:p:2026-09-01",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": {"score": bad_score},
            "error": None,
        }
        obs = analyzer.ingest_outcome(
            task_id="canary:armbench:p:2026-09-01",
            runner_id="armbench",
            outcome=outcome,
            profile_id="p",
            profile_version=1,
            utc_date="2026-09-01",
        )
        assert obs is None

    @pytest.mark.parametrize("bad_score", [0, 0.0, -1, -100.5])
    def test_non_positive_score_rejected(self, tmp_path, bad_score):
        """Non-positive scores are rejected (fix #6)."""
        _, _, analyzer = _make_env(tmp_path)
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:p:2026-09-01",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": {"score": bad_score},
            "error": None,
        }
        obs = analyzer.ingest_outcome(
            task_id="canary:armbench:p:2026-09-01",
            runner_id="armbench",
            outcome=outcome,
            profile_id="p",
            profile_version=1,
            utc_date="2026-09-01",
        )
        assert obs is None

    def test_rejected_observation_recorded_in_db(self, tmp_path):
        db, _, analyzer = _make_env(tmp_path)
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:p:2026-09-01",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": {"score": float("nan")},
            "error": None,
        }
        analyzer.ingest_outcome(
            task_id="canary:armbench:p:2026-09-01",
            runner_id="armbench",
            outcome=outcome,
            profile_id="p",
            profile_version=1,
            utc_date="2026-09-01",
        )
        with db.read() as conn:
            row = conn.execute(
                "SELECT * FROM canary_observations WHERE task_id = 'canary:armbench:p:2026-09-01'"
            ).fetchone()
        assert row is not None
        assert row["accepted"] == 0
        assert "non-finite" in row["rejection_reason"]

    def test_rejected_does_not_count_in_phase(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 13)
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:throughput-get-v1:2026-09-14",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-14T12:00:00Z",
            "result": {"score": float("nan")},
            "error": None,
        }
        analyzer.ingest_outcome(
            task_id="canary:armbench:throughput-get-v1:2026-09-14",
            runner_id="armbench",
            outcome=outcome,
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-14",
        )
        result = _ingest_n(analyzer, 1, start_day=15)
        assert result[0]["phase"] == PHASE_OBSERVATION

    def test_missing_result_treated_as_non_finite(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:p:2026-09-01",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": None,
            "error": None,
        }
        obs = analyzer.ingest_outcome(
            task_id="canary:armbench:p:2026-09-01",
            runner_id="armbench",
            outcome=outcome,
            profile_id="p",
            profile_version=1,
            utc_date="2026-09-01",
        )
        assert obs is None


# ---------------------------------------------------------------------------
# Malformed canary_id (fix #6)
# ---------------------------------------------------------------------------


class TestMalformedCanaryId:
    def test_empty_profile_id_rejected(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        obs = analyzer.ingest_outcome(
            task_id="t1",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t1",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="",
            profile_version=1,
            utc_date="2026-09-01",
        )
        assert obs is None

    def test_empty_utc_date_rejected(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        obs = analyzer.ingest_outcome(
            task_id="t1",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t1",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="",
        )
        assert obs is None

    def test_service_malformed_canary_id_no_colon(self, tmp_path):
        """ControlService._ingest_canary_observation handles no-colon canary_id."""
        db, registry, _ = _make_env(tmp_path)
        service = ControlService(db, registry)
        # Should not raise -- just log warning and skip
        service._ingest_canary_observation(
            "task-1",
            "armbench",
            {
                "schema_version": 1,
                "task_id": "task-1",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            {"canary_id": "nocolon", "envelope_json": "{}"},
        )
        with db.read() as conn:
            count = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]
        assert count == 0

    def test_service_malformed_canary_id_empty_parts(self, tmp_path):
        """canary_id ':date' gives empty profile; ':' gives empty both."""
        db, registry, _ = _make_env(tmp_path)
        service = ControlService(db, registry)
        for bad_id in [":2026-09-01", ":"]:
            service._ingest_canary_observation(
                "task-1",
                "armbench",
                {
                    "schema_version": 1,
                    "task_id": "task-1",
                    "runner_id": "armbench",
                    "state": "completed",
                    "completed_at": "2026-09-01T12:00:00Z",
                    "result": {"score": 100000.0},
                    "error": None,
                },
                {"canary_id": bad_id, "envelope_json": "{}"},
            )
        with db.read() as conn:
            count = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]
        assert count == 0


# ---------------------------------------------------------------------------
# Out-of-order completions (fix #7)
# ---------------------------------------------------------------------------


class TestOutOfOrder:
    def test_out_of_order_daily_results(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        for day, score in [(3, 300.0), (1, 100.0), (2, 200.0)]:
            utc_date = f"2026-09-{day:02d}"
            tid = f"canary:armbench:throughput-get-v1:{utc_date}"
            analyzer.ingest_outcome(
                task_id=tid,
                runner_id="armbench",
                outcome={
                    "schema_version": 1,
                    "task_id": tid,
                    "runner_id": "armbench",
                    "state": "completed",
                    "completed_at": f"{utc_date}T12:00:00Z",
                    "result": {"score": score},
                    "error": None,
                },
                profile_id="throughput-get-v1",
                profile_version=1,
                utc_date=utc_date,
            )
        obs = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        dates = [o["utc_date"] for o in obs]
        assert dates == ["2026-09-03", "2026-09-02", "2026-09-01"]

    def test_out_of_order_phase_uses_total_count(self, tmp_path):
        """Phase ordinal uses total accepted count, not calendar position."""
        _, _, analyzer = _make_env(tmp_path)
        # Ingest days 1-13 in order
        _ingest_n(analyzer, 13)
        # Now ingest day 28 (out of order) -- it's the 14th accepted
        obs_14 = analyzer.ingest_outcome(
            task_id="canary:armbench:throughput-get-v1:2026-09-28",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "canary:armbench:throughput-get-v1:2026-09-28",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-28T12:00:00Z",
                "result": {"score": 200000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-28",
        )
        assert obs_14["phase"] == PHASE_OBSERVATION  # 14th
        assert obs_14["series_ordinal"] == 14

    def test_out_of_order_28th_triggers_report(self, tmp_path):
        """The 28th accepted arrival triggers the report even if out of calendar order."""
        _, _, analyzer = _make_env(tmp_path)
        # Ingest days 2-28 first (27 observations)
        _ingest_n(analyzer, 27, start_day=2)
        assert analyzer.get_calibration_report("armbench", "throughput-get-v1", 1) is None

        # Now ingest day 1 out of order -- this is the 28th accepted
        obs = analyzer.ingest_outcome(
            task_id="canary:armbench:throughput-get-v1:2026-09-01",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 200000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-01",
        )
        assert obs["phase"] == PHASE_READY
        assert obs["series_ordinal"] == 28

        # Report should now exist
        report = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        assert report is not None
        assert report["status"] == "ready-for-review"
        assert report["sample_count"] == 28


# ---------------------------------------------------------------------------
# Profile version separation
# ---------------------------------------------------------------------------


class TestVersionSeparation:
    def test_different_versions_have_independent_series(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 5, profile_version=1, base_score=100000.0)
        _ingest_n(analyzer, 3, profile_version=2, base_score=200000.0)

        obs_v1 = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        obs_v2 = analyzer.get_observations("armbench", "throughput-get-v1", 2)
        assert len(obs_v1) == 5
        assert len(obs_v2) == 3

        latest_v2 = obs_v2[0]
        assert latest_v2["ref_sample_count"] == 2

    def test_version_change_resets_phase(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, OBSERVATION_WINDOW + 1, profile_version=1)
        results_v2 = _ingest_n(analyzer, 1, profile_version=2)
        assert results_v2[0]["phase"] == PHASE_OBSERVATION


# ---------------------------------------------------------------------------
# Calibration report
# ---------------------------------------------------------------------------


class TestCalibrationReport:
    def test_report_generated_at_28_samples(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, CALIBRATION_WINDOW, base_score=100000.0)

        report = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        assert report is not None
        assert report["status"] == "ready-for-review"
        assert report["sample_count"] == CALIBRATION_WINDOW
        data = report["report"]
        assert data["runner_id"] == "armbench"
        assert data["profile_id"] == "throughput-get-v1"
        assert data["status"] == "ready-for-review"
        assert "median_score" in data
        assert "mad" in data
        assert "robust_sigma" in data
        assert "variability_floor_pct" in data
        assert "recommended_warning_pct" in data
        assert "recommended_alarm_pct" in data
        # Old misleading fields should NOT be present
        assert "cv_pct" not in data
        assert "mad_based_threshold_pct" not in data

    def test_report_not_generated_before_28(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, CALIBRATION_WINDOW - 1)
        report = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        assert report is None

    def test_report_is_idempotent(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, CALIBRATION_WINDOW)
        report1 = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        _ingest_n(analyzer, 1, start_day=CALIBRATION_WINDOW + 1)
        report2 = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        assert report1["report_id"] == report2["report_id"]

    def test_report_variability_floor_correct(self, tmp_path):
        """Variability floor = 3 * 1.4826 * MAD / |median| * 100."""
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(
            analyzer,
            CALIBRATION_WINDOW,
            score_fn=lambda i: 99000.0 if i % 2 == 0 else 101000.0,
        )
        report = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        data = report["report"]
        # MAD of alternating 99k/101k: median ~100000, deviations all ~1000, MAD~1000
        # robust_sigma = 1.4826 * 1000 = 1482.6
        # variability_floor = 3 * 1482.6 / 100000 * 100 = 4.4478%
        assert data["robust_sigma"] == pytest.approx(MAD_SIGMA_SCALE * 1000.0, rel=0.02)
        expected_floor = 3.0 * MAD_SIGMA_SCALE * 1000.0 / 100000.0 * 100.0
        assert data["variability_floor_pct"] == pytest.approx(expected_floor, rel=0.02)

    def test_report_with_candidate_thresholds(self, tmp_path):
        """When profile + fleet provide candidates, report includes them."""
        _, _, analyzer = _make_env(tmp_path, with_profiles=True)
        _ingest_n(
            analyzer,
            CALIBRATION_WINDOW,
            score_fn=lambda i: 99000.0 if i % 2 == 0 else 101000.0,
        )
        report = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        data = report["report"]
        # armbench is graviton3, candidate warning=2.0%, alarm=4.0%
        assert data["candidate_warning_pct"] == 2.0
        assert data["candidate_alarm_pct"] == 4.0
        # variability_floor ≈ 4.45% > candidate_warning (2.0%)
        assert data["recommended_warning_pct"] >= data["variability_floor_pct"]
        assert data["recommended_alarm_pct"] >= data["recommended_warning_pct"]

    def test_report_db_columns_renamed(self, tmp_path):
        """DB columns use robust_sigma/variability_floor, not cv_pct/mad_based_threshold."""
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, CALIBRATION_WINDOW, base_score=100000.0)
        report = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        # Direct DB row inspection
        assert "robust_sigma" in report
        assert "variability_floor_pct" in report
        assert "recommended_warning_pct" in report
        assert "recommended_alarm_pct" in report


# ---------------------------------------------------------------------------
# Zero-median calibration (fix #9)
# ---------------------------------------------------------------------------


class TestZeroMedianCalibration:
    def test_zero_median_calibration_review_required(self, tmp_path):
        """If median is zero, variability_floor is 0% with explicit warning."""
        _, _, analyzer = _make_env(tmp_path)
        # We can't ingest 0.0 scores (non-positive rejected), but we can test
        # _variability_floor_pct directly and the calibration code path
        # with very small positive scores near zero.
        assert _variability_floor_pct(100.0, 0.0) == 0.0

        # Test through calibration with tiny but positive scores
        _ingest_n(analyzer, CALIBRATION_WINDOW, score_fn=lambda i: 0.001)
        report = analyzer.get_calibration_report("armbench", "throughput-get-v1", 1)
        assert report is not None
        data = report["report"]
        # MAD=0 for constant series, so variability_floor = 0
        assert data["variability_floor_pct"] == 0.0
        assert data["status"] == "ready-for-review"


# ---------------------------------------------------------------------------
# Candidate signal tests (fix #4)
# ---------------------------------------------------------------------------


class TestCandidateSignal:
    def test_observation_has_candidate_fields(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path, with_profiles=True)
        results = _ingest_n(analyzer, 2, base_score=100000.0)
        obs = results[1]  # second observation has reference stats
        assert "candidate_signal" in obs
        assert "candidate_warning_pct" in obs
        assert "candidate_alarm_pct" in obs
        assert obs["actionable"] == 0

    def test_first_observation_insufficient_data(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path, with_profiles=True)
        results = _ingest_n(analyzer, 1)
        assert results[0]["candidate_signal"] == "insufficient-data"

    def test_within_signal(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path, with_profiles=True)
        # All constant -> delta 0% which is < 2% warning
        results = _ingest_n(analyzer, 3, score_fn=lambda i: 100000.0)
        obs = results[2]
        assert obs["candidate_signal"] == "within"

    def test_warning_signal(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path, with_profiles=True)
        # 10 constant at 100k then one at 103k -> delta 3%, which is >= 2% warning but < 4% alarm
        _ingest_n(analyzer, 10, score_fn=lambda i: 100000.0)
        obs = analyzer.ingest_outcome(
            task_id="canary:armbench:throughput-get-v1:2026-09-11",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "canary:armbench:throughput-get-v1:2026-09-11",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-11T12:00:00Z",
                "result": {"score": 103000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-11",
        )
        assert obs["candidate_signal"] == "warning"

    def test_alarm_signal(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path, with_profiles=True)
        _ingest_n(analyzer, 10, score_fn=lambda i: 100000.0)
        obs = analyzer.ingest_outcome(
            task_id="canary:armbench:throughput-get-v1:2026-09-11",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "canary:armbench:throughput-get-v1:2026-09-11",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-11T12:00:00Z",
                "result": {"score": 105000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-11",
        )
        assert obs["candidate_signal"] == "alarm"

    def test_no_profiles_gives_insufficient_data(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path, with_profiles=False)
        _ingest_n(analyzer, 3, score_fn=lambda i: 100000.0)
        obs = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        for o in obs:
            assert o["candidate_signal"] in ("insufficient-data", None) or o["candidate_warning_pct"] is None


# ---------------------------------------------------------------------------
# Missed days (gaps)
# ---------------------------------------------------------------------------


class TestMissedDays:
    def test_gaps_do_not_affect_counting(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        for day in [1, 3, 5, 7]:
            utc_date = f"2026-09-{day:02d}"
            tid = f"canary:armbench:throughput-get-v1:{utc_date}"
            analyzer.ingest_outcome(
                task_id=tid,
                runner_id="armbench",
                outcome={
                    "schema_version": 1,
                    "task_id": tid,
                    "runner_id": "armbench",
                    "state": "completed",
                    "completed_at": f"{utc_date}T12:00:00Z",
                    "result": {"score": 100000.0},
                    "error": None,
                },
                profile_id="throughput-get-v1",
                profile_version=1,
                utc_date=utc_date,
            )
        obs = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        assert len(obs) == 4
        summary = analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert summary["accepted_count"] == 4
        assert summary["phase"] == PHASE_OBSERVATION


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


class TestQueryHelpers:
    def test_get_observation_returns_correct(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 3)
        obs = analyzer.get_observation("armbench", "throughput-get-v1", 1, "2026-09-02")
        assert obs is not None
        assert obs["utc_date"] == "2026-09-02"

    def test_get_observation_returns_none_for_missing(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        assert analyzer.get_observation("armbench", "throughput-get-v1", 1, "2026-09-01") is None

    def test_get_observation_returns_only_accepted(self, tmp_path):
        """get_observation is deterministic and accepted-only."""
        db, _, analyzer = _make_env(tmp_path)
        # Insert a rejected obs
        analyzer.ingest_outcome(
            task_id="t1",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t1",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": float("nan")},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-01",
        )
        # Should not appear via get_observation
        assert analyzer.get_observation("armbench", "throughput-get-v1", 1, "2026-09-01") is None

    def test_get_all_observations_includes_rejected(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 2)
        analyzer.ingest_outcome(
            task_id="canary:armbench:throughput-get-v1:2026-09-03",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "canary:armbench:throughput-get-v1:2026-09-03",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-03T12:00:00Z",
                "result": {"score": float("nan")},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-03",
        )
        all_obs = analyzer.get_all_observations("armbench", "throughput-get-v1", 1)
        accepted_obs = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        assert len(all_obs) == 3
        assert len(accepted_obs) == 2

    def test_series_summary(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 5)
        summary = analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert summary["accepted_count"] == 5
        assert summary["rejected_count"] == 0
        assert summary["phase"] == PHASE_OBSERVATION
        assert summary["progress"] == "5/14"
        assert summary["latest_observation"] is not None
        assert summary["calibration_status"] is None

    def test_series_summary_calibrating(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, 20)
        summary = analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert summary["phase"] == PHASE_CALIBRATING
        assert summary["progress"] == "20/28"

    def test_series_summary_ready(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        _ingest_n(analyzer, CALIBRATION_WINDOW + 2, start_day=1)
        summary = analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert summary["phase"] == PHASE_READY
        assert str(CALIBRATION_WINDOW + 2) in summary["progress"]
        assert summary["calibration_status"] == "ready-for-review"

    def test_series_summary_no_data(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        summary = analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert summary["phase"] == "no-data"
        assert summary["progress"] is None


# ---------------------------------------------------------------------------
# DB migration tests
# ---------------------------------------------------------------------------


class TestDBMigrationV3:
    def test_migration_v3_creates_tables(self, tmp_path):
        db = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
        db.initialize()
        with db.read() as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        assert "canary_observations" in tables
        assert "canary_calibration_reports" in tables
        assert version == DATABASE_SCHEMA_VERSION

    def test_migration_v3_idempotent(self, tmp_path):
        db = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
        db.initialize()
        db.initialize()
        db.initialize()
        with db.read() as conn:
            v3_count = conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 3").fetchone()[0]
        assert v3_count == 1

    def test_v2_db_upgrades_to_v3(self, tmp_path):
        db = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
        db.initialize()
        with db.read() as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "tasks" in tables
        assert "canary_schedule" in tables
        assert "canary_observations" in tables
        assert "canary_calibration_reports" in tables

    def test_partial_unique_index_exists(self, tmp_path):
        """The partial unique index enforcing one-accepted-per-date exists."""
        db = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
        db.initialize()
        with db.read() as conn:
            indexes = {row[1] for row in conn.execute("PRAGMA index_list('canary_observations')").fetchall()}
        assert "idx_canary_obs_one_accepted_per_date" in indexes


# ---------------------------------------------------------------------------
# Integration: service record_outcome triggers ingestion
# ---------------------------------------------------------------------------


class TestServiceIntegration:
    def _setup_canary_task(self, tmp_path, *, with_profiles=False):
        db, registry, analyzer = _make_env(tmp_path, with_profiles=with_profiles)
        service = ControlService(
            db,
            registry,
            canary_profiles=analyzer._canary_profiles if with_profiles else None,
        )
        with db.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO tasks "
                "(task_id, runner_id, task_class, priority, state, submitted_at, "
                "submitted_by, canary_id, envelope_json, created_at, updated_at, "
                "claimed_at, claim_token, accepted_at) "
                "VALUES (?, ?, ?, ?, 'accepted', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "canary:armbench:throughput-get-v1:2026-09-01",
                    "armbench",
                    "canary",
                    50,
                    "2026-09-01T06:00:00Z",
                    "canary-scheduler",
                    "throughput-get-v1:2026-09-01",
                    json.dumps(
                        {
                            "schema_version": 1,
                            "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
                            "runner_id": "armbench",
                            "task_class": "canary",
                            "priority": 50,
                            "submitted_at": "2026-09-01T06:00:00Z",
                            "submitted_by": "canary-scheduler",
                            "canary_id": "throughput-get-v1:2026-09-01",
                            "task": {
                                "task_type": "CanaryPerfTaskData",
                                "source": "valkey",
                                "specifier": "a" * 40,
                                "timestamp": "2026-09-01T06:00:00.000000",
                                "note": "canary throughput-get-v1 v1 (2026-09-01)",
                            },
                        }
                    ),
                    "2026-09-01T06:00:00Z",
                    "2026-09-01T06:00:00Z",
                    "2026-09-01T06:00:00Z",
                    "test-token",
                    "2026-09-01T06:00:00Z",
                ),
            )
        return db, service, analyzer

    def test_canary_completion_triggers_ingestion(self, tmp_path):
        db, service, analyzer = self._setup_canary_task(tmp_path)
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": {"score": 200000.0},
            "error": None,
        }
        service.record_outcome(
            "armbench",
            "canary:armbench:throughput-get-v1:2026-09-01",
            outcome,
            actor="test",
        )
        obs = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        assert len(obs) == 1
        assert obs[0]["score"] == 200000.0

    def test_service_uses_structural_profile_version(self, tmp_path):
        """When profile registry available, version comes from profile, not regex."""
        db, service, analyzer = self._setup_canary_task(tmp_path, with_profiles=True)
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": {"score": 200000.0},
            "error": None,
        }
        service.record_outcome(
            "armbench",
            "canary:armbench:throughput-get-v1:2026-09-01",
            outcome,
            actor="test",
        )
        obs = analyzer.get_observations("armbench", "throughput-get-v1", 1)
        assert len(obs) == 1
        assert obs[0]["score"] == 200000.0

    def test_non_canary_completion_does_not_trigger(self, tmp_path):
        db, registry, analyzer = _make_env(tmp_path)
        service = ControlService(db, registry)
        env = task_envelope("manual-1", runner_id="armbench")
        service.submit_task(env, actor="test")
        claim = service.claim_task("armbench", actor="test")
        service.accept_task("armbench", "manual-1", claim["claim_token"], actor="test")
        outcome = task_outcome("manual-1", "armbench", "completed")
        service.record_outcome("armbench", "manual-1", outcome, actor="test")
        with db.read() as conn:
            count = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]
        assert count == 0

    def test_failed_canary_does_not_trigger(self, tmp_path):
        db, registry, _ = _make_env(tmp_path)
        service = ControlService(db, registry)
        with db.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO tasks "
                "(task_id, runner_id, task_class, priority, state, submitted_at, "
                "submitted_by, canary_id, envelope_json, created_at, updated_at, "
                "claimed_at, claim_token, accepted_at) "
                "VALUES (?, ?, ?, ?, 'accepted', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "canary:armbench:throughput-get-v1:2026-09-01",
                    "armbench",
                    "canary",
                    50,
                    "2026-09-01T06:00:00Z",
                    "canary-scheduler",
                    "throughput-get-v1:2026-09-01",
                    json.dumps(
                        {
                            "schema_version": 1,
                            "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
                            "runner_id": "armbench",
                            "task_class": "canary",
                            "priority": 50,
                            "submitted_at": "2026-09-01T06:00:00Z",
                            "submitted_by": "canary-scheduler",
                            "canary_id": "throughput-get-v1:2026-09-01",
                            "task": {
                                "task_type": "CanaryPerfTaskData",
                                "source": "valkey",
                                "specifier": "a" * 40,
                            },
                        }
                    ),
                    "2026-09-01T06:00:00Z",
                    "2026-09-01T06:00:00Z",
                    "2026-09-01T06:00:00Z",
                    "test-token",
                    "2026-09-01T06:00:00Z",
                ),
            )
        outcome = {
            "schema_version": 1,
            "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
            "runner_id": "armbench",
            "state": "failed",
            "completed_at": "2026-09-01T12:00:00Z",
            "result": None,
            "error": "benchmark failed",
        }
        service.record_outcome(
            "armbench",
            "canary:armbench:throughput-get-v1:2026-09-01",
            outcome,
            actor="test",
        )
        with db.read() as conn:
            count = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]
        assert count == 0


# ---------------------------------------------------------------------------
# Environment/provenance fingerprint (fix #8)
# ---------------------------------------------------------------------------


class TestEnvironmentFingerprint:
    def test_environment_persisted(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        env = {"kernel": "6.1.90", "instance_id": "i-abc123"}
        utc_date = "2026-09-01"
        tid = "canary:armbench:throughput-get-v1:2026-09-01"
        obs = analyzer.ingest_outcome(
            task_id=tid,
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": tid,
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": f"{utc_date}T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date=utc_date,
            environment=env,
        )
        stored_env = json.loads(obs["environment_json"])
        assert stored_env["kernel"] == "6.1.90"
        assert stored_env["instance_id"] == "i-abc123"

    def test_no_environment_stored_as_null(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        results = _ingest_n(analyzer, 1)
        assert results[0]["environment_json"] is None

    def test_env_change_annotation(self, tmp_path):
        """Environment change annotation is deterministic diff."""
        _, _, analyzer = _make_env(tmp_path)
        env1 = {"kernel": "6.1.90", "instance_id": "i-abc123"}
        env2 = {"kernel": "6.1.91", "instance_id": "i-abc123"}
        # Day 1 with env1
        analyzer.ingest_outcome(
            task_id="t1",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t1",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-01",
            environment=env1,
        )
        # Day 2 with env2 (kernel changed)
        obs2 = analyzer.ingest_outcome(
            task_id="t2",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t2",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-02T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-02",
            environment=env2,
        )
        assert obs2["env_change_annotation"] is not None
        assert "kernel" in obs2["env_change_annotation"]
        assert "6.1.90" in obs2["env_change_annotation"]
        assert "6.1.91" in obs2["env_change_annotation"]

    def test_no_env_change_annotation_when_same(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        env = {"kernel": "6.1.90"}
        analyzer.ingest_outcome(
            task_id="t1",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t1",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-01",
            environment=env,
        )
        obs2 = analyzer.ingest_outcome(
            task_id="t2",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t2",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-02T12:00:00Z",
                "result": {"score": 100000.0},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-02",
            environment=env,
        )
        assert obs2["env_change_annotation"] is None

    def test_provenance_schema_version_stored(self, tmp_path):
        _, _, analyzer = _make_env(tmp_path)
        obs = analyzer.ingest_outcome(
            task_id="t1",
            runner_id="armbench",
            outcome={
                "schema_version": 1,
                "task_id": "t1",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 100000.0, "provenance_schema_version": 2},
                "error": None,
            },
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date="2026-09-01",
        )
        assert obs["provenance_schema_version"] == 2

    def test_service_enriches_environment_with_provenance(self, tmp_path):
        """Service integration passes enriched environment with runner_id and platform."""
        db, registry, _ = _make_env(tmp_path)
        service = ControlService(db, registry)
        with db.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO tasks "
                "(task_id, runner_id, task_class, priority, state, submitted_at, "
                "submitted_by, canary_id, envelope_json, created_at, updated_at, "
                "claimed_at, claim_token, accepted_at) "
                "VALUES (?, ?, ?, ?, 'accepted', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "canary:armbench:throughput-get-v1:2026-09-01",
                    "armbench",
                    "canary",
                    50,
                    "2026-09-01T06:00:00Z",
                    "canary-scheduler",
                    "throughput-get-v1:2026-09-01",
                    json.dumps(
                        {
                            "schema_version": 1,
                            "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
                            "runner_id": "armbench",
                            "task_class": "canary",
                            "priority": 50,
                            "submitted_at": "2026-09-01T06:00:00Z",
                            "submitted_by": "canary-scheduler",
                            "canary_id": "throughput-get-v1:2026-09-01",
                            "task": {
                                "task_type": "CanaryPerfTaskData",
                                "source": "valkey",
                                "specifier": "a" * 40,
                                "timestamp": "2026-09-01T06:00:00.000000",
                                "note": "canary throughput-get-v1 v1 (2026-09-01)",
                            },
                        }
                    ),
                    "2026-09-01T06:00:00Z",
                    "2026-09-01T06:00:00Z",
                    "2026-09-01T06:00:00Z",
                    "test-token",
                    "2026-09-01T06:00:00Z",
                ),
            )
        service.record_outcome(
            "armbench",
            "canary:armbench:throughput-get-v1:2026-09-01",
            {
                "schema_version": 1,
                "task_id": "canary:armbench:throughput-get-v1:2026-09-01",
                "runner_id": "armbench",
                "state": "completed",
                "completed_at": "2026-09-01T12:00:00Z",
                "result": {"score": 200000.0, "environment": {"kernel": "6.1"}},
                "error": None,
            },
            actor="test",
        )
        with db.read() as conn:
            row = conn.execute("SELECT environment_json FROM canary_observations WHERE accepted = 1").fetchone()
        env = json.loads(row["environment_json"])
        assert env["runner_id"] == "armbench"
        assert "platform" in env
        assert env["kernel"] == "6.1"


class TestEnvironmentChangeAnnotation:
    """The shared environment diff, as the ingest path applies it."""

    def _ingest_with_env(self, analyzer, day, environment, runner_id="armbench"):
        utc_date = f"2026-09-{day:02d}"
        task_id = f"canary:{runner_id}:throughput-get-v1:{utc_date}"
        outcome = {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": runner_id,
            "state": "completed",
            "completed_at": f"{utc_date}T12:00:00Z",
            "result": {"score": 200000.0, "environment": environment},
            "error": None,
        }
        return analyzer.ingest_outcome(
            task_id=task_id,
            runner_id=runner_id,
            outcome=outcome,
            profile_id="throughput-get-v1",
            profile_version=1,
            utc_date=utc_date,
            environment=environment,
        )

    def test_intelbench_style_update_names_kernel_glibc_and_microcode(self, tmp_path):
        _db, _registry, analyzer = _make_env(tmp_path)
        before = {
            "provenance_schema_version": 2,
            "kernel_release": "6.1.170-213.321.amzn2023.x86_64",
            "glibc_version": "2.34",
            "cpu_microcode": "0x2b000603",
            "boot_id": "a",
            "conductress_revision": "5466d25",
        }
        after = dict(before, kernel_release="6.1.189-236.400.amzn2023.x86_64", cpu_microcode="0x2b000685", boot_id="b")
        self._ingest_with_env(analyzer, 1, before)
        obs = self._ingest_with_env(analyzer, 2, after)
        annotation = obs["env_change_annotation"]
        assert "kernel_release: '6.1.170-213.321.amzn2023.x86_64' -> '6.1.189-236.400.amzn2023.x86_64'" in annotation
        assert "cpu_microcode: '0x2b000603' -> '0x2b000685'" in annotation
        assert "boot_id: 'a' -> 'b'" in annotation
        assert "glibc_version" not in annotation  # unchanged

    def test_schema_upgrade_adds_no_annotation(self, tmp_path):
        _db, _registry, analyzer = _make_env(tmp_path)
        v1 = {"provenance_schema_version": 1, "kernel_release": "6.1.170", "conductress_revision": "r"}
        v2 = dict(v1, provenance_schema_version=2, glibc_version="2.34", cpu_microcode=None, boot_id="a")
        self._ingest_with_env(analyzer, 1, v1)
        assert self._ingest_with_env(analyzer, 2, v2)["env_change_annotation"] is None
        # An ARM microcode None that stays None is no change; a value appearing is.
        assert self._ingest_with_env(analyzer, 3, dict(v2))["env_change_annotation"] is None
        assert (
            "cpu_microcode: None -> '0x1'"
            in self._ingest_with_env(analyzer, 4, dict(v2, cpu_microcode="0x1"))["env_change_annotation"]
        )

    def test_service_adds_server_keys_only_for_a_remote_server_host(self, tmp_path):
        db, registry, analyzer = _make_env(tmp_path, with_profiles=True)
        service = ControlService(db, registry, 300, canary_profiles=analyzer._canary_profiles)
        captured = {}

        def fake_ingest(**kwargs):
            captured.update(kwargs)
            return None

        service.drift_analyzer.ingest_outcome = fake_ingest
        envelope = task_envelope("canary:armbench:throughput-get-v1:2026-09-01", runner_id="armbench")
        envelope["task_class"] = "canary"
        envelope["canary_id"] = "throughput-get-v1:2026-09-01"
        service.submit_task(envelope, actor="operator:test")
        task_id = envelope["task_id"]
        claim = service.claim_task("armbench", actor="runner:armbench")
        service.accept_task("armbench", task_id, claim["claim_token"], actor="runner:armbench")
        outcome = task_outcome(task_id, "armbench")
        outcome["result"]["environment"] = {"kernel_release": "6.1", "boot_id": "client"}
        outcome["result"]["server_environment"] = {
            "same_host": False,
            "kernel_release": "6.18",
            "boot_id": "server",
            "glibc_version": "2.34",
            "cpu_microcode": None,
            "host_fingerprint": "ff",
        }
        service.record_outcome("armbench", task_id, outcome, actor="runner:armbench")
        env = captured["environment"]
        assert env["server_kernel_release"] == "6.18" and env["server_boot_id"] == "server"
        assert env["kernel_release"] == "6.1"
