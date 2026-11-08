"""Tests for perf metric export functionality."""

import json
from pathlib import Path

import pytest

from conductress.sweep.exporter import PERF_METRICS, _compute_metric, export_manifest, export_perf_metrics
from conductress.sweep.planner import BenchmarkPoint, Landmark, PointStatus, SweepState


@pytest.fixture
def sample_state():
    """Create a SweepState with perf counter data."""
    state = SweepState(
        merge_commits=["aaa", "bbb", "ccc"],
        commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01", "ccc": "2024-03-01"},
        commit_prs={"bbb": 42},
        commit_titles={"bbb": "Optimize hash table"},
        landmarks=[Landmark(commit="aaa", date="2024-01-01", label="8.0")],
    )
    state.points["aaa"] = BenchmarkPoint(
        commit="aaa",
        date="2024-01-01",
        value=2000000.0,
        cv=0.5,
        reps=5,
        status=PointStatus.COMPLETED,
        perf_counters={
            "instructions": 900_000_000_000,
            "cycles": 300_000_000_000,
            "L1-icache-load-misses": 30_000_000,
            "branch-misses": 5_000_000,
            "branches": 100_000_000_000,
            "stalled-cycles-frontend": 15_000_000_000,
            "stalled-cycles-backend": 75_000_000_000,
            "LLC-load-misses": 9_000_000,
        },
        perf_duration_seconds=30.0,
        perf_rps=2000000.0,
    )
    state.points["bbb"] = BenchmarkPoint(
        commit="bbb",
        date="2024-02-01",
        value=1900000.0,
        cv=0.4,
        reps=5,
        status=PointStatus.COMPLETED,
        perf_counters={
            "instructions": 950_000_000_000,
            "cycles": 320_000_000_000,
            "L1-icache-load-misses": 35_000_000,
            "branch-misses": 6_000_000,
            "branches": 105_000_000_000,
            "stalled-cycles-frontend": 18_000_000_000,
            "stalled-cycles-backend": 80_000_000_000,
            "LLC-load-misses": 10_000_000,
        },
        perf_duration_seconds=30.0,
        perf_rps=1900000.0,
    )
    # Point without perf counters (should be skipped)
    state.points["ccc"] = BenchmarkPoint(
        commit="ccc",
        date="2024-03-01",
        value=1950000.0,
        cv=0.3,
        reps=5,
        status=PointStatus.COMPLETED,
    )
    return state


class TestComputeMetric:
    def test_ipc(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "ipc")
        assert result == pytest.approx(3.0, rel=1e-6)

    def test_icache_mpki(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "icache-mpki")
        # 30M / 900B * 1000 = 0.0333...
        assert result == pytest.approx(0.0333, rel=1e-2)

    def test_branch_mpki(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "branch-mpki")
        # 5M / 900B * 1000 = 0.00556
        assert result == pytest.approx(0.00556, rel=1e-2)

    def test_frontend_stall_pct(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "frontend-stall-pct")
        # 15B / 300B * 100 = 5.0%
        assert result == pytest.approx(5.0, rel=1e-6)

    def test_backend_stall_pct(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "backend-stall-pct")
        # 75B / 300B * 100 = 25.0%
        assert result == pytest.approx(25.0, rel=1e-6)

    def test_llc_mpki(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "llc-mpki")
        # 9M / 900B * 1000 = 0.01
        assert result == pytest.approx(0.01, rel=1e-6)

    def test_instructions_per_req(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "instructions-per-req")
        # Counters are summed across reps (reps=5), while rps/duration describe one
        # rep — so the per-request value divides by the rep count:
        # 900B / (2M * 30 * 5) = 3000
        assert result == pytest.approx(3000.0, rel=1e-6)

    def test_cycles_per_req(self, sample_state):
        point = sample_state.points["aaa"]
        result = _compute_metric(point, "cycles-per-req")
        # Same rep correction as instructions-per-req: 300B / (2M * 30 * 5) = 1000
        assert result == pytest.approx(1000.0, rel=1e-6)
        # and the two agree with IPC: 3000 insn / 1000 cyc = 3.0
        assert _compute_metric(point, "instructions-per-req") / result == pytest.approx(
            _compute_metric(point, "ipc"), rel=1e-6
        )

    def test_no_counters_returns_none(self):
        point = BenchmarkPoint(commit="x", date="2024-01-01", status=PointStatus.COMPLETED)
        assert _compute_metric(point, "ipc") is None

    def test_missing_event_returns_none(self, sample_state):
        point = sample_state.points["aaa"]
        point.perf_counters = {"instructions": 100}  # no cycles
        assert _compute_metric(point, "ipc") is None


class TestRepCountNormalization:
    """Regression tests for the bimodal instructions-per-req bug.

    Root cause: raw perf counters are SUMMED across all collected reps (for
    statistical robustness), but the instructions-per-req divisor uses a single
    rep's request count (rps * duration). The result scaled linearly with the rep
    count, producing a bimodal distribution on the dashboard: fixed 3-rep runs read
    ~3x the true value and adaptive 10-rep runs read ~10x, even at identical RPS.
    The fix divides the summed counter by the rep count (perf_rep_count, falling
    back to reps). Ratio metrics (IPC, MPKI, stall%) are unaffected because the rep
    factor cancels in numerator and denominator.
    """

    # A synthetic main-thread counter set whose TRUE per-request instruction count
    # is exactly 3200 at 2.0M rps over a 30s window: 3200 * 2_000_000 * 30 per rep.
    _PER_REP_INSTR = 3200 * 2_000_000 * 30  # 192_000_000_000
    _PER_REP_CYCLES = _PER_REP_INSTR // 2  # IPC = 2.0

    def _point(self, reps: int, perf_rep_count, summed_reps: int) -> BenchmarkPoint:
        """Build a point whose counters were summed over ``summed_reps`` reps."""
        return BenchmarkPoint(
            commit="z" * 8,
            date="2024-01-01",
            value=2_000_000.0,
            cv=0.5,
            reps=reps,
            status=PointStatus.COMPLETED,
            perf_counters={
                "instructions": self._PER_REP_INSTR * summed_reps,
                "cycles": self._PER_REP_CYCLES * summed_reps,
            },
            perf_duration_seconds=30.0,
            perf_rps=2_000_000.0,
            perf_rep_count=perf_rep_count,
        )

    def test_3rep_and_10rep_yield_identical_per_req(self):
        """The original bug: 3-rep and 10-rep points at the same RPS read 3x vs 10x.

        With the fix they must read the same true value (~3200), not 9600 vs 32000.
        """
        p3 = self._point(reps=3, perf_rep_count=3, summed_reps=3)
        p10 = self._point(reps=10, perf_rep_count=10, summed_reps=10)
        v3 = _compute_metric(p3, "instructions-per-req")
        v10 = _compute_metric(p10, "instructions-per-req")
        assert v3 == pytest.approx(3200.0, rel=1e-6)
        assert v10 == pytest.approx(3200.0, rel=1e-6)
        assert v3 == pytest.approx(v10, rel=1e-9)

    def test_bug_would_be_bimodal_without_normalization(self):
        """Sanity check the fixture actually triggers the old bug shape.

        Without dividing by reps the divisor is rps*duration only, giving the old
        inflated values (9600 for 3 reps, 32000 for 10 reps).
        """
        p3 = self._point(reps=3, perf_rep_count=3, summed_reps=3)
        p10 = self._point(reps=10, perf_rep_count=10, summed_reps=10)
        # Recreate the buggy computation (no reps divisor).
        buggy3 = p3.perf_counters["instructions"] / (p3.perf_rps * p3.perf_duration_seconds)
        buggy10 = p10.perf_counters["instructions"] / (p10.perf_rps * p10.perf_duration_seconds)
        assert buggy3 == pytest.approx(9600.0, rel=1e-6)
        assert buggy10 == pytest.approx(32000.0, rel=1e-6)
        # The fix collapses them back to one value.
        assert _compute_metric(p3, "instructions-per-req") != pytest.approx(buggy3)

    def test_ratio_metrics_unaffected_by_rep_count(self):
        """IPC (a ratio) must be rep-invariant: the rep factor cancels."""
        p3 = self._point(reps=3, perf_rep_count=3, summed_reps=3)
        p10 = self._point(reps=10, perf_rep_count=10, summed_reps=10)
        assert _compute_metric(p3, "ipc") == pytest.approx(2.0, rel=1e-6)
        assert _compute_metric(p10, "ipc") == pytest.approx(2.0, rel=1e-6)

    def test_falls_back_to_reps_for_legacy_points(self):
        """Historical points predate perf_rep_count; reps == summed-rep count there.

        A legacy point (perf_rep_count=None) summed over 3 reps with reps=3 must
        still normalize correctly via the reps fallback.
        """
        legacy = self._point(reps=3, perf_rep_count=None, summed_reps=3)
        assert _compute_metric(legacy, "instructions-per-req") == pytest.approx(3200.0, rel=1e-6)

    def test_prefers_perf_rep_count_over_reps(self):
        """When perf collection captured fewer reps than throughput reps, the exact
        perf_rep_count (not reps) is the correct divisor."""
        # Counters summed over only 2 reps even though 5 throughput reps ran.
        p = self._point(reps=5, perf_rep_count=2, summed_reps=2)
        assert _compute_metric(p, "instructions-per-req") == pytest.approx(3200.0, rel=1e-6)


class TestExportPerfMetrics:
    def test_exports_metric_files(self, sample_state, tmp_path):
        exported = export_perf_metrics(sample_state, tmp_path, platform="amd64", workload="get16b-t7-p10")
        assert "ipc" in exported
        assert exported["ipc"] == 2  # aaa and bbb have counters, ccc doesn't

        ipc_file = tmp_path / "series-amd64-get16b-t7-p10-ipc.json"
        assert ipc_file.exists()
        data = json.loads(ipc_file.read_text())
        assert data["metadata"]["metric"] == "ipc"
        assert data["metadata"]["platform"] == "amd64"
        assert len(data["points"]) == 2
        assert data["points"][0]["value"] == pytest.approx(3.0, rel=1e-3)

    def test_includes_pr_info(self, sample_state, tmp_path):
        export_perf_metrics(sample_state, tmp_path, platform="amd64", workload="get16b-t7-p10")
        ipc_file = tmp_path / "series-amd64-get16b-t7-p10-ipc.json"
        data = json.loads(ipc_file.read_text())
        # bbb has PR info
        bbb_point = [p for p in data["points"] if p["commit"] == "bbb"][0]
        assert bbb_point["pr"] == 42
        assert bbb_point["pr_title"] == "Optimize hash table"

    def test_includes_landmarks(self, sample_state, tmp_path):
        export_perf_metrics(sample_state, tmp_path, platform="amd64", workload="get16b-t7-p10")
        ipc_file = tmp_path / "series-amd64-get16b-t7-p10-ipc.json"
        data = json.loads(ipc_file.read_text())
        assert len(data["landmarks"]) == 1
        assert data["landmarks"][0]["label"] == "8.0"

    def test_skips_metrics_without_required_events(self, tmp_path):
        """If counters lack LLC-load-misses, llc-mpki should not be exported."""
        state = SweepState(merge_commits=["aaa"], commit_dates={"aaa": "2024-01-01"})
        state.points["aaa"] = BenchmarkPoint(
            commit="aaa",
            date="2024-01-01",
            value=1000000.0,
            status=PointStatus.COMPLETED,
            perf_counters={"instructions": 100, "cycles": 50},  # no LLC data
            perf_duration_seconds=10.0,
            perf_rps=1000000.0,
        )
        exported = export_perf_metrics(state, tmp_path, platform="arm64", workload="get16b-t9-p10")
        assert "llc-mpki" not in exported
        assert "ipc" in exported

    def test_empty_state_returns_empty(self, tmp_path):
        state = SweepState()
        exported = export_perf_metrics(state, tmp_path, platform="amd64", workload="test")
        assert exported == {}


class TestExportManifest:
    def test_writes_manifest(self, tmp_path):
        from unittest.mock import patch

        with patch("conductress.publisher.detect_platform", return_value=("amd64", "amd64/test")):
            export_manifest(tmp_path, platforms=["amd64", "arm64"], workloads=[("get16b-t7-p10", "throughput")])
        manifest_file = tmp_path / "manifest-amd64.json"
        assert manifest_file.exists()
        data = json.loads(manifest_file.read_text())
        assert data["version"] == 4
        assert data["epoch"] == "v1"
        assert [epoch["id"] for epoch in data["epochs"]] == ["v1"]
        assert data["platform"] == "amd64"
        group_ids = [g["id"] for g in data["groups"]]
        assert "throughput" in group_ids
        assert "efficiency" in group_ids
        assert "cache" in group_ids
        assert "pipeline" in group_ids
        assert "branching" in group_ids


class TestKernelMetrics:
    """syscalls-per-req and context-switches-per-sec, fed by the privileged perf stat of PR #196/#202."""

    def _point(self, **counters):
        return BenchmarkPoint(
            commit="aaa",
            date="2024-01-01",
            value=2_000_000.0,
            reps=5,
            status=PointStatus.COMPLETED,
            perf_counters={"instructions": 100, "cycles": 50, **counters},
            perf_duration_seconds=30.0,
            perf_rps=2_000_000.0,
            perf_rep_count=5,
        )

    def test_syscalls_per_req_divides_by_summed_reps(self):
        # 0.12 syscalls/request over 5 reps x 30 s x 2M rps = 36M syscalls summed.
        point = self._point(**{"raw_syscalls:sys_enter": 36_000_000})
        assert _compute_metric(point, "syscalls-per-req") == pytest.approx(0.12)

    def test_context_switches_per_sec_divides_by_summed_window(self):
        point = self._point(**{"context-switches": 5 * 30 * 40})
        assert _compute_metric(point, "context-switches-per-sec") == pytest.approx(40.0)

    def test_zero_count_is_skipped_not_plotted(self):
        # A user-only row (pre-#196) records 0 for both kernel events; the guard
        # must return None so the series omits the point instead of drawing 0.
        point = self._point(**{"raw_syscalls:sys_enter": 0, "context-switches": 0})
        assert _compute_metric(point, "syscalls-per-req") is None
        assert _compute_metric(point, "context-switches-per-sec") is None

    def test_missing_events_return_none(self):
        point = self._point()
        assert _compute_metric(point, "syscalls-per-req") is None
        assert _compute_metric(point, "context-switches-per-sec") is None

    def test_kernel_group_in_manifest_with_per_thread_variants(self, tmp_path):
        from unittest.mock import patch

        with patch("conductress.publisher.detect_platform", return_value=("amd64", "amd64/test")):
            export_manifest(tmp_path, platforms=["amd64"], workloads=[("get16b-t7-p10", "throughput")])
        data = json.loads((tmp_path / "manifest-amd64.json").read_text())
        groups = {g["id"]: g for g in data["groups"]}
        assert groups["syscalls"]["series"] == ["syscalls-per-req"]
        assert groups["context-switches"]["series"] == ["context-switches-per-sec"]
        assert groups["syscalls-main"]["series"] == ["syscalls-per-req-main"]
        assert groups["context-switches-main"]["series"] == ["context-switches-per-sec-main"]
        assert groups["syscalls-io"]["per_thread"] == "io"
        assert groups["context-switches-io"]["per_thread"] == "io"
        # No longer a combined 'kernel' group; the two metrics live on their own charts.
        assert "kernel" not in groups
        for metric_id in ("syscalls-per-req", "context-switches-per-sec"):
            assert metric_id in PERF_METRICS


class TestScopeTagging:
    """Every exported perf point names the privilege scope its counters were taken under."""

    def test_legacy_points_are_user_scope(self, sample_state, tmp_path):
        export_perf_metrics(sample_state, tmp_path, platform="amd64", workload="get16b-t7-p10")
        data = json.loads((tmp_path / "series-amd64-get16b-t7-p10-ipc.json").read_text())
        assert all(p["scope"] == "user" for p in data["points"])
        assert data["metadata"]["scopes"] == ["user"]

    def test_mixed_scopes_tagged_per_point_and_listed_in_order(self, sample_state, tmp_path):
        sample_state.points["bbb"].perf_counters_scope = "user+kernel"
        export_perf_metrics(sample_state, tmp_path, platform="amd64", workload="get16b-t7-p10")
        data = json.loads((tmp_path / "series-amd64-get16b-t7-p10-ipc.json").read_text())
        by_commit = {p["commit"]: p["scope"] for p in data["points"]}
        assert by_commit == {"aaa": "user", "bbb": "user+kernel"}
        assert data["metadata"]["scopes"] == ["user", "user+kernel"]

    def test_per_thread_variants_carry_scope_too(self, sample_state, tmp_path):
        sample_state.points["aaa"].perf_counters_main = {"instructions": 10, "cycles": 5}
        sample_state.points["aaa"].perf_counters_scope = "user+kernel"
        export_perf_metrics(sample_state, tmp_path, platform="amd64", workload="get16b-t7-p10")
        data = json.loads((tmp_path / "series-amd64-get16b-t7-p10-ipc-main.json").read_text())
        assert [p["scope"] for p in data["points"]] == ["user+kernel"]
