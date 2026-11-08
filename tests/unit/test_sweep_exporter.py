"""Unit tests for the sweep exporter."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from conductress.sweep.exporter import NotableSource, export_manifest, export_notable, export_series
from conductress.sweep.planner import BenchmarkPoint, Landmark, PointStatus, SweepState


@pytest.fixture
def tmp_path():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def make_state_with_results() -> SweepState:
    """Create a state with some benchmark results for export testing."""
    state = SweepState(
        merge_commits=["a", "b", "c", "d", "e"],
        commit_dates={
            "a": "2024-03-20",
            "b": "2024-04-15",
            "c": "2024-05-10",
            "d": "2024-06-01",
            "e": "2024-07-01",
        },
        landmarks=[Landmark(commit="c", date="2024-05-10", label="8.0.0")],
    )
    state.points["a"] = BenchmarkPoint(
        commit="a",
        date="2024-03-20",
        value=148000,
        cv=0.19,
        status=PointStatus.COMPLETED,
    )
    state.points["b"] = BenchmarkPoint(
        commit="b",
        date="2024-04-15",
        value=139000,
        cv=0.21,
        pr=1847,
        pr_title="Refactor output buffer",
        status=PointStatus.COMPLETED,
    )
    state.points["c"] = BenchmarkPoint(
        commit="c",
        date="2024-05-10",
        value=140000,
        cv=0.18,
        status=PointStatus.COMPLETED,
    )
    state.points["e"] = BenchmarkPoint(
        commit="e",
        date="2024-07-01",
        value=152000,
        cv=0.20,
        status=PointStatus.COMPLETED,
    )
    return state


class TestExporter:
    """Tests for series.json export."""

    def test_export_creates_file(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        assert output.exists()

    def test_export_valid_json(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        assert "metadata" in data
        assert "points" in data
        assert "landmarks" in data
        assert "annotations" in data

    def test_export_metadata(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output, platform="test-platform", workload="GET_16B_t1_p1")
        data = json.loads(output.read_text())
        assert data["metadata"]["repo"] == "valkey-io/valkey"
        assert data["metadata"]["branch"] == "unstable"
        assert data["metadata"]["platform"] == "test-platform"

    def test_export_points_ordered(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        points = data["points"]
        # Should be in commit order (a, b, c, e — d has no result)
        commits = [p["commit"] for p in points]
        assert commits == ["a", "b", "c", "e"]

    def test_export_includes_pr_info(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        # Point "b" has PR info
        b_point = next(p for p in data["points"] if p["commit"] == "b")
        assert b_point["pr"] == 1847

    def test_export_landmarks(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        assert len(data["landmarks"]) == 1
        assert data["landmarks"][0]["label"] == "8.0.0"

    def test_export_annotations_for_pinpointed_changes(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        # a->b is adjacent (index 0->1), delta = (139000-148000)/148000 = -6.08%
        annotations = data["annotations"]
        assert len(annotations) >= 1
        regression = next((a for a in annotations if a["commit"] == "b"), None)
        assert regression is not None
        assert regression["type"] == "decrease"
        assert regression["good"] is False
        assert regression["delta"] < 0
        assert regression["pr"] == 1847

    def test_export_annotations_include_commit_date(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        regression = next(a for a in data["annotations"] if a["commit"] == "b")
        assert regression["date"] == "2024-04-15"

    def test_annotation_suppressed_when_not_significant(self, tmp_path):
        """A >=4% delta between noisy points (high CV) must not be annotated.

        Simulates the bimodal-Intel scenario: adjacent commits differ by 5%
        but both points have CV 7% over 10 reps — the delta is noise, not a
        pinpointed change (Welch's t-test fails at p<0.05).
        """
        state = SweepState(
            merge_commits=["a", "b"],
            commit_dates={"a": "2024-03-20", "b": "2024-04-15"},
        )
        state.points["a"] = BenchmarkPoint(
            commit="a", date="2024-03-20", value=1_000_000, cv=7.0, reps=10, status=PointStatus.COMPLETED
        )
        state.points["b"] = BenchmarkPoint(
            commit="b", date="2024-04-15", value=1_050_000, cv=7.0, reps=10, status=PointStatus.COMPLETED
        )
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        assert data["annotations"] == []

    def test_annotation_kept_when_significant(self, tmp_path):
        """The same 5% delta with tight points (low CV) is annotated."""
        state = SweepState(
            merge_commits=["a", "b"],
            commit_dates={"a": "2024-03-20", "b": "2024-04-15"},
        )
        state.points["a"] = BenchmarkPoint(
            commit="a", date="2024-03-20", value=1_000_000, cv=0.5, reps=10, status=PointStatus.COMPLETED
        )
        state.points["b"] = BenchmarkPoint(
            commit="b", date="2024-04-15", value=1_050_000, cv=0.5, reps=10, status=PointStatus.COMPLETED
        )
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        assert len(data["annotations"]) == 1
        assert data["annotations"][0]["commit"] == "b"

    def test_annotation_legacy_points_without_cv_fall_back_to_threshold(self, tmp_path):
        """Points with no CV (legacy data) keep the threshold-only behavior."""
        state = SweepState(
            merge_commits=["a", "b"],
            commit_dates={"a": "2024-03-20", "b": "2024-04-15"},
        )
        state.points["a"] = BenchmarkPoint(
            commit="a", date="2024-03-20", value=1_000_000, cv=None, status=PointStatus.COMPLETED
        )
        state.points["b"] = BenchmarkPoint(
            commit="b", date="2024-04-15", value=1_050_000, cv=None, status=PointStatus.COMPLETED
        )
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        assert len(data["annotations"]) == 1
        assert data["annotations"][0]["commit"] == "b"

    def test_export_empty_state(self, tmp_path):
        state = SweepState()
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        assert data["points"] == []
        assert data["annotations"] == []

    def test_export_creates_parent_dirs(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "nested" / "dir" / "series.json"
        export_series(state, output)
        assert output.exists()

    def test_export_skips_build_failures(self, tmp_path):
        state = make_state_with_results()
        state.points["d"] = BenchmarkPoint(
            commit="d",
            date="2024-06-01",
            status=PointStatus.BUILD_FAILED,
        )
        output = tmp_path / "series.json"
        export_series(state, output)
        data = json.loads(output.read_text())
        commits = [p["commit"] for p in data["points"]]
        assert "d" not in commits


def make_memory_state() -> SweepState:
    """Create a state whose adjacent-commit change is an increase (bad for memory)."""
    state = SweepState(
        merge_commits=["a", "b", "c"],
        commit_dates={"a": "2024-05-01", "b": "2024-08-20", "c": "2024-09-01"},
    )
    state.points["a"] = BenchmarkPoint(
        commit="a",
        date="2024-05-01",
        value=100.0,
        status=PointStatus.COMPLETED,
    )
    state.points["b"] = BenchmarkPoint(
        commit="b",
        date="2024-08-20",
        value=110.0,
        pr=2001,
        pr_title="Grow per-key metadata",
        status=PointStatus.COMPLETED,
    )
    return state


class TestExportNotable:
    """Tests for the combined notable-changes export."""

    def test_aggregates_across_workloads_and_metrics(self, tmp_path):
        sources = [
            NotableSource(state=make_state_with_results(), workload="get-16b", metric="throughput"),
            NotableSource(state=make_memory_state(), workload="set-m20", metric="memory", lower_is_better=True),
        ]
        output = tmp_path / "notable.json"
        export_notable(sources, output, platform="test-platform")
        data = json.loads(output.read_text())
        assert data["metadata"]["platform"] == "test-platform"
        pairs = {(a["workload"], a["metric"]) for a in data["annotations"]}
        assert ("get-16b", "throughput") in pairs
        assert ("set-m20", "memory") in pairs

    def test_entries_sorted_newest_first(self, tmp_path):
        sources = [
            NotableSource(state=make_state_with_results(), workload="get-16b", metric="throughput"),
            NotableSource(state=make_memory_state(), workload="set-m20", metric="memory", lower_is_better=True),
        ]
        output = tmp_path / "notable.json"
        export_notable(sources, output, platform="test-platform")
        data = json.loads(output.read_text())
        dates = [a["date"] for a in data["annotations"]]
        assert dates == sorted(dates, reverse=True)
        # Memory entry (2024-08-20) is newer than throughput entry (2024-04-15)
        assert data["annotations"][0]["metric"] == "memory"

    def test_lower_is_better_polarity(self, tmp_path):
        sources = [
            NotableSource(state=make_memory_state(), workload="set-m20", metric="memory", lower_is_better=True),
        ]
        output = tmp_path / "notable.json"
        export_notable(sources, output, platform="test-platform")
        data = json.loads(output.read_text())
        entry = next(a for a in data["annotations"] if a["commit"] == "b")
        # +10% memory is an increase and NOT good
        assert entry["type"] == "increase"
        assert entry["good"] is False
        assert entry["pr"] == 2001

    def test_empty_sources_produce_valid_file(self, tmp_path):
        output = tmp_path / "notable.json"
        export_notable([], output, platform="test-platform")
        data = json.loads(output.read_text())
        assert data["annotations"] == []
        assert data["metadata"]["platform"] == "test-platform"

    def test_source_without_pinpointed_changes_contributes_nothing(self, tmp_path):
        state = SweepState(
            merge_commits=["a", "b"],
            commit_dates={"a": "2024-01-01", "b": "2024-02-01"},
        )
        # Only one completed point — no adjacent pair, no annotations
        state.points["a"] = BenchmarkPoint(commit="a", date="2024-01-01", value=100.0, status=PointStatus.COMPLETED)
        output = tmp_path / "notable.json"
        export_notable([NotableSource(state=state, workload="w", metric="throughput")], output, platform="p")
        data = json.loads(output.read_text())
        assert data["annotations"] == []


class TestEpochManifestExport:
    EPOCHS = [
        {"id": "v1", "label": "Legacy v1 (stock generator)", "generator": "stock"},
        {"id": "v2", "label": "Scalable v2 (patched generator)", "generator": "patched"},
    ]

    def test_v1_manifest_keeps_legacy_filename_and_advertises_epochs(self, tmp_path):
        with patch("conductress.publisher.detect_platform", return_value=("arm64", "ARM")):
            export_manifest(
                tmp_path,
                platforms=["arm64"],
                workloads=[("get-k16-v16-t7-p10", "throughput")],
                epoch_id="v1",
                epochs=self.EPOCHS,
            )
        path = tmp_path / "manifest-arm64.json"
        payload = json.loads(path.read_text())
        assert payload["version"] == 3
        assert payload["epoch"] == "v1"
        assert [e["id"] for e in payload["epochs"]] == ["v1", "v2"]

    def test_v2_manifest_uses_epoch_filename_and_mixed_workload(self, tmp_path):
        with patch("conductress.publisher.detect_platform", return_value=("arm64", "ARM")):
            export_manifest(
                tmp_path,
                platforms=["arm64"],
                workloads=[("mixed-s20-k16-v16-t7-p10", "throughput")],
                epoch_id="v2",
                epochs=self.EPOCHS,
            )
        path = tmp_path / "manifest-arm64.epoch-v2.json"
        payload = json.loads(path.read_text())
        assert payload["epoch"] == "v2"
        assert payload["throughput_workloads"] == ["mixed-s20-k16-v16-t7-p10"]


class TestClientBudgetMetadata:
    """The v3 export records the client budget (connections + client_threads)."""

    def test_export_series_records_client_budget_when_supplied(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output, connections=400, client_threads=16)
        meta = json.loads(output.read_text())["metadata"]
        assert meta["connections"] == 400
        assert meta["client_threads"] == 16

    def test_export_series_omits_budget_keys_when_not_supplied(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        meta = json.loads(output.read_text())["metadata"]
        assert "connections" not in meta
        assert "client_threads" not in meta

    def _export_roster_series(self, tmp_path, workload_id):
        from unittest.mock import patch as _patch

        from conductress.sweep.coordinator_v3 import create_v3_coordinators
        from conductress.sweep.planner import BenchmarkPoint, PointStatus

        with _patch("conductress.sweep.coordinator_v3._ensure_v3_state_dir"):
            with _patch("conductress.sweep.coordinator_v3.V3_STATE_DIR", tmp_path):
                coords = {c.workload_id: c for c in create_v3_coordinators(tmp_path)}
        coord = coords[workload_id]
        coord.state.merge_commits = ["a"]
        coord.state.commit_dates = {"a": "2026-01-01"}
        coord.state.points["a"] = BenchmarkPoint(
            commit="a", date="2026-01-01", value=1_000_000.0, cv=0.3, status=PointStatus.COMPLETED
        )
        output = tmp_path / f"series-{workload_id}.json"
        coord.export(output, platform="arm64/c7g.metal/graviton3")
        return json.loads(output.read_text())["metadata"]

    def test_p1_get_series_exports_sixteen_client_threads(self, tmp_path):
        meta = self._export_roster_series(tmp_path, "get-k16-v16-t7-p1")
        assert meta["client_threads"] == 16
        assert meta["connections"] == 400

    def test_p10_get_series_exports_eight_client_threads(self, tmp_path):
        meta = self._export_roster_series(tmp_path, "get-k16-v16-t7-p10")
        assert meta["client_threads"] == 8
        assert meta["connections"] == 400


class TestSettledMetadata:
    """A memory series says whether its points were sampled settled."""

    def test_export_series_records_settled_when_supplied(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output, settled=True)
        assert json.loads(output.read_text())["metadata"]["settled"] is True

    def test_export_series_omits_settled_when_not_supplied(self, tmp_path):
        state = make_state_with_results()
        output = tmp_path / "series.json"
        export_series(state, output)
        assert "settled" not in json.loads(output.read_text())["metadata"]

    def test_memory_coordinator_export_is_settled(self, tmp_path, monkeypatch):
        import conductress.config as config
        import conductress.sweep.memory_coordinator as mc
        from conductress.sweep.memory_coordinator import MemorySweepCoordinator, MemoryWorkload
        from conductress.sweep.planner import BenchmarkPoint, PointStatus

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey"])
        monkeypatch.setattr(mc, "MEMORY_STATE_DIR", tmp_path)
        wl = MemoryWorkload(command="sadd", key_size=0, value_size=20, label="sadd-m20", user_data_bytes=20)
        coord = MemorySweepCoordinator(tmp_path / "repo", wl)
        coord.state.merge_commits = ["a"]
        coord.state.commit_dates = {"a": "2026-01-01"}
        coord.state.points["a"] = BenchmarkPoint(
            commit="a", date="2026-01-01", value=18.93, cv=0.0, status=PointStatus.COMPLETED
        )
        output = tmp_path / "series.json"
        coord.export(output, platform="arm64/c7g.metal/graviton3")
        assert json.loads(output.read_text())["metadata"]["settled"] is True


def test_export_manifest_advertises_generations(tmp_path):
    from unittest.mock import patch

    from conductress.sweep.exporter import export_manifest

    closed = [{"id": "2026-11-06", "epoch": "v3", "until": "2026-11-06T16:14:23Z", "label": "OS update"}]
    with patch("conductress.publisher.detect_platform", return_value=("intel", "Intel")):
        export_manifest(
            tmp_path, platforms=["intel"], workloads=[("get", "throughput")], epoch_id="v3", generations=closed
        )
        export_manifest(tmp_path, platforms=["intel"], workloads=[("get", "throughput")], epoch_id="v3")
    live = json.loads((tmp_path / "manifest-intel.epoch-v3.json").read_text())
    # The second call (no generations) wins the live file name: an empty list, never a missing key.
    assert live["generations"] == []
    with patch("conductress.publisher.detect_platform", return_value=("intel", "Intel")):
        export_manifest(
            tmp_path, platforms=["intel"], workloads=[("get", "throughput")], epoch_id="v3", generations=closed
        )
    live = json.loads((tmp_path / "manifest-intel.epoch-v3.json").read_text())
    assert live["generations"] == closed and "generation" not in live
