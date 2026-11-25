"""Tests for exporter breakdown and categories metadata."""

import json
from pathlib import Path

import pytest

from conductress.sweep.exporter import export_series
from conductress.sweep.planner import BenchmarkPoint, PointStatus, SweepState


@pytest.fixture
def state_with_breakdown():
    state = SweepState(
        merge_commits=["aaa", "bbb"],
        commit_dates={"aaa": "2024-01-01", "bbb": "2024-06-01"},
    )
    state.points["aaa"] = BenchmarkPoint(
        commit="aaa",
        date="2024-01-01",
        value=84.15,
        cv=0.0,
        reps=1,
        status=PointStatus.COMPLETED,
        breakdown={"robj_embval": 40.0, "sds": 24.0, "hashtable": 0, "dict": 24.0, "other": 0.3},
    )
    state.points["bbb"] = BenchmarkPoint(
        commit="bbb",
        date="2024-06-01",
        value=54.92,
        cv=0.0,
        reps=1,
        status=PointStatus.COMPLETED,
        breakdown={"robj_embval": 56.0, "sds": 0, "hashtable": 12.8, "dict": 0, "other": 0.3},
    )
    return state


@pytest.fixture
def state_without_breakdown():
    state = SweepState(
        merge_commits=["aaa"],
        commit_dates={"aaa": "2024-01-01"},
    )
    state.points["aaa"] = BenchmarkPoint(
        commit="aaa",
        date="2024-01-01",
        value=84.15,
        cv=0.0,
        reps=1,
        status=PointStatus.COMPLETED,
    )
    return state


class TestExportBreakdown:
    def test_includes_breakdown_in_points(self, state_with_breakdown, tmp_path):
        output = tmp_path / "series.json"
        export_series(state_with_breakdown, output, platform="arm64", workload="memory")

        data = json.loads(output.read_text())
        assert data["points"][0]["breakdown"]["robj_embval"] == 40.0
        assert data["points"][1]["breakdown"]["hashtable"] == 12.8

    def test_includes_categories_in_metadata(self, state_with_breakdown, tmp_path):
        output = tmp_path / "series.json"
        export_series(state_with_breakdown, output, platform="arm64", workload="memory")

        data = json.loads(output.read_text())
        assert "categories" in data["metadata"]
        assert "robj_embval" in data["metadata"]["categories"]
        assert "other" in data["metadata"]["categories"]

    def test_no_categories_without_breakdown(self, state_without_breakdown, tmp_path):
        output = tmp_path / "series.json"
        export_series(state_without_breakdown, output, platform="arm64", workload="memory")

        data = json.loads(output.read_text())
        assert "categories" not in data["metadata"]

    def test_no_breakdown_key_without_data(self, state_without_breakdown, tmp_path):
        output = tmp_path / "series.json"
        export_series(state_without_breakdown, output, platform="arm64", workload="memory")

        data = json.loads(output.read_text())
        assert "breakdown" not in data["points"][0]

    def test_recategorizes_from_raw_stacks_at_export(self, tmp_path):
        """When num_keys is provided, export uses raw_stacks instead of stored breakdown."""
        state = SweepState(
            merge_commits=["aaa"],
            commit_dates={"aaa": "2024-01-01"},
        )
        state.points["aaa"] = BenchmarkPoint(
            commit="aaa",
            date="2024-01-01",
            value=50.0,
            cv=0.0,
            reps=1,
            status=PointStatus.COMPLETED,
            breakdown={"sds": 999.0},  # stale breakdown — should be ignored
            raw_stacks=[
                [["je_malloc", "sdsnewlen", "setCommand"], 40_000_000],
                [["je_malloc", "hashtableExpand", "dbAdd"], 10_000_000],
            ],
        )
        output = tmp_path / "series.json"
        export_series(state, output, platform="arm64", workload="memory", num_keys=5_000_000)

        data = json.loads(output.read_text())
        bd = data["points"][0]["breakdown"]
        assert bd["sds"] == 8.0  # 40M / 5M
        assert bd["hashtable"] == 2.0  # 10M / 5M
        # Stale breakdown value (999.0) was NOT used
        assert bd["sds"] != 999.0

    def test_falls_back_to_breakdown_without_raw_stacks(self, tmp_path):
        """Points without raw_stacks use stored breakdown (backward compat)."""
        state = SweepState(
            merge_commits=["aaa"],
            commit_dates={"aaa": "2024-01-01"},
        )
        state.points["aaa"] = BenchmarkPoint(
            commit="aaa",
            date="2024-01-01",
            value=50.0,
            cv=0.0,
            reps=1,
            status=PointStatus.COMPLETED,
            breakdown={"robj_embval": 40.0, "hashtable": 10.0},
        )
        output = tmp_path / "series.json"
        export_series(state, output, platform="arm64", workload="memory", num_keys=5_000_000)

        data = json.loads(output.read_text())
        assert data["points"][0]["breakdown"]["robj_embval"] == 40.0


class TestExportMemoryObject:
    """Memory series carry a per-point ``memory`` object and series metadata for it."""

    RECORD = {
        "engine": {"server_name": "valkey", "version": "9.1.0"},
        "binary": {"sha256": "c" * 64},
        "profiling": {"profiled": False},
        "settle": {"settled": True, "passes": 1},
        "item_count": 5000000,
        "total_bytes_per_item": 110.93,
        "rss_bytes_per_item": 113.5,
        "rss_source": "proc_vmrss",
        "conductress_revision": "0123456789abcdef0123456789abcdef01234567",
    }

    @pytest.fixture
    def state(self):
        state = SweepState(merge_commits=["old", "new"], commit_dates={"old": "2024-01-01", "new": "2024-06-01"})
        state.points["old"] = BenchmarkPoint(
            commit="old", date="2024-01-01", value=84.15, cv=0.0, reps=1, status=PointStatus.COMPLETED
        )
        state.points["new"] = BenchmarkPoint(
            commit="new",
            date="2024-06-01",
            value=30.93,
            cv=0.0,
            reps=1,
            status=PointStatus.COMPLETED,
            memory_record=dict(self.RECORD),
        )
        return state

    def _export(self, state, tmp_path, **kwargs):
        output = tmp_path / "series.json"
        export_series(state, output, platform="arm64", workload="memory-set", **kwargs)
        return json.loads(output.read_text())

    def test_points_and_metadata(self, state, tmp_path):
        data = self._export(state, tmp_path, user_data_bytes=80, item_count=5_000_000, profiled=False)
        by_commit = {p["commit"]: p for p in data["points"]}
        new = by_commit["new"]
        assert new["results"]["memory-set"]["rps"] == 30.93  # overhead stays where it was
        assert new["memory"] == {
            "total_bytes_per_item": 110.93,
            "rss_bytes_per_item": 113.5,
            "rss_source": "proc_vmrss",
            "n": 5000000,
            "settled": True,
            "settle_passes": 1,
            "profiled": False,
            "engine_server_name": "valkey",
            "engine_version": "9.1.0",
            "binary_sha256": "c" * 64,
            "conductress_revision": "0123456789ab",
        }
        assert data["metadata"]["user_data_bytes"] == 80
        assert data["metadata"]["item_count"] == 5_000_000
        assert data["metadata"]["profiled"] is False

    def test_old_point_has_unknown_settle_and_rebuilt_total(self, state, tmp_path):
        data = self._export(state, tmp_path, user_data_bytes=80, item_count=5_000_000, profiled=False)
        old = {p["commit"]: p for p in data["points"]}["old"]["memory"]
        assert old["settled"] is None
        assert old["profiled"] is None
        assert old["total_bytes_per_item"] == pytest.approx(164.15)
        assert old["rss_bytes_per_item"] is None
        assert old["engine_server_name"] is None
        assert old["binary_sha256"] is None

    def test_other_series_are_unchanged(self, state, tmp_path):
        data = self._export(state, tmp_path)
        assert all("memory" not in p for p in data["points"])
        assert "user_data_bytes" not in data["metadata"]
        assert "item_count" not in data["metadata"]

    def test_stored_state_is_not_rewritten_by_export(self, state, tmp_path):
        before = {c: (p.value, p.memory_record) for c, p in state.points.items()}
        self._export(state, tmp_path, user_data_bytes=80, item_count=5_000_000, profiled=False)
        assert {c: (p.value, p.memory_record) for c, p in state.points.items()} == before

    def test_state_round_trip_keeps_the_record(self, state, tmp_path):
        path = tmp_path / "state.json"
        state.save(path)
        loaded = SweepState.load(path)
        assert loaded.points["new"].memory_record == self.RECORD
        assert loaded.points["old"].memory_record is None  # a point from before the record existed

    def test_state_file_from_before_the_field_loads(self, state, tmp_path):
        path = tmp_path / "state.json"
        state.save(path)
        raw = json.loads(path.read_text())
        for point in raw["points"].values():
            del point["memory_record"]
        path.write_text(json.dumps(raw))
        assert SweepState.load(path).points["new"].memory_record is None
