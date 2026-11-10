"""Tests for the retired epoch-1 latency series.

The coordinator exists to publish the history already in its state file; it
must never claim, create, or record a task.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from conductress.config import LATENCY_TARGET_RPS
from conductress.sweep.latency_coordinator import LatencySweepCoordinator
from conductress.sweep.planner import BenchmarkPoint, PointStatus, SweepPlanner, SweepTask, TaskPriority
from conductress.tasks.task_cachecannon import CachecannonTaskData
from conductress.topology import TopologySpec


@pytest.fixture(autouse=True)
def patch_repo_names(monkeypatch):
    monkeypatch.setattr("conductress.task_queue.config.REPO_NAMES", ["valkey"])


@pytest.fixture
def latency_state_file(tmp_path, monkeypatch):
    state_file = tmp_path / "latency_state.json"
    monkeypatch.setattr("conductress.sweep.latency_coordinator.LATENCY_STATE_FILE", state_file)
    return state_file


@pytest.fixture
def coordinator(tmp_path, latency_state_file):
    repo = tmp_path / "valkey"
    repo.mkdir()
    coord = LatencySweepCoordinator(repo)
    coord.state.merge_commits = ["aaa", "bbb", "ccc", "ddd", "eee"]
    coord.planner = SweepPlanner(coord.state)
    return coord


def _cachecannon_task() -> CachecannonTaskData:
    task = CachecannonTaskData(
        source="valkey",
        specifier="abc123",
        make_args="",
        topology=TopologySpec.standalone(),
        note="",
        requirements={},
        test="get",
        set_ratio=0,
        val_size=16,
        io_threads=7,
        pipelining=1,
        connections=400,
        threads=8,
        warmup=10,
        duration=30,
        keyspace_count=3_000_000,
        distribution="uniform",
        rate_limit=LATENCY_TARGET_RPS,
        score_metric="p99",
    )
    task.sweep_commit = "aaa"  # type: ignore[attr-defined]
    return task


class TestIdentity:
    def test_metric_and_workload(self, coordinator):
        assert coordinator.metric_id == "latency"
        assert coordinator.workload_id == "get-k16-v16"
        assert coordinator.metric_unit == "µs"
        assert coordinator.lower_is_better is True
        assert coordinator.epoch_id == "v1"

    def test_export_filename_no_double_suffix(self, coordinator):
        """Publisher names files {workload_id}-{metric_id}.json; the workload must not repeat the metric."""
        filename = f"series-arm64-{coordinator.workload_id}-{coordinator.metric_id}.json"
        assert filename.count("latency") == 1


class TestRetired:
    def test_is_retired(self, coordinator):
        assert coordinator.retired is True

    def test_no_schedule_key_even_with_no_points(self, coordinator):
        assert coordinator.schedule_key() is None

    def test_never_queues(self, coordinator):
        with patch("conductress.sweep.coordinator.TaskQueue") as queue:
            assert coordinator.queue_next_if_needed() is False
        queue.return_value.submit_task.assert_not_called()

    def test_create_task_is_unreachable(self, coordinator):
        with pytest.raises(RuntimeError, match="retired"):
            coordinator._create_task(
                SweepTask(commit="aaa", date="2026-01-01", priority=TaskPriority.NIGHTLY, reason="test")
            )

    def test_claims_no_task(self, coordinator):
        assert coordinator._is_my_task(_cachecannon_task()) is False

    def test_completion_of_a_latency_shaped_cell_records_nothing(self, coordinator):
        with patch.object(coordinator, "record_result") as record:
            coordinator.on_task_completed(_cachecannon_task())
        record.assert_not_called()


class TestHistoryExport:
    def test_exports_recorded_points(self, coordinator, tmp_path):
        coordinator.state.points["aaa"] = BenchmarkPoint(
            commit="aaa", date="2026-01-01", value=812.0, cv=1.1, reps=3, status=PointStatus.COMPLETED
        )
        coordinator.state.points["ccc"] = BenchmarkPoint(
            commit="ccc", date="2026-01-03", value=790.0, cv=0.9, reps=3, status=PointStatus.COMPLETED
        )
        out = tmp_path / "series.json"
        count = coordinator.export(out, platform="arm64")
        assert count == 2
        payload = json.loads(out.read_text())
        assert payload["metadata"]["target_rps"] == LATENCY_TARGET_RPS
        assert payload["metadata"]["tool"] == "memtier_benchmark"
        assert len(payload["points"]) == 2
