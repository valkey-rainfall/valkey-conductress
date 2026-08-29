"""Tests for status_export module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress.status_export import (
    DEFAULT_TASK_DURATION,
    _estimate_current_remaining,
    _get_current_task,
    _get_queue_info,
    _get_recent_results,
    _publish_status,
    export_status,
)


class TestEstimateCurrentRemaining:
    """Pure math — no mocking needed."""

    def test_no_task_returns_zero(self):
        assert _estimate_current_remaining(None) == 0

    def test_low_progress_returns_default(self):
        # Progress < 5% — not enough data to extrapolate
        task = {"progress_pct": 3.0, "elapsed_sec": 10}
        assert _estimate_current_remaining(task) == DEFAULT_TASK_DURATION

    def test_extrapolates_from_progress(self):
        # 50% done in 60s → 60s remaining
        task = {"progress_pct": 50.0, "elapsed_sec": 60}
        result = _estimate_current_remaining(task)
        assert abs(result - 60.0) < 1.0

    def test_nearly_complete_returns_small_value(self):
        # 90% done in 90s → ~10s remaining
        task = {"progress_pct": 90.0, "elapsed_sec": 90}
        result = _estimate_current_remaining(task)
        assert result < 15.0

    def test_zero_elapsed_returns_default(self):
        task = {"progress_pct": 50.0, "elapsed_sec": 0}
        assert _estimate_current_remaining(task) == DEFAULT_TASK_DURATION


class TestGetRecentResults:
    def test_returns_empty_when_no_file(self, tmp_path):
        with patch("conductress.status_export.CONDUCTRESS_OUTPUT", tmp_path / "nonexistent.jsonl"):
            assert _get_recent_results() == []

    def test_parses_valid_entries(self, tmp_path):
        output = tmp_path / "output.jsonl"
        entries = [
            {
                "task_id": "t1",
                "method": "perf-get",
                "score": 1950000,
                "commit_hash": "abc123def456",
                "source": "valkey",
                "specifier": "unstable",
                "note": "[sweep]",
                "end_time": "2026-05-23",
            },
            {
                "task_id": "t2",
                "method": "perf-get",
                "score": 1960000,
                "commit_hash": "def789abc012",
                "source": "valkey",
                "specifier": "unstable",
                "note": "",
                "end_time": "2026-05-23",
            },
        ]
        output.write_text("\n".join(json.dumps(e) for e in entries))

        with patch("conductress.status_export.CONDUCTRESS_OUTPUT", output):
            results = _get_recent_results()

        assert len(results) == 2
        # Most recent first (reversed)
        assert results[0]["task_id"] == "t2"
        assert results[0]["score"] == 1960000
        assert results[0]["commit"] == "def789ab"  # truncated to 8 chars

    def test_skips_malformed_lines(self, tmp_path):
        output = tmp_path / "output.jsonl"
        output.write_text('{"task_id": "good", "score": 100}\nnot json\n{"task_id": "also_good", "score": 200}\n')

        with patch("conductress.status_export.CONDUCTRESS_OUTPUT", output):
            results = _get_recent_results()

        assert len(results) == 2

    def test_limits_to_5_results(self, tmp_path):
        output = tmp_path / "output.jsonl"
        lines = [json.dumps({"task_id": f"t{i}", "score": i * 100}) for i in range(10)]
        output.write_text("\n".join(lines))

        with patch("conductress.status_export.CONDUCTRESS_OUTPUT", output):
            results = _get_recent_results()

        assert len(results) == 5


class TestGetCurrentTask:
    def test_returns_none_when_no_active_tasks(self):
        with patch("conductress.status_export.FileProtocol.get_active_task_ids", return_value={}):
            assert _get_current_task() is None

    def test_returns_task_info(self):
        mock_status = MagicMock()
        mock_status.task_type = "perf-get"
        mock_status.state = "running"
        mock_status.steps_completed = 50
        mock_status.steps_total = 100
        mock_status.start_time = 1000.0

        with (
            patch("conductress.status_export.FileProtocol.get_active_task_ids", return_value={"task_123": mock_status}),
            patch("conductress.status_export.time.time", return_value=1060.0),
        ):
            result = _get_current_task()

        assert result["id"] == "task_123"
        assert result["type"] == "perf-get"
        assert result["progress_pct"] == 50.0
        assert result["elapsed_sec"] == 60


class TestGetQueueInfo:
    def test_empty_queue(self, tmp_path):
        with patch("conductress.status_export.TaskQueue") as MockQueue:
            MockQueue.return_value.get_all_tasks.return_value = []
            result = _get_queue_info()

        assert result["depth"] == 0
        assert result["tasks"] == []

    def test_queue_with_tasks(self):
        task = MagicMock()
        task.task_id = "t1"
        task.task_type = "PerfTaskData"
        task.note = "test"
        task.source = "valkey"
        task.specifier = "main"

        with patch("conductress.status_export.TaskQueue") as MockQueue:
            MockQueue.return_value.get_all_tasks.return_value = [task]
            result = _get_queue_info()

        assert result["depth"] == 1
        assert result["tasks"][0]["id"] == "t1"


class TestExportStatus:
    def test_writes_valid_json(self, tmp_path):
        with (
            patch("conductress.status_export.STATUS_EXPORT_DIR", tmp_path),
            patch("conductress.status_export.STATUS_EXPORT_FILE", tmp_path / "status.json"),
            patch(
                "conductress.status_export.get_runner_info",
                return_value={
                    "runner_id": "armbench",
                    "platform": {"label": "arm64/c7g.metal/graviton3"},
                },
            ),
            patch(
                "conductress.status_export._get_runner_info",
                return_value={"pid": None, "state": "stopped", "uptime_hours": None},
            ),
            patch("conductress.status_export._get_current_task", return_value=None),
            patch("conductress.status_export._get_queue_info", return_value={"depth": 0, "tasks": []}),
            patch("conductress.status_export._get_recent_results", return_value=[]),
        ):
            path = export_status()

        assert path.exists()
        data = json.loads(path.read_text())
        assert "timestamp" in data
        assert data["schema_version"] == 1
        assert data["runner_id"] == "armbench"
        assert data["platform"] == "arm64/c7g.metal/graviton3"
        assert data["runner"]["state"] == "stopped"
        assert data["eta_minutes"] == 0.0


class TestPublishStatus:
    def test_stages_runner_and_legacy_paths_in_one_rsync(self, tmp_path):
        status_file = tmp_path / "status.json"
        status_file.write_text('{"runner_id": "armbench"}', encoding="utf-8")
        observed = {}

        def capture_rsync(args, destination, timeout):
            root = Path(args[-2])
            observed["legacy"] = (root / "status" / "arm.json").read_text(encoding="utf-8")
            observed["canonical"] = (root / "status" / "runners" / "armbench.json").read_text(encoding="utf-8")
            observed["destination"] = destination
            observed["timeout"] = timeout

        with (
            patch("conductress.status_export.STATUS_EXPORT_FILE", status_file),
            patch("conductress.publisher.detect_platform", return_value=("arm64", "arm64/c7g.metal/graviton3")),
            patch("conductress.status_export.get_runner_info", return_value={"runner_id": "armbench"}),
            patch("conductress.utility.run_rsync", side_effect=capture_rsync) as mock_rsync,
        ):
            _publish_status("user@data:/var/www/data")

        mock_rsync.assert_called_once()
        assert observed == {
            "legacy": '{"runner_id": "armbench"}',
            "canonical": '{"runner_id": "armbench"}',
            "destination": "user@data:/var/www/data/",
            "timeout": 15,
        }
