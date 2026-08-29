"""Tests for the dashboard publisher."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress.publisher import DashboardPublisher, detect_platform


class TestDetectPlatform:
    @pytest.mark.parametrize(
        ("platform_info", "expected_id", "label_fragment"),
        [
            (("arm64", "arm64/c7g.metal/graviton3", ["graviton3", "arm64"]), "arm64", "graviton3"),
            (("graviton4", "arm64/c8g.metal/graviton4", ["graviton4"]), "graviton4", "graviton4"),
            (("amd64", "amd64/epyc-9r14/zen4", ["amd64", "amd"]), "amd64", "zen4"),
            (("intel", "intel/xeon-8488c/sapphire-rapids", ["intel"]), "intel", "sapphire"),
        ],
    )
    def test_shared_platform_detection(self, platform_info, expected_id, label_fragment):
        with patch("conductress.platform.get_local_platform_info", return_value=platform_info):
            platform_id, label = detect_platform()

        assert platform_id == expected_id
        assert label_fragment in label


class TestDashboardPublisher:
    def test_init(self, tmp_path):
        coord = MagicMock()
        coord.workload_id = "get16b-t7-p10"
        coord.metric_id = "throughput"
        pub = DashboardPublisher("user@host:/path", [coord])
        assert pub.target == "user@host:/path"
        assert pub.coordinators == [coord]

    def test_on_task_failed_is_noop(self):
        pub = DashboardPublisher("user@host:/path", [])
        pub.on_task_failed(MagicMock())  # should not raise

    def test_on_queue_empty_is_noop(self):
        pub = DashboardPublisher("user@host:/path", [])
        pub.on_queue_empty()  # should not raise

    @patch("conductress.utility.subprocess.run")
    def test_publish_calls_rsync(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        coord = MagicMock()
        coord.workload_id = "get16b-t7-p10"
        coord.metric_id = "throughput"
        coord.state = MagicMock()
        coord.export.return_value = 5

        pub = DashboardPublisher("user@host:/path", [coord])

        with patch("conductress.sweep.exporter.export_perf_metrics", return_value={}):
            with patch("conductress.sweep.exporter.export_manifest"):
                pub.on_task_completed(MagicMock())

        # rsync was called
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[0] == "rsync"
        assert "user@host:/path" in call_args[-1]

    @patch("conductress.utility.subprocess.run")
    def test_publish_failure_does_not_raise(self, mock_run):
        """Publish failures are non-fatal."""
        mock_run.side_effect = Exception("network error")
        coord = MagicMock()
        coord.workload_id = "get16b-t7-p10"
        coord.metric_id = "throughput"
        coord.state = MagicMock()
        coord.export.return_value = 5

        pub = DashboardPublisher("user@host:/path", [coord])

        with patch("conductress.sweep.exporter.export_perf_metrics", return_value={}):
            with patch("conductress.sweep.exporter.export_manifest"):
                pub.on_task_completed(MagicMock())  # should not raise

    @patch("conductress.utility.subprocess.run")
    def test_perf_metrics_exported_for_all_throughput_coordinators(self, mock_run):
        """Regression: perf metrics must export for every throughput coordinator, not just the first."""
        mock_run.return_value = MagicMock(returncode=0)
        coord_16b = MagicMock()
        coord_16b.workload_id = "get-k16-v16-t7-p10"
        coord_16b.metric_id = "throughput"
        coord_16b.state = MagicMock()
        coord_16b.export.return_value = 5

        coord_64b = MagicMock()
        coord_64b.workload_id = "get-k16-v64-t7-p10"
        coord_64b.metric_id = "throughput"
        coord_64b.state = MagicMock()
        coord_64b.export.return_value = 2

        pub = DashboardPublisher("user@host:/path", [coord_16b, coord_64b])

        with patch("conductress.sweep.exporter.export_perf_metrics") as mock_perf:
            with patch("conductress.sweep.exporter.export_manifest"):
                pub.on_task_completed(MagicMock())

        # Both coordinators must have perf metrics exported
        assert mock_perf.call_count == 2
        workload_ids = [call.args[3] for call in mock_perf.call_args_list]
        assert "get-k16-v16-t7-p10" in workload_ids
        assert "get-k16-v64-t7-p10" in workload_ids

    @patch("conductress.utility.subprocess.run")
    def test_notable_export_includes_valkey_throughput_and_memory_only(self, mock_run):
        """Notable feed aggregates Valkey throughput+memory series; Redis and latency are excluded."""
        mock_run.return_value = MagicMock(returncode=0)

        def make_coord(workload_id, metric_id, engine):
            coord = MagicMock()
            coord.workload_id = workload_id
            coord.metric_id = metric_id
            coord.engine = engine
            coord.lower_is_better = metric_id != "throughput"
            coord.state = MagicMock()
            coord.export.return_value = 1
            return coord

        valkey_engine = MagicMock()
        valkey_engine.source = "valkey"
        redis_engine = MagicMock()
        redis_engine.source = "redis"

        coords = [
            make_coord("get-k16-v16-t7-p10", "throughput", valkey_engine),
            make_coord("set-m20", "memory", None),  # legacy state, no engine -> Valkey
            make_coord("redis-get-k16-v16-t7-p10", "throughput", redis_engine),
            make_coord("get-lat", "latency", valkey_engine),
        ]
        pub = DashboardPublisher("user@host:/path", coords)

        with patch("conductress.sweep.exporter.export_perf_metrics"):
            with patch("conductress.sweep.exporter.export_manifest"):
                with patch("conductress.sweep.exporter.export_notable") as mock_notable:
                    pub.on_task_completed(MagicMock())

        mock_notable.assert_called_once()
        sources = mock_notable.call_args.args[0]
        included = {(s.workload, s.metric) for s in sources}
        assert included == {("get-k16-v16-t7-p10", "throughput"), ("set-m20", "memory")}
        # Output filename is platform-scoped
        output_path = mock_notable.call_args.args[1]
        assert output_path.name == f"notable-{pub._platform_id}.json"
