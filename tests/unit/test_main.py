"""Unit tests for src/__main__.py subcommand dispatch.

Validates: Requirements 9.6
"""

from unittest.mock import MagicMock, patch

import pytest

from conductress.__main__ import main


class TestTuiSubcommand:
    """Test that the 'tui' subcommand dispatches to conductress.tui.BenchmarkApp."""

    @patch("sys.argv", ["conductress", "tui"])
    @patch("conductress.__main__.logging")
    def test_tui_dispatches_to_benchmark_app(self, mock_logging):
        mock_app = MagicMock()
        with patch("conductress.tui.BenchmarkApp", return_value=mock_app) as mock_cls:
            main()
            mock_cls.assert_called_once()
            mock_app.run.assert_called_once()


class TestRunSubcommand:
    """Test that the 'run' subcommand dispatches to conductress.task_runner.TaskRunner."""

    @patch("sys.argv", ["conductress", "run"])
    @patch("conductress.__main__.logging")
    def test_run_dispatches_to_task_runner(self, mock_logging):
        mock_runner = MagicMock()
        with (
            patch("conductress.task_runner.TaskRunner", return_value=mock_runner) as mock_cls,
            patch("asyncio.run") as mock_asyncio_run,
        ):
            main()
            mock_cls.assert_called_once()
            mock_asyncio_run.assert_called_once_with(mock_runner.run())


class TestSetupSubcommand:
    """Test that the 'setup' subcommand dispatches to conductress.bootstrap functions."""

    @patch("sys.argv", ["conductress", "setup"])
    @patch("conductress.__main__.logging")
    def test_setup_dispatches_to_bootstrap(self, mock_logging):
        with (
            patch("conductress.bootstrap.ensure_ssh_key") as mock_ssh_key,
            patch("conductress.bootstrap.ensure_server_ssh_fingerprints") as mock_fingerprints,
            patch("conductress.bootstrap.update_host_list") as mock_update,
            patch("conductress.bootstrap.SERVERS", [MagicMock()]),
            patch("asyncio.run") as mock_asyncio_run,
        ):
            main()
            mock_ssh_key.assert_called_once()
            # asyncio.run is called twice: once for fingerprints, once for update_host_list
            assert mock_asyncio_run.call_count == 2


class TestQueueSubcommand:
    """Test that the 'queue' subcommand dispatches to conductress.cli.main()."""

    @patch("sys.argv", ["conductress", "queue"])
    @patch("conductress.__main__.logging")
    def test_queue_dispatches_to_cli_main(self, mock_logging):
        with patch("conductress.cli.main", return_value=0) as mock_cli_main:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
            mock_cli_main.assert_called_once_with(["queue"])

    @patch("sys.argv", ["conductress", "queue"])
    @patch("conductress.__main__.logging")
    def test_queue_propagates_nonzero_exit_code(self, mock_logging):
        with patch("conductress.cli.main", return_value=1) as mock_cli_main:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


class TestCompareSubcommand:
    """Test that the 'compare' subcommand dispatches to conductress.analysis.main()."""

    @patch("sys.argv", ["conductress", "compare", "branch-a", "branch-b"])
    @patch("conductress.__main__.logging")
    def test_compare_dispatches_to_analysis_main(self, mock_logging):
        with patch("conductress.analysis.main", return_value=0) as mock_analysis_main:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
            mock_analysis_main.assert_called_once_with(["branch-a", "branch-b"])

    @patch("sys.argv", ["conductress", "compare"])
    @patch("conductress.__main__.logging")
    def test_compare_dispatches_with_no_extra_args(self, mock_logging):
        with patch("conductress.analysis.main", return_value=0) as mock_analysis_main:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
            mock_analysis_main.assert_called_once_with([])

    @patch(
        "sys.argv",
        ["conductress", "compare", "branch-a", "branch-b", "--source", "valkey"],
    )
    @patch("conductress.__main__.logging")
    def test_compare_passes_remaining_args(self, mock_logging):
        with patch("conductress.analysis.main", return_value=0) as mock_analysis_main:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
            mock_analysis_main.assert_called_once_with(["branch-a", "branch-b", "--source", "valkey"])

    @patch("sys.argv", ["conductress", "compare", "branch-a", "branch-b"])
    @patch("conductress.__main__.logging")
    def test_compare_propagates_nonzero_exit_code(self, mock_logging):
        with patch("conductress.analysis.main", return_value=1) as mock_analysis_main:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


class TestNoSubcommand:
    """Test that invoking without a subcommand prints usage information."""

    @patch("sys.argv", ["conductress"])
    @patch("conductress.__main__.logging")
    def test_no_subcommand_prints_usage(self, mock_logging, capsys):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "usage:" in captured.out.lower() or "Usage:" in captured.out


class TestRunnerInfoSubcommand:
    RUNNER_INFO = {
        "schema_version": 1,
        "runner_id": "armbench",
        "display_name": "Graviton 3",
        "hostname": "host-a",
        "platform": {
            "id": "arm64",
            "label": "arm64/c7g.metal/graviton3",
            "aliases": ["graviton3", "arm64"],
        },
        "environment": {
            "host_fingerprint": "abc123",
            "instance_id": "i-test",
            "kernel_release": "6.1-test",
            "machine": "aarch64",
            "cpu_model": "Neoverse-V1",
            "conductress_revision": "deadbeef",
        },
    }

    @patch("sys.argv", ["conductress", "runner-info", "--json"])
    @patch("conductress.__main__.logging")
    def test_json_output_is_machine_readable(self, mock_logging, capsys):
        import json

        with patch("conductress.runner_identity.get_runner_info", return_value=self.RUNNER_INFO):
            main()

        assert json.loads(capsys.readouterr().out) == self.RUNNER_INFO

    @patch("sys.argv", ["conductress", "runner-info"])
    @patch("conductress.__main__.logging")
    def test_human_output_contains_identity(self, mock_logging, capsys):
        with patch("conductress.runner_identity.get_runner_info", return_value=self.RUNNER_INFO):
            main()

        output = capsys.readouterr().out
        assert "armbench" in output
        assert "arm64/c7g.metal/graviton3" in output
        assert "abc123" in output


class TestFleetControlSubcommands:
    @pytest.mark.parametrize(
        ("argv", "expected"),
        [
            (["conductress", "fleet", "list", "--json"], ["fleet", "list", "--json"]),
            (["conductress", "remote", "list"], ["remote", "list"]),
        ],
    )
    @patch("conductress.__main__.logging")
    def test_dispatches_to_fleet_cli(self, mock_logging, argv, expected):
        with (
            patch("sys.argv", argv),
            patch("conductress.fleet_cli.main", return_value=0) as fleet_main,
            pytest.raises(SystemExit) as exit_info,
        ):
            main()

        assert exit_info.value.code == 0
        fleet_main.assert_called_once_with(expected)


class TestFleetRunnerOptions:
    @patch(
        "sys.argv",
        ["conductress", "run", "--fleet-mode", "shadow", "--management-settle", "1.5"],
    )
    @patch("conductress.__main__.logging")
    def test_run_passes_fleet_mode_and_settle(self, mock_logging):
        runner = MagicMock()
        with (
            patch("conductress.task_runner.TaskRunner", return_value=runner) as runner_class,
            patch("asyncio.run"),
        ):
            main()

        kwargs = runner_class.call_args.kwargs
        assert kwargs["fleet_mode"] == "shadow"
        assert kwargs["management_settle_seconds"] == 1.5

    @patch("sys.argv", ["conductress", "run", "--management-settle", "-1"])
    @patch("conductress.__main__.logging")
    def test_negative_settle_is_usage_error(self, mock_logging):
        with pytest.raises(SystemExit) as exit_info:
            main()
        assert exit_info.value.code == 2
