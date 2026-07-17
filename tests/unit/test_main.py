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


class TestSetupConsoleLogging:
    """Setup must surface log output from ALL conductress modules on the console.

    Regression test for the silent-exit bug: ensure_ssh_key() logs a fatal
    error through conductress.bootstrap's module logger and then sys.exit(1)s.
    The old code attached the console handler only to conductress.__main__,
    so the error never reached the console and setup died with no output.
    """

    def test_bootstrap_logger_output_reaches_console(self, capsys):
        import logging

        from conductress.__main__ import _configure_setup_console_logging

        pkg_logger = _configure_setup_console_logging()
        handler = pkg_logger.handlers[-1]
        try:
            bootstrap_logger = logging.getLogger("conductress.bootstrap")
            bootstrap_logger.error("keyfile-missing-marker")
            captured = capsys.readouterr()
            assert "keyfile-missing-marker" in captured.err
        finally:
            pkg_logger.removeHandler(handler)

    def test_main_logger_output_reaches_console(self, capsys):
        import logging

        from conductress.__main__ import _configure_setup_console_logging

        pkg_logger = _configure_setup_console_logging()
        handler = pkg_logger.handlers[-1]
        try:
            logging.getLogger("conductress.__main__").info("banner-marker")
            captured = capsys.readouterr()
            assert "banner-marker" in captured.err
        finally:
            pkg_logger.removeHandler(handler)
