"""Unit tests for the pathological-workload scenario task."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress import config
from conductress.cli import main
from conductress.task_queue import TaskQueue
from conductress.tasks.task_mixed import set_ratio_to_memtier_ratio
from conductress.tasks.task_scenario import (
    LARGE_VALUE_READER_DEFAULT_SIZE,
    LARGE_VALUE_READER_KEYSPACE,
    OVERLAY_CLIENTS,
    OVERLAY_THREADS,
    SCENARIO_CHOICES,
    ScenarioTaskData,
    ScenarioTaskRunner,
    build_overlay_command,
    compute_dip_metrics,
    encode_resp_array,
    generate_multi_exec_resp_payload,
    parse_memtier_json_intervals,
    parse_memtier_stdout_intervals,
    validate_scenario,
)


class TestValidateScenario:
    """Tests for scenario name validation."""

    def test_valid_scenarios(self):
        for name in SCENARIO_CHOICES:
            assert validate_scenario(name) is True

    def test_invalid_scenario(self):
        assert validate_scenario("nonexistent") is False
        assert validate_scenario("") is False
        assert validate_scenario("eval_storm") is False  # underscore not dash


class TestComputeDipMetrics:
    """Tests for throughput dip analysis."""

    def test_no_dip(self):
        # Steady at 1M rps, no dip
        baseline = 1_000_000.0
        intervals = [1_000_000.0] * 30
        result = compute_dip_metrics(baseline, intervals)
        assert result["dip_depth_pct"] == 0.0
        assert result["dip_duration_seconds"] == 0
        assert result["min_rps"] == 1_000_000.0
        assert result["recovery_seconds"] == 0

    def test_full_dip(self):
        # Drop to zero for 5 seconds
        baseline = 1_000_000.0
        intervals = [1_000_000.0] * 10 + [0.0] * 5 + [1_000_000.0] * 15
        result = compute_dip_metrics(baseline, intervals)
        assert result["dip_depth_pct"] == 100.0
        assert result["dip_duration_seconds"] == 5
        assert result["min_rps"] == 0.0

    def test_partial_dip(self):
        # Drop to 50% for 3 seconds
        baseline = 1_000_000.0
        intervals = [1_000_000.0] * 10 + [500_000.0] * 3 + [1_000_000.0] * 17
        result = compute_dip_metrics(baseline, intervals)
        assert result["dip_depth_pct"] == 50.0
        assert result["dip_duration_seconds"] == 3
        assert result["min_rps"] == 500_000.0

    def test_empty_intervals(self):
        result = compute_dip_metrics(1_000_000.0, [])
        assert result["dip_depth_pct"] == 0.0

    def test_zero_baseline(self):
        result = compute_dip_metrics(0.0, [100.0, 200.0])
        assert result["dip_depth_pct"] == 0.0

    def test_recovery_time(self):
        # Drop below 80%, then recover above 90% after 4 seconds
        baseline = 1_000_000.0
        intervals = (
            [1_000_000.0] * 5
            + [700_000.0]  # below 80% (800K threshold)
            + [750_000.0]  # still below 80%
            + [850_000.0]  # below 90% (900K threshold)
            + [950_000.0]  # above 90% -> recovered
            + [1_000_000.0] * 21
        )
        result = compute_dip_metrics(baseline, intervals)
        assert result["recovery_seconds"] == 4  # from first dip to recovery


class TestBuildOverlayCommand:
    """Tests for overlay command construction."""

    def test_eval_storm(self):
        cmd = build_overlay_command("eval-storm", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert "valkey-benchmark" in cmd
        assert "EVAL" in cmd
        assert "-h 127.0.0.1" in cmd
        assert "-p 6379" in cmd
        assert "-r 3000000" in cmd

    def test_scan_churn(self):
        cmd = build_overlay_command("scan-churn", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert "valkey-cli" in cmd
        assert "--scan" in cmd
        assert "SECONDS" in cmd  # bash time loop

    def test_multi_exec(self):
        cmd = build_overlay_command("multi-exec", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert "valkey-cli" in cmd
        assert "--pipe" in cmd
        assert "/tmp/multi_exec_payload.resp" in cmd
        assert "valkey-benchmark" not in cmd
        # Should loop-replay until duration expires
        assert "SECONDS" in cmd
        # Should clean up payload file at end of loop
        assert "rm -f" in cmd

    def test_flushall_spike(self):
        cmd = build_overlay_command("flushall-spike", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert "FLUSHALL ASYNC" in cmd
        assert "memtier_benchmark" in cmd  # re-prefill
        assert "sleep" in cmd

    def test_expiry_heavy(self):
        cmd = build_overlay_command("expiry-heavy", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert "valkey-benchmark" in cmd
        assert "EX 3" in cmd
        assert "-r 3000000" in cmd

    def test_invalid_scenario_raises(self):
        with pytest.raises(ValueError, match="Unknown scenario"):
            build_overlay_command("bogus", "127.0.0.1", 6379, 30, 3_000_000, 512)

    def test_overlay_connections(self):
        """Overlay uses limited connections to avoid dominating."""
        cmd = build_overlay_command("eval-storm", "127.0.0.1", 6379, 30, 3_000_000, 512)
        expected_conns = OVERLAY_THREADS * OVERLAY_CLIENTS
        assert f"-c {expected_conns}" in cmd


class TestEncodeRespArray:
    """Tests for RESP array encoding."""

    def test_single_arg(self):
        result = encode_resp_array("PING")
        assert result == b"*1\r\n$4\r\nPING\r\n"

    def test_multi_arg(self):
        result = encode_resp_array("SET", "mykey", "myvalue")
        assert result == b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"

    def test_multi_command(self):
        result = encode_resp_array("MULTI")
        assert result == b"*1\r\n$5\r\nMULTI\r\n"

    def test_exec_command(self):
        result = encode_resp_array("EXEC")
        assert result == b"*1\r\n$4\r\nEXEC\r\n"

    def test_empty_value(self):
        result = encode_resp_array("SET", "key", "")
        assert result == b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n"

    def test_byte_exact_transaction(self):
        """Verify a full MULTI/GET/SET/EXEC transaction is byte-correct."""
        parts = []
        parts.append(encode_resp_array("MULTI"))
        parts.append(encode_resp_array("GET", "memtier-42"))
        parts.append(encode_resp_array("SET", "memtier-42", "xx"))
        parts.append(encode_resp_array("EXEC"))
        full = b"".join(parts)
        expected = (
            b"*1\r\n$5\r\nMULTI\r\n"
            b"*2\r\n$3\r\nGET\r\n$10\r\nmemtier-42\r\n"
            b"*3\r\n$3\r\nSET\r\n$10\r\nmemtier-42\r\n$2\r\nxx\r\n"
            b"*1\r\n$4\r\nEXEC\r\n"
        )
        assert full == expected


class TestGenerateMultiExecRespPayload:
    """Tests for MULTI/EXEC RESP payload generation."""

    def test_basic_structure(self):
        """Generated payload has correct number of commands (4 per transaction)."""
        payload = generate_multi_exec_resp_payload(num_transactions=5, keyspace=100, val_size=8)
        # Each transaction: MULTI + GET + SET + EXEC = 4 commands
        # Count MULTI occurrences
        assert payload.count(b"MULTI") == 5
        assert payload.count(b"EXEC") == 5
        assert payload.count(b"GET") == 5
        assert payload.count(b"SET") == 5

    def test_key_format(self):
        """Keys use memtier-<N> format."""
        payload = generate_multi_exec_resp_payload(num_transactions=10, keyspace=1000, val_size=4)
        # All keys should have memtier- prefix
        assert b"memtier-" in payload

    def test_value_size(self):
        """Value is exactly val_size bytes."""
        payload = generate_multi_exec_resp_payload(num_transactions=1, keyspace=100, val_size=16)
        # Value should be 16 'x' characters, encoded as $16\r\nxxxxxxxxxxxxxxxx\r\n
        assert b"$16\r\n" + b"x" * 16 + b"\r\n" in payload

    def test_keyspace_bounds(self):
        """All keys should be within the specified keyspace."""
        import random

        random.seed(42)
        payload = generate_multi_exec_resp_payload(num_transactions=100, keyspace=50, val_size=4)
        # Extract key numbers from the payload
        decoded = payload.decode()
        import re

        keys = re.findall(r"memtier-(\d+)", decoded)
        for k in keys:
            assert 1 <= int(k) <= 50

    def test_valid_resp_format(self):
        """Each line follows RESP protocol conventions."""
        payload = generate_multi_exec_resp_payload(num_transactions=2, keyspace=100, val_size=4)
        # Should start with *1\r\n (MULTI is a 1-arg command)
        assert payload.startswith(b"*1\r\n$5\r\nMULTI\r\n")
        # Should end with EXEC
        assert payload.endswith(b"*1\r\n$4\r\nEXEC\r\n")

    def test_payload_not_empty(self):
        payload = generate_multi_exec_resp_payload(num_transactions=1, keyspace=10, val_size=1)
        assert len(payload) > 0

    def test_deterministic_with_seed(self):
        """Same seed produces same payload."""
        import random

        random.seed(123)
        p1 = generate_multi_exec_resp_payload(num_transactions=5, keyspace=100, val_size=8)
        random.seed(123)
        p2 = generate_multi_exec_resp_payload(num_transactions=5, keyspace=100, val_size=8)
        assert p1 == p2


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestParseMemtierJsonIntervals:
    """Tests for memtier JSON interval parsing against real fixture."""

    def test_empty_content(self):
        assert parse_memtier_json_intervals("/tmp/test.json", "") == []

    def test_invalid_json(self):
        assert parse_memtier_json_intervals("/tmp/test.json", "not json") == []

    def test_no_all_stats(self):
        assert parse_memtier_json_intervals("/tmp/test.json", "{}") == []

    def test_no_time_serie(self):
        """JSON with Totals but no Time-Serie returns empty."""
        data = {"ALL STATS": {"Totals": {"Ops/sec": 100000.0}}}
        result = parse_memtier_json_intervals("/tmp/test.json", json.dumps(data))
        assert result == []

    def test_real_fixture(self):
        """Parse the real memtier JSON fixture captured from g4bench."""
        fixture_path = FIXTURES_DIR / "memtier_real_output.json"
        content = fixture_path.read_text()
        result = parse_memtier_json_intervals(str(fixture_path), content)
        # Real fixture has a 5s run -> 6 Time-Serie entries, last is partial -> 5 returned
        assert len(result) >= 4, f"Expected at least 4 interval values, got {len(result)}"
        # All values should be positive and represent real ops counts
        for val in result:
            assert val > 0, f"Expected positive ops count, got {val}"
        # Sanity: values should be in reasonable range (tens of thousands for 2t/4c/P10)
        assert all(v > 1000 for v in result), f"Values too low for a real run: {result}"

    def test_single_interval(self):
        """JSON with only one Time-Serie entry returns it (can't detect partial)."""
        data = {"ALL STATS": {"Totals": {"Time-Serie": {"0": {"Count": 50000}}}}}
        result = parse_memtier_json_intervals("/tmp/test.json", json.dumps(data))
        assert result == [50000.0]

    def test_excludes_last_partial_second(self):
        """Last Time-Serie entry (partial second) is excluded."""
        data = {
            "ALL STATS": {
                "Totals": {
                    "Time-Serie": {
                        "0": {"Count": 90000},
                        "1": {"Count": 88000},
                        "2": {"Count": 89000},
                        "3": {"Count": 80},  # partial second at end
                    }
                }
            }
        }
        result = parse_memtier_json_intervals("/tmp/test.json", json.dumps(data))
        assert result == [90000.0, 88000.0, 89000.0]

    def test_handles_zero_count(self):
        """Interval with Count=0 is still returned (valid data point — server stall)."""
        data = {
            "ALL STATS": {
                "Totals": {
                    "Time-Serie": {
                        "0": {"Count": 90000},
                        "1": {"Count": 0},
                        "2": {"Count": 85000},
                        "3": {"Count": 50},  # partial
                    }
                }
            }
        }
        result = parse_memtier_json_intervals("/tmp/test.json", json.dumps(data))
        assert result == [90000.0, 0.0, 85000.0]


class TestParseMemtierStdoutIntervals:
    """Tests for memtier stdout progress line parsing."""

    def test_empty_input(self):
        assert parse_memtier_stdout_intervals("") == []

    def test_real_fixture(self):
        """Parse real captured stdout from g4bench."""
        fixture_path = FIXTURES_DIR / "memtier_real_stdout.txt"
        content = fixture_path.read_text()
        result = parse_memtier_stdout_intervals(content)
        # Real stdout has per-second progress lines
        assert len(result) >= 4, f"Expected at least 4 interval values, got {len(result)}"
        for val in result:
            assert val > 0, f"Expected positive ops/sec, got {val}"

    def test_cr_separated_lines(self):
        """Parse \\r-separated progress lines (TTY mode)."""
        stdout = (
            "[RUN #1 20%,   1 secs]  2 threads  8 conns:  90270 ops,  90271 (avg:  90271) ops/sec, 3.41MB/sec\r"
            "[RUN #1 40%,   2 secs]  2 threads  8 conns: 178350 ops,  88074 (avg:  89172) ops/sec, 3.33MB/sec\r"
            "[RUN #1 60%,   3 secs]  2 threads  8 conns: 266869 ops,  88513 (avg:  88953) ops/sec, 3.35MB/sec\r"
        )
        result = parse_memtier_stdout_intervals(stdout)
        assert result == [90271.0, 88074.0, 88513.0]

    def test_newline_separated(self):
        """Parse \\n-separated progress lines (non-TTY / captured)."""
        stdout = (
            "[RUN #1 25%,   1 secs]  4 threads  50 conns: 500000 ops, 500000 (avg: 500000) ops/sec, 10MB/sec\n"
            "[RUN #1 50%,   2 secs]  4 threads  50 conns: 980000 ops, 480000 (avg: 490000) ops/sec, 9.8MB/sec\n"
        )
        result = parse_memtier_stdout_intervals(stdout)
        assert result == [500000.0, 480000.0]

    def test_ignores_non_progress_lines(self):
        """Non-progress lines (launch messages, summary) are skipped."""
        stdout = (
            "[RUN #1] Preparing benchmark client...\n"
            "[RUN #1] Launching threads now...\n"
            "[RUN #1 50%,   1 secs]  2 threads  4 conns: 100000 ops, 100000 (avg: 100000) ops/sec, 2MB/sec\n"
            "Totals       100000.00   ...\n"
        )
        result = parse_memtier_stdout_intervals(stdout)
        assert result == [100000.0]


class TestScenarioTaskDataValidation:
    """Tests for ScenarioTaskData construction and validation."""

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_valid_scenario(self):
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="eval-storm",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
        )
        assert task.scenario == "eval-storm"
        assert task.task_type == "ScenarioTaskData"

    def test_invalid_scenario_raises(self):
        with pytest.raises(ValueError, match="Unknown scenario"):
            ScenarioTaskData(
                source="valkey",
                specifier="unstable",
                make_args="",
                replicas=0,
                note="",
                requirements={},
                scenario="bogus-scenario",
                val_size=512,
                io_threads=9,
                pipelining=10,
                duration=30,
            )

    def test_all_valid_scenarios_accepted(self):
        for scenario in SCENARIO_CHOICES:
            task = ScenarioTaskData(
                source="valkey",
                specifier="unstable",
                make_args="",
                replicas=0,
                note="",
                requirements={},
                scenario=scenario,
                val_size=512,
                io_threads=9,
                pipelining=10,
                duration=30,
            )
            assert task.scenario == scenario

    def test_short_description(self):
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="flushall-spike",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=60,
        )
        desc = task.short_description()
        assert "flushall-spike" in desc
        assert "io=9" in desc

    def test_serialization_round_trip(self, tmp_path):
        """Task can be saved to JSON and reloaded."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="regression test",
            requirements={},
            scenario="expiry-heavy",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
            repetitions=5,
            perf_stat_enabled=True,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert isinstance(loaded, ScenarioTaskData)
        assert loaded.scenario == "expiry-heavy"
        assert loaded.val_size == 512
        assert loaded.io_threads == 9
        assert loaded.pipelining == 10
        assert loaded.duration == 30
        assert loaded.repetitions == 5
        assert loaded.perf_stat_enabled is True
        assert loaded.note == "regression test"


class TestCliAddScenario:
    """Tests for 'queue add-scenario' CLI subcommand."""

    @pytest.fixture(autouse=True)
    def isolate_queue(self, tmp_path):
        """Patch TaskQueue to use temp dir."""
        queue_path = tmp_path / "queue"
        queue_path.mkdir()

        _OriginalTaskQueue = TaskQueue

        class _IsolatedTaskQueue(_OriginalTaskQueue):
            def __init__(self, queue_dir_override=None):
                super().__init__(queue_dir=queue_path)

        with patch("conductress.cli.TaskQueue", _IsolatedTaskQueue):
            self.queue_path = queue_path
            yield

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_basic_add_scenario(self):
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "eval-storm",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        assert len(tasks) == 1

        data = json.loads(tasks[0].read_text())
        assert data["task_type"] == "ScenarioTaskData"
        assert data["scenario"] == "eval-storm"

    def test_all_scenarios_accepted(self):
        for scenario in SCENARIO_CHOICES:
            exit_code = main(
                [
                    "queue",
                    "add-scenario",
                    "--scenario",
                    scenario,
                    "--source",
                    "valkey",
                    "--specifier",
                    "unstable",
                ]
            )
            assert exit_code == 0

    def test_invalid_scenario_rejected(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            main(
                [
                    "queue",
                    "add-scenario",
                    "--scenario",
                    "nonexistent",
                    "--source",
                    "valkey",
                    "--specifier",
                    "unstable",
                ]
            )
        assert exc_info.value.code != 0

    def test_invalid_source_rejected(self, capsys):
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "eval-storm",
                "--source",
                "nosuchrepo",
                "--specifier",
                "unstable",
            ]
        )
        assert exit_code == 1
        assert "Invalid source" in capsys.readouterr().err

    def test_perf_stat_flag(self):
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "scan-churn",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--perf-stat",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["perf_stat_enabled"] is True

    def test_note_stored(self):
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "multi-exec",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--note",
                "testing concurrency",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["note"] == "testing concurrency"

    def test_custom_duration_and_reps(self):
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "eval-storm",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--duration",
                "2m",
                "--repetitions",
                "5",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["duration"] == 120
        assert data["repetitions"] == 5

    def test_custom_io_threads_and_pipelining(self):
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "expiry-heavy",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--io-threads",
                "7",
                "--pipelining",
                "50",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["io_threads"] == 7
        assert data["pipelining"] == 50


class TestBackgroundSetRatio:
    """Tests for background_set_ratio feature (Feature 1)."""

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_default_ratio_zero(self):
        """Default background_set_ratio is 0 (pure GET, backward compatible)."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="eval-storm",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
        )
        assert task.background_set_ratio == 0

    def test_valid_ratio_accepted(self):
        """Ratios 0-100 are accepted."""
        for ratio in (0, 1, 20, 50, 100):
            task = ScenarioTaskData(
                source="valkey",
                specifier="unstable",
                make_args="",
                replicas=0,
                note="",
                requirements={},
                scenario="eval-storm",
                val_size=512,
                io_threads=9,
                pipelining=10,
                duration=30,
                background_set_ratio=ratio,
            )
            assert task.background_set_ratio == ratio

    def test_invalid_ratio_rejected(self):
        """Ratios outside 0-100 raise ValueError."""
        for bad_ratio in (-1, 101, 200):
            with pytest.raises(ValueError, match="background_set_ratio must be 0-100"):
                ScenarioTaskData(
                    source="valkey",
                    specifier="unstable",
                    make_args="",
                    replicas=0,
                    note="",
                    requirements={},
                    scenario="eval-storm",
                    val_size=512,
                    io_threads=9,
                    pipelining=10,
                    duration=30,
                    background_set_ratio=bad_ratio,
                )

    def test_ratio_conversion_uses_set_ratio_to_memtier_ratio(self):
        """Verify set_ratio_to_memtier_ratio converts correctly for scenario use."""
        assert set_ratio_to_memtier_ratio(0) == "0:1"
        assert set_ratio_to_memtier_ratio(20) == "1:4"
        assert set_ratio_to_memtier_ratio(50) == "1:1"
        assert set_ratio_to_memtier_ratio(100) == "1:0"

    def test_short_description_shows_ratio_when_nonzero(self):
        """short_description includes SET ratio only when > 0."""
        task_zero = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="eval-storm",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
            background_set_ratio=0,
        )
        assert "SET=" not in task_zero.short_description()

        task_20 = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="eval-storm",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
            background_set_ratio=20,
        )
        assert "SET=20%" in task_20.short_description()

    def test_serialization_round_trip_with_ratio(self, tmp_path):
        """background_set_ratio survives save/load cycle."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="flushall-spike",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
            background_set_ratio=35,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert isinstance(loaded, ScenarioTaskData)
        assert loaded.background_set_ratio == 35

    def test_serialization_round_trip_default_ratio(self, tmp_path):
        """Default ratio=0 survives round trip (backward compat with old tasks)."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="eval-storm",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert loaded.background_set_ratio == 0


class TestBackgroundSetRatioCli:
    """CLI tests for --background-set-ratio."""

    @pytest.fixture(autouse=True)
    def isolate_queue(self, tmp_path):
        queue_path = tmp_path / "queue"
        queue_path.mkdir()
        _OriginalTaskQueue = TaskQueue

        class _IsolatedTaskQueue(_OriginalTaskQueue):
            def __init__(self, queue_dir_override=None):
                super().__init__(queue_dir=queue_path)

        with patch("conductress.cli.TaskQueue", _IsolatedTaskQueue):
            self.queue_path = queue_path
            yield

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_cli_default_ratio_zero(self):
        """CLI without --background-set-ratio defaults to 0."""
        exit_code = main(
            ["queue", "add-scenario", "--scenario", "eval-storm", "--source", "valkey", "--specifier", "unstable"]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["background_set_ratio"] == 0

    def test_cli_explicit_ratio(self):
        """CLI with --background-set-ratio stores the value."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "eval-storm",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--background-set-ratio",
                "20",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["background_set_ratio"] == 20

    def test_cli_invalid_ratio_rejected(self, capsys):
        """CLI rejects ratio outside 0-100."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "eval-storm",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--background-set-ratio",
                "150",
            ]
        )
        assert exit_code == 1
        assert "background-set-ratio must be 0-100" in capsys.readouterr().err

    def test_cli_negative_ratio_rejected(self, capsys):
        """CLI rejects negative ratio."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "eval-storm",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--background-set-ratio",
                "-5",
            ]
        )
        assert exit_code == 1
        assert "background-set-ratio must be 0-100" in capsys.readouterr().err


class TestBgsaveScenario:
    """Tests for the bgsave scenario (Feature 2)."""

    def test_bgsave_in_choices(self):
        """bgsave is a valid scenario name."""
        assert "bgsave" in SCENARIO_CHOICES
        assert validate_scenario("bgsave") is True

    def test_bgsave_overlay_command(self):
        """bgsave overlay is a sleep-then-fire one-shot command."""
        cmd = build_overlay_command("bgsave", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert "BGSAVE" in cmd
        assert "valkey-cli" in cmd
        assert "sleep" in cmd
        # Fire at ~40% of duration: 30 * 2/5 = 12
        assert "sleep 12" in cmd

    def test_bgsave_delay_calculation(self):
        """bgsave fires at ~40% of duration (min 2s)."""
        # Short duration: floor at 2s
        cmd_short = build_overlay_command("bgsave", "127.0.0.1", 6379, 3, 3_000_000, 512)
        assert "sleep 2" in cmd_short

        # Long duration: 40% of 60 = 24
        cmd_long = build_overlay_command("bgsave", "127.0.0.1", 6379, 60, 3_000_000, 512)
        assert "sleep 24" in cmd_long

    def test_bgsave_uses_correct_host_port(self):
        """bgsave overlay targets the correct server."""
        cmd = build_overlay_command("bgsave", "10.0.0.5", 7777, 30, 3_000_000, 512)
        assert "-h 10.0.0.5" in cmd
        assert "-p 7777" in cmd

    def test_bgsave_is_one_shot(self):
        """bgsave overlay doesn't loop -- it's a single BGSAVE command."""
        cmd = build_overlay_command("bgsave", "127.0.0.1", 6379, 30, 3_000_000, 512)
        # Should NOT have a while/until loop pattern
        assert "while" not in cmd
        assert "end=" not in cmd
        # Just sleep + BGSAVE
        assert cmd.count("BGSAVE") == 1

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_bgsave_task_data_accepted(self):
        """ScenarioTaskData accepts bgsave scenario."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="bgsave",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
        )
        assert task.scenario == "bgsave"

    def test_bgsave_serialization_round_trip(self, tmp_path):
        """bgsave scenario survives serialization."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="fork cost test",
            requirements={},
            scenario="bgsave",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=60,
            background_set_ratio=10,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert isinstance(loaded, ScenarioTaskData)
        assert loaded.scenario == "bgsave"
        assert loaded.background_set_ratio == 10
        assert loaded.note == "fork cost test"


class TestBgsaveCliScenario:
    """CLI tests for bgsave scenario."""

    @pytest.fixture(autouse=True)
    def isolate_queue(self, tmp_path):
        queue_path = tmp_path / "queue"
        queue_path.mkdir()
        _OriginalTaskQueue = TaskQueue

        class _IsolatedTaskQueue(_OriginalTaskQueue):
            def __init__(self, queue_dir_override=None):
                super().__init__(queue_dir=queue_path)

        with patch("conductress.cli.TaskQueue", _IsolatedTaskQueue):
            self.queue_path = queue_path
            yield

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_cli_add_bgsave(self):
        """CLI accepts bgsave scenario."""
        exit_code = main(
            ["queue", "add-scenario", "--scenario", "bgsave", "--source", "valkey", "--specifier", "unstable"]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        assert len(tasks) == 1
        data = json.loads(tasks[0].read_text())
        assert data["scenario"] == "bgsave"
        assert data["task_type"] == "ScenarioTaskData"

    def test_cli_bgsave_with_ratio(self):
        """bgsave + background-set-ratio together."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "bgsave",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--background-set-ratio",
                "50",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["scenario"] == "bgsave"
        assert data["background_set_ratio"] == 50


class TestLargeValueReaderScenario:
    """Tests for the large-value-reader scenario overlay."""

    def test_large_value_reader_in_choices(self):
        """large-value-reader is a valid scenario name."""
        assert "large-value-reader" in SCENARIO_CHOICES
        assert validate_scenario("large-value-reader") is True

    def test_overlay_command_uses_memtier(self):
        """large-value-reader overlay uses memtier (SAME tool as prefill for key
        format agreement; valkey-benchmark zero-pads __rand_int__ keys)."""
        cmd = build_overlay_command("large-value-reader", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert "memtier_benchmark" in cmd
        assert "valkey-benchmark" not in cmd
        assert "--ratio 0:1" in cmd  # GET-only
        assert "--key-prefix lvr:" in cmd

    def test_overlay_command_uses_dedicated_keyspace(self):
        """Overlay targets the dedicated lvr: keyspace with the SAME key range
        as the prefill."""
        cmd = build_overlay_command("large-value-reader", "10.0.0.5", 7777, 60, 3_000_000, 512)
        assert "--key-minimum 1" in cmd
        assert f"--key-maximum {LARGE_VALUE_READER_KEYSPACE}" in cmd

    def test_overlay_command_limited_connections(self):
        """Overlay uses restricted thread/client count (memtier convention)."""
        cmd = build_overlay_command("large-value-reader", "127.0.0.1", 6379, 30, 3_000_000, 512)
        assert f"--threads {OVERLAY_THREADS}" in cmd
        assert f"--clients {OVERLAY_CLIENTS}" in cmd

    def test_overlay_command_correct_host_port(self):
        """Overlay targets the specified host and port (memtier flags)."""
        cmd = build_overlay_command("large-value-reader", "192.168.1.10", 6380, 30, 3_000_000, 512)
        assert "--server 192.168.1.10" in cmd
        assert "--port 6380" in cmd

    def test_overlay_command_request_count_scales_with_duration(self):
        """Request count is proportional to duration (50K/s target)."""
        cmd_short = build_overlay_command("large-value-reader", "127.0.0.1", 6379, 10, 3_000_000, 512)
        cmd_long = build_overlay_command("large-value-reader", "127.0.0.1", 6379, 60, 3_000_000, 512)
        assert "--requests 500000" in cmd_short
        assert "--requests 3000000" in cmd_long

    def test_overlay_value_size_param_accepted(self):
        """overlay_value_size affects the prefill, not the GET command."""
        cmd = build_overlay_command(
            "large-value-reader", "127.0.0.1", 6379, 30, 3_000_000, 512, overlay_value_size=20480
        )
        assert "memtier_benchmark" in cmd

    def test_key_format_agreement_regression(self):
        """REGRESSION (review, Aug 21): prefill (memtier SET) and overlay (GET)
        must use the same tool and key contract. valkey-benchmark zero-pads
        __rand_int__ keys (lvr:000000000042) while memtier writes lvr:42 —
        mixing tools makes every overlay GET a miss and the neighbor weighs
        nothing."""
        cmd = build_overlay_command("large-value-reader", "10.0.0.1", 6379, 30, 3_000_000, 512)
        assert "memtier_benchmark" in cmd
        assert "valkey-benchmark" not in cmd
        assert "--key-prefix lvr:" in cmd
        assert "--key-minimum 1" in cmd
        assert f"--key-maximum {LARGE_VALUE_READER_KEYSPACE}" in cmd
        assert "--ratio 0:1" in cmd

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_task_data_accepted(self):
        """ScenarioTaskData accepts large-value-reader scenario."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="large-value-reader",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
        )
        assert task.scenario == "large-value-reader"
        assert task.overlay_value_size == 0  # default

    def test_task_data_with_overlay_value_size(self):
        """ScenarioTaskData accepts custom overlay_value_size."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="large-value-reader",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
            overlay_value_size=20480,
        )
        assert task.overlay_value_size == 20480

    def test_task_data_negative_overlay_value_size_rejected(self):
        """Negative overlay_value_size raises ValueError."""
        with pytest.raises(ValueError, match="overlay_value_size must be >= 0"):
            ScenarioTaskData(
                source="valkey",
                specifier="unstable",
                make_args="",
                replicas=0,
                note="",
                requirements={},
                scenario="large-value-reader",
                val_size=512,
                io_threads=9,
                pipelining=10,
                duration=30,
                overlay_value_size=-1,
            )

    def test_short_description_shows_overlay_size(self):
        """short_description includes overlay value size for large-value-reader."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="large-value-reader",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
            overlay_value_size=20480,
        )
        desc = task.short_description()
        assert "large-value-reader" in desc
        assert "ovl=" in desc

    def test_short_description_default_overlay_size(self):
        """short_description uses default 10KB when overlay_value_size=0."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="large-value-reader",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
        )
        desc = task.short_description()
        assert "ovl=" in desc

    def test_serialization_round_trip(self, tmp_path):
        """large-value-reader scenario with overlay_value_size survives save/load."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="w13 overlay test",
            requirements={},
            scenario="large-value-reader",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
            overlay_value_size=51200,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert isinstance(loaded, ScenarioTaskData)
        assert loaded.scenario == "large-value-reader"
        assert loaded.overlay_value_size == 51200
        assert loaded.note == "w13 overlay test"

    def test_serialization_round_trip_default_overlay_size(self, tmp_path):
        """Default overlay_value_size=0 survives round trip (backward compat)."""
        task = ScenarioTaskData(
            source="valkey",
            specifier="unstable",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            scenario="large-value-reader",
            val_size=512,
            io_threads=9,
            pipelining=10,
            duration=30,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert loaded.overlay_value_size == 0


class TestLargeValueReaderCli:
    """CLI tests for large-value-reader scenario."""

    @pytest.fixture(autouse=True)
    def isolate_queue(self, tmp_path):
        queue_path = tmp_path / "queue"
        queue_path.mkdir()
        _OriginalTaskQueue = TaskQueue

        class _IsolatedTaskQueue(_OriginalTaskQueue):
            def __init__(self, queue_dir_override=None):
                super().__init__(queue_dir=queue_path)

        with patch("conductress.cli.TaskQueue", _IsolatedTaskQueue):
            self.queue_path = queue_path
            yield

    @pytest.fixture(autouse=True)
    def patch_sources(self):
        with (
            patch.object(config, "REPO_NAMES", ["valkey", "testrepo"]),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "testrepo"]),
            patch.object(config, "MANUALLY_UPLOADED", "manually_uploaded"),
            patch("conductress.task_queue.config.MANUALLY_UPLOADED", "manually_uploaded"),
        ):
            yield

    def test_cli_add_large_value_reader(self):
        """CLI accepts large-value-reader scenario."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "large-value-reader",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        assert len(tasks) == 1
        data = json.loads(tasks[0].read_text())
        assert data["scenario"] == "large-value-reader"
        assert data["task_type"] == "ScenarioTaskData"
        assert data["overlay_value_size"] == 0  # default

    def test_cli_with_overlay_value_size(self):
        """CLI passes --overlay-value-size to task data."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "large-value-reader",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--overlay-value-size",
                "20480",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["overlay_value_size"] == 20480

    def test_cli_overlay_value_size_ignored_for_other_scenarios(self):
        """--overlay-value-size is accepted but stored regardless of scenario."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "eval-storm",
                "--source",
                "valkey",
                "--specifier",
                "unstable",
                "--overlay-value-size",
                "5000",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["overlay_value_size"] == 5000
        assert data["scenario"] == "eval-storm"

    def test_cli_with_all_options(self):
        """CLI accepts large-value-reader with all scenario options."""
        exit_code = main(
            [
                "queue",
                "add-scenario",
                "--scenario",
                "large-value-reader",
                "--source",
                "valkey",
                "--specifier",
                "abc123",
                "--io-threads",
                "7",
                "--pipelining",
                "50",
                "--duration",
                "2m",
                "--repetitions",
                "5",
                "--overlay-value-size",
                "102400",
                "--background-set-ratio",
                "10",
                "--note",
                "W13 heavyweight neighbor test",
            ]
        )
        assert exit_code == 0
        tasks = list(self.queue_path.glob("task_*.json"))
        data = json.loads(tasks[0].read_text())
        assert data["scenario"] == "large-value-reader"
        assert data["overlay_value_size"] == 102400
        assert data["io_threads"] == 7
        assert data["pipelining"] == 50
        assert data["duration"] == 120
        assert data["repetitions"] == 5
        assert data["background_set_ratio"] == 10
        assert data["note"] == "W13 heavyweight neighbor test"
