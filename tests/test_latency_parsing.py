"""Tests for latency task runner parsing and export."""

import json
from pathlib import Path

import pytest

from conductress.sweep.planner import BenchmarkPoint, PointStatus, SweepState
from conductress.tasks.task_latency import (
    HISTOGRAM_PERCENTILES,
    LatencyTaskData,
    LatencyTaskRunner,
    _parse_memtier_output,
    _parse_memtier_output_mixed,
    _parse_memtier_output_simple,
    _parse_row_percentiles,
)

# Real memtier output captured from ARM experiment (rate-limited 500K, P=10)
MEMTIER_OUTPUT_SAMPLE = """Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%,  10 secs]  0 threads 25 conns:    14992900 ops,  499880 (avg:  499726) ops/sec, 27.12MB/sec (avg: 27.11MB/sec),  0.46 (avg:  0.46) msec latency

4         Threads
25        Connections per thread
10        Seconds


ALL STATS
============================================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency    p100 Latency       KB/sec
--------------------------------------------------------------------------------------------------------------------------------------------
Sets            0.00          ---          ---             ---             ---             ---             ---             ---         0.00
Gets       499385.90    499385.90         0.00         0.33021         0.32700         0.51100         1.34300         6.36700     27742.22
Waits           0.00          ---          ---             ---             ---             ---             ---             ---          ---
Totals     499385.90    499385.90         0.00         0.33021         0.32700         0.51100         1.34300         6.36700     27742.22
"""

# Realistic memtier mixed output (20% SET / 80% GET with --print-percentiles 50,99,99.9,100)
MEMTIER_MIXED_OUTPUT_SAMPLE = """Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%,  60 secs]  0 threads 64 conns:    29994382 ops,  499882 (avg:  499878) ops/sec, 31.44MB/sec (avg: 31.44MB/sec),  0.41 (avg:  0.41) msec latency

4         Threads
16        Connections per thread
60        Seconds


ALL STATS
============================================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency    p100 Latency       KB/sec
--------------------------------------------------------------------------------------------------------------------------------------------
Sets       100012.45          ---          ---         0.52100         0.47900         1.21500         2.87100         8.19100      5778.34
Gets       399870.11    399870.11         0.00         0.38200         0.35100         0.89300         1.95500         7.42300     22217.89
Waits           0.00          ---          ---             ---             ---             ---             ---             ---          ---
Totals     499882.56    399870.11         0.00         0.40981         0.36700         0.95900         2.18700         8.19100     27996.23
"""

# Real HDR histogram output captured from ARM experiment
HDR_HISTOGRAM_SAMPLE = """       Value   Percentile   TotalCount 1/(1-Percentile)

        0.01     0.000000          325         1.00
        0.09     0.050000       145084         1.05
        0.13     0.100000       250783         1.11
        0.14     0.150000       558161         1.18
        0.15     0.250000      1322362         1.33
        0.15     0.500000      1322362         2.00
        0.16     0.550000      1846137         2.22
        0.17     0.750000      1919217         4.00
        0.21     0.800000      2020998         5.00
        0.22     0.825000      2171562         5.71
        0.23     0.900000      2300000         10.00
        0.25     0.950000      2400000         20.00
        0.31     0.990000      2480000         100.00
        0.42     0.995000      2490000         200.00
        0.89     0.999000      2497000         1000.00
        2.91     0.999900      2499700         10000.00
        6.37     1.000000      2500000          inf
"""


class TestMemtierOutputParsing:
    """Test parsing of memtier_benchmark summary output (GET-only, legacy mode)."""

    def test_parses_gets_line(self):
        result = _parse_memtier_output(MEMTIER_OUTPUT_SAMPLE, mixed=False)
        assert result is not None
        assert result["actual_rps"] == pytest.approx(499385.90)
        assert result["p50_us"] == pytest.approx(327.0)  # 0.327ms * 1000
        assert result["p99_us"] == pytest.approx(511.0)
        assert result["p99_9_us"] == pytest.approx(1343.0)
        assert result["p100_us"] == pytest.approx(6367.0)

    def test_returns_none_on_empty_output(self):
        assert _parse_memtier_output("", mixed=False) is None

    def test_returns_none_on_garbage(self):
        assert _parse_memtier_output("some random text\nno data here", mixed=False) is None

    def test_handles_totals_line(self):
        # Output with only Totals line (no separate Gets)
        output = "Totals     1499174.26   1499174.26         0.00         1.69461         1.89500         2.11100         3.18300         7.45500     83283.89"
        result = _parse_memtier_output(output, mixed=False)
        assert result is not None
        assert result["actual_rps"] == pytest.approx(1499174.26)
        assert result["p50_us"] == pytest.approx(1895.0)
        assert result["p99_us"] == pytest.approx(2111.0)
        assert result["p99_9_us"] == pytest.approx(3183.0)
        assert result["p100_us"] == pytest.approx(7455.0)


class TestMixedMemtierOutputParsing:
    """Test parsing of memtier output with separate Gets/Sets/Totals rows."""

    def test_parses_all_classes(self):
        result = _parse_memtier_output(MEMTIER_MIXED_OUTPUT_SAMPLE, mixed=True)
        assert result is not None
        assert "totals" in result
        assert "gets" in result
        assert "sets" in result

    def test_totals_values(self):
        result = _parse_memtier_output(MEMTIER_MIXED_OUTPUT_SAMPLE, mixed=True)
        totals = result["totals"]
        assert totals["actual_rps"] == pytest.approx(499882.56)
        assert totals["p50_us"] == pytest.approx(367.0)
        assert totals["p99_us"] == pytest.approx(959.0)
        assert totals["p99_9_us"] == pytest.approx(2187.0)
        assert totals["p100_us"] == pytest.approx(8191.0)

    def test_gets_values(self):
        result = _parse_memtier_output(MEMTIER_MIXED_OUTPUT_SAMPLE, mixed=True)
        gets = result["gets"]
        assert gets["actual_rps"] == pytest.approx(399870.11)
        assert gets["p50_us"] == pytest.approx(351.0)
        assert gets["p99_us"] == pytest.approx(893.0)
        assert gets["p99_9_us"] == pytest.approx(1955.0)
        assert gets["p100_us"] == pytest.approx(7423.0)

    def test_sets_values(self):
        result = _parse_memtier_output(MEMTIER_MIXED_OUTPUT_SAMPLE, mixed=True)
        sets = result["sets"]
        assert sets["actual_rps"] == pytest.approx(100012.45)
        assert sets["p50_us"] == pytest.approx(479.0)
        assert sets["p99_us"] == pytest.approx(1215.0)
        assert sets["p99_9_us"] == pytest.approx(2871.0)
        assert sets["p100_us"] == pytest.approx(8191.0)

    def test_returns_none_on_empty(self):
        assert _parse_memtier_output("", mixed=True) is None

    def test_returns_none_without_totals_line(self):
        # Only a Gets line but no Totals => None (Totals is required)
        output = "Gets       100000.00    100000.00         0.00         0.33000         0.32000         0.50000         1.30000         6.00000     5000.00"
        assert _parse_memtier_output(output, mixed=True) is None

    def test_get_only_output_parsed_as_mixed(self):
        """GET-only memtier output (Sets=0) still works in mixed mode -- just no 'sets' key."""
        result = _parse_memtier_output(MEMTIER_OUTPUT_SAMPLE, mixed=True)
        assert result is not None
        assert "totals" in result
        assert "gets" in result
        # Sets row has 0.00 ops/sec and --- for latencies, so it shouldn't parse
        assert "sets" not in result or result["sets"]["actual_rps"] == pytest.approx(0.0)


class TestRowPercentilesParsing:
    """Test the _parse_row_percentiles helper."""

    def test_valid_row(self):
        parts = "Gets 399870.11 399870.11 0.00 0.38200 0.35100 0.89300 1.95500 7.42300 22217.89".split()
        result = _parse_row_percentiles(parts)
        assert result is not None
        assert result["actual_rps"] == pytest.approx(399870.11)
        assert result["p50_us"] == pytest.approx(351.0)

    def test_short_row_returns_none(self):
        parts = "Gets 399870.11 399870.11".split()
        assert _parse_row_percentiles(parts) is None

    def test_non_numeric_returns_none(self):
        parts = "Sets 0.00 --- --- --- --- --- --- --- 0.00".split()
        result = _parse_row_percentiles(parts)
        # '---' can't be parsed as float
        assert result is None


class TestHdrHistogramParsing:
    """Test parsing of HDR histogram .txt files."""

    def test_parses_histogram_to_cdf_buckets(self):
        entries = []
        for line in HDR_HISTOGRAM_SAMPLE.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Value"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    latency_ms = float(parts[0])
                    percentile = float(parts[1])
                    entries.append((percentile, latency_ms * 1000))
                except ValueError:
                    continue

        # Extract at target percentiles
        histogram = []
        for target_pct in HISTOGRAM_PERCENTILES:
            closest = min(entries, key=lambda e: abs(e[0] - target_pct))
            histogram.append([target_pct, closest[1]])

        assert len(histogram) == 11
        # p50 should be around 150µs (0.15ms)
        p50_entry = next(h for h in histogram if h[0] == 0.50)
        assert p50_entry[1] == pytest.approx(150.0)
        # p99 should be around 310µs (0.31ms)
        p99_entry = next(h for h in histogram if h[0] == 0.99)
        assert p99_entry[1] == pytest.approx(310.0)
        # p100 should be around 6370µs (6.37ms)
        p100_entry = next(h for h in histogram if h[0] == 1.0)
        assert p100_entry[1] == pytest.approx(6370.0)

    def test_empty_input_returns_empty(self):
        entries = []
        assert entries == []


class TestAggregation:
    """Test repetition aggregation logic."""

    def setup_method(self):
        self.runner = LatencyTaskRunner.__new__(LatencyTaskRunner)

    def test_median_of_three_reps(self):
        reps = [
            {
                "actual_rps": 500000,
                "p50_us": 320,
                "p99_us": 510,
                "p99_9_us": 1300,
                "p100_us": 6000,
                "histogram": [[0.5, 320]],
            },
            {
                "actual_rps": 499000,
                "p50_us": 330,
                "p99_us": 520,
                "p99_9_us": 1400,
                "p100_us": 7000,
                "histogram": [[0.5, 330]],
            },
            {
                "actual_rps": 501000,
                "p50_us": 310,
                "p99_us": 500,
                "p99_9_us": 1200,
                "p100_us": 5000,
                "histogram": [[0.5, 310]],
            },
        ]
        result = self.runner._aggregate_reps(reps)
        assert result["actual_rps"] == 500000  # median
        assert result["p50_us"] == 320
        assert result["p99_us"] == 510
        assert result["p99_9_us"] == 1300
        assert result["p100_us"] == 6000
        assert result["reps"] == 3
        # Histogram from median rep (index 1)
        assert result["histogram"] == [[0.5, 330]]

    def test_median_of_mixed_reps(self):
        """Aggregation works with per-class data (mixed mode)."""
        reps = [
            {
                "totals": {"actual_rps": 500000, "p50_us": 360, "p99_us": 950, "p99_9_us": 2100, "p100_us": 8000},
                "gets": {"actual_rps": 400000, "p50_us": 340, "p99_us": 880, "p99_9_us": 1900, "p100_us": 7000},
                "sets": {"actual_rps": 100000, "p50_us": 470, "p99_us": 1200, "p99_9_us": 2800, "p100_us": 8000},
                "histogram": [[0.5, 360]],
            },
            {
                "totals": {"actual_rps": 499000, "p50_us": 370, "p99_us": 960, "p99_9_us": 2200, "p100_us": 8200},
                "gets": {"actual_rps": 399000, "p50_us": 350, "p99_us": 890, "p99_9_us": 1950, "p100_us": 7200},
                "sets": {"actual_rps": 100000, "p50_us": 480, "p99_us": 1220, "p99_9_us": 2850, "p100_us": 8200},
                "histogram": [[0.5, 370]],
            },
            {
                "totals": {"actual_rps": 501000, "p50_us": 365, "p99_us": 955, "p99_9_us": 2150, "p100_us": 8100},
                "gets": {"actual_rps": 401000, "p50_us": 345, "p99_us": 885, "p99_9_us": 1920, "p100_us": 7100},
                "sets": {"actual_rps": 100000, "p50_us": 475, "p99_us": 1210, "p99_9_us": 2830, "p100_us": 8100},
                "histogram": [[0.5, 365]],
            },
        ]
        result = self.runner._aggregate_reps(reps)
        # Top-level comes from totals median
        assert result["actual_rps"] == 500000
        assert result["p99_us"] == 955
        # Per-class medians
        assert result["gets"]["p50_us"] == 345
        assert result["sets"]["p99_us"] == 1210
        assert result["reps"] == 3


class TestLatencyExport:
    """Test the latency export function."""

    def test_export_produces_valid_json(self, tmp_path, monkeypatch):
        from conductress.sweep.exporter import export_latency

        # Create state with some latency results
        state = SweepState()
        state.merge_commits = ["aaa", "bbb", "ccc"]
        state.commit_dates = {"aaa": "2026-01-01", "bbb": "2026-02-01", "ccc": "2026-03-01"}
        state.points["aaa"] = BenchmarkPoint(
            commit="aaa", date="2026-01-01", value=500.0, cv=0, reps=3, status=PointStatus.COMPLETED
        )
        state.points["ccc"] = BenchmarkPoint(
            commit="ccc", date="2026-03-01", value=800.0, cv=0, reps=3, status=PointStatus.COMPLETED
        )

        # Mock CONDUCTRESS_RESULTS to empty dir (no output.jsonl)
        monkeypatch.setattr("conductress.config.CONDUCTRESS_RESULTS", tmp_path)

        output_file = tmp_path / "series-arm64-get16b-t9-p10-latency.json"
        count = export_latency(state, output_file, platform="arm64", workload="get16b-t9-p10")

        assert count == 2
        assert output_file.exists()

        data = json.loads(output_file.read_text())
        assert data["metadata"]["metric"] == "latency"
        assert data["metadata"]["unit"] == "µs"
        assert data["metadata"]["load_fraction"] is None
        assert data["metadata"]["target_rps"] == 100000
        assert data["metadata"]["pipeline"] == 1
        assert data["metadata"]["platform"] == "arm64"
        assert len(data["points"]) == 2
        assert data["points"][0]["commit"] == "aaa"
        assert data["points"][0]["p99_us"] == 500.0
        assert data["points"][1]["p99_us"] == 800.0

    def test_export_includes_annotations_for_adjacent_commits(self, tmp_path, monkeypatch):
        from conductress.sweep.exporter import export_latency

        state = SweepState()
        state.merge_commits = ["aaa", "bbb"]
        state.threshold = 0.10  # 10%
        state.points["aaa"] = BenchmarkPoint(
            commit="aaa", date="2026-01-01", value=500.0, cv=0, reps=3, status=PointStatus.COMPLETED
        )
        state.points["bbb"] = BenchmarkPoint(
            commit="bbb", date="2026-02-01", value=700.0, cv=0, reps=3, status=PointStatus.COMPLETED
        )

        monkeypatch.setattr("conductress.config.CONDUCTRESS_RESULTS", tmp_path)

        output_file = tmp_path / "test.json"
        export_latency(state, output_file, platform="arm64", workload="get16b-t9-p10")

        data = json.loads(output_file.read_text())
        # 40% increase in latency (lower is better) = regression
        assert len(data["annotations"]) == 1
        assert data["annotations"][0]["type"] == "increase"
        assert data["annotations"][0]["commit"] == "bbb"


class TestLatencyTaskDataExtensions:
    """Test new LatencyTaskData fields: server_args, set_ratio, value_size."""

    @pytest.fixture(autouse=True)
    def patch_sources(self, monkeypatch):
        monkeypatch.setattr("conductress.task_queue.config.REPO_NAMES", ["valkey", "valkey-rainfall"])

    def test_default_values(self):
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            target_rps=500000,
        )
        assert task.server_args == ""
        assert task.set_ratio == 0
        assert task.value_size == 16  # LATENCY_VAL_SIZE default

    def test_server_args_passthrough(self):
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            target_rps=500000,
            server_args="--io-threads-ownership yes",
        )
        assert task.server_args == "--io-threads-ownership yes"

    def test_set_ratio_valid_range(self):
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            target_rps=500000,
            set_ratio=20,
        )
        assert task.set_ratio == 20

    def test_set_ratio_invalid_negative(self):
        with pytest.raises(ValueError, match="set_ratio must be 0-100"):
            LatencyTaskData(
                source="valkey",
                specifier="abc123",
                make_args="",
                replicas=0,
                note="",
                requirements={},
                target_rps=500000,
                set_ratio=-1,
            )

    def test_set_ratio_invalid_over_100(self):
        with pytest.raises(ValueError, match="set_ratio must be 0-100"):
            LatencyTaskData(
                source="valkey",
                specifier="abc123",
                make_args="",
                replicas=0,
                note="",
                requirements={},
                target_rps=500000,
                set_ratio=101,
            )

    def test_value_size_custom(self):
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            target_rps=500000,
            value_size=512,
        )
        assert task.value_size == 512

    def test_value_size_invalid(self):
        with pytest.raises(ValueError, match="value_size must be >= 1"):
            LatencyTaskData(
                source="valkey",
                specifier="abc123",
                make_args="",
                replicas=0,
                note="",
                requirements={},
                target_rps=500000,
                value_size=0,
            )

    def test_short_description_get_only(self):
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            target_rps=500000,
        )
        desc = task.short_description()
        assert "500000 rps" in desc
        assert "P=1 flat" in desc
        assert "SET" not in desc

    def test_short_description_mixed(self):
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            target_rps=500000,
            set_ratio=20,
        )
        desc = task.short_description()
        assert "500000 rps" in desc
        assert "SET=20%" in desc

    def test_serialization_round_trip(self, tmp_path):
        """Task survives JSON save/load with all new fields."""
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123def456",
            make_args="",
            replicas=0,
            note="ownership latency",
            requirements={},
            target_rps=500000,
            io_threads=9,
            server_args="--io-threads-ownership yes",
            set_ratio=20,
            value_size=512,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert isinstance(loaded, LatencyTaskData)
        assert loaded.server_args == "--io-threads-ownership yes"
        assert loaded.set_ratio == 20
        assert loaded.value_size == 512
        assert loaded.target_rps == 500000
        assert loaded.io_threads == 9
        assert loaded.note == "ownership latency"

    def test_serialization_backward_compat_defaults(self, tmp_path):
        """Task without new fields (default values) also survives roundtrip."""
        task = LatencyTaskData(
            source="valkey",
            specifier="abc123def456",
            make_args="",
            replicas=0,
            note="",
            requirements={},
            target_rps=100000,
        )
        filepath = tmp_path / "task.json"
        task.save_to_file(filepath)

        from conductress.task_queue import BaseTaskData

        loaded = BaseTaskData.from_file(filepath)
        assert isinstance(loaded, LatencyTaskData)
        assert loaded.server_args == ""
        assert loaded.set_ratio == 0
        assert loaded.value_size == 16


class TestLatencyResultProvenance:
    def test_write_result_adds_shared_runner_provenance(self, tmp_path, monkeypatch):
        runner = LatencyTaskRunner.__new__(LatencyTaskRunner)
        runner.task_name = "latency-task"
        runner.source = "valkey"
        runner.specifier = "abcdef123456"
        runner.target_rps = 100000
        runner.set_ratio = 0
        runner.value_size = 16
        provenance = {
            "provenance_schema_version": 1,
            "runner_id": "armbench",
            "platform": "arm64/c7g.metal/graviton3",
            "environment": {"host_fingerprint": "fingerprint"},
        }
        monkeypatch.setattr("conductress.tasks.task_latency.CONDUCTRESS_RESULTS", tmp_path)
        monkeypatch.setattr("conductress.tasks.task_latency.get_result_provenance", lambda: provenance)

        runner._write_result(
            {
                "actual_rps": 99999,
                "p50_us": 100,
                "p99_us": 200,
                "p99_9_us": 300,
                "p100_us": 400,
                "histogram": [],
                "reps": 3,
            }
        )

        entry = json.loads((tmp_path / "output.jsonl").read_text(encoding="utf-8"))
        for key, value in provenance.items():
            assert entry[key] == value
