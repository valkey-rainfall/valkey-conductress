"""Epoch 3 covers latency and memory, and retires the epoch-1 series it replaces.

Four contracts:

* ``CachecannonTaskData.score_metric``: a rate-limited cell scores p99 in
  microseconds, bounds its adaptive stop on p99, and records the per-rep p99
  series; a throughput cell is unchanged.
* ``CachecannonLatencySweepCoordinatorV3``: the v3 latency series, owning only
  its own cells, bisecting on p99 (lower is better) and exporting the
  percentile set with ``tool: cachecannon``.
* Retirement: an epoch-1 series listed in ``SWEEP_V1_RETIRED_SERIES`` keeps
  exporting but never queues, and the scheduler never consults it.
* Generator-independent series: memory schedules under one epoch and is
  published under every epoch it belongs to, from one state.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress import config
from conductress.config import (
    SWEEP_GENERATOR_INDEPENDENT_EPOCHS,
    SWEEP_V1_RETIRED_SERIES,
    SWEEP_V3_CLIENT_THREADS,
    SWEEP_V3_CONNECTIONS,
    SWEEP_V3_KEYSPACE,
    SWEEP_V3_LATENCY_MAX_REPS,
    SWEEP_V3_LATENCY_RATE,
    SWEEP_V3_LATENCY_REPETITIONS,
    SWEEP_V3_LATENCY_TARGET_CV,
)
from conductress.sweep.planner import SweepPlanner, SweepTask, TaskPriority
from conductress.tasks.task_cachecannon import SCORE_METRICS, CachecannonTaskData, score_from_parsed
from conductress.topology import TopologySpec


@pytest.fixture(autouse=True)
def _ensure_valid_source(monkeypatch):
    if "valkey" not in config.REPO_NAMES:
        monkeypatch.setattr(config, "REPO_NAMES", config.REPO_NAMES + ["valkey"])


def _sweep_task(commit: str = "a" * 40) -> SweepTask:
    return SweepTask(commit=commit, reason="unit test", date="2026-01-01", priority=1)


def _v3_latency(tmp_path: Path, **overrides):
    from conductress.sweep.coordinator_v3 import CachecannonLatencySweepCoordinatorV3

    with patch("conductress.sweep.coordinator_v3._ensure_v3_state_dir"):
        with patch("conductress.sweep.coordinator_v3.V3_STATE_DIR", tmp_path):
            return CachecannonLatencySweepCoordinatorV3(tmp_path, **overrides)


def _task(**overrides) -> CachecannonTaskData:
    fields = dict(
        source="valkey",
        specifier="abc123",
        make_args="",
        topology=TopologySpec.standalone(),
        note="",
        requirements={},
    )
    fields.update(overrides)
    return CachecannonTaskData(**fields)


def _parsed(throughput: float, p99_ms: float) -> dict:
    return {
        "throughput_rps": throughput,
        "error_pct": 0.0,
        "hit_rate": {"percent": 100.0, "hits": 1.0, "misses": 0.0},
        "latency": {
            "command": "GET",
            "p50_ms": p99_ms / 2,
            "p90_ms": p99_ms * 0.8,
            "p99_ms": p99_ms,
            "p999_ms": p99_ms * 2,
            "p9999_ms": p99_ms * 4,
            "max_ms": p99_ms * 8,
            "count": 1000,
        },
    }


# ---------------------------------------------------------------------------
# score_metric
# ---------------------------------------------------------------------------


class TestScoreMetric:
    def test_default_is_throughput_and_serializes(self, tmp_path):
        task = _task()
        assert task.score_metric == "throughput"
        path = tmp_path / "t.json"
        task.save_to_file(path)
        assert json.loads(path.read_text())["score_metric"] == "throughput"

    def test_documents_without_the_field_load_as_throughput(self, tmp_path):
        """Task files written before the field existed keep meaning throughput."""
        from conductress.task_queue import BaseTaskData

        task = _task()
        path = tmp_path / "t.json"
        task.save_to_file(path)
        doc = json.loads(path.read_text())
        del doc["score_metric"]
        path.write_text(json.dumps(doc))
        assert BaseTaskData.from_file(path).score_metric == "throughput"

    def test_unknown_metric_rejected(self):
        with pytest.raises(ValueError, match="score_metric"):
            _task(score_metric="p50")

    def test_registry_is_throughput_and_p99(self):
        assert SCORE_METRICS == {"throughput", "p99"}

    def test_p99_score_is_microseconds(self):
        assert score_from_parsed(_parsed(100_000.0, p99_ms=0.412), "p99") == pytest.approx(412.0)

    def test_throughput_score_is_ops_per_second(self):
        assert score_from_parsed(_parsed(3_104_882.0, p99_ms=1.0), "throughput") == 3_104_882.0

    def test_p99_without_latency_block_is_an_error_not_zero(self):
        parsed = _parsed(1.0, 1.0)
        parsed["latency"] = None
        with pytest.raises(ValueError, match="no latency block"):
            score_from_parsed(parsed, "p99")

    def test_runner_rejects_unknown_metric(self):
        from conductress.tasks.task_cachecannon import CachecannonTaskRunner

        with pytest.raises(ValueError, match="score_metric"):
            CachecannonTaskRunner(
                task_id="t",
                server_infos=[],
                source="valkey",
                specifier="abc",
                make_args="",
                io_threads=7,
                test="get",
                val_size=16,
                pipelining=1,
                connections=400,
                threads=8,
                warmup=10,
                duration=30,
                repetitions=5,
                keyspace_count=3_000_000,
                cachecannon_binary="cc",
                server_args="",
                server_cpu_override="",
                benchmark_cpu_override="",
                note="",
                score_metric="mean",
            )


class TestRecordedResultForP99:
    """``_record_result`` scores the p99 series and keeps throughput alongside."""

    @pytest.fixture
    def runner(self, tmp_path):
        task = _task(rate_limit=100_000, pipelining=1, score_metric="p99", repetitions=3)
        runner = task.prepare_task_runner([config.ServerInfo(ip="127.0.0.1")])
        runner.file_protocol = MagicMock()
        runner.management_cpus = []
        return runner

    @pytest.mark.asyncio
    async def test_score_cv_and_reps_describe_p99(self, runner):
        server = MagicMock()

        async def _lscpu(_cmd):
            return ("lscpu", "")

        server.run_host_command = _lscpu
        server.server_cpus = "0-3"
        results = [_parsed(100_000.0, 0.400), _parsed(100_000.0, 0.420), _parsed(100_000.0, 0.410)]
        per_run_rps = [r["throughput_rps"] for r in results]
        per_run_scores = [score_from_parsed(r, "p99") for r in results]
        await runner._record_result(server, per_run_rps, results, "toml", None, per_run_scores)
        recorded = runner.file_protocol.write_results.call_args.args[0]
        assert recorded.score == pytest.approx(410.0)
        assert recorded.reps == 3
        assert recorded.cv == pytest.approx(2.439, abs=0.01)
        data = recorded.data
        assert data["score_metric"] == "p99"
        assert data["per_run_p99_us"] == pytest.approx([400.0, 420.0, 410.0])
        assert data["mean_p99_us"] == pytest.approx(410.0)
        # Throughput is still recorded: it is how one checks the rate was held.
        assert data["per_run_rps"] == per_run_rps
        assert data["mean_rps"] == pytest.approx(100_000.0)
        assert data["rate_limit"] == 100_000
        assert data["latency"]["p99_ms"] == pytest.approx(0.410)

    @pytest.mark.asyncio
    async def test_throughput_record_is_unchanged(self, tmp_path):
        task = _task(repetitions=2)
        runner = task.prepare_task_runner([config.ServerInfo(ip="127.0.0.1")])
        runner.file_protocol = MagicMock()
        runner.management_cpus = []
        server = MagicMock()

        async def _lscpu(_cmd):
            return ("lscpu", "")

        server.run_host_command = _lscpu
        server.server_cpus = "0-3"
        results = [_parsed(3_000_000.0, 0.5), _parsed(3_010_000.0, 0.5)]
        rps = [r["throughput_rps"] for r in results]
        await runner._record_result(server, rps, results, "toml", None, rps)
        recorded = runner.file_protocol.write_results.call_args.args[0]
        assert recorded.score == pytest.approx(3_005_000.0)
        assert recorded.data["score_metric"] == "throughput"
        assert "per_run_p99_us" not in recorded.data


# ---------------------------------------------------------------------------
# The v3 latency series
# ---------------------------------------------------------------------------


class TestLatencySweepCoordinatorV3:
    def test_identity(self, tmp_path):
        coord = _v3_latency(tmp_path)
        assert coord.epoch_id == "v3"
        assert coord.metric_id == "latency"
        assert coord.metric_unit == "µs"
        assert coord.lower_is_better is True
        assert coord.workload_id == "get-k16-v16-t7-p1-r100k"
        assert coord.retired is False

    def test_cell_is_the_v3_shape_at_a_fixed_rate_scored_on_p99(self, tmp_path):
        coord = _v3_latency(tmp_path)
        task = coord._create_task(_sweep_task())
        assert isinstance(task, CachecannonTaskData)
        assert task.rate_limit == SWEEP_V3_LATENCY_RATE == 100_000
        assert task.pipelining == 1
        assert task.score_metric == "p99"
        assert task.test == "get" and task.set_ratio == 0
        assert task.connections == SWEEP_V3_CONNECTIONS
        assert task.threads == SWEEP_V3_CLIENT_THREADS
        assert task.keyspace_count == SWEEP_V3_KEYSPACE
        assert task.repetitions == SWEEP_V3_LATENCY_REPETITIONS
        assert task.max_reps == SWEEP_V3_LATENCY_MAX_REPS
        assert task.target_cv == SWEEP_V3_LATENCY_TARGET_CV
        assert "[cachecannon-sweep-v3:valkey/get-k16-v16-t7-p1-r100k]" in task.note

    def test_needs_a_positive_rate(self, tmp_path):
        with pytest.raises(ValueError, match="fixed request rate"):
            _v3_latency(tmp_path, rate_limit=0)

    def test_owns_only_its_own_cells(self, tmp_path):
        coord = _v3_latency(tmp_path)
        own = coord._create_task(_sweep_task())
        own.sweep_commit = "a" * 40
        assert coord._is_my_task(own)

        manual = coord._create_task(_sweep_task())
        assert not coord._is_my_task(manual), "no sweep_commit: a manual cell"

        for field, value in (("rate_limit", 50_000), ("score_metric", "throughput"), ("pipelining", 10)):
            other = coord._create_task(_sweep_task())
            other.sweep_commit = "a" * 40
            setattr(other, field, value)
            assert not coord._is_my_task(other), field

    def test_result_is_p99_with_cv_over_the_p99_series(self, tmp_path):
        coord = _v3_latency(tmp_path)
        task = coord._create_task(_sweep_task())
        entry = {
            "score": 410.0,
            "data": {
                "per_run_rps": [100_000.0, 100_000.0, 100_000.0],
                "per_run_p99_us": [400.0, 420.0, 410.0],
                "score_metric": "p99",
            },
        }
        with patch.object(coord, "_find_task_entry", return_value=entry):
            value, cv, reps = coord._extract_result(task)
        assert value == 410.0
        assert reps == 3
        assert cv == pytest.approx(2.439, abs=0.01), "CV must come from the p99 series, not throughput"

    def test_latency_detail_maps_cachecannon_percentiles_to_microseconds(self):
        from conductress.sweep.coordinator_v3 import CachecannonLatencySweepCoordinatorV3

        entry = {
            "data": {
                "latency": {"p50_ms": 0.2, "p99_ms": 0.41, "p999_ms": 0.9, "max_ms": 3.2},
                "rate_limit": 100_000,
                "mean_rps": 99_998.5,
            }
        }
        detail = CachecannonLatencySweepCoordinatorV3.latency_data_from_entry(entry)
        assert detail == {
            "p50_us": pytest.approx(200.0),
            "p99_us": pytest.approx(410.0),
            "p99_9_us": pytest.approx(900.0),
            "p100_us": pytest.approx(3200.0),
            "target_rps": 100_000,
            "actual_rps": 99_998.5,
        }
        assert "histogram" not in detail

    def test_completion_records_p99_and_keeps_the_detail_for_export(self, tmp_path):
        coord = _v3_latency(tmp_path)
        task = coord._create_task(_sweep_task())
        task.sweep_commit = "a" * 40
        coord.state.commit_dates["a" * 40] = "2026-01-01"
        entry = {
            "score": 410.0,
            "data": {
                "per_run_p99_us": [400.0, 420.0, 410.0],
                "latency": {"p50_ms": 0.2, "p99_ms": 0.41, "p999_ms": 0.9, "max_ms": 3.2},
                "rate_limit": 100_000,
                "mean_rps": 99_998.5,
            },
        }
        with patch.object(coord, "_find_task_entry", return_value=entry), patch.object(coord.state, "save"):
            with patch("conductress.sweep.coordinator.get_head", side_effect=Exception("no repo")):
                coord.on_task_completed(task)
        point = coord.state.points["a" * 40]
        assert point.value == 410.0
        assert point.latency_data["p99_9_us"] == pytest.approx(900.0)

    def test_completion_lifts_perf_counters_at_the_achieved_rate(self, tmp_path):
        """A latency cell collects the same counters as a throughput cell; the
        point must carry them so the per-request series can be published, and
        the rate they are normalised by is the achieved request rate, not p99."""
        coord = _v3_latency(tmp_path)
        task = coord._create_task(_sweep_task())
        task.sweep_commit = "a" * 40
        coord.state.commit_dates["a" * 40] = "2026-01-01"
        entry = {
            "score": 410.0,
            "data": {
                "per_run_p99_us": [400.0, 420.0, 410.0],
                "latency": {"p50_ms": 0.2, "p99_ms": 0.41, "p999_ms": 0.9, "max_ms": 3.2},
                "rate_limit": 100_000,
                "mean_rps": 99_998.5,
                "perf_counters": {
                    "all": {"instructions": 900, "cycles": 300},
                    "main": {"instructions": 500, "cycles": 200},
                    "io": {"instructions": 400, "cycles": 100},
                },
                "perf_counters_scope": "user+kernel",
                "perf_duration_seconds": 28.7,
                "perf_rep_count": 3,
                "cpu_stacks_main": {"stacks": [["a", "b"]], "samples": [1]},
            },
        }
        with patch.object(coord, "_find_task_entry", return_value=entry), patch.object(coord.state, "save"):
            with patch("conductress.sweep.coordinator.get_head", side_effect=Exception("no repo")):
                coord.on_task_completed(task)
        point = coord.state.points["a" * 40]
        assert point.value == 410.0
        assert point.latency_data["actual_rps"] == 99_998.5
        assert point.perf_counters == {"instructions": 900, "cycles": 300}
        assert point.perf_counters_main == {"instructions": 500, "cycles": 200}
        assert point.perf_counters_io == {"instructions": 400, "cycles": 100}
        assert point.perf_counters_scope == "user+kernel"
        assert (point.perf_duration_seconds, point.perf_rep_count) == (28.7, 3)
        assert point.perf_rps == 99_998.5, "normalise by the achieved rate, never by the p99 score"
        assert point.cpu_stacks_main is not None

    def test_completion_without_a_result_records_nothing(self, tmp_path):
        coord = _v3_latency(tmp_path)
        task = coord._create_task(_sweep_task())
        task.sweep_commit = "a" * 40
        with patch.object(coord, "_find_task_entry", return_value=None), patch.object(coord.state, "save"):
            coord.on_task_completed(task)
        point = coord.state.points.get("a" * 40)
        assert point is None or (point.value is None and point.perf_counters is None)

    def test_export_names_cachecannon_as_the_tool(self, tmp_path):
        coord = _v3_latency(tmp_path)
        commit = "a" * 40
        coord.state.merge_commits = [commit]
        coord.state.commit_dates[commit] = "2026-01-01"
        with patch("conductress.sweep.coordinator.get_head", side_effect=Exception("no repo")):
            coord.record_result(commit, 410.0, 2.4, 3)
        coord.state.points[commit].latency_data = {
            "p50_us": 200.0,
            "p99_us": 410.0,
            "p99_9_us": 900.0,
            "p100_us": 3200.0,
            "target_rps": 100_000,
            "actual_rps": 99_998.5,
        }
        out = tmp_path / "series.json"
        assert coord.export(out, platform="Test") == 1
        series = json.loads(out.read_text())
        assert series["metadata"]["tool"] == "cachecannon"
        assert series["metadata"]["tool_version"] == config.SWEEP_V3_CACHECANNON_COMMIT[:8]
        assert series["metadata"]["metric"] == "latency"
        assert series["metadata"]["target_rps"] == 100_000
        (point,) = series["points"]
        assert point["p99_us"] == 410.0 and point["p99_9_us"] == 900.0
        assert "histogram" not in point, "cachecannon emits no histogram; the key is omitted, not null"

    def test_schedule_key_follows_the_planner(self, tmp_path):
        coord = _v3_latency(tmp_path)
        assert coord.schedule_key() is None, "no commits enumerated yet: nothing to queue"
        commits = [f"{i:040x}" for i in range(3)]
        coord.state.merge_commits = commits
        coord.state.commit_dates.update({c: "2026-01-01" for c in commits})
        coord.planner = SweepPlanner(coord.state)
        assert coord.schedule_key() == (TaskPriority.BACKFILL.value, -3.0, 0.0), "skeleton pass over 3 commits"


# ---------------------------------------------------------------------------
# Retirement of epoch-1 series with a v3 replacement
# ---------------------------------------------------------------------------


class TestRetiredSeries:
    def test_registry_names_every_replaced_valkey_series(self):
        # Every v1 Valkey throughput series now has a v3 counterpart, so the
        # retired set is the default GET series plus every roster entry, plus
        # the v1 latency series -- derived from the same roster data that
        # defines the v1 series, never hand-listed.
        from conductress.config import (
            SWEEP_IO_THREADS,
            SWEEP_PIPELINING,
            SWEEP_THROUGHPUT_WORKLOADS,
            sweep_throughput_label,
        )

        expected = {f"throughput:{sweep_throughput_label()}", "latency:get-k16-v16"}
        for wl in SWEEP_THROUGHPUT_WORKLOADS:
            label = sweep_throughput_label(
                test=wl.get("test", "get"),
                val_size=wl["val_size"],
                io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                pipelining=wl.get("pipelining", SWEEP_PIPELINING),
            )
            expected.add(f"throughput:{label}")
        valkey_only = {s for s in SWEEP_V1_RETIRED_SERIES if not s.split(":", 1)[1].startswith("redis-")}
        assert valkey_only == expected
        # The two originally-replaced series are still in the set.
        assert {"throughput:get-k16-v16-t7-p10", "latency:get-k16-v16"} <= valkey_only

    def test_v1_default_get_sweep_is_retired(self, tmp_path):
        from conductress.sweep.coordinator import SweepCoordinator

        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_path):
            coord = SweepCoordinator(tmp_path)
        assert coord.workload_id == "get-k16-v16-t7-p10"
        assert coord.retired is True

    def test_all_v1_throughput_workloads_are_retired(self, tmp_path):
        from conductress.config import (
            SWEEP_IO_THREADS,
            SWEEP_PIPELINING,
            SWEEP_TEST,
            SWEEP_THROUGHPUT_WORKLOADS,
            SWEEP_VAL_SIZE,
        )
        from conductress.sweep.coordinator import SweepCoordinator

        rosters = [
            dict(val_size=SWEEP_VAL_SIZE, test=SWEEP_TEST, io_threads=SWEEP_IO_THREADS, pipelining=SWEEP_PIPELINING)
        ] + [
            dict(
                val_size=wl["val_size"],
                test=wl.get("test", SWEEP_TEST),
                io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                pipelining=wl.get("pipelining", SWEEP_PIPELINING),
            )
            for wl in SWEEP_THROUGHPUT_WORKLOADS
        ]
        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_path):
            for kw in rosters:
                assert SweepCoordinator(tmp_path, **kw).retired is True, kw

    def test_no_v1_valkey_throughput_coordinator_is_schedulable(self, tmp_path):
        """The behaviour the retirement exists to guarantee: every v1 Valkey
        throughput coordinator refuses to queue, so only v3 measures."""
        from conductress.config import (
            SWEEP_IO_THREADS,
            SWEEP_PIPELINING,
            SWEEP_TEST,
            SWEEP_THROUGHPUT_WORKLOADS,
            SWEEP_VAL_SIZE,
        )
        from conductress.sweep.coordinator import SweepCoordinator

        rosters = [
            dict(val_size=SWEEP_VAL_SIZE, test=SWEEP_TEST, io_threads=SWEEP_IO_THREADS, pipelining=SWEEP_PIPELINING)
        ] + [
            dict(
                val_size=wl["val_size"],
                test=wl.get("test", SWEEP_TEST),
                io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                pipelining=wl.get("pipelining", SWEEP_PIPELINING),
            )
            for wl in SWEEP_THROUGHPUT_WORKLOADS
        ]
        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_path):
            for kw in rosters:
                coord = SweepCoordinator(tmp_path, **kw)
                with (
                    patch.object(coord, "_get_next_task") as next_task,
                    patch("conductress.sweep.coordinator.TaskQueue"),
                ):
                    assert coord.queue_next_if_needed() is False, kw
                next_task.assert_not_called()

    def test_v1_latency_sweep_is_retired(self, tmp_path):
        from conductress.sweep.latency_coordinator import LatencySweepCoordinator

        with patch("conductress.sweep.latency_coordinator.LATENCY_STATE_FILE", tmp_path / "lat.json"):
            coord = LatencySweepCoordinator(tmp_path)
        assert coord.retired is True

    def test_v3_series_are_never_retired(self, tmp_path):
        from conductress.sweep.coordinator_v3 import create_v3_coordinators

        with patch("conductress.sweep.coordinator_v3._ensure_v3_state_dir"):
            with patch("conductress.sweep.coordinator_v3.V3_STATE_DIR", tmp_path):
                assert all(c.retired is False for c in create_v3_coordinators(tmp_path))

    def test_retired_series_never_queues(self, tmp_path):
        from conductress.sweep.coordinator import SweepCoordinator

        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_path):
            coord = SweepCoordinator(tmp_path)
        with patch.object(coord, "_get_next_task") as next_task, patch("conductress.sweep.coordinator.TaskQueue"):
            assert coord.queue_next_if_needed() is False
        next_task.assert_not_called()

    def test_retired_series_still_exports(self, tmp_path):
        from conductress.sweep.coordinator import SweepCoordinator

        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_path):
            coord = SweepCoordinator(tmp_path)
        commit = "b" * 40
        coord.state.merge_commits = [commit]
        coord.state.commit_dates[commit] = "2026-01-01"
        coord.planner.record_result(commit, 3_000_000.0, 0.3, 5)
        out = tmp_path / "series.json"
        assert coord.export(out, platform="Test") == 1
        assert len(json.loads(out.read_text())["points"]) == 1

    def test_scheduler_skips_retired_series(self):
        from conductress.task_runner import TaskRunner

        runner = TaskRunner.__new__(TaskRunner)
        retired = MagicMock(epoch_id="v1", workload_id="get-k16-v16-t7-p10", retired=True)
        live = MagicMock(epoch_id="v3", workload_id="get-k16-v16-t7-p10", retired=False)
        for sub in (retired, live):
            sub.has_nightly_task.return_value = False
            sub.schedule_key.return_value = (TaskPriority.BACKFILL.value, -5.0, 0.0)
        runner._subscribers = [retired, live]
        with (
            patch("conductress.task_runner.TaskQueue") as queue_cls,
            patch("conductress.task_runner.load_sweep_config") as cfg,
        ):
            cfg.return_value.is_allowed.return_value = True
            queue_cls.return_value.get_all_tasks.return_value = ["queued"]
            runner._schedule_next()
        live.on_queue_empty.assert_called_once()
        retired.on_queue_empty.assert_not_called()
        retired.has_nightly_task.assert_not_called()
        retired.schedule_key.assert_not_called()


# ---------------------------------------------------------------------------
# Generator-independent series belong to every epoch
# ---------------------------------------------------------------------------


class TestGeneratorIndependentEpochs:
    def test_memory_schedules_under_the_first_epoch_and_publishes_under_all(self, tmp_path):
        from conductress.sweep.memory_coordinator import create_memory_coordinators

        with patch("conductress.sweep.memory_coordinator.MEMORY_STATE_DIR", tmp_path):
            coords = create_memory_coordinators(tmp_path)
        assert coords, "the memory roster is not empty"
        for coord in coords:
            # v1 is archived, so memory now publishes only under v3.
            assert coord.epoch_ids == SWEEP_GENERATOR_INDEPENDENT_EPOCHS == ("v3",)
            assert coord.epoch_id == "v3"
            assert coord.retired is False

    def test_generator_bound_series_belong_to_one_epoch(self, tmp_path):
        from conductress.sweep.coordinator import SweepCoordinator

        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_path):
            coord = SweepCoordinator(tmp_path, val_size=64)
        assert coord.epoch_ids == ("v1",)

    def test_publisher_reads_every_epoch_of_a_series(self):
        from conductress.publisher import DashboardPublisher

        memory = MagicMock(epoch_id="v3", epoch_ids=("v3", "v1"))
        v1_only = MagicMock(epoch_id="v1", epoch_ids=("v1",))
        legacy = MagicMock(spec=["epoch_id"], epoch_id="v1")  # a coordinator without epoch_ids
        assert DashboardPublisher._coord_epochs(memory) == ("v3", "v1")
        assert DashboardPublisher._coord_epochs(v1_only) == ("v1",)
        assert DashboardPublisher._coord_epochs(legacy) == ("v1",)

    def test_publish_writes_memory_series_under_v3_and_advertises_archived_v1(self, tmp_path, monkeypatch):
        """Memory publishes only under v3 now that v1 is archived, and the
        publisher does not rewrite the v1 manifest (its file persists from
        before archival) but advertises v1 in the live manifests' epoch list."""
        from conductress import config
        from conductress.publisher import DashboardPublisher

        def _export(output_path, platform):
            output_path.write_text(json.dumps({"metadata": {}, "points": []}))
            return 0

        memory = MagicMock(workload_id="memory-set-k16-v64", metric_id="memory", epoch_id="v3", epoch_ids=("v3",))
        memory.export.side_effect = _export
        memory.engine = None
        memory.lower_is_better = True
        v3_get = MagicMock(workload_id="get-k16-v16-t7-p10", metric_id="throughput", epoch_id="v3", epoch_ids=("v3",))
        v3_get.export.side_effect = _export
        v3_get.engine = None
        v3_get.lower_is_better = False
        v3_get._sweep_ref = "origin/unstable"

        publisher = DashboardPublisher.__new__(DashboardPublisher)
        publisher.coordinators = [memory, v3_get]
        publisher._export_dir = tmp_path
        publisher._platform_id = "graviton4"
        publisher._platform_label = "Graviton 4"
        publisher._rsync = MagicMock()
        with (
            patch("conductress.publisher.detect_platform", return_value=("graviton4", "Graviton 4")),
            patch("conductress.sweep.exporter.export_perf_metrics"),
            patch("conductress.publisher.should_profile_internals", return_value=False),
            patch.object(config, "SWEEP_ARCHIVED_EPOCHS", ("v1",)),
        ):
            publisher._publish()

        # Memory is written only under v3; the legacy unqualified name is NOT
        # regenerated (a persisted archived copy would live there instead).
        assert (tmp_path / "series-graviton4-memory-set-k16-v64-memory.epoch-v3.json").exists()
        assert not (tmp_path / "series-graviton4-memory-set-k16-v64-memory.json").exists()
        assert (
            json.loads((tmp_path / "series-graviton4-memory-set-k16-v64-memory.epoch-v3.json").read_text())["metadata"][
                "epoch"
            ]
            == "v3"
        )
        # Only the v3 manifest is written; the v1 manifest is left to its
        # persisted archived copy, so the publisher does not create one here.
        v3_manifest = json.loads((tmp_path / "manifest-graviton4.epoch-v3.json").read_text())
        assert not (tmp_path / "manifest-graviton4.json").exists()
        assert v3_manifest["memory_workloads"] == ["memory-set-k16-v64"]
        assert v3_manifest["throughput_workloads"] == ["get-k16-v16-t7-p10"]
        # The live v3 manifest advertises the archived v1 epoch so the
        # dashboard's epoch selector keeps offering v1 history.
        advertised = [e["id"] for e in v3_manifest["epochs"]]
        assert "v3" in advertised
        assert "v1" in advertised
        flags = {e["id"]: e["archived"] for e in v3_manifest["epochs"]}
        assert flags["v1"] is True and flags["v3"] is False
        publisher._rsync.assert_called_once()

    @staticmethod
    def _publisher_with_one_v3_coordinator(tmp_path):
        from conductress.publisher import DashboardPublisher

        def _export(output_path, platform):
            output_path.write_text(json.dumps({"metadata": {}, "points": []}))
            return 0

        v3_get = MagicMock(workload_id="get-k16-v16-t7-p10", metric_id="memory", epoch_id="v3", epoch_ids=("v3",))
        v3_get.export.side_effect = _export
        v3_get.engine = None
        v3_get.lower_is_better = False
        publisher = DashboardPublisher.__new__(DashboardPublisher)
        publisher.coordinators = [v3_get]
        publisher._export_dir = tmp_path
        publisher._platform_id = "graviton4"
        publisher._platform_label = "Graviton 4"
        publisher._rsync = MagicMock()
        return publisher

    def test_publish_refreshes_epoch_list_in_persisted_archived_manifest(self, tmp_path):
        """The archived v1 manifest persists from before archival with a stale
        epoch list (v1 first, no ``archived`` flags). The dashboard reads that
        manifest to discover epochs before it picks one, so a publish must
        rewrite its epoch list to match the live manifests while leaving the
        archived epoch's own workload lists untouched."""
        from conductress import config

        stale = {
            "version": 3,
            "platform": "graviton4",
            "epoch": "v1",
            "epochs": [
                {"id": "v1", "label": "Legacy v1 (stock generator)", "generator": "stock"},
                {"id": "v3", "label": "Cachecannon v3 (io_uring generator)", "generator": "cachecannon"},
            ],
            "throughput_workloads": ["get-k16-v16-t7-p10", "get-k16-v64-t7-p10"],
            "memory_workloads": ["memory-set-k16-v64"],
            "latency_workloads": ["get-k16-v16-t7-p1-r100k"],
            "groups": [{"id": "throughput"}],
        }
        legacy = tmp_path / "manifest-graviton4.json"
        legacy.write_text(json.dumps(stale))
        publisher = self._publisher_with_one_v3_coordinator(tmp_path)

        with (
            patch("conductress.publisher.detect_platform", return_value=("graviton4", "Graviton 4")),
            patch.object(config, "SWEEP_ARCHIVED_EPOCHS", ("v1",)),
        ):
            publisher._publish()

        refreshed = json.loads(legacy.read_text())
        live = json.loads((tmp_path / "manifest-graviton4.epoch-v3.json").read_text())
        # Same list, same order, same flags as the live manifest: v3 first.
        assert refreshed["epochs"] == live["epochs"]
        assert [e["id"] for e in refreshed["epochs"]] == ["v3", "v1"]
        assert {e["id"]: e["archived"] for e in refreshed["epochs"]} == {"v1": True, "v3": False}
        # Everything that describes the archived epoch's own data is untouched.
        assert refreshed["epoch"] == "v1"
        assert refreshed["throughput_workloads"] == stale["throughput_workloads"]
        assert refreshed["memory_workloads"] == stale["memory_workloads"]
        assert refreshed["latency_workloads"] == stale["latency_workloads"]
        assert refreshed["groups"] == stale["groups"]

    def test_publish_leaves_absent_archived_manifest_absent(self, tmp_path):
        """A wiped export dir has no v1 manifest; the publisher must not invent
        one (it has no v1 workload lists), leaving that to export-archived."""
        from conductress import config

        publisher = self._publisher_with_one_v3_coordinator(tmp_path)
        with (
            patch("conductress.publisher.detect_platform", return_value=("graviton4", "Graviton 4")),
            patch.object(config, "SWEEP_ARCHIVED_EPOCHS", ("v1",)),
        ):
            publisher._publish()
        assert not (tmp_path / "manifest-graviton4.json").exists()
        assert (tmp_path / "manifest-graviton4.epoch-v3.json").exists()

    def test_publish_does_not_rewrite_archived_manifest_already_current(self, tmp_path):
        """An archived manifest whose epoch list already matches is left with
        its mtime intact, so rsync's quick check skips it."""
        import os
        import time

        from conductress import config
        from conductress.publisher import DashboardPublisher

        publisher = self._publisher_with_one_v3_coordinator(tmp_path)
        legacy = tmp_path / "manifest-graviton4.json"
        with patch.object(config, "SWEEP_ARCHIVED_EPOCHS", ("v1",)):
            current = DashboardPublisher.advertised_epoch_defs(["v3"])
        legacy.write_text(json.dumps({"epoch": "v1", "epochs": current, "throughput_workloads": []}))
        old = time.time() - 3600
        os.utime(legacy, (old, old))

        with (
            patch("conductress.publisher.detect_platform", return_value=("graviton4", "Graviton 4")),
            patch.object(config, "SWEEP_ARCHIVED_EPOCHS", ("v1",)),
        ):
            publisher._publish()
        assert abs(legacy.stat().st_mtime - old) < 1
