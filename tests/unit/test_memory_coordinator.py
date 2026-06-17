"""Tests for MemorySweepCoordinator (data-driven multi-workload)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress.sweep.memory_coordinator import (
    MEMORY_WORKLOADS,
    MemorySweepCoordinator,
    MemoryWorkload,
    create_memory_coordinators,
)
from conductress.sweep.planner import SweepState
from conductress.tasks.task_mem_efficiency import MemTaskData
from conductress.tasks.task_perf_benchmark import PerfTaskData

SET_WORKLOAD = MemoryWorkload(command="set", key_size=16, value_size=64, label="set-v64", user_data_bytes=80)
ZADD_WORKLOAD = MemoryWorkload(command="zadd", key_size=0, value_size=64, label="zadd-m64", user_data_bytes=72)
EXPIRE_WORKLOAD = MemoryWorkload(
    command="set", key_size=16, value_size=64, has_expire=True, label="set-v64-expire", user_data_bytes=80
)


@pytest.fixture
def tmp_state(tmp_path):
    state_dir = tmp_path / "sweep_data"
    state_dir.mkdir(parents=True)
    state_file = state_dir / "memory_state_set-v64.json"
    state = SweepState(
        merge_commits=["aaa", "bbb", "ccc"],
        commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01", "ccc": "2024-03-01"},
    )
    state.save(state_file)
    return tmp_path


@pytest.fixture
def coordinator(tmp_state, monkeypatch):
    import conductress.config as config
    import conductress.sweep.memory_coordinator as mc

    monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "rainsupreme"])
    monkeypatch.setattr(mc, "MEMORY_STATE_DIR", tmp_state / "sweep_data")

    wl = MemoryWorkload(command="set", key_size=16, value_size=64, label="set-v64", user_data_bytes=80)
    coord = MemorySweepCoordinator(tmp_state / "repo", wl)
    coord.initialize()
    return coord


class TestMemoryWorkload:
    def test_state_file_path(self):
        wl = MemoryWorkload(command="zadd", key_size=0, value_size=64, label="zadd-m64", user_data_bytes=72)
        assert "memory_state_zadd-m64.json" in str(wl.state_file)

    def test_frozen(self):
        wl = SET_WORKLOAD
        with pytest.raises(Exception):
            wl.command = "zadd"  # type: ignore


class TestCoordinatorInit:
    def test_metric_id_is_memory(self, coordinator):
        assert coordinator.metric_id == "memory"

    def test_workload_id_includes_label(self, coordinator):
        assert coordinator.workload_id == "memory-set-v64"

    def test_metric_unit(self, coordinator):
        assert coordinator.metric_unit == "bytes/item"

    def test_lower_is_better(self, coordinator):
        assert coordinator.lower_is_better is True


class TestTaskCreation:
    def test_creates_correct_task_type(self, coordinator):
        from conductress.sweep.planner import SweepTask, TaskPriority

        sweep_task = SweepTask(commit="aaa", date="2024-01-01", priority=TaskPriority.BACKFILL, reason="test")
        task = coordinator._create_task(sweep_task)

        assert isinstance(task, MemTaskData)
        assert task.type == "set"
        assert task.val_sizes == [64]
        assert task.has_expire is False
        assert task.enable_profiling is True
        assert "[memory-sweep:set-v64]" in task.note


class TestTaskFiltering:
    def test_accepts_matching_task(self, coordinator):
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = "aaa"
        task.source = "valkey"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [64]
        assert coordinator._is_my_task(task) is True

    def test_rejects_different_test_type(self, coordinator):
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = "aaa"
        task.source = "valkey"
        task.type = "zadd"
        task.has_expire = False
        task.val_sizes = [64]
        assert coordinator._is_my_task(task) is False

    def test_rejects_different_expire_flag(self, coordinator):
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = "aaa"
        task.source = "valkey"
        task.type = "set"
        task.has_expire = True
        task.val_sizes = [64]
        assert coordinator._is_my_task(task) is False

    def test_rejects_non_sweep_task(self, coordinator):
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = ""
        task.source = "valkey"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [64]
        assert coordinator._is_my_task(task) is False

    def test_rejects_perf_task(self, coordinator):
        task = MagicMock(spec=PerfTaskData)
        task.sweep_commit = "aaa"
        assert coordinator._is_my_task(task) is False

    def test_rejects_different_value_size(self, coordinator):
        """zadd-m64 task must not be claimed by zadd-m20 coordinator (and vice versa)."""
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = "aaa"
        task.source = "valkey"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [20]  # Different from coordinator's 64
        assert coordinator._is_my_task(task) is False

    def test_rejects_different_source(self, coordinator):
        """Valkey coordinator must not claim Redis tasks."""
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = "aaa"
        task.source = "redis"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [64]
        assert coordinator._is_my_task(task) is False


class TestFactory:
    def test_creates_all_workloads(self, tmp_path, monkeypatch):
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey"])
        coordinators = create_memory_coordinators(tmp_path / "repo")
        assert len(coordinators) == len(MEMORY_WORKLOADS)
        labels = [c._workload.label for c in coordinators]
        assert "set-k16-v64" in labels
        assert "set-k16-v64-expire" in labels

    def test_each_has_unique_state_file(self, tmp_path, monkeypatch):
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey"])
        coordinators = create_memory_coordinators(tmp_path / "repo")
        state_files = [c.state_file for c in coordinators]
        assert len(set(state_files)) == len(state_files)  # all unique


class TestResultExtraction:
    def test_extracts_score(self, coordinator, tmp_path):
        output_file = tmp_path / "results" / "output.jsonl"
        output_file.parent.mkdir(parents=True)
        entry = {"task_id": "test_task", "score": 50.26}
        output_file.write_text(json.dumps(entry) + "\n")

        task = MagicMock()
        task.task_id = "test_task"

        with patch("conductress.sweep.memory_coordinator.CONDUCTRESS_RESULTS", tmp_path / "results"):
            result = coordinator._extract_result(task)

        assert result == (50.26, 0.0, 1)


class TestBreakdownExtraction:
    def test_extracts_breakdown_from_output(self, coordinator, tmp_path):
        output_file = tmp_path / "results" / "output.jsonl"
        output_file.parent.mkdir(parents=True)
        entry = {
            "task_id": "test_task",
            "score": 50.26,
            "data": {"results": [{"breakdown": {"robj_embval": 40.0, "hashtable": 10.0}}]},
        }
        output_file.write_text(json.dumps(entry) + "\n")

        task = MagicMock()
        task.task_id = "test_task"

        with patch("conductress.sweep.memory_coordinator.CONDUCTRESS_RESULTS", tmp_path / "results"):
            breakdown = coordinator._extract_breakdown(task)

        assert breakdown == {"robj_embval": 40.0, "hashtable": 10.0}

    def test_returns_none_when_no_breakdown(self, coordinator, tmp_path):
        output_file = tmp_path / "results" / "output.jsonl"
        output_file.parent.mkdir(parents=True)
        entry = {"task_id": "test_task", "score": 50.26, "data": {"results": [{"per_item_overhead": 50.26}]}}
        output_file.write_text(json.dumps(entry) + "\n")

        task = MagicMock()
        task.task_id = "test_task"

        with patch("conductress.sweep.memory_coordinator.CONDUCTRESS_RESULTS", tmp_path / "results"):
            breakdown = coordinator._extract_breakdown(task)

        assert breakdown is None


class TestOnTaskCompleted:
    def test_records_result_and_breakdown(self, coordinator, tmp_path):
        """Full flow: task completes → result recorded → breakdown attached → state saved."""
        output_file = tmp_path / "results" / "output.jsonl"
        output_file.parent.mkdir(parents=True)
        entry = {
            "task_id": "test_task",
            "score": 30.92,
            "data": {"results": [{"breakdown": {"robj_embval": 48.0, "hashtable": 12.8, "other": 0.1}}]},
        }
        output_file.write_text(json.dumps(entry) + "\n")

        task = MagicMock(spec=MemTaskData)
        task.task_id = "test_task"
        task.sweep_commit = "aaa"
        task.source = "valkey"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [64]

        with patch("conductress.sweep.memory_coordinator.CONDUCTRESS_RESULTS", tmp_path / "results"):
            coordinator.on_task_completed(task)

        # Verify result was recorded
        assert "aaa" in coordinator.state.points
        point = coordinator.state.points["aaa"]
        assert point.value == 30.92

        # Verify breakdown was attached
        assert point.breakdown == {"robj_embval": 48.0, "hashtable": 12.8, "other": 0.1}

    def test_records_result_without_breakdown(self, coordinator, tmp_path):
        """Result recorded even when breakdown is missing."""
        output_file = tmp_path / "results" / "output.jsonl"
        output_file.parent.mkdir(parents=True)
        entry = {"task_id": "test_task", "score": 54.92, "data": {"results": [{}]}}
        output_file.write_text(json.dumps(entry) + "\n")

        task = MagicMock(spec=MemTaskData)
        task.task_id = "test_task"
        task.sweep_commit = "bbb"
        task.source = "valkey"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [64]

        with patch("conductress.sweep.memory_coordinator.CONDUCTRESS_RESULTS", tmp_path / "results"):
            coordinator.on_task_completed(task)

        point = coordinator.state.points["bbb"]
        assert point.value == 54.92
        assert point.breakdown is None

    def test_ignores_wrong_workload(self, coordinator, tmp_path):
        """Tasks from other workloads are ignored."""
        task = MagicMock(spec=MemTaskData)
        task.task_id = "test_task"
        task.sweep_commit = "aaa"
        task.source = "valkey"
        task.type = "zadd"  # wrong type for set-64b coordinator
        task.has_expire = False

        coordinator.on_task_completed(task)
        assert "aaa" not in coordinator.state.points


class TestNamingConventions:
    """Ensure filenames and IDs are correct and distinguishable across workloads."""

    def test_workload_ids_are_unique(self, monkeypatch):
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey"])
        coordinators = create_memory_coordinators(Path("/tmp"))
        ids = [c.workload_id for c in coordinators]
        assert len(ids) == len(set(ids)), f"Duplicate workload_ids: {ids}"

    def test_metric_id_is_just_memory(self, monkeypatch):
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey"])
        for coord in create_memory_coordinators(Path("/tmp")):
            assert coord.metric_id == "memory", f"{coord._workload.label} has metric_id={coord.metric_id}"

    def test_filename_not_doubled(self, monkeypatch):
        """Regression: metric_id was previously f'memory-{label}', causing doubled filenames."""
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey"])
        for coord in create_memory_coordinators(Path("/tmp")):
            filename = f"series-amd64-{coord.workload_id}-{coord.metric_id}.json"
            # Should NOT contain the label twice
            assert filename.count(coord._workload.label) == 1, f"Doubled label in: {filename}"

    def test_state_files_are_unique(self, monkeypatch):
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey"])
        coordinators = create_memory_coordinators(Path("/tmp"))
        files = [str(c.state_file) for c in coordinators]
        assert len(files) == len(set(files)), f"Duplicate state files: {files}"


class TestFlatnessDiscount:
    """Tests for memory urgency flatness discount."""

    def test_flat_segments_reduce_urgency(self, tmp_path):
        """When most segments are flat (<1% delta), urgency is discounted."""
        from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS, MemorySweepCoordinator
        from conductress.sweep.planner import BenchmarkPoint, SweepPlanner, SweepState

        # Create a state with many flat segments
        commits = [f"c{i:04d}" for i in range(100)]
        state = SweepState(
            merge_commits=commits,
            commit_dates={c: f"2024-01-{(i%28)+1:02d}" for i, c in enumerate(commits)},
        )
        # Points at every 10th commit, all same value (flat)
        for i in range(0, 100, 10):
            state.points[commits[i]] = BenchmarkPoint(commit=commits[i], date="2024-01-01", value=100.0, cv=0.0, reps=3)
        state_file = tmp_path / "test_state.json"
        state.save(state_file)

        with patch.object(MemorySweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = MemorySweepCoordinator.__new__(MemorySweepCoordinator)
            coord.repo_path = tmp_path
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord._workload = MEMORY_WORKLOADS[0]

        score = coord.get_urgency_score()
        # All segments are flat (0% delta), should be heavily discounted
        assert score < 1.0  # Base backfill score ~3.3, discounted by 0.2x to ~0.66

    def test_non_flat_segments_no_discount(self, tmp_path):
        """When segments have significant deltas, no discount applied."""
        from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS, MemorySweepCoordinator
        from conductress.sweep.planner import BenchmarkPoint, SweepPlanner, SweepState

        commits = [f"c{i:04d}" for i in range(100)]
        state = SweepState(
            merge_commits=commits,
            commit_dates={c: f"2024-01-{(i%28)+1:02d}" for i, c in enumerate(commits)},
        )
        # Points with alternating values (big deltas)
        for i in range(0, 100, 10):
            val = 100.0 if (i // 10) % 2 == 0 else 120.0
            state.points[commits[i]] = BenchmarkPoint(commit=commits[i], date="2024-01-01", value=val, cv=0.0, reps=3)
        state_file = tmp_path / "test_state.json"
        state.save(state_file)

        with patch.object(MemorySweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = MemorySweepCoordinator.__new__(MemorySweepCoordinator)
            coord.repo_path = tmp_path
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord._workload = MEMORY_WORKLOADS[0]

        score = coord.get_urgency_score()
        # 20% deltas everywhere — no discount, should stay at base (~3.3)
        assert score > 3.0


class TestEngineSupport:
    """Tests for multi-engine (Redis) memory sweep support."""

    @pytest.fixture
    def redis_engine(self):
        from conductress.config import get_sweep_engine

        return get_sweep_engine("redis")

    @pytest.fixture
    def redis_coordinator(self, tmp_path, monkeypatch, redis_engine):
        import conductress.config as config
        import conductress.sweep.memory_coordinator as mc

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "redis"])
        monkeypatch.setattr(mc, "MEMORY_STATE_DIR", tmp_path / "sweep_data")
        (tmp_path / "sweep_data").mkdir(parents=True, exist_ok=True)

        wl = MemoryWorkload(command="set", key_size=16, value_size=64, label="set-v64", user_data_bytes=80)
        coord = MemorySweepCoordinator(tmp_path / "redis", wl, engine=redis_engine)
        return coord

    def test_state_file_has_engine_prefix(self, redis_engine):
        wl = MemoryWorkload(command="set", key_size=16, value_size=64, label="set-v64", user_data_bytes=80)
        sf = wl.state_file_for_engine(redis_engine)
        assert "redis-" in sf.name
        assert sf.name == "memory_state_redis-set-v64.json"

    def test_state_file_no_prefix_for_valkey(self):
        from conductress.config import get_sweep_engine

        valkey_engine = get_sweep_engine("valkey")
        wl = MemoryWorkload(command="set", key_size=16, value_size=64, label="set-v64", user_data_bytes=80)
        sf = wl.state_file_for_engine(valkey_engine)
        assert "redis-" not in sf.name
        assert sf.name == "memory_state_set-v64.json"

    def test_state_file_no_prefix_for_none(self):
        wl = MemoryWorkload(command="set", key_size=16, value_size=64, label="set-v64", user_data_bytes=80)
        sf = wl.state_file_for_engine(None)
        assert sf.name == "memory_state_set-v64.json"

    def test_workload_id_has_engine_prefix(self, redis_coordinator):
        assert redis_coordinator.workload_id == "memory-redis-set-v64"

    def test_create_task_uses_engine_source(self, redis_coordinator):
        from conductress.sweep.planner import SweepTask, TaskPriority

        sweep_task = SweepTask(commit="abc123", date="2024-01-01", priority=TaskPriority.BACKFILL, reason="test")
        task = redis_coordinator._create_task(sweep_task)
        assert task.source == "redis"

    def test_create_task_make_args_include_jemalloc_prof(self, redis_coordinator):
        from conductress.heap_profiler import JEMALLOC_PROF_CONFIGURE_OPTS
        from conductress.sweep.planner import SweepTask, TaskPriority

        sweep_task = SweepTask(commit="abc123", date="2024-01-01", priority=TaskPriority.BACKFILL, reason="test")
        task = redis_coordinator._create_task(sweep_task)
        assert JEMALLOC_PROF_CONFIGURE_OPTS in task.make_args

    def test_is_my_task_matches_engine_source(self, redis_coordinator):
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = "abc123"
        task.source = "redis"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [64]
        assert redis_coordinator._is_my_task(task) is True

    def test_is_my_task_rejects_wrong_source(self, redis_coordinator):
        task = MagicMock(spec=MemTaskData)
        task.sweep_commit = "abc123"
        task.source = "valkey"
        task.type = "set"
        task.has_expire = False
        task.val_sizes = [64]
        assert redis_coordinator._is_my_task(task) is False

    def test_factory_with_engine(self, tmp_path, monkeypatch, redis_engine):
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "redis"])
        coordinators = create_memory_coordinators(tmp_path / "redis", engine=redis_engine)
        assert len(coordinators) == len(MEMORY_WORKLOADS)
        # All should have redis prefix in workload_id
        for c in coordinators:
            assert c.workload_id.startswith("memory-redis-")
        # State files should have redis prefix
        for c in coordinators:
            assert "redis-" in c.state_file.name
