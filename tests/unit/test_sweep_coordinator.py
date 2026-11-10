"""Unit tests for the sweep coordinator."""

import tempfile
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from conductress.sweep.coordinator import SWEEP_IO_THREADS, SWEEP_TEST, SWEEP_VAL_SIZE, SweepCoordinator
from conductress.sweep.planner import BenchmarkPoint, Landmark, PointStatus, SweepPlanner, SweepState
from conductress.tasks.task_perf_benchmark import PerfTaskData
from conductress.topology import TopologySpec


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def mock_repo(tmp_dir):
    """Create a minimal git repo for testing."""
    import subprocess

    repo = tmp_dir / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", str(repo)], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@test.com"],
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Test"],
        capture_output=True,
        check=True,
    )
    # Create initial commit
    (repo / "file.txt").write_text("hello")
    subprocess.run(["git", "-C", str(repo), "add", "."], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-m", "Initial"],
        capture_output=True,
        check=True,
    )
    return repo


class TestSweepCoordinatorInit:
    """Tests for SweepCoordinator initialization."""

    @patch("conductress.sweep.coordinator.SWEEP_STATE_DIR")
    @patch("conductress.sweep.coordinator.get_merge_commits")
    def test_initialize_populates_commits(self, mock_commits, mock_state_file, tmp_dir):
        from conductress.sweep.git_ops import MergeCommit

        mock_state_file.__class__ = Path
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"

        mock_commits.return_value = [
            MergeCommit(hash="aaa111", date="2024-03-20", pr=100, pr_title="First PR"),
            MergeCommit(hash="bbb222", date="2024-04-15", pr=200, pr_title="Second PR"),
        ]

        with (
            patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_dir),
            patch("conductress.sweep.coordinator.get_release_branch_points", return_value=[]),
        ):
            coordinator = SweepCoordinator(tmp_dir / "repo")
            coordinator.initialize()

        assert len(coordinator.state.merge_commits) == 2
        assert coordinator.state.merge_commits[0] == "aaa111"
        assert coordinator.state.commit_dates["bbb222"] == "2024-04-15"

    @patch("conductress.sweep.coordinator.SWEEP_STATE_DIR")
    def test_initialize_skips_if_already_populated(self, mock_state_file, tmp_dir):
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        # Pre-populate state
        state = SweepState(
            merge_commits=["abc", "def"],
            commit_dates={"abc": "2024-01-01", "def": "2024-02-01"},
        )
        state.save(state_file)

        with (
            patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_dir),
            patch("conductress.sweep.coordinator.get_merge_commits") as mock_git,
        ):
            coordinator = SweepCoordinator(tmp_dir / "repo")
            coordinator.initialize()
            # Should NOT call git since commits already populated
            mock_git.assert_not_called()


class TestSeriesFloor:
    """A series floor bounds one series' commit range; it outranks the engine floor.

    Resolution order in ``_populate_commits``: series ``floor_tag``, else the
    engine's ``floor_tag``, else the repository fork point.
    """

    @staticmethod
    def _populate(tmp_dir, engine, floor_tag):
        from conductress.sweep.coordinator import BaseSweepCoordinator

        class Coordinator(BaseSweepCoordinator):
            metric_id = "throughput"
            metric_unit = "ops/sec"
            workload_id = "floor-test"

            def _create_task(self, sweep_task):  # pragma: no cover - not exercised
                raise NotImplementedError

            def _extract_result(self, task):  # pragma: no cover - not exercised
                return None

            def _is_my_task(self, task):  # pragma: no cover - not exercised
                return False

        coord = Coordinator(tmp_dir / "repo", tmp_dir / "state.json", engine=engine, floor_tag=floor_tag)
        with (
            patch("conductress.sweep.git_ops.resolve_floor_commit", return_value="floor-sha") as resolve,
            patch("conductress.sweep.git_ops.find_fork_point", return_value="fork-sha") as fork,
            patch("conductress.sweep.coordinator.get_merge_commits", return_value=[]) as merges,
        ):
            coord._populate_commits()
        return resolve, fork, merges

    def test_series_floor_overrides_the_engine_floor(self, tmp_dir):
        from conductress.config import get_sweep_engine

        resolve, fork, merges = self._populate(tmp_dir, get_sweep_engine("redis"), "9.0.0")
        resolve.assert_called_once_with(tmp_dir / "repo", "9.0.0", "origin/unstable")
        fork.assert_not_called()
        assert merges.call_args.kwargs["since_commit"] == "floor-sha"

    def test_engine_floor_applies_when_the_series_has_none(self, tmp_dir):
        from conductress.config import get_sweep_engine

        resolve, fork, _ = self._populate(tmp_dir, get_sweep_engine("redis"), None)
        resolve.assert_called_once_with(tmp_dir / "repo", "8.0.0", "origin/unstable")
        fork.assert_not_called()

    def test_series_floor_applies_without_an_engine(self, tmp_dir):
        resolve, fork, merges = self._populate(tmp_dir, None, "9.0.0")
        resolve.assert_called_once()
        assert resolve.call_args.args[1] == "9.0.0"
        fork.assert_not_called()
        assert merges.call_args.kwargs["since_commit"] == "floor-sha"

    def test_no_floor_falls_back_to_the_fork_point(self, tmp_dir):
        resolve, fork, merges = self._populate(tmp_dir, None, None)
        resolve.assert_not_called()
        fork.assert_called_once()
        assert merges.call_args.kwargs["since_commit"] == "fork-sha"

    def test_unresolvable_floor_tag_warns_and_sweeps_everything(self, tmp_dir, caplog):
        """A floor tag missing from the repo must be loud, not a silent full backfill."""
        import logging

        from conductress.sweep.coordinator import BaseSweepCoordinator

        class Coordinator(BaseSweepCoordinator):
            metric_id = "throughput"
            metric_unit = "ops/sec"
            workload_id = "floor-test"

            def _create_task(self, sweep_task):  # pragma: no cover
                raise NotImplementedError

            def _extract_result(self, task):  # pragma: no cover
                return None

            def _is_my_task(self, task):  # pragma: no cover
                return False

        coord = Coordinator(tmp_dir / "repo", tmp_dir / "state.json", floor_tag="no-such-tag")
        with (
            patch("conductress.sweep.git_ops.resolve_floor_commit", return_value=None),
            patch("conductress.sweep.coordinator.get_merge_commits", return_value=[]) as merges,
            caplog.at_level(logging.WARNING, logger="conductress.sweep.coordinator"),
        ):
            coord._populate_commits()
        assert merges.call_args.kwargs["since_commit"] is None
        assert "no-such-tag" in caplog.text and "does not resolve" in caplog.text


class TestSweepCoordinatorTaskGeneration:
    """Tests for sweep task generation."""

    @patch("conductress.sweep.coordinator.SWEEP_STATE_DIR")
    @patch("conductress.sweep.coordinator.get_head")
    def test_generates_task_for_new_head(self, mock_head, mock_state_file, tmp_dir):
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb", "ccc"],
            commit_dates={
                "aaa": "2024-01-01",
                "bbb": "2024-02-01",
                "ccc": "2024-03-01",
            },
        )
        state.points["aaa"] = BenchmarkPoint(
            commit="aaa",
            date="2024-01-01",
            value=100000,
            cv=0.2,
            status=PointStatus.COMPLETED,
        )
        state.save(state_file)
        mock_head.return_value = "ccc"

        with (
            patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_dir),
            patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "valkey-rainfall"]),
        ):
            coordinator = SweepCoordinator(tmp_dir / "repo")
            coordinator.initialize()
            task = coordinator.get_next_sweep_task()

        assert task is not None
        assert task.specifier == "ccc"
        assert task.test == SWEEP_TEST
        assert task.val_size == SWEEP_VAL_SIZE
        assert task.io_threads == SWEEP_IO_THREADS
        assert "[perf-sweep:" in task.note

    @patch("conductress.sweep.coordinator.SWEEP_STATE_DIR")
    @patch("conductress.sweep.coordinator.get_head")
    def test_returns_none_when_all_done(self, mock_head, mock_state_file, tmp_dir):
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01"},
        )
        state.points["aaa"] = BenchmarkPoint(
            commit="aaa",
            date="2024-01-01",
            value=100000,
            cv=0.2,
            status=PointStatus.COMPLETED,
        )
        state.points["bbb"] = BenchmarkPoint(
            commit="bbb",
            date="2024-02-01",
            value=100000,
            cv=0.2,
            status=PointStatus.COMPLETED,
        )
        state.save(state_file)
        mock_head.return_value = "bbb"
        state.last_benchmarked_head = "bbb"
        state.save(state_file)

        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_dir):
            coordinator = SweepCoordinator(tmp_dir / "repo")
            coordinator.initialize()
            task = coordinator.get_next_sweep_task()

        assert task is None


class TestSweepCoordinatorResults:
    """Tests for recording results and cleanup."""

    @patch("conductress.sweep.coordinator.SWEEP_STATE_DIR")
    @patch("conductress.sweep.coordinator.get_head")
    def test_record_result_persists(self, mock_head, mock_state_file, tmp_dir):
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01"},
        )
        state.save(state_file)
        mock_head.return_value = "bbb"

        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_dir):
            coordinator = SweepCoordinator(tmp_dir / "repo")
            coordinator.initialize()
            coordinator.record_result("aaa", value=150000, cv=0.19, reps=3)

        # Reload and verify
        loaded = SweepState.load(state_file)
        assert "aaa" in loaded.points
        assert loaded.points["aaa"].value == 150000
        assert loaded.points["aaa"].status == PointStatus.COMPLETED

    @patch("conductress.sweep.coordinator.SWEEP_STATE_DIR")
    def test_record_build_failure_persists(self, mock_state_file, tmp_dir):
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa"],
            commit_dates={"aaa": "2024-01-01"},
        )
        state.save(state_file)

        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_dir):
            coordinator = SweepCoordinator(tmp_dir / "repo")
            coordinator.initialize()
            coordinator.record_build_failure("aaa")

        loaded = SweepState.load(state_file)
        assert loaded.points["aaa"].status == PointStatus.BUILD_FAILED


class TestTaskRunnerSweepIntegration:
    """Tests for TaskRunner with sweep mode."""

    def test_task_runner_accepts_sweep_flag(self):
        """Verify TaskRunner can be instantiated with sweep=False without errors."""
        from conductress.task_runner import TaskRunner

        runner = TaskRunner(sweep=False)
        assert runner._subscribers == []

    @patch("conductress.platform.get_local_platform_tag", return_value="amd64")
    def test_task_runner_sweep_mode_archives_v1_by_default(self, mock_platform, tmp_dir):
        """With v1 archived (the default), sweep mode builds no v1 SweepCoordinator
        or LatencySweepCoordinator; only the generator-independent memory
        subscribers register (v3 is off by default)."""
        from unittest.mock import MagicMock

        from conductress.task_runner import TaskRunner

        with (
            patch("conductress.sweep.coordinator.SweepCoordinator") as MockCoord,
            patch("conductress.sweep.latency_coordinator.LatencySweepCoordinator") as MockLatency,
            patch(
                "conductress.sweep.memory_coordinator.create_memory_coordinators",
                return_value=[MagicMock(initialize=MagicMock()) for _ in range(5)],
            ),
        ):
            runner = TaskRunner(sweep=True, repo_path=tmp_dir)
            assert len(runner._subscribers) == 5  # memory only
            MockCoord.assert_not_called()
            MockLatency.assert_not_called()

    @patch("conductress.sweep.coordinator.SweepCoordinator.initialize")
    @patch("conductress.sweep.coordinator.get_merge_commits", return_value=[])
    @patch("conductress.sweep.coordinator.get_release_branch_points", return_value=[])
    @patch("conductress.platform.get_local_platform_tag", return_value="amd64")
    def test_task_runner_sweep_mode_builds_v1_roster_when_not_archived(
        self, mock_platform, mock_tags, mock_commits, mock_init, tmp_dir
    ):
        """When v1 is NOT archived, sweep mode builds the primary + latency +
        memory subscribers, and Intel/ARM add their platform-specific v1
        workloads on top of the AMD baseline."""
        from conductress.task_runner import TaskRunner

        with patch("conductress.config.SWEEP_ARCHIVED_EPOCHS", ()):
            runner = TaskRunner(sweep=True, repo_path=tmp_dir)
            # Should have subscribers (at least primary throughput + latency + memory)
            assert len(runner._subscribers) >= 3
            amd_count = len(runner._subscribers)

            # Intel and ARM should get additional platform-specific workloads
            mock_platform.return_value = "intel"
            runner_intel = TaskRunner(sweep=True, repo_path=tmp_dir)
            assert len(runner_intel._subscribers) > amd_count

            mock_platform.return_value = "arm64"
            runner_arm = TaskRunner(sweep=True, repo_path=tmp_dir)
            assert len(runner_arm._subscribers) > amd_count


class TestIsMyTask:
    """Tests for _is_my_task with io_threads and pipelining matching."""

    @patch("conductress.sweep.coordinator.SweepCoordinator.initialize")
    @patch("conductress.sweep.coordinator.get_merge_commits", return_value=[])
    @patch("conductress.sweep.coordinator.get_release_branch_points", return_value=[])
    @patch("conductress.config.REPO_NAMES", ["valkey", "rainsupreme"])
    def test_is_my_task_matches_io_threads_and_pipelining(self, mock_tags, mock_commits, mock_init, tmp_dir):
        coord_default = SweepCoordinator(tmp_dir / "repo")
        coord_intel = SweepCoordinator(tmp_dir / "repo", io_threads=24, pipelining=100)

        task_default = PerfTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            topology=TopologySpec.standalone(),
            note="",
            requirements={},
            test="get",
            val_size=SWEEP_VAL_SIZE,
            io_threads=SWEEP_IO_THREADS,
            pipelining=10,
            warmup=5,
            duration=30,
            perf_stat_enabled=True,
            has_expire=False,
            preload_keys=True,
        )
        task_default.sweep_commit = "abc123"

        task_intel = PerfTaskData(
            source="valkey",
            specifier="abc123",
            make_args="",
            topology=TopologySpec.standalone(),
            note="",
            requirements={},
            test="get",
            val_size=SWEEP_VAL_SIZE,
            io_threads=24,
            pipelining=100,
            warmup=5,
            duration=30,
            perf_stat_enabled=True,
            has_expire=False,
            preload_keys=True,
        )
        task_intel.sweep_commit = "abc123"

        # Default coordinator matches default task, not intel task
        assert coord_default._is_my_task(task_default) is True
        assert coord_default._is_my_task(task_intel) is False

        # Intel coordinator matches intel task, not default task
        assert coord_intel._is_my_task(task_intel) is True
        assert coord_intel._is_my_task(task_default) is False


class TestFetchAndRefresh:
    """Tests for _fetch_and_refresh stale commit detection."""

    def test_refreshes_when_head_not_in_commit_list(self, tmp_dir):
        """Regression: stale state with HEAD not in merge_commits must trigger refresh."""
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb", "ccc"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01", "ccc": "2024-03-01"},
        )
        state.save(state_file)

        with patch.object(SweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = SweepCoordinator.__new__(SweepCoordinator)
            coord.repo_path = tmp_dir
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord.engine = None
            coord._last_fetch_time = 0.0

        new_head = "ddd"  # Not in merge_commits

        with (
            patch("conductress.sweep.coordinator.get_head", return_value=new_head),
            patch("conductress.sweep.coordinator.fetch_ref"),
            patch.object(coord, "_refresh_commits") as mock_refresh,
        ):
            coord._fetch_and_refresh()
            mock_refresh.assert_called_once()

    def test_skips_refresh_when_head_in_commit_list_and_unchanged(self, tmp_dir):
        """No refresh needed when HEAD is already in the commit list and didn't move."""
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb", "ccc"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01", "ccc": "2024-03-01"},
        )
        state.save(state_file)

        with patch.object(SweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = SweepCoordinator.__new__(SweepCoordinator)
            coord.repo_path = tmp_dir
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord.engine = None
            coord._last_fetch_time = 0.0

        with (
            patch("conductress.sweep.coordinator.get_head", return_value="ccc"),
            patch("conductress.sweep.coordinator.fetch_ref"),
            patch.object(coord, "_refresh_commits") as mock_refresh,
        ):
            coord._fetch_and_refresh()
            mock_refresh.assert_not_called()


class TestHasNightlyTask:
    """Tests for has_nightly_task detection."""

    def test_returns_true_when_head_untested_and_in_list(self, tmp_dir):
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb", "ccc"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01", "ccc": "2024-03-01"},
        )
        state.last_benchmarked_head = "aaa"
        state.save(state_file)

        with patch.object(SweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = SweepCoordinator.__new__(SweepCoordinator)
            coord.repo_path = tmp_dir
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord.engine = None

        with patch("conductress.sweep.coordinator.get_head", return_value="ccc"):
            assert coord.has_nightly_task() is True

    def test_returns_false_when_head_already_benchmarked(self, tmp_dir):
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb", "ccc"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01", "ccc": "2024-03-01"},
        )
        state.last_benchmarked_head = "ccc"
        state.save(state_file)

        with patch.object(SweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = SweepCoordinator.__new__(SweepCoordinator)
            coord.repo_path = tmp_dir
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord.engine = None

        with patch("conductress.sweep.coordinator.get_head", return_value="ccc"):
            assert coord.has_nightly_task() is False

    def test_returns_true_after_refresh_when_head_not_in_list(self, tmp_dir):
        """When HEAD is not in commit list, triggers refresh and then finds it."""
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01"},
        )
        state.save(state_file)

        with patch.object(SweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = SweepCoordinator.__new__(SweepCoordinator)
            coord.repo_path = tmp_dir
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord.engine = None
            coord._last_fetch_time = 0.0

        def fake_refresh():
            # Simulate refresh adding the new commit
            coord.state.merge_commits.append("zzz")
            coord.planner = SweepPlanner(coord.state)
            coord.engine = None

        with (
            patch("conductress.sweep.coordinator.get_head", return_value="zzz"),
            patch.object(coord, "_fetch_and_refresh", side_effect=fake_refresh),
        ):
            assert coord.has_nightly_task() is True

    def test_returns_true_when_head_in_points_but_incomplete(self, tmp_dir):
        """Planner pre-creates placeholder points with value=None.

        has_nightly_task should still return True for these since they
        haven't actually been benchmarked.
        """
        state_file = tmp_dir / "state_get-k16-v16-t7-p10.json"
        state = SweepState(
            merge_commits=["aaa", "bbb", "ccc"],
            commit_dates={"aaa": "2024-01-01", "bbb": "2024-02-01", "ccc": "2024-03-01"},
        )
        # HEAD exists as a placeholder (no value, not completed)
        state.points["ccc"] = BenchmarkPoint(commit="ccc", date="2024-03-01", value=None)
        state.last_benchmarked_head = "aaa"
        state.save(state_file)

        with patch.object(SweepCoordinator, "__init__", lambda self, *a, **kw: None):
            coord = SweepCoordinator.__new__(SweepCoordinator)
            coord.repo_path = tmp_dir
            coord.state_file = state_file
            coord.state = state
            coord.planner = SweepPlanner(state)
            coord.engine = None

        with patch("conductress.sweep.coordinator.get_head", return_value="ccc"):
            assert coord.has_nightly_task() is True
