"""Tests for cross-series scheduling in TaskRunner._schedule_next.

The runner ranks every series' next task by (schedule_tier, schedule_key) where
schedule_key is (priority, -magnitude, last_scheduled_at): the planner's own
TaskPriority first, so a LANDMARK in any series runs before a BISECTION in any
other; then the larger task; then the series that queued least recently.
"""

from unittest.mock import MagicMock, patch

import pytest

from conductress.sweep.planner import BenchmarkPoint, Landmark, PointStatus, SweepPlanner, SweepState, TaskPriority
from conductress.task_runner import TaskRunner

LANDMARK = TaskPriority.LANDMARK.value
BISECTION = TaskPriority.BISECTION.value
BACKFILL = TaskPriority.BACKFILL.value


@pytest.fixture
def runner():
    """TaskRunner with no sweep initialization."""
    return TaskRunner()


@pytest.fixture
def mock_config():
    """Patch load_sweep_config to allow all workloads."""
    config = MagicMock()
    config.is_allowed.return_value = True
    with patch("conductress.task_runner.load_sweep_config", return_value=config):
        yield config


def _make_subscriber(workload_id="get-k16-v16-t7-p10", key=(BISECTION, -0.05, 0.0), has_nightly=False, tier=0):
    """Create a mock subscriber with configurable scheduling answers."""
    sub = MagicMock()
    sub.workload_id = workload_id
    sub.schedule_key = MagicMock(return_value=key)
    sub.has_nightly_task = MagicMock(return_value=has_nightly)
    sub.schedule_tier = MagicMock(return_value=tier)
    sub.on_queue_empty = MagicMock()
    return sub


def _run(runner, queued_by):
    """Drive _schedule_next; ``queued_by`` names the subscribers whose on_queue_empty queues a task."""
    state = {"queued": False}

    def make_side_effect(sub):
        def on_queue_empty():
            if sub in queued_by:
                state["queued"] = True

        return on_queue_empty

    for sub in runner._subscribers:
        sub.on_queue_empty = MagicMock(side_effect=make_side_effect(sub))
    with patch("conductress.task_runner.TaskQueue") as MockQueue:
        queue = MagicMock()
        queue.get_all_tasks.side_effect = lambda: [MagicMock()] if state["queued"] else []
        MockQueue.return_value = queue
        runner._schedule_next()


class TestScheduleNext:
    """Global order across series, driven by mock subscribers."""

    def test_landmark_anywhere_beats_bisection_anywhere(self, runner, mock_config):
        """A pending release landmark in A runs before a 6% bisection in B."""
        bisect = _make_subscriber("mixed-s20-k16-v16-t7-p10", key=(BISECTION, -0.06, 0.0))
        landmark = _make_subscriber("set-k16-v16-t7-p10", key=(LANDMARK, 0.0, 0.0))
        runner._subscribers = [bisect, landmark]

        _run(runner, queued_by={landmark, bisect})

        landmark.on_queue_empty.assert_called_once()
        bisect.on_queue_empty.assert_not_called()

    def test_bisection_larger_delta_first(self, runner, mock_config):
        """Within BISECTION a 10% step beats a 3% step."""
        small = _make_subscriber("get-k16-v16-t7-p1", key=(BISECTION, -0.03, 0.0))
        large = _make_subscriber("mixed-s20-k16-v16-t7-p10", key=(BISECTION, -0.10, 0.0))
        runner._subscribers = [small, large]

        _run(runner, queued_by={small, large})

        large.on_queue_empty.assert_called_once()
        small.on_queue_empty.assert_not_called()

    def test_backfill_larger_gap_first(self, runner, mock_config):
        """Within BACKFILL the larger unsampled commit span wins."""
        narrow = _make_subscriber("get-k16-v16-t7-p1", key=(BACKFILL, -40.0, 0.0))
        wide = _make_subscriber("get-k16-v1024-t7-p10", key=(BACKFILL, -411.0, 0.0))
        runner._subscribers = [narrow, wide]

        _run(runner, queued_by={narrow, wide})

        wide.on_queue_empty.assert_called_once()
        narrow.on_queue_empty.assert_not_called()

    def test_landmarks_rotate_least_recent_first(self, runner, mock_config):
        """Equal landmarks: the series that queued least recently goes first."""
        recent = _make_subscriber("set-k16-v16-t7-p10", key=(LANDMARK, 0.0, 500.0))
        stale = _make_subscriber("get-k16-v16-t7-p1-r100k", key=(LANDMARK, 0.0, 100.0))
        runner._subscribers = [recent, stale]

        _run(runner, queued_by={recent, stale})

        stale.on_queue_empty.assert_called_once()
        recent.on_queue_empty.assert_not_called()

    def test_nightly_preempts_landmark(self, runner, mock_config):
        """NIGHTLY (untested HEAD) wins over a landmark elsewhere."""
        landmark = _make_subscriber("get-k16-v16-t7-p10", key=(LANDMARK, 0.0, 0.0), has_nightly=False)
        nightly = _make_subscriber("memory-redis-set-k16-v64", key=(BACKFILL, -1.0, 0.0), has_nightly=True)
        runner._subscribers = [landmark, nightly]

        with patch("conductress.task_runner.TaskQueue") as MockQueue:
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        nightly.on_queue_empty.assert_called_once()
        landmark.on_queue_empty.assert_not_called()

    def test_series_with_nothing_planned_is_asked_last(self, runner, mock_config):
        """A None key (planner has nothing) sorts below every real task."""
        idle = _make_subscriber("get-k16-v16-t7-p10-tls", key=None)
        backfill = _make_subscriber("get-k16-v16-t7-p1", key=(BACKFILL, -3.0, 0.0))
        runner._subscribers = [idle, backfill]

        _run(runner, queued_by={backfill})

        backfill.on_queue_empty.assert_called_once()
        idle.on_queue_empty.assert_not_called()

    def test_disallowed_workload_skipped(self, runner):
        """Coordinators for disallowed workloads are not scheduled."""
        blocked = _make_subscriber("get-k16-v16-t7-p10", key=(LANDMARK, 0.0, 0.0))
        allowed = _make_subscriber("memory-set-k16-v64", key=(BACKFILL, -1.0, 0.0))

        config = MagicMock()
        config.is_allowed.side_effect = lambda wid, epoch="v1": wid != "get-k16-v16-t7-p10"
        runner._subscribers = [blocked, allowed]

        with (
            patch("conductress.task_runner.load_sweep_config", return_value=config),
            patch("conductress.task_runner.TaskQueue") as MockQueue,
        ):
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        blocked.on_queue_empty.assert_not_called()
        allowed.on_queue_empty.assert_called_once()

    def test_multiple_nightly_first_one_wins(self, runner, mock_config):
        """When multiple coordinators have NIGHTLY, the first one encountered wins."""
        nightly1 = _make_subscriber("memory-redis-set-k16-v64", has_nightly=True)
        nightly2 = _make_subscriber("memory-redis-zadd-m20", has_nightly=True)
        runner._subscribers = [nightly1, nightly2]

        with patch("conductress.task_runner.TaskQueue") as MockQueue:
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        nightly1.on_queue_empty.assert_called_once()
        nightly2.on_queue_empty.assert_not_called()

    def test_falls_through_when_no_tasks_available(self, runner, mock_config):
        """If no coordinator can produce a task, _schedule_next returns cleanly."""
        sub = _make_subscriber("get-k16-v16-t7-p10")
        runner._subscribers = [sub]

        _run(runner, queued_by=set())

        sub.on_queue_empty.assert_called_once()

    def test_decision_is_logged(self, runner, mock_config, caplog):
        """Every decision logs the sorted order with tier, priority and magnitude."""
        bisect = _make_subscriber("mixed-s20-k16-v16-t7-p10", key=(BISECTION, -0.06, 0.0))
        landmark = _make_subscriber("set-k16-v16-t7-p10", key=(LANDMARK, 0.0, 0.0))
        runner._subscribers = [bisect, landmark]

        with caplog.at_level("INFO", logger="conductress.task_runner"):
            _run(runner, queued_by={landmark})

        line = next(r.getMessage() for r in caplog.records if "schedule order" in r.getMessage())
        assert line.index("set-k16-v16-t7-p10(t0,p2,m0)") < line.index("mixed-s20-k16-v16-t7-p10(t0,p3,m0.06)")


class TestScheduleTier:
    """A tier-1 (deferred) series queues only after every tier-0 series has nothing to queue."""

    def test_deferred_memory_backfill_waits_for_any_normal_series(self, runner, mock_config):
        perf = _make_subscriber("get-k16-v16-t7-p10", key=(BACKFILL, -3.0, 0.0), tier=0)
        memory = _make_subscriber("memory-sadd-m20", key=(LANDMARK, 0.0, 0.0), tier=1)
        runner._subscribers = [memory, perf]

        _run(runner, queued_by={perf})

        perf.on_queue_empty.assert_called_once()
        memory.on_queue_empty.assert_not_called()

    def test_deferred_series_runs_when_normal_series_have_nothing(self, runner, mock_config):
        perf = _make_subscriber("get-k16-v16-t7-p10", key=None, tier=0)
        memory = _make_subscriber("memory-sadd-m20", key=(BACKFILL, -800.0, 0.0), tier=1)
        runner._subscribers = [memory, perf]

        _run(runner, queued_by={memory})

        # perf was asked first (tier 0) and queued nothing; memory then ran.
        perf.on_queue_empty.assert_called_once()
        memory.on_queue_empty.assert_called_once()

    def test_tier_zero_memory_bisection_competes_by_priority(self, runner, mock_config):
        """A memory bisection (tier 0) still outranks a throughput backfill."""
        perf = _make_subscriber("get-k16-v16-t7-p10", key=(BACKFILL, -411.0, 0.0), tier=0)
        memory = _make_subscriber("memory-sadd-m20", key=(BISECTION, -0.02, 0.0), tier=0)
        runner._subscribers = [perf, memory]

        _run(runner, queued_by={memory})

        memory.on_queue_empty.assert_called_once()
        perf.on_queue_empty.assert_not_called()

    def test_subscriber_without_tier_is_normal(self, runner, mock_config):
        """Duck-typed subscribers that predate tiers schedule as tier 0."""
        legacy = _make_subscriber("get-k16-v16-t7-p10", key=(BACKFILL, -1.0, 0.0))
        del legacy.schedule_tier
        memory = _make_subscriber("memory-sadd-m20", key=(LANDMARK, 0.0, 0.0), tier=1)
        runner._subscribers = [memory, legacy]

        _run(runner, queued_by={legacy})

        legacy.on_queue_empty.assert_called_once()
        memory.on_queue_empty.assert_not_called()


def _state(n=100, landmarks=()):
    commits = [f"c{i:03d}" for i in range(n)]
    state = SweepState(merge_commits=commits, commit_dates={c: "2024-01-01" for c in commits})
    state.landmarks = [Landmark(commit=commits[i], date="2024-01-01", label=label) for i, label in landmarks]
    return state


def _point(state, i, value):
    c = state.merge_commits[i]
    state.points[c] = BenchmarkPoint(
        commit=c, date="2024-01-01", value=value, cv=0.5, reps=3, status=PointStatus.COMPLETED
    )


def _coordinator(state, tmp_path):
    """A real SweepCoordinator over ``state`` with git and I/O left out."""
    from conductress.sweep.coordinator import SweepCoordinator

    coord = SweepCoordinator.__new__(SweepCoordinator)
    coord.repo_path = tmp_path
    coord.state = state
    coord.state_file = tmp_path / "state.json"
    coord.planner = SweepPlanner(state)
    coord.engine = None
    coord._label = "get-k16-v16-t7-p10"
    return coord


class TestScheduleKey:
    """schedule_key carries the planner's next task: its priority and size."""

    def test_pending_landmark_is_landmark_priority(self, tmp_path):
        state = _state(landmarks=[(50, "8.0")])
        _point(state, 0, 1000.0)
        _point(state, 99, 1500.0)  # a 50% step, which a bisection would otherwise chase
        key = _coordinator(state, tmp_path).schedule_key()
        assert key == (LANDMARK, 0.0, 0.0)

    def test_new_series_is_not_absolute(self, tmp_path):
        """A series with <2 points ranks by its next task, not by an infinite urgency."""
        state = _state()
        key = _coordinator(state, tmp_path).schedule_key()
        assert key[0] == BACKFILL, "an empty series without landmarks starts with a skeleton pass"
        assert key[1] == -100.0, "the whole history is unsampled"
        with_landmark = _state(landmarks=[(10, "7.2.4")])
        assert _coordinator(with_landmark, tmp_path).schedule_key()[0] == LANDMARK

    def test_bisection_magnitude_is_the_segment_delta(self, tmp_path):
        state = _state()
        _point(state, 0, 1000.0)
        _point(state, 99, 1100.0)
        key = _coordinator(state, tmp_path).schedule_key()
        assert key[0] == BISECTION
        assert key[1] == pytest.approx(-0.10)

    def test_backfill_magnitude_is_the_gap(self, tmp_path):
        state = _state()
        for i in range(0, 100, 10):
            _point(state, i, 1000.0 + i * 0.1)  # flat: nothing to bisect
        key = _coordinator(state, tmp_path).schedule_key()
        assert key[0] == BACKFILL
        assert key[1] == -9.0, "largest gap: 9 unsampled commits between adjacent points"

    def test_completed_series_has_no_key(self, tmp_path):
        state = _state(n=3)
        for i, v in enumerate((1000.0, 1001.0, 1002.0)):
            _point(state, i, v)
        assert _coordinator(state, tmp_path).schedule_key() is None

    def test_queueing_stamps_last_scheduled_at(self, tmp_path):
        state = _state(landmarks=[(50, "8.0")])
        coord = _coordinator(state, tmp_path)
        coord._get_next_task = lambda: coord.planner.get_next_task(None)
        coord._create_task = MagicMock(return_value=MagicMock())
        coord._is_my_task = MagicMock(return_value=False)
        assert coord.schedule_key()[2] == 0.0
        with (
            patch("conductress.sweep.coordinator.TaskQueue") as MockQueue,
            patch.object(type(coord), "retired", False),
        ):
            MockQueue.return_value.get_all_tasks.return_value = []
            assert coord.queue_next_if_needed() is True
        assert coord.schedule_key()[2] > 0.0
