"""What 'need' means for a sweep series (BaseSweepCoordinator.has_need), and that asking is read-only."""

import copy
import dataclasses
from pathlib import Path
from unittest.mock import PropertyMock, patch

import pytest

from conductress.sweep.coordinator import SweepCoordinator
from conductress.sweep.planner import BenchmarkPoint, Landmark, PointStatus, SweepPlanner, SweepState

COMMITS = ["a", "b", "c", "d", "e"]


def _state(points=None, landmarks=None, threshold=0.02, last_head=None):
    state = SweepState(
        merge_commits=list(COMMITS),
        commit_dates={c: f"2026-01-{i + 1:02d}" for i, c in enumerate(COMMITS)},
        threshold=threshold,
        last_benchmarked_head=last_head,
        landmarks=landmarks or [],
    )
    for commit, value in (points or {}).items():
        state.points[commit] = BenchmarkPoint(
            commit=commit, date="2026-01-01", value=value, cv=0.2, status=PointStatus.COMPLETED
        )
    return state


@pytest.fixture
def make_coordinator(tmp_path):
    def build(state, *, retired=False, last_head=None):
        with patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", tmp_path):
            coordinator = SweepCoordinator(tmp_path / "repo")
        coordinator.state = state
        coordinator.planner = SweepPlanner(state)
        coordinator.state_file = tmp_path / "state.json"
        coordinator._last_head = last_head  # pylint: disable=protected-access
        patcher = patch.object(SweepCoordinator, "retired", new_callable=PropertyMock, return_value=retired)
        patcher.start()
        build.patchers.append(patcher)
        return coordinator

    build.patchers = []
    yield build
    for patcher in build.patchers:
        patcher.stop()


def test_a_gap_over_the_threshold_with_commits_between_is_need(make_coordinator):
    # 10 percent between a and e, three commits in between.
    assert make_coordinator(_state({"a": 100000, "e": 90000})).has_need() is True


def test_a_gap_inside_the_threshold_is_not_need(make_coordinator):
    # 0.5 percent is under the 2 percent threshold. Only backfill is left, and backfill never counts.
    assert make_coordinator(_state({"a": 100000, "e": 99500})).has_need() is False


def test_a_gap_with_no_commit_left_to_test_is_not_need(make_coordinator):
    state = _state({"a": 100000, "b": 90000})
    assert make_coordinator(state).has_need() is False


def test_a_missing_release_landmark_is_need(make_coordinator):
    state = _state({"a": 100000, "e": 100000}, landmarks=[Landmark(commit="c", date="2026-01-03", label="9.0.0")])
    assert make_coordinator(state).has_need() is True


def test_backfill_only_is_not_need(make_coordinator):
    # Two points, no gap: the planner would only gap-fill.
    state = _state({"a": 100000, "e": 100000})
    coordinator = make_coordinator(state)
    assert coordinator.planner.get_next_task().priority.name == "BACKFILL"
    assert coordinator.has_need() is False


def test_an_unmeasured_tip_is_need_once_the_scheduler_has_seen_it(make_coordinator):
    state = _state({"a": 100000, "b": 100000, "c": 100000, "d": 100000}, last_head="d")
    assert make_coordinator(state, last_head=None).has_need() is False  # tip not seen yet: only git can tell
    assert make_coordinator(state, last_head="e").has_need() is True  # seen at the last scheduling pass, not measured


def test_a_measured_tip_is_not_need(make_coordinator):
    state = _state({"a": 100000, "e": 100000})
    assert make_coordinator(state, last_head="e").has_need() is False


def test_a_retired_series_has_no_need(make_coordinator):
    assert make_coordinator(_state({"a": 100000, "e": 90000}), retired=True).has_need() is False


def test_asking_runs_no_git_and_writes_nothing(make_coordinator, tmp_path):
    state = _state({"a": 100000, "e": 90000}, landmarks=[Landmark(commit="c", date="2026-01-03", label="9.0.0")])
    coordinator = make_coordinator(state, last_head="e")
    before = copy.deepcopy(dataclasses.asdict(coordinator.state))
    with (
        patch("conductress.sweep.coordinator.get_head", side_effect=AssertionError("git must not run")),
        patch("conductress.sweep.coordinator.fetch_ref", side_effect=AssertionError("git must not run")),
        patch("subprocess.run", side_effect=AssertionError("no subprocess")),
    ):
        results = {coordinator.has_need() for _ in range(5)}
    assert results == {True}
    assert dataclasses.asdict(coordinator.state) == before
    assert not Path(coordinator.state_file).exists()
