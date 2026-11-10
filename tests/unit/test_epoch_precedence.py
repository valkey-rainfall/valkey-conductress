"""Tests for explicit epoch scheduling precedence and epoch-aware sweep selectors.

Two behaviours are covered:

1. ``SWEEP_EPOCH_PRECEDENCE`` decides which epoch's coordinators are scanned
   first. Because NIGHTLY has absolute priority and returns on the first match,
   the scan order decides which epoch measures each new HEAD. This used to be an
   accident of subscriber registration order.

2. Sweep selectors may be epoch-qualified. Workload ids are shared across epochs
   on purpose (v1 and v3 both call the canonical GET sweep
   ``get-k16-v16-t7-p10``), so a bare id pauses BOTH epochs. Qualifying it as
   ``v1:get-k16-v16-t7-p10`` pauses exactly one.
"""

from unittest.mock import MagicMock, patch

import pytest

from conductress.sweep.planner import TaskPriority
from conductress.sweep_config import SweepConfig, parse_selector, selector_matches
from conductress.task_runner import TaskRunner

CANONICAL_GET = "get-k16-v16-t7-p10"


def _sub(workload_id=CANONICAL_GET, epoch_id="v1", key=(TaskPriority.BACKFILL.value, -1.0, 0.0), has_nightly=False):
    sub = MagicMock()
    sub.workload_id = workload_id
    sub.epoch_id = epoch_id
    sub.schedule_key = MagicMock(return_value=key)
    sub.has_nightly_task = MagicMock(return_value=has_nightly)
    sub.on_queue_empty = MagicMock()
    return sub


class TestParseSelector:
    def test_bare_workload_has_no_epoch(self):
        assert parse_selector(CANONICAL_GET) == (None, CANONICAL_GET)

    def test_epoch_qualified(self):
        assert parse_selector(f"v3:{CANONICAL_GET}") == ("v3", CANONICAL_GET)

    def test_epoch_wildcard(self):
        assert parse_selector("v3:*") == ("v3", "*")

    def test_whitespace_tolerated(self):
        assert parse_selector(" v3 : get-x ") == ("v3", "get-x")


class TestSelectorMatches:
    def test_bare_selector_matches_every_epoch(self):
        """Pre-epoch selectors keep their old meaning: all epochs."""
        assert selector_matches(CANONICAL_GET, CANONICAL_GET, "v1") is True
        assert selector_matches(CANONICAL_GET, CANONICAL_GET, "v3") is True

    def test_bare_selector_does_not_match_other_workload(self):
        assert selector_matches(CANONICAL_GET, "set-k16-v16-t7-p1", "v1") is False

    def test_qualified_selector_matches_only_its_epoch(self):
        assert selector_matches(f"v1:{CANONICAL_GET}", CANONICAL_GET, "v1") is True
        assert selector_matches(f"v1:{CANONICAL_GET}", CANONICAL_GET, "v3") is False

    def test_epoch_wildcard_matches_all_workloads_in_epoch(self):
        assert selector_matches("v3:*", CANONICAL_GET, "v3") is True
        assert selector_matches("v3:*", "mixed-s20-k16-v16-t7-p10", "v3") is True
        assert selector_matches("v3:*", CANONICAL_GET, "v1") is False

    def test_trailing_colon_means_whole_epoch(self):
        assert selector_matches("v3:", CANONICAL_GET, "v3") is True
        assert selector_matches("v3:", CANONICAL_GET, "v1") is False


class TestSweepConfigEpochAware:
    def test_bare_pause_stops_both_epochs(self):
        """Documented trap: a bare shared id pauses every epoch that uses it."""
        c = SweepConfig(mode="paused", paused=[CANONICAL_GET])
        assert c.is_allowed(CANONICAL_GET, "v1") is False
        assert c.is_allowed(CANONICAL_GET, "v3") is False

    def test_qualified_pause_stops_one_epoch_only(self):
        """The actual fix: pause v1's GET while v3's GET keeps running."""
        c = SweepConfig(mode="paused", paused=[f"v1:{CANONICAL_GET}"])
        assert c.is_allowed(CANONICAL_GET, "v1") is False
        assert c.is_allowed(CANONICAL_GET, "v3") is True

    def test_pause_whole_epoch(self):
        c = SweepConfig(mode="paused", paused=["v1:*"])
        assert c.is_allowed(CANONICAL_GET, "v1") is False
        assert c.is_allowed("memory-set-k16-v64", "v1") is False
        assert c.is_allowed(CANONICAL_GET, "v3") is True

    def test_focus_whole_epoch(self):
        c = SweepConfig(mode="focus", target="v3:*")
        assert c.is_allowed(CANONICAL_GET, "v3") is True
        assert c.is_allowed("mixed-s20-k16-v16-t7-p10", "v3") is True
        assert c.is_allowed(CANONICAL_GET, "v1") is False

    def test_focus_qualified_excludes_same_id_other_epoch(self):
        c = SweepConfig(mode="focus", target=f"v3:{CANONICAL_GET}")
        assert c.is_allowed(CANONICAL_GET, "v3") is True
        assert c.is_allowed(CANONICAL_GET, "v1") is False

    def test_focus_with_no_target_allows_nothing(self):
        c = SweepConfig(mode="focus", target=None)
        assert c.is_allowed(CANONICAL_GET, "v1") is False

    def test_default_epoch_keeps_pre_epoch_callers_working(self):
        """Callers that omit epoch_id behave exactly as before."""
        c = SweepConfig(mode="paused", paused=["throughput"])
        assert c.is_allowed("throughput") is False
        assert c.is_allowed("memory-set-k16-v64") is True


class TestEpochPrecedenceOrdering:
    def test_v3_scanned_before_v1_by_default(self):
        runner = TaskRunner()
        v1 = _sub(epoch_id="v1")
        v3 = _sub(epoch_id="v3")
        runner._subscribers = [v1, v3]  # registration order puts v1 first

        with patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")):
            assert runner._epoch_ordered_subscribers() == [v3, v1]

    def test_order_within_an_epoch_is_preserved(self):
        """Stable sort: coordinators in one epoch keep registration order."""
        runner = TaskRunner()
        a = _sub(workload_id="a", epoch_id="v1")
        b = _sub(workload_id="b", epoch_id="v1")
        c = _sub(workload_id="c", epoch_id="v1")
        runner._subscribers = [a, b, c]

        with patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")):
            assert runner._epoch_ordered_subscribers() == [a, b, c]

    def test_unlisted_epoch_sorts_last(self):
        runner = TaskRunner()
        v2 = _sub(workload_id="old", epoch_id="v2")
        v1 = _sub(workload_id="legacy", epoch_id="v1")
        v3 = _sub(workload_id="new", epoch_id="v3")
        runner._subscribers = [v2, v1, v3]

        with patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")):
            assert runner._epoch_ordered_subscribers() == [v3, v1, v2]

    def test_precedence_is_overridable(self):
        """Reversing precedence restores v1-first without a code change."""
        runner = TaskRunner()
        v1 = _sub(epoch_id="v1")
        v3 = _sub(epoch_id="v3")
        runner._subscribers = [v3, v1]

        with patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v1", "v3")):
            assert runner._epoch_ordered_subscribers() == [v1, v3]

    def test_subscriber_without_epoch_attribute_defaults_to_v1(self):
        runner = TaskRunner()
        plain = MagicMock(spec=[])  # no epoch_id attribute at all
        v3 = _sub(epoch_id="v3")
        runner._subscribers = [plain, v3]

        with patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")):
            assert runner._epoch_ordered_subscribers() == [v3, plain]


class TestNightlyPrecedenceIntegration:
    """The behaviour change: when both epochs have an untested HEAD, v3 measures first."""

    @pytest.fixture
    def allow_all(self):
        config = MagicMock()
        config.is_allowed.return_value = True
        with patch("conductress.task_runner.load_sweep_config", return_value=config):
            yield config

    def test_v3_nightly_wins_over_v1_nightly(self, allow_all):
        runner = TaskRunner()
        v1 = _sub(epoch_id="v1", has_nightly=True)
        v3 = _sub(epoch_id="v3", has_nightly=True)
        runner._subscribers = [v1, v3]  # v1 registered first

        with (
            patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")),
            patch("conductress.task_runner.TaskQueue") as MockQueue,
        ):
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        v3.on_queue_empty.assert_called_once()
        v1.on_queue_empty.assert_not_called()

    def test_v1_still_runs_when_v3_has_no_nightly(self, allow_all):
        """v1 is deprioritized, not disabled -- it wins when v3 has nothing to do."""
        runner = TaskRunner()
        v1 = _sub(epoch_id="v1", has_nightly=True)
        v3 = _sub(epoch_id="v3", has_nightly=False)
        runner._subscribers = [v1, v3]

        with (
            patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")),
            patch("conductress.task_runner.TaskQueue") as MockQueue,
        ):
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        v1.on_queue_empty.assert_called_once()
        v3.on_queue_empty.assert_not_called()

    def test_equal_key_breaks_toward_higher_precedence_epoch(self, allow_all):
        runner = TaskRunner()
        v1 = _sub(epoch_id="v1", key=(TaskPriority.BISECTION.value, -0.05, 0.0), has_nightly=False)
        v3 = _sub(epoch_id="v3", key=(TaskPriority.BISECTION.value, -0.05, 0.0), has_nightly=False)
        runner._subscribers = [v1, v3]

        with (
            patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")),
            patch("conductress.task_runner.TaskQueue") as MockQueue,
        ):
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        v3.on_queue_empty.assert_called_once()
        v1.on_queue_empty.assert_not_called()

    def test_higher_priority_v1_still_beats_lower_priority_v3(self, allow_all):
        """Precedence is a tie-break for the schedule key, not an override of it."""
        runner = TaskRunner()
        v1 = _sub(epoch_id="v1", key=(TaskPriority.LANDMARK.value, 0.0, 0.0), has_nightly=False)
        v3 = _sub(epoch_id="v3", key=(TaskPriority.BISECTION.value, -0.10, 0.0), has_nightly=False)
        runner._subscribers = [v3, v1]

        with (
            patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")),
            patch("conductress.task_runner.TaskQueue") as MockQueue,
        ):
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        v1.on_queue_empty.assert_called_once()
        v3.on_queue_empty.assert_not_called()

    def test_paused_epoch_is_skipped_even_with_nightly(self):
        """Epoch-qualified pause beats NIGHTLY absolute priority."""
        runner = TaskRunner()
        v1 = _sub(epoch_id="v1", has_nightly=True)
        v3 = _sub(epoch_id="v3", has_nightly=True)
        runner._subscribers = [v1, v3]

        config = SweepConfig(mode="paused", paused=["v3:*"])

        with (
            patch("conductress.task_runner.SWEEP_EPOCH_PRECEDENCE", ("v3", "v1")),
            patch("conductress.task_runner.load_sweep_config", return_value=config),
            patch("conductress.task_runner.TaskQueue") as MockQueue,
        ):
            queue = MagicMock()
            queue.get_all_tasks.return_value = [MagicMock()]
            MockQueue.return_value = queue
            runner._schedule_next()

        v1.on_queue_empty.assert_called_once()
        v3.on_queue_empty.assert_not_called()


class TestPrecedenceEnvParsing:
    def test_valid_list_parsed(self, monkeypatch):
        from conductress.config import _env_epoch_list

        monkeypatch.setenv("X_PREC", "v3, v1 ,v2")
        assert _env_epoch_list("X_PREC", ("v1",)) == ("v3", "v1", "v2")

    def test_unset_returns_default(self, monkeypatch):
        from conductress.config import _env_epoch_list

        monkeypatch.delenv("X_PREC", raising=False)
        assert _env_epoch_list("X_PREC", ("v3", "v1")) == ("v3", "v1")

    def test_blank_value_fails_closed(self, monkeypatch):
        """An explicitly blank override is an error, not a silent default."""
        from conductress.config import _env_epoch_list

        monkeypatch.setenv("X_PREC", "  , ")
        with pytest.raises(ValueError, match="comma-separated epoch list"):
            _env_epoch_list("X_PREC", ("v3", "v1"))

    def test_duplicate_epochs_rejected(self, monkeypatch):
        from conductress.config import _env_epoch_list

        monkeypatch.setenv("X_PREC", "v3,v1,v3")
        with pytest.raises(ValueError, match="duplicate"):
            _env_epoch_list("X_PREC", ("v3", "v1"))


class TestArchivedEpochs:
    """v1 is an archived epoch: it renders history but never schedules."""

    def test_v1_archived_by_default(self):
        from conductress import config

        assert config.epoch_is_archived("v1") is True
        assert config.epoch_is_archived("v3") is False

    def test_default_precedence_is_v3_only(self):
        """The shipped precedence lists only live epochs; v1 is gone from it."""
        from conductress.config import SWEEP_EPOCH_PRECEDENCE

        assert SWEEP_EPOCH_PRECEDENCE == ("v3",)

    def test_memory_epochs_are_v3_only(self):
        from conductress.config import SWEEP_GENERATOR_INDEPENDENT_EPOCHS

        assert SWEEP_GENERATOR_INDEPENDENT_EPOCHS == ("v3",)

    def test_archived_epoch_dropped_from_precedence_with_log(self, caplog):
        """A stale 'v3,v1' override cannot resurrect v1 scheduling: v1 is
        filtered out and the drop is logged."""
        import logging

        from conductress.config import _resolve_epoch_precedence

        with caplog.at_level(logging.INFO):
            resolved = _resolve_epoch_precedence(("v3", "v1"))
        assert resolved == ("v3",)
        assert any("archived" in rec.message.lower() for rec in caplog.records)

    def test_resolve_keeps_all_live_epochs_in_order(self):
        from conductress.config import _resolve_epoch_precedence

        assert _resolve_epoch_precedence(("v3", "v2")) == ("v3", "v2")
