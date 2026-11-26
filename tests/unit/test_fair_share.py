"""The pure maths of fair share: decay, cut-off, score and tie-breaks (multi-agent queue note, section 4.4)."""

from datetime import datetime, timedelta, timezone

import pytest

from conductress.control.fair_share import CUTOFF_HALF_LIVES, Head, choose_head, decayed_seconds, recent_usage

NOW = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
HALF_LIFE = 24 * 3600.0


def _ago(**kwargs):
    return NOW - timedelta(**kwargs)


def test_a_task_that_ended_now_counts_in_full():
    assert decayed_seconds(600.0, NOW, NOW, HALF_LIFE) == 600.0


def test_one_half_life_halves_the_weight_and_two_quarter_it():
    assert decayed_seconds(600.0, _ago(hours=24), NOW, HALF_LIFE) == pytest.approx(300.0)
    assert decayed_seconds(600.0, _ago(hours=48), NOW, HALF_LIFE) == pytest.approx(150.0)


def test_the_cut_off_is_eight_half_lives():
    at_cutoff = _ago(hours=24 * CUTOFF_HALF_LIVES)
    assert decayed_seconds(600.0, at_cutoff, NOW, HALF_LIFE) == pytest.approx(600.0 / 256)
    assert decayed_seconds(600.0, at_cutoff - timedelta(seconds=1), NOW, HALF_LIFE) == 0.0


def test_a_clock_that_runs_ahead_does_not_increase_the_weight():
    assert decayed_seconds(600.0, NOW + timedelta(hours=1), NOW, HALF_LIFE) == 600.0


def test_usage_adds_up_per_login():
    rows = [
        ("example-a", 100.0, NOW),
        ("example-a", 200.0, _ago(hours=24)),
        ("example-b", 50.0, _ago(hours=48)),
    ]
    assert recent_usage(rows, NOW, HALF_LIFE) == pytest.approx({"example-a": 200.0, "example-b": 12.5})


def _head(agent, task_id, submitted_at, expected):
    return Head(agent, task_id, submitted_at, float(expected))


def _order(heads, usage):
    """Claim every head in turn, each time removing the chosen one, like repeated claims by different logins."""
    remaining, order = list(heads), []
    while remaining:
        chosen = choose_head(remaining, usage)
        order.append(chosen.agent)
        remaining = [head for head in remaining if head is not chosen]
    return order


def test_the_example_table_of_the_design_note():
    heads = [
        _head("a", "t-a", "2026-08-30T00:00:00Z", 600),
        _head("b", "t-b", "2026-08-30T00:00:01Z", 7200),
        _head("c", "t-c", "2026-08-30T00:00:02Z", 300),
    ]
    usage = {"a": 3600.0, "b": 900.0, "c": 0.0}
    # Scores: a 4200, b 8100, c 300.
    assert _order(heads, usage) == ["c", "a", "b"]


def test_a_new_login_goes_before_a_login_that_used_time_even_if_it_queued_later():
    heads = [_head("heavy", "t1", "2026-08-30T00:00:00Z", 300), _head("light", "t2", "2026-08-30T00:05:00Z", 300)]
    assert choose_head(heads, {"heavy": 5000.0}).agent == "light"


def test_with_no_recorded_usage_the_plain_order_is_kept():
    # The second head is shorter, but without any usage there is nothing to share: first in, first out.
    heads = [_head("a", "t1", "2026-08-30T00:00:00Z", 7200), _head("b", "t2", "2026-08-30T00:00:01Z", 60)]
    assert choose_head(heads, {}).agent == "a"
    assert choose_head(heads, {"a": 0.0, "b": 0.0}).agent == "a"


def test_equal_scores_go_to_the_older_submit_time_then_the_lower_task_id():
    older = _head("a", "t-9", "2026-08-30T00:00:00Z", 300)
    newer = _head("b", "t-1", "2026-08-30T00:00:05Z", 300)
    assert choose_head([newer, older], {"a": 100.0, "b": 100.0}) is older
    same_time_low = _head("a", "t-1", "2026-08-30T00:00:00Z", 300)
    same_time_high = _head("b", "t-2", "2026-08-30T00:00:00Z", 300)
    assert choose_head([same_time_high, same_time_low], {"a": 100.0, "b": 100.0}) is same_time_low


def test_the_choice_does_not_depend_on_the_order_of_the_input_when_scores_differ():
    heads = [_head("a", "t1", "2026-08-30T00:00:00Z", 300), _head("b", "t2", "2026-08-30T00:00:01Z", 300)]
    usage = {"a": 10.0, "b": 500.0}
    assert choose_head(heads, usage).agent == choose_head(list(reversed(heads)), usage).agent == "a"


def test_nothing_to_choose_from():
    assert choose_head([], {"a": 1.0}) is None
