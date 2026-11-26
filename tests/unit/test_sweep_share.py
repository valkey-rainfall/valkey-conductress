"""The sweep share rule: window maths, the decision, and a long run with and without the share (rollout step 6)."""

import pytest

from conductress.sweep.share import HALF_LIFE_SECONDS, ShareWindow, sweep_goes_first

HOUR = 3600.0


def test_an_empty_window_has_fraction_zero():
    assert ShareWindow().fraction(now=0.0) == 0.0


def test_fraction_is_the_sweep_part_of_recorded_time():
    window = ShareWindow()
    window.add(300, is_sweep=True, now=0.0)
    window.add(900, is_sweep=False, now=0.0)
    assert window.fraction(0.0) == pytest.approx(0.25)


def test_decay_halves_both_parts_so_the_fraction_stays_and_new_time_counts_more():
    window = ShareWindow()
    window.add(300, is_sweep=True, now=0.0)
    window.add(900, is_sweep=False, now=0.0)
    assert window.fraction(HALF_LIFE_SECONDS) == pytest.approx(0.25)
    window.add(600, is_sweep=False, now=HALF_LIFE_SECONDS)
    # sweep 150, other 450 + 600
    assert window.fraction(HALF_LIFE_SECONDS) == pytest.approx(150 / 1200)


def test_negative_seconds_and_a_clock_that_goes_back_are_harmless():
    window = ShareWindow()
    window.add(-5, is_sweep=True, now=100.0)
    window.add(100, is_sweep=False, now=50.0)
    assert window.fraction(40.0) == 0.0


@pytest.mark.parametrize(
    "share, need, fraction, expected",
    [
        (0, True, 0.0, False),  # off: never
        (25, False, 0.0, False),  # no need: never (today's order)
        (25, True, 0.0, True),  # need and under the share
        (25, True, 0.24, True),
        (25, True, 0.25, False),  # at the share: remote first
        (25, True, 0.9, False),
        (100, True, 0.99, True),
    ],
)
def test_the_decision(share, need, fraction, expected):
    assert sweep_goes_first(share, need, fraction) is expected


def _simulate(share, need, slots=3000, manual_seconds=600.0, sweep_seconds=300.0, switch=None):
    """A runner that always has manual tasks waiting and, when ``need``, sweep work. Returns seconds per class."""
    window = ShareWindow()
    now = 0.0
    used = {"sweep": 0.0, "manual": 0.0}
    for slot in range(slots):
        current_share = switch(slot) if switch else share
        if sweep_goes_first(current_share, need, window.fraction(now)):
            kind, seconds = "sweep", sweep_seconds
        else:
            kind, seconds = "manual", manual_seconds
        used[kind] += seconds
        window.add(seconds, is_sweep=kind == "sweep", now=now + seconds)
        now += seconds
    return used


def test_with_a_25_percent_share_neither_side_starves_the_other():
    used = _simulate(share=25, need=True)
    total = used["sweep"] + used["manual"]
    assert used["sweep"] / total == pytest.approx(0.25, abs=0.03)
    assert used["manual"] / total >= 0.70


def test_without_a_share_the_sweep_gets_nothing_while_manual_tasks_wait():
    assert _simulate(share=0, need=True)["sweep"] == 0.0


def test_without_need_the_sweep_gets_nothing_even_with_a_share():
    assert _simulate(share=25, need=False)["sweep"] == 0.0


def test_the_share_holds_for_other_task_lengths():
    used = _simulate(share=25, need=True, manual_seconds=3600.0, sweep_seconds=120.0, slots=4000)
    assert used["sweep"] / (used["sweep"] + used["manual"]) == pytest.approx(0.25, abs=0.03)


def test_switching_the_share_on_and_off_needs_nothing_stored():
    # Off for the first 1000 slots, on for 1000, off again: the sweep runs only in the middle part.
    def switch(slot):
        return 25 if 1000 <= slot < 2000 else 0

    window_used = _simulate(share=0, need=True, slots=3000, switch=switch)
    assert 0 < window_used["sweep"]
    off_only = _simulate(share=0, need=True, slots=1000)
    assert off_only["sweep"] == 0.0
