"""Tests for the canary findings module (events, statistics, ranking, consistency)."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from conductress.control import findings as fx
from conductress.control.findings import (
    Explanation,
    FindingsConfig,
    ManualEvent,
    Observation,
    PairedSeries,
    RunnerInput,
    build_findings,
)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "canary_findings" / "backtest.json"
FAST = FindingsConfig(bootstrap=2000, permutations=4000)
# Insufficient-data events of the back-test fixture after the clearing rule: the 11-09 deploy on 4
# runners and its fleet row, 11-06 and 09-19 (too few days after), and 09-22, 09-23, 08-31 (1-2 days
# before).
INSUFFICIENT_COUNT = 10


def ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def series(
    runner_id: str,
    scores: list[float],
    *,
    start: str = "2026-09-01",
    env_at: dict[int, dict] | None = None,
    base_env: dict | None = None,
) -> RunnerInput:
    """A runner with one observation per day; ``env_at[i]`` updates the environment from day i on."""
    env = dict(base_env or {"kernel_release": "k1", "conductress_revision": "rev-1", "provenance_schema_version": 2})
    day0 = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    obs = []
    for i, score in enumerate(scores):
        if env_at and i in env_at:
            env = {**env, **env_at[i]}
        when = day0 + timedelta(days=i, hours=6, minutes=30)
        obs.append(Observation(when.date().isoformat(), score, dict(env), when))
    return RunnerInput(runner_id, runner_id.title(), obs)


def noisy(n: int, level: float, seed: int, sd: float = 0.0025) -> list[float]:
    rng = np.random.default_rng(seed)
    return list(level * (1 + rng.normal(0, sd, n)))


# --------------------------------------------------------------------------- events


class TestFieldChanges:
    def test_new_schema_fields_are_not_changes(self):
        old = {"kernel_release": "a", "provenance_schema_version": 1}
        new = {"kernel_release": "a", "glibc_version": "2.34", "provenance_schema_version": 2}
        assert fx.field_changes(old, new) == []

    def test_identity_fields_are_ignored(self):
        old = {"runner_id": "x-pre", "platform": "p", "kernel_release": "a"}
        new = {"runner_id": "x", "platform": "p", "kernel_release": "a"}
        assert fx.field_changes(old, new) == []

    def test_packages_are_flattened(self):
        old = {"packages": {"glibc": "2.34-1", "tuned": "2.20"}}
        new = {"packages": {"glibc": "2.34-2", "tuned": "2.20"}}
        assert fx.field_changes(old, new) == [("packages.glibc", "2.34-1", "2.34-2")]

    def test_none_to_value_is_a_change(self):
        old = {"cpu_microcode": None, "provenance_schema_version": 2}
        new = {"cpu_microcode": "0x1", "provenance_schema_version": 2}
        assert fx.field_changes(old, new) == [("cpu_microcode", None, "0x1")]


class TestEventKind:
    @pytest.mark.parametrize(
        "names, kind",
        [
            (["conductress_revision"], fx.KIND_HARNESS),
            (["boot_id"], fx.KIND_REBOOT),
            (["kernel_release", "conductress_revision"], fx.KIND_KERNEL),
            (["instance_id", "kernel_release"], fx.KIND_HOST),
            (["glibc_version"], fx.KIND_LIBRARY),
            (["packages.microcode_ctl"], fx.KIND_KERNEL),
            (["packages.tuned"], fx.KIND_LIBRARY),
            (["cachecannon_commit"], fx.KIND_GENERATOR),
            (["server_kernel_release"], fx.KIND_KERNEL),
            (["something_new"], fx.KIND_OTHER),
        ],
    )
    def test_kinds(self, names, kind):
        assert fx.event_kind(names, manual=False) == kind

    def test_manual_words_map_to_recorded_fields(self):
        runner = series("r", [1.0] * 4)
        runner.manual_events.append(ManualEvent(runner.observations[1].when + timedelta(hours=1), "", ("glibc",)))
        (event,) = fx.build_events(runner)
        assert event.kind == fx.KIND_LIBRARY
        assert not event.harness_only


class TestBuildEvents:
    def test_changes_in_one_gap_form_one_event(self):
        runner = series("r", [1.0] * 6, env_at={3: {"kernel_release": "k2", "conductress_revision": "rev-2"}})
        events = fx.build_events(runner)
        assert [(e.position, e.date) for e in events] == [(3, "2026-09-04")]
        assert sorted(events[0].field_names) == ["conductress_revision", "kernel_release"]

    def test_manual_event_joins_the_gap_that_contains_it(self):
        runner = series("r", [1.0] * 6, env_at={3: {"kernel_release": "k2"}})
        runner.manual_events.append(ManualEvent(runner.observations[2].when + timedelta(hours=8), "update", ("glibc",)))
        events = fx.build_events(runner)
        assert len(events) == 1 and events[0].position == 3 and len(events[0].manual) == 1

    def test_manual_event_in_a_quiet_gap_makes_its_own_event(self):
        runner = series("r", [1.0] * 6)
        runner.manual_events.append(ManualEvent(runner.observations[1].when + timedelta(hours=8), "bios", ()))
        (event,) = fx.build_events(runner)
        assert event.position == 2 and event.kind == fx.KIND_MANUAL

    def test_manual_event_after_the_last_observation_waits_for_data(self):
        runner = series("r", [1.0] * 3)
        runner.manual_events.append(ManualEvent(runner.observations[-1].when + timedelta(hours=2), "update", ()))
        (event,) = fx.build_events(runner)
        assert event.position == 3 and event.last_seen is None

    def test_manual_event_before_the_first_observation_is_ignored(self):
        runner = series("r", [1.0] * 3)
        runner.manual_events.append(ManualEvent(runner.observations[0].when - timedelta(days=1), "old", ()))
        assert fx.build_events(runner) == []


# --------------------------------------------------------------------------- statistics


class TestStatistics:
    def test_rank_test_reaches_its_exact_minimum(self):
        before = noisy(28, 100.0, 1)
        after = [120.0, 121.0, 122.0]
        p = fx.mann_whitney_p(before, after, np.random.default_rng(1), 40000)
        exact = 2 / math.comb(31, 3)  # two-sided: all three highest
        assert p == pytest.approx(exact, abs=0.0006)

    def test_rank_test_is_deterministic_and_null_is_large(self):
        before, after = noisy(20, 100.0, 2), noisy(10, 100.0, 3)
        p1 = fx.mann_whitney_p(before, after, np.random.default_rng(5), 5000)
        p2 = fx.mann_whitney_p(before, after, np.random.default_rng(5), 5000)
        assert p1 == p2 and p1 > 0.05

    def test_bootstrap_interval_covers_the_true_ratio(self):
        before, after = noisy(28, 100.0, 4), noisy(28, 105.0, 5)
        lo, hi = fx.median_ratio_ci(before, after, np.random.default_rng(1), 5000)
        assert lo < 0.05 < hi and hi - lo < 0.01

    def test_paired_test(self):
        before = noisy(20, 100.0, 6, sd=0.01)
        after = [b * 1.1 for b in before]
        result = fx.paired_test(before, after, np.random.default_rng(1), FAST)
        assert result["effect"] == pytest.approx(0.1, abs=1e-9)
        assert result["ci_lo"] == pytest.approx(0.1, abs=1e-9) and result["same_direction"] == 1.0
        assert result["p"] < 0.001

    def test_benjamini_hochberg(self):
        q = fx.benjamini_hochberg([0.01, 0.04, 0.03, 0.2])
        assert q == pytest.approx([0.04, 0.04 * 4 / 3, 0.04 * 4 / 3, 0.2], abs=1e-12)
        assert fx.benjamini_hochberg([]) == []

    def test_robust_sd(self):
        assert fx.robust_sd([1.0, 2.0, 3.0, 4.0, 100.0]) == pytest.approx(1.4826)


class TestLocateSteps:
    def test_finds_a_step_at_an_event(self):
        scores = noisy(30, 100.0, 7) + noisy(10, 104.0, 8)
        runner = series("r", scores, env_at={30: {"kernel_release": "k2"}, 15: {"conductress_revision": "rev-2"}})
        events = fx.build_events(runner)
        steps = fx.locate_steps("r", scores, events, FAST)
        assert [s.position for s in steps] == [30]
        assert steps[0].effect == pytest.approx(0.04, abs=0.006)

    def test_lists_an_adjacent_event_as_an_alternative_location(self):
        scores = noisy(30, 100.0, 9) + [100.0] + noisy(10, 104.0, 10)
        # Events at 30 and 31; the value at 30 sits between the levels, so either position fits.
        scores[30] = 102.0
        runner = series(
            "r", scores, env_at={30: {"conductress_revision": "rev-2"}, 31: {"conductress_revision": "rev-3"}}
        )
        steps = fx.locate_steps("r", scores, fx.build_events(runner), FAST)
        assert len(steps) == 1
        assert set([steps[0].position] + steps[0].alternatives) == {30, 31}

    def test_no_step_without_events_or_effect(self):
        scores = noisy(40, 100.0, 11)
        runner = series("r", scores, env_at={20: {"conductress_revision": "rev-2"}})
        assert fx.locate_steps("r", scores, fx.build_events(runner), FAST) == []
        assert fx.locate_steps("r", scores, [], FAST) == []


class TestFloorAndClearing:
    def test_short_before_window_takes_the_noise_of_the_after_window(self):
        scores = noisy(30, 100.0, 80, sd=0.01) + noisy(20, 104.0, 81, sd=0.002)
        cuts = [30]
        floor, sd = fx.runner_floor(scores, 33, cuts, FAST)  # 3 in-segment days before, 17 after
        assert sd == pytest.approx(fx.robust_sd(scores[33:50]) / float(np.median(scores[33:50])))
        assert floor == pytest.approx(max(0.005, 3 * sd))

    def test_cleared_with_fewer_days_before_but_never_confirmed(self):
        flat = {
            "enough": False,
            "enough_to_clear": True,
            "floor": 0.008,
            "q": 0.9,
            "p": 0.9,
            "effect": 0.0,
            "ci_lo": -0.004,
            "ci_hi": 0.003,
            "kind": "canary",
            "n_before": 4,
            "n_after": 28,
        }
        assert fx._state(flat, FAST) == fx.STATE_CLEARED
        step = dict(flat, q=0.001, p=0.001, effect=0.03, ci_lo=0.02, ci_hi=0.04)
        assert fx._state(step, FAST) == fx.STATE_INSUFFICIENT
        assert step["reason"] == "4 day(s) before the event, needs 7 to confirm"
        tiny = dict(flat, enough_to_clear=False, n_before=2)
        assert fx._state(tiny, FAST) == fx.STATE_INSUFFICIENT and tiny["reason"].startswith("2 day(s) before")


class TestProposal:
    def runner(self, from_generation: bool) -> RunnerInput:
        scores = noisy(30, 100.0, 90) + noisy(10, 110.0, 91)
        runner = series("rg", scores, env_at={30: {"kernel_release": "k2"}})
        cut = runner.observations[29].when + timedelta(hours=2)
        runner.paired.append(
            PairedSeries(
                "throughput a",
                cut,
                tuple(noisy(8, 100.0, 92)),
                tuple(noisy(8, 110.0, 93)),
                from_generation=from_generation,
            )
        )
        return runner

    def test_confirmed_event_proposes_a_generation_close(self):
        findings, _ = build_findings([self.runner(False)], now=NOW, cfg=FAST)
        top = findings["events"][0]
        assert top["state"] == fx.STATE_CONFIRMED and "close-generation" in top["proposal"]
        assert top["generation_closed"] is False

    def test_no_proposal_when_the_generation_is_already_closed(self):
        findings, _ = build_findings([self.runner(True)], now=NOW, cfg=FAST)
        top = findings["events"][0]
        assert top["state"] == fx.STATE_CONFIRMED and top["proposal"] is None and top["generation_closed"] is True


class TestStates:
    def base(self, **kw):
        test = {
            "enough": True,
            "floor": 0.008,
            "q": 0.5,
            "p": 0.5,
            "effect": 0.0,
            "ci_lo": -0.002,
            "ci_hi": 0.003,
            "kind": "canary",
            "n_before": 28,
            "n_after": 28,
        }
        test.update(kw)
        return test

    def test_insufficient(self):
        assert (
            fx._state(self.base(enough=False, q=0.0, ci_lo=0.2, ci_hi=0.3, effect=0.25), FAST) == fx.STATE_INSUFFICIENT
        )

    def test_confirmed_needs_q_interval_and_floor(self):
        assert fx._state(self.base(q=0.01, effect=0.02, ci_lo=0.015, ci_hi=0.025), FAST) == fx.STATE_CONFIRMED
        # Significant but smaller than the floor: cleared (a small shift).
        assert fx._state(self.base(q=0.01, p=0.001, effect=0.005, ci_lo=0.002, ci_hi=0.007), FAST) == fx.STATE_CLEARED

    def test_cleared_and_suspected_and_wide(self):
        assert fx._state(self.base(), FAST) == fx.STATE_CLEARED
        assert fx._state(self.base(p=0.01, q=0.2, effect=0.012, ci_lo=-0.001, ci_hi=0.02), FAST) == fx.STATE_SUSPECTED
        assert fx._state(self.base(effect=0.001, ci_lo=-0.012, ci_hi=0.011), FAST) == fx.STATE_INSUFFICIENT

    def test_severity(self):
        assert fx._severity({"ci_lo": 0.10, "ci_hi": 0.12, "q": 0.01}) == pytest.approx(9.9)
        assert fx._severity({"ci_lo": -0.01, "ci_hi": 0.12, "q": 0.0}) == 0.0
        assert fx._severity({"ci_lo": float("nan"), "ci_hi": float("nan"), "q": 0.0}) == 0.0


# --------------------------------------------------------------------------- whole documents


NOW = datetime(2026, 11, 9, 12, tzinfo=timezone.utc)


class TestFleet:
    def make(self, bump: float, days_after: int) -> list[RunnerInput]:
        runners = []
        for i, name in enumerate(["ra", "rb", "rc", "rd"]):
            scores = noisy(30, 100.0 + i, 20 + i, sd=0.002)
            scores += [s * (1 + bump) for s in noisy(days_after, 100.0 + i, 40 + i, sd=0.0005)]
            scores += noisy(5, 100.0 + i, 60 + i, sd=0.002)
            runners.append(
                series(
                    name,
                    scores,
                    env_at={30: {"conductress_revision": "rev-2"}, 30 + days_after: {"conductress_revision": "rev-3"}},
                )
            )
        return runners

    def test_one_day_fleet_move_is_suspected_fleet_and_not_open(self):
        findings, _ = build_findings(self.make(0.006, 1), now=NOW, cfg=FAST)
        fleet = [e for e in findings["events"] if e["event_id"].startswith("fleet:")]
        assert fleet[0]["state"] == fx.STATE_SUSPECTED_FLEET
        assert fleet[0]["fleet"]["same_direction"] == "4/4" and fleet[0]["fleet"]["z"] > 3
        assert findings["open_count"] == 0

    def test_no_fleet_row_for_two_runners(self):
        runners = self.make(0.006, 1)[:2]
        findings, _ = build_findings(runners, now=NOW, cfg=FAST)
        assert not [e for e in findings["events"] if e["event_id"].startswith("fleet:")]


class TestPublicOutput:
    def private_runner(self) -> RunnerInput:
        rev_a, rev_b = "a" * 40, "b" * 40
        base = {
            "kernel_release": "k1",
            "conductress_revision": rev_a,
            "boot_id": "11111111-2222-3333-4444-555555555555",
            "instance_id": "i-0123456789abcdef0",
            "host_fingerprint": "0f1e2d3c4b5a6978",
            "provenance_schema_version": 2,
        }
        scores = noisy(30, 100.0, 70) + noisy(10, 105.0, 71)
        runner = series(
            "rp",
            scores,
            base_env=base,
            env_at={
                30: {"conductress_revision": rev_b, "boot_id": "99999999-2222-3333-4444-555555555555"},
                35: {"instance_id": "i-0fedcba9876543210", "host_fingerprint": "0000000000000000"},
            },
        )
        runner.manual_events.append(
            ManualEvent(runner.observations[33].when + timedelta(hours=1), "moved to 10.0.0.9", ("glibc",))
        )
        runner.explanations.append(Explanation(runner.observations[35].utc_date, "see /home/someone/notes"))
        return runner

    def test_no_private_value_reaches_the_documents(self):
        findings, consistency = build_findings([self.private_runner()], now=NOW, cfg=FAST)
        text = json.dumps([findings, consistency])
        for pattern in (
            r"[0-9a-f]{40}",
            r"i-0[0-9a-f]{16}",
            r"\d+\.\d+\.\d+\.\d+",
            r"/home/",
            r"[0-9a-f]{8}-[0-9a-f]{4}-",
            "0f1e2d3c",
        ):
            assert not re.search(pattern, text), pattern
        event = next(e for e in findings["events"] if e["date"] == "2026-10-01")
        labels = {f["label"]: (f["old"], f["new"]) for f in event["fields"]}
        assert labels == {"harness revision changed": (None, None), "reboot": (None, None)}
        assert event["attribution_note"].startswith("2 fields changed together")
        host = next(e for e in findings["events"] if e["date"] == "2026-10-06")
        assert [f["label"] for f in host["fields"]] == ["host replaced"]

    def test_values_only_for_allowlisted_fields(self):
        event = fx.Event(
            "r",
            1,
            "2026-10-01",
            None,
            None,
            [
                ("kernel_release", "k1", "k2"),
                ("packages.glibc", "2.34-1", "2.34-2"),
                ("server_cpu_microcode", "0x1", "0x2"),
                ("a_new_unclassified_field", "secret-1", "secret-2"),
                ("boot_id", "x", "y"),
                ("server_boot_id", "x", "y"),
            ],
            [ManualEvent(NOW, "update", ("glibc", "reboot"))],
        )
        fields = fx._public_changes(event)
        assert {f["label"]: (f["old"], f["new"]) for f in fields} == {
            "kernel": ("k1", "k2"),
            "glibc package": ("2.34-1", "2.34-2"),
            "server microcode": ("0x1", "0x2"),
            "other recorded field changed": (None, None),
            "reboot": (None, None),
            "server reboot": (None, None),
            "glibc": (None, None),
        }
        assert "secret" not in json.dumps(fields)

    def test_documents_are_deterministic_and_json_safe(self):
        runner = self.private_runner()
        a = build_findings([runner], now=NOW, cfg=FAST)
        b = build_findings([runner], now=NOW, cfg=FAST)
        assert json.dumps(a, sort_keys=True, allow_nan=False) == json.dumps(b, sort_keys=True, allow_nan=False)


# --------------------------------------------------------------------------- regression: real back-test


def load_fixture(with_explanations: bool) -> tuple[list[RunnerInput], datetime]:
    doc = json.loads(FIXTURE.read_text())
    runners = []
    for r in doc["runners"]:
        explanations = []
        if with_explanations and r["runner_id"] in doc["explanations"]:
            explanations.append(Explanation(doc["explanations"][r["runner_id"]], "calibration-period harness deploys"))
        runners.append(
            RunnerInput(
                r["runner_id"],
                r["display_name"],
                [
                    Observation(o["utc_date"], o["score"], o["environment"], ts(o["measured_at"]))
                    for o in r["observations"]
                ],
                [ManualEvent(ts(m["at"]), m["label"], tuple(m["fields"]), ts(m["until"])) for m in r["manual_events"]],
                explanations,
                [
                    # The fixture's pairs come from a closed series generation.
                    PairedSeries(p["series"], ts(p["cut"]), tuple(p["before"]), tuple(p["after"]), from_generation=True)
                    for p in r["paired"]
                ],
            )
        )
    return runners, ts(doc["now"])


@pytest.fixture(scope="module")
def backtest_documents():
    runners, now = load_fixture(with_explanations=True)
    return build_findings(runners, now=now)


@pytest.fixture(scope="module")
def backtest(backtest_documents):
    return backtest_documents[0]


@pytest.fixture(scope="module")
def backtest_consistency(backtest_documents):
    return backtest_documents[1]


@pytest.fixture(scope="module")
def backtest_raw():
    runners, now = load_fixture(with_explanations=False)
    return build_findings(runners, now=now)[0]


def by_id(findings):
    return {e["event_id"]: e for e in findings["events"]}


class TestBacktestRegression:
    """The approved back-test outcome on real (anonymized) data must not regress."""

    def test_host_update_with_the_large_step_ranks_first_confirmed(self, backtest):
        top = backtest["events"][0]
        assert top["event_id"] == "runner-b:2026-11-07:0"
        assert top["state"] == fx.STATE_CONFIRMED and top["located_step"]
        assert 11.0 < top["effect_pct"] < 13.5
        assert top["ci_pct"][0] > 11.0 and top["q"] < 0.01
        paired = {t["series"]: t for t in top["tests"] if t["kind"] == "paired"}
        assert paired["throughput GET 16B P1"]["state"] == fx.STATE_CONFIRMED
        assert paired["throughput 80:20 GET/SET 16B P10"]["state"] == fx.STATE_CONFIRMED
        assert 16.0 < paired["throughput 80:20 GET/SET 16B P10"]["effect_pct"] < 19.0
        # The generation is already closed at this event: nothing to propose.
        assert top["generation_closed"] is True and top["proposal"] is None
        assert backtest["open_count"] == 1

    def test_other_runners_stayed_flat_at_that_host_update(self, backtest):
        top = by_id(backtest)["runner-b:2026-11-07:0"]
        assert all(abs(c["z"]) < 3 for c in top["cross_runner"])
        for runner in ("runner-a", "runner-c", "runner-d"):
            assert by_id(backtest)[f"{runner}:2026-11-07:0"]["state"] in (fx.STATE_CLEARED, fx.STATE_INSUFFICIENT)

    def test_the_latest_harness_deploy_is_insufficient_data_everywhere(self, backtest):
        events = by_id(backtest)
        for runner in ("runner-a", "runner-b", "runner-c", "runner-d"):
            event = events[f"{runner}:2026-11-09:0"]
            assert event["kind"] == fx.KIND_HARNESS and event["state"] == fx.STATE_INSUFFICIENT
        fleet = events["fleet:2026-11-09:0"]
        assert fleet["state"] == fx.STATE_INSUFFICIENT and abs(fleet["fleet"]["z"]) < 3

    def test_the_one_day_low_value_is_not_an_event(self, backtest):
        assert not [e for e in backtest["events"] if e["date"] == "2026-11-08"]
        assert by_id(backtest)["runner-a:2026-11-07:0"]["state"] not in fx.OPEN_STATES

    def test_the_one_day_fleet_move_is_suspected_fleet(self, backtest):
        fleet = by_id(backtest)["fleet:2026-11-06:0"]
        assert fleet["state"] == fx.STATE_SUSPECTED_FLEET and fleet["fleet"]["same_direction"] == "4/4"

    def test_september_steps_are_explained(self, backtest, backtest_raw):
        raw = by_id(backtest_raw)
        assert raw["runner-a:2026-09-21:0"]["state"] == fx.STATE_CONFIRMED
        assert raw["runner-a:2026-09-21:0"]["alternative_dates"] == ["2026-09-19", "2026-09-22", "2026-09-23"]
        explained = by_id(backtest)["runner-a:2026-09-21:0"]
        assert explained["state"] == fx.STATE_EXPLAINED
        assert explained["state_before_explanation"] == fx.STATE_CONFIRMED
        assert by_id(backtest)["runner-d:2026-09-19:0"]["state"] == fx.STATE_EXPLAINED
        assert backtest_raw["open_count"] == 2

    def test_consistency_signals(self, backtest_consistency):
        consistency = backtest_consistency
        rows = {r["runner_id"]: r for r in consistency["runners"]}
        assert rows["runner-b"]["streak_end_reason"] == "not enough data since the last step"
        assert rows["runner-c"]["streak_days"] > 40
        assert rows["runner-a"]["missing_days_30d"] == ["2026-10-31", "2026-11-01", "2026-11-02"]
        assert consistency["fleet"]["latest_deploy_date"] == "2026-11-09"

    def test_ranking_order(self, backtest):
        order = [fx.STATE_ORDER[e["state"]] for e in backtest["events"]]
        assert order == sorted(order)

    def test_flat_events_soon_after_a_step_are_cleared(self, backtest):
        events = by_id(backtest)
        # Runner A, 2026-09-25: 4 days after its 2026-09-21 step, 28 days after, -0.06%, CI -0.35..+0.30.
        flat = events["runner-a:2026-09-25:0"]
        assert flat["state"] == fx.STATE_CLEARED
        canary = next(t for t in flat["tests"] if t["kind"] == "canary")
        assert canary["n_before"] == 4 and canary["n_after"] == 28
        assert -0.2 < flat["effect_pct"] < 0.1 and flat["ci_pct"][0] > -0.5 and flat["ci_pct"][1] < 0.5
        # Early-history events with 5 or 6 days before them and a flat interval.
        for event_id in (
            "runner-a:2026-09-05:0",
            "runner-b:2026-09-04:0",
            "runner-b:2026-09-05:0",
            "runner-c:2026-09-04:0",
            "runner-c:2026-09-05:0",
            "runner-d:2026-09-04:0",
            "runner-d:2026-09-05:0",
        ):
            assert events[event_id]["state"] == fx.STATE_CLEARED, event_id

    def test_every_insufficient_event_says_why(self, backtest):
        insufficient = [
            e
            for e in backtest["events"]
            if e["state"] == fx.STATE_INSUFFICIENT and not e["event_id"].startswith("fleet:")
        ]
        assert insufficient and all(e["reason"] for e in insufficient)
        assert by_id(backtest)["runner-c:2026-11-09:0"]["reason"] == "1 day(s) after the event, needs 3"
        assert by_id(backtest)["runner-a:2026-09-23:0"]["reason"] == "2 day(s) before the event, needs 3"
        assert backtest["state_counts"][fx.STATE_INSUFFICIENT] == INSUFFICIENT_COUNT

    @pytest.mark.parametrize(
        "event_id, n_before, effect_range",
        [
            # The rows of the first live review (page rank, runner, date), all flat and inside the floor.
            ("runner-a:2026-09-25:0", 4, (-0.2, 0.1)),  # #9: -0.06%, CI -0.35..+0.30, 28 days after
            ("runner-c:2026-09-05:0", 6, (-0.1, 0.1)),  # #13: 0.00%, CI -0.36..+0.20
            ("runner-b:2026-09-02:0", 3, (-0.1, 0.1)),  # #22: -0.03%, CI -0.22..+0.28, 3 days of history
            ("runner-a:2026-09-02:0", 3, (-0.5, -0.2)),  # #2: -0.37%, CI -0.56..-0.08, floor 1.26%
        ],
    )
    def test_review_cases_are_cleared(self, backtest, event_id, n_before, effect_range):
        event = by_id(backtest)[event_id]
        canary = next(t for t in event["tests"] if t["kind"] == "canary")
        assert canary["n_before"] == n_before and canary["n_after"] >= 19
        assert effect_range[0] < event["effect_pct"] < effect_range[1]
        assert -event["floor_pct"] <= event["ci_pct"][0] and event["ci_pct"][1] <= event["floor_pct"]
        assert event["state"] == fx.STATE_CLEARED, event_id

    def test_one_day_before_has_no_interval_and_stays_insufficient(self, backtest):
        # A day right after a located step (runner A 09-22) or the second day of the history
        # (runner C 08-31): one value before cannot give an interval.
        for event_id in ("runner-a:2026-09-22:0", "runner-c:2026-08-31:0"):
            event = by_id(backtest)[event_id]
            assert event["ci_pct"] == [None, None]
            assert event["state"] == fx.STATE_INSUFFICIENT
            assert event["reason"] == "1 day(s) before the event, needs 3"

    def test_explained_events_carry_no_proposal(self, backtest):
        assert by_id(backtest)["runner-a:2026-09-21:0"]["proposal"] is None
