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


class TestStates:
    def base(self, **kw):
        test = {"enough": True, "floor": 0.008, "q": 0.5, "p": 0.5, "effect": 0.0, "ci_lo": -0.002, "ci_hi": 0.003}
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
            "host_fingerprint": "c4c46be36d50f57d",
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
            "c4c46be3",
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
                [PairedSeries(p["series"], ts(p["cut"]), tuple(p["before"]), tuple(p["after"])) for p in r["paired"]],
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
        assert top["proposal"] and "close-generation" in top["proposal"]
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
