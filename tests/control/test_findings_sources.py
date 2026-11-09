"""Tests for gathering and publishing the canary findings on the data host."""

from __future__ import annotations

import json
import os
import stat
from datetime import datetime, timedelta, timezone

import pytest

from conductress.control.config import ControlConfig
from conductress.control.db import ControlDatabase
from conductress.control.findings_sources import load_observations, load_runner_inputs, load_sweep_inputs
from conductress.control.fleet_registry import FleetRegistry
from conductress.control.service import ControlService

PLATFORM = "x86_64/test-cpu/test-family"


def manifest():
    return {
        "schema_version": 1,
        "runners": [
            {
                "runner_id": "runner-x",
                "display_name": "Runner X",
                "platform": PLATFORM,
                "platform_aliases": ["testx"],
                "enabled": True,
                "canary_profile": "throughput-get-v1",
                "status_ttl_seconds": 900,
            },
            {
                "runner_id": "runner-x-pre-2026-10-11",
                "display_name": "Runner X (until 2026-10-10)",
                "platform": PLATFORM,
                "platform_aliases": [],
                "enabled": False,
                "archived": True,
                "archive_of": "runner-x",
                "archived_until": "2026-10-10",
                "archive_note": "host update",
                "canary_profile": "throughput-get-v1",
                "status_ttl_seconds": 900,
            },
            {
                "runner_id": "runner-y",
                "display_name": "Runner Y",
                "platform": "arm/test",
                "platform_aliases": [],
                "enabled": True,
                "canary_profile": None,
                "status_ttl_seconds": 900,
            },
        ],
    }


def insert(database, runner_id, day, score, env, *, accepted=1, profile_version=1):
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO canary_observations(runner_id, profile_id, profile_version, utc_date, task_id, score, "
            "completed_at, phase, accepted, environment_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                runner_id,
                "throughput-get-v1",
                profile_version,
                day,
                f"canary:{runner_id}:throughput-get-v1:{day}",
                score if accepted else None,
                f"{day}T06:40:00Z",
                "ready",
                accepted,
                json.dumps(env),
                f"{day}T06:41:00Z",
            ),
        )


@pytest.fixture
def setup(tmp_path):
    database = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
    database.initialize()
    registry = FleetRegistry(manifest())
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    for i in range(50):
        day = (start + timedelta(days=i)).date().isoformat()
        runner = "runner-x-pre-2026-10-11" if day <= "2026-10-10" else "runner-x"
        kernel = "k1" if day <= "2026-10-10" else "k2"
        score = 1000.0 + (i % 3) if kernel == "k1" else 1100.0 + (i % 3)
        env = {"runner_id": runner, "kernel_release": kernel, "provenance_schema_version": 2}
        insert(database, runner, day, score, env)
    insert(database, "runner-x", "2026-10-25", 5.0, {"kernel_release": "k2"}, accepted=0)  # rejected: ignored
    sweep = tmp_path / "www"
    sweep.mkdir()
    (sweep / "manifest-testx.epoch-v3.json").write_text(
        json.dumps(
            {
                "platform": "testx",
                "throughput_workloads": ["get-a"],
                "environment_events": [
                    {
                        "at": "2026-10-10T16:00:00Z",
                        "until": "2026-10-10T18:00:00Z",
                        "label": "update",
                        "fields": ["glibc"],
                    }
                ],
                "explanations": [{"event_date": "2026-10-11", "text": "known host update"}],
                "generations": [
                    {"id": "2026-10-10", "epoch": "v3", "until": "2026-10-10T16:00:00Z", "workloads": ["get-a"]}
                ],
            }
        )
    )
    # A closed generation's own manifest is not a live manifest.
    (sweep / "manifest-testx.epoch-v3.gen-2026-10-10.json").write_text(json.dumps({"platform": "testx"}))

    def points(scale):
        return [{"commit": f"c{i}", "results": {"get-a": {"rps": 100.0 * scale + i}}} for i in range(8)]

    (sweep / "series-testx-get-a-throughput.epoch-v3.json").write_text(
        json.dumps({"metadata": {"platform": PLATFORM, "workload": "get-a"}, "points": points(1.1)[:6]})
    )
    (sweep / "series-testx-get-a-throughput.epoch-v3.gen-2026-10-10.json").write_text(
        json.dumps({"metadata": {"platform": PLATFORM, "workload": "get-a"}, "points": points(1.0)})
    )
    return database, registry, sweep


def test_observations_stitch_the_archive_and_skip_rejected(setup):
    database, _, _ = setup
    obs = load_observations(database, ["runner-x", "runner-x-pre-2026-10-11"], "throughput-get-v1")
    assert len(obs) == 50
    assert obs[0].utc_date == "2026-09-01" and obs[-1].utc_date == "2026-10-20"
    assert obs[0].environment["profile_version"] == 1
    assert obs[0].measured_at == datetime(2026, 9, 1, 6, 40, tzinfo=timezone.utc)
    assert load_observations(database, [], "throughput-get-v1") == []


def test_sweep_inputs_by_platform(setup):
    _, _, sweep = setup
    inputs = load_sweep_inputs(sweep)
    assert list(inputs) == [PLATFORM]
    entry = inputs[PLATFORM]
    (event,) = entry["manual_events"].values()
    assert event.fields == ("glibc",) and event.until == datetime(2026, 10, 10, 18, tzinfo=timezone.utc)
    assert list(entry["explanations"]) == ["2026-10-11"]
    (paired,) = entry["paired"]
    assert paired.series == "throughput get-a" and len(paired.before) == 6
    assert paired.after[0] / paired.before[0] == pytest.approx(1.1)
    assert load_sweep_inputs(None) == {} and load_sweep_inputs(sweep / "missing") == {}


def test_runner_inputs_and_findings(setup):
    database, registry, sweep = setup
    (runner,) = load_runner_inputs(database, registry, sweep)  # runner-y has no canary profile
    assert runner.runner_id == "runner-x" and len(runner.observations) == 50
    service = ControlService(database, registry, published_canary_dir=sweep)
    findings, consistency = service.canary_findings(now=datetime(2026, 10, 21, tzinfo=timezone.utc))
    event = next(e for e in findings["events"] if e["event_id"] == "runner-x:2026-10-11:0")
    assert event["state"] == "explained" and event["state_before_explanation"] == "confirmed"
    assert event["kind"] == "kernel or firmware"
    assert {t["kind"] for t in event["tests"]} == {"canary", "paired"}
    assert consistency["runners"][0]["runner_id"] == "runner-x"


def test_publish_writes_public_files_and_throttles(setup):
    database, registry, sweep = setup
    service = ControlService(database, registry, published_canary_dir=sweep, findings_interval_seconds=3600)
    assert service.publish_canary_findings_if_due() is True
    for name in ("findings.json", "consistency.json"):
        path = sweep / "canary" / name
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o644
        assert json.loads(path.read_text())["schema_version"] == 1
    assert service.publish_canary_findings_if_due() is False  # within the interval
    service._findings_published_at -= 3601
    assert service.publish_canary_findings_if_due() is True


def test_publish_is_skipped_without_a_directory_or_while_running(setup):
    database, registry, sweep = setup
    assert ControlService(database, registry).publish_canary_findings() is False
    service = ControlService(database, registry, published_canary_dir=sweep)
    service._findings_lock.acquire()
    try:
        assert service.publish_canary_findings() is False
    finally:
        service._findings_lock.release()


def test_publish_failure_is_logged_not_raised(setup, monkeypatch, caplog):
    database, registry, sweep = setup
    service = ControlService(database, registry, published_canary_dir=sweep)
    monkeypatch.setattr(service, "canary_findings", lambda now=None: (_ for _ in ()).throw(RuntimeError("boom")))
    assert service.publish_canary_findings() is False
    assert "unable to publish canary findings" in caplog.text
    assert service.publish_canary_findings_if_due() is False  # the failed run also waits for the interval


def test_config_reads_the_sweep_dir_and_interval(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISHED_CANARY_DIR", str(tmp_path / "web"))
    monkeypatch.delenv("PUBLISHED_SWEEP_DIR", raising=False)
    monkeypatch.delenv("FINDINGS_INTERVAL_SECONDS", raising=False)
    config = ControlConfig.from_env()
    assert config.published_sweep_dir == tmp_path / "web" and config.findings_interval_seconds == 3600
    monkeypatch.setenv("PUBLISHED_SWEEP_DIR", str(tmp_path / "sweep"))
    monkeypatch.setenv("FINDINGS_INTERVAL_SECONDS", "30")
    config = ControlConfig.from_env()
    assert config.published_sweep_dir == tmp_path / "sweep"
    with pytest.raises(ValueError, match="findings_interval_seconds"):
        config.validate()


def _write_confirm(sweep, runs):
    (sweep / "confirm-testx.epoch-v3.json").write_text(
        json.dumps({"schema_version": 1, "metadata": {"epoch": "v3", "platform": PLATFORM}, "runs": runs})
    )


def _confirm_run(status="confirmed", ended="2026-10-11T12:00:00Z", pairs=None):
    return {
        "run_id": "confirm-20261011T080000Z",
        "event_at": "2026-10-10T17:00:00Z",
        "label": "kernel: k1 -> k2",
        "trigger": "automatic",
        "budget": 8,
        "status": status,
        "started_at": "2026-10-11T08:00:00Z",
        "ended_at": ended,
        "pairs": (
            pairs
            if pairs is not None
            else [
                {"workload": f"get-{i}", "metric": "throughput", "commit": f"c{i}", "before": 100.0, "after": 110.0}
                for i in range(3)
            ]
            + [{"workload": "get-9", "metric": "throughput", "commit": "c9", "before": 100.0, "after": None}]
        ),
    }


def test_confirmation_runs_are_read_as_paired_series(setup):
    _, _, sweep = setup
    _write_confirm(sweep, [_confirm_run(), {"event_at": "bad"}, "junk"])
    entry = load_sweep_inputs(sweep)[PLATFORM]
    confirm = [p for p in entry["paired"] if p.series == "confirmation run"]
    (paired,) = confirm
    assert paired.cut == datetime(2026, 10, 10, 17, tzinfo=timezone.utc)
    assert paired.before == (100.0, 100.0, 100.0) and paired.after == (110.0, 110.0, 110.0)
    assert not paired.lower_is_better
    (run,) = entry["confirmation_runs"]
    assert (run.date, run.pairs, run.status) == ("2026-10-11", 3, "confirmed")


def test_a_run_without_measured_pairs_gives_no_paired_series(setup):
    _, _, sweep = setup
    _write_confirm(sweep, [_confirm_run(status="running", ended=None, pairs=[])])
    entry = load_sweep_inputs(sweep)[PLATFORM]
    assert not [p for p in entry["paired"] if p.series == "confirmation run"]
    (run,) = entry["confirmation_runs"]
    assert (run.date, run.pairs, run.status) == ("2026-10-11", 0, "running")  # started_at date


def test_last_confirmation_run_fills_the_consistency_panel(setup):
    database, registry, sweep = setup
    service = ControlService(database, registry, published_canary_dir=sweep)
    _, consistency = service.canary_findings(now=datetime(2026, 10, 21, tzinfo=timezone.utc))
    assert consistency["runners"][0]["last_confirmation_run"] is None
    older = dict(_confirm_run(status="cleared", ended="2026-09-20T10:00:00Z"), run_id="confirm-20260920T080000Z")
    _write_confirm(sweep, [older, _confirm_run()])
    findings, consistency = service.canary_findings(now=datetime(2026, 10, 21, tzinfo=timezone.utc))
    assert consistency["runners"][0]["last_confirmation_run"] == {
        "date": "2026-10-11",
        "pairs": 3,
        "status": "confirmed",
    }
    event = next(e for e in findings["events"] if e["event_id"] == "runner-x:2026-10-11:0")
    assert "confirmation run" in {t["series"] for t in event["tests"]}
