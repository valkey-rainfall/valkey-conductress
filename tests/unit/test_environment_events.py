"""Tests for the runner's operator notes (manual environment events and explanations)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from conductress.public_fields import WITHHELD
from conductress.sweep import environment_events as ee
from conductress.sweep import exporter

AT = datetime(2026, 11, 6, 16, 14, 23, tzinfo=timezone.utc)
UNTIL = datetime(2026, 11, 6, 18, 21, 58, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 9, tzinfo=timezone.utc)


def test_add_event_writes_and_validates(tmp_path):
    path = tmp_path / "environment_events.json"
    entry = ee.add_event(
        at=AT, until=UNTIL, label=" package  update ", fields=["glibc", "Microcode_ctl"], path=path, now=NOW
    )
    assert entry == {
        "at": "2026-11-06T16:14:23Z",
        "until": "2026-11-06T18:21:58Z",
        "label": "package update",
        "fields": ["glibc", "microcode_ctl"],
    }
    stored = json.loads(path.read_text())
    assert stored["events"][0]["recorded_by"] == "operator" and stored["schema_version"] == 1
    with pytest.raises(ValueError, match="already recorded"):
        ee.add_event(at=AT, label="package update", path=path)
    with pytest.raises(ValueError, match="IPv4"):
        ee.add_event(at=AT, label="moved to 10.0.0.1", path=path)
    with pytest.raises(ValueError, match="before"):
        ee.add_event(at=UNTIL, until=AT, label="x", path=path)
    with pytest.raises(ValueError, match="field"):
        ee.add_event(at=AT, label="y", fields=["no/slash"], path=path)


def test_dry_run_writes_nothing(tmp_path):
    path = tmp_path / "environment_events.json"
    ee.add_event(at=AT, label="update", path=path, dry_run=True)
    ee.add_explanation(event_date="2026-11-07", text="ok", path=path, dry_run=True)
    assert not path.exists()


def test_explanations_replace_by_date(tmp_path):
    path = tmp_path / "environment_events.json"
    ee.add_explanation(event_date="2026-09-21", text="first", path=path, now=NOW)
    ee.add_explanation(event_date="2026-09-21", text="calibration-period harness deploys", path=path, now=NOW)
    (only,) = ee.load_document(path)["explanations"]
    assert only["text"] == "calibration-period harness deploys"
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        ee.add_explanation(event_date="21.09.2026", text="x", path=path)
    with pytest.raises(ValueError, match="path"):
        ee.add_explanation(event_date="2026-09-22", text="see /home/someone/x", path=path)


def test_legacy_list_form_is_read(tmp_path):
    path = tmp_path / "environment_events.json"
    path.write_text(json.dumps([{"at": "2026-11-01T00:00:00Z", "label": "bios"}]))
    assert ee.load_document(path)["events"][0]["label"] == "bios"
    assert exporter.load_environment_events(path)[0]["at"] == datetime(2026, 11, 1, tzinfo=timezone.utc)
    assert ee.load_document(tmp_path / "missing.json") == {"schema_version": 1, "events": [], "explanations": []}


def test_manifest_notes_drop_the_recorder_and_check_text(tmp_path):
    path = tmp_path / "environment_events.json"
    path.write_text(
        json.dumps(
            {
                "events": [
                    {"at": "2026-11-06T16:14:23Z", "label": "update", "fields": ["glibc"], "recorded_by": "someone"},
                    {"at": "2026-11-07T00:00:00Z", "label": "host 10.1.1.1", "recorded_by": "someone"},
                    {"at": "not a time", "label": "bad"},
                ],
                "explanations": [{"event_date": "2026-09-21", "text": "known", "recorded_by": "someone"}],
            }
        )
    )
    events, explanations = ee.manifest_notes(path)
    assert events == [
        {"at": "2026-11-06T16:14:23Z", "label": "update", "fields": ["glibc"]},
        {"at": "2026-11-07T00:00:00Z", "label": WITHHELD},
    ]
    assert explanations == [{"event_date": "2026-09-21", "text": "known"}]
    path.write_text("{broken")
    assert ee.manifest_notes(path) == ([], [])


def test_export_manifest_publishes_notes_and_no_recorder(tmp_path):
    generation = {
        "id": "2026-11-06",
        "epoch": "v3",
        "until": "2026-11-06T16:14:23Z",
        "label": "kernel and glibc",
        "reason": "canary step",
        "workloads": ["get-a"],
        "closed_by": "someone",
        "closed_at": "2026-11-08T07:47:49Z",
    }
    with patch("conductress.publisher.detect_platform", return_value=("testx", "label")):
        exporter.export_manifest(
            tmp_path,
            platforms=["testx"],
            workloads=[("get-a", "throughput")],
            epoch_id="v3",
            generations=[generation],
            environment_events=[{"at": "2026-11-06T16:14:23Z", "label": "update"}],
            explanations=[{"event_date": "2026-09-21", "text": "known"}],
        )
        exporter.export_manifest(
            tmp_path, platforms=["testx"], workloads=[("get-a", "throughput")], epoch_id="v3", generation=generation
        )
    live = json.loads((tmp_path / "manifest-testx.epoch-v3.json").read_text())
    assert "closed_by" not in live["generations"][0] and live["generations"][0]["closed_at"]
    assert live["environment_events"] == [{"at": "2026-11-06T16:14:23Z", "label": "update"}]
    assert live["explanations"] == [{"event_date": "2026-09-21", "text": "known"}]
    archived = json.loads((tmp_path / "manifest-testx.epoch-v3.gen-2026-11-06.json").read_text())
    assert "closed_by" not in archived["generation"]
    assert archived["environment_events"] == [] and archived["explanations"] == []
