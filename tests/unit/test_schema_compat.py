"""Compatibility contracts for current tasks and future fleet documents."""

import json
from pathlib import Path

import pytest

from conductress.task_queue import BaseTaskData

ROOT = Path(__file__).resolve().parents[2]
GOLDEN_DIR = ROOT / "tests" / "fixtures" / "golden_tasks"
SCHEMA_DIR = ROOT / "schemas"
EXPECTED_TASK_TYPES = {
    "PerfTaskData",
    "MemTaskData",
    "MixedTaskData",
    "ScenarioTaskData",
    "LatencyTaskData",
    "CachecannonTaskData",
}


@pytest.mark.parametrize("fixture", sorted(GOLDEN_DIR.glob("*.json")), ids=lambda path: path.stem)
def test_golden_task_round_trip_is_byte_compatible(tmp_path, fixture, monkeypatch):
    from conductress import task_queue

    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
    original = fixture.read_text(encoding="utf-8")
    task = BaseTaskData.from_file(fixture)
    output = tmp_path / fixture.name

    task.save_to_file(output)

    assert output.read_text(encoding="utf-8") == original
    assert task.task_type == fixture.stem


def test_golden_fixtures_cover_every_registered_production_task():
    assert {path.stem for path in GOLDEN_DIR.glob("*.json")} == EXPECTED_TASK_TYPES


@pytest.mark.parametrize("schema", sorted(SCHEMA_DIR.glob("*.schema.json")), ids=lambda path: path.stem)
def test_schema_documents_are_versioned_and_closed_at_top_level(schema):
    document = json.loads(schema.read_text(encoding="utf-8"))

    assert document["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert document["$id"].endswith("-v1.json")
    assert document["properties"]["schema_version"] == {"const": 1}
    assert document["additionalProperties"] is False
