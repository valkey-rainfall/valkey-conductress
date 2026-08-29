import json
from pathlib import Path

import pytest

from conductress import task_queue
from conductress.task_envelope import build_task_envelope, serialize_task
from conductress.task_queue import BaseTaskData

ROOT = Path(__file__).resolve().parents[2]
GOLDEN_DIR = ROOT / "tests" / "fixtures" / "golden_tasks"


@pytest.mark.parametrize("fixture", sorted(GOLDEN_DIR.glob("*.json")), ids=lambda path: path.stem)
def test_all_golden_tasks_build_versioned_remote_envelopes(fixture, monkeypatch):
    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
    task = BaseTaskData.from_file(fixture)
    envelope = build_task_envelope(
        task,
        runner_id="armbench",
        priority=125,
        submitted_by="rain",
    )

    assert envelope["schema_version"] == 1
    assert envelope["task_id"] == task.task_id
    assert envelope["runner_id"] == "armbench"
    assert envelope["task_class"] == "manual"
    assert envelope["priority"] == 125
    assert envelope["submitted_by"] == "rain"
    assert envelope["task"] == json.loads(fixture.read_text(encoding="utf-8"))
    assert serialize_task(task) == envelope["task"]


def test_submitter_falls_back_when_user_lookup_fails(monkeypatch):
    from conductress.task_envelope import _default_submitter

    def fail_user_lookup():
        raise KeyError("no user")

    monkeypatch.setattr("conductress.task_envelope.getpass.getuser", fail_user_lookup)
    assert _default_submitter() == "unknown"
