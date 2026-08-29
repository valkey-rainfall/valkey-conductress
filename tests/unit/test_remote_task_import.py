import json
from datetime import datetime
from pathlib import Path

import pytest

from conductress import task_queue
from conductress.task_queue import BaseTaskData, TaskQueue

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests" / "fixtures" / "golden_tasks" / "PerfTaskData.json"


@pytest.fixture(autouse=True)
def valid_source(monkeypatch):
    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])


def task_document():
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def test_from_dict_does_not_mutate_document():
    document = task_document()
    original = dict(document)
    task = BaseTaskData.from_dict(document)
    assert document == original
    assert task.timestamp == datetime.fromisoformat(document["timestamp"])


def test_atomic_import_is_idempotent_and_rejects_conflicting_content(tmp_path):
    queue = TaskQueue(tmp_path)
    document = task_document()
    task = queue.import_task(document)
    path = queue.task_path(task.task_id)
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == document
    assert not list(tmp_path.glob("*.tmp"))

    replay = queue.import_task(document)
    assert replay.task_id == task.task_id

    changed = {**document, "note": "different"}
    with pytest.raises(ValueError, match="different content"):
        queue.import_task(changed)
