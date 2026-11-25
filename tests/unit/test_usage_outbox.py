"""Runner side of usage accounting: the outbox, the mailbox hooks, and how the runner labels a local task."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from conductress.fleet_client import FleetClientError
from conductress.runner_mailbox import RunnerMailbox
from conductress.task_runner import TaskRunner
from conductress.usage_outbox import UsageOutbox


def _record(task_id):
    return {"schema_version": 1, "task_id": task_id, "task_class": "local"}


def test_outbox_keeps_a_record_until_it_is_delivered(tmp_path):
    outbox = UsageOutbox(tmp_path / "outbox.json")
    outbox.add(_record("a"))
    outbox.add(_record("b"))

    def offline(_record):
        raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)

    assert outbox.flush(offline) == 0
    assert [r["task_id"] for r in outbox.pending()] == ["a", "b"]

    delivered = []
    assert outbox.flush(delivered.append) == 2
    assert [r["task_id"] for r in delivered] == ["a", "b"]
    assert outbox.pending() == []


def test_outbox_stops_at_the_first_failure_and_keeps_order(tmp_path):
    outbox = UsageOutbox(tmp_path / "outbox.json")
    for task_id in ("a", "b", "c"):
        outbox.add(_record(task_id))
    calls = []

    def fail_on_b(record):
        calls.append(record["task_id"])
        if record["task_id"] == "b":
            raise RuntimeError("boom")

    assert outbox.flush(fail_on_b) == 1
    assert calls == ["a", "b"]
    assert [r["task_id"] for r in outbox.pending()] == ["b", "c"]


def test_outbox_drops_the_oldest_records_past_its_cap(tmp_path):
    outbox = UsageOutbox(tmp_path / "outbox.json", max_pending=2)
    for task_id in ("a", "b", "c"):
        outbox.add(_record(task_id))
    assert [r["task_id"] for r in outbox.pending()] == ["b", "c"]


def test_outbox_survives_a_damaged_file(tmp_path):
    path = tmp_path / "outbox.json"
    path.write_text("{not json", encoding="utf-8")
    outbox = UsageOutbox(path)
    assert outbox.pending() == []
    outbox.add(_record("a"))
    assert json.loads(path.read_text(encoding="utf-8"))[0]["task_id"] == "a"


class FakeUsageClient:
    def __init__(self, *, failures=0):
        self.failures = failures
        self.posted = []

    def post_usage(self, runner_id, record):
        if self.failures:
            self.failures -= 1
            raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)
        self.posted.append((runner_id, record))
        return {"stored": True}


def _mailbox(tmp_path, client, mode="live"):
    return RunnerMailbox(tmp_path / "journal.json", mode=mode, client_factory=lambda: client)


def _task(task_id="local-1"):
    return SimpleNamespace(task_id=task_id)


def test_live_mailbox_posts_usage_of_a_local_task(tmp_path):
    client = FakeUsageClient()
    mailbox = _mailbox(tmp_path, client)
    mailbox.record_local_usage(_task(), task_class="sweep", started_at="2026-08-29T00:00:00Z", outcome="completed")
    runner_id, record = client.posted[0]
    assert runner_id == mailbox.runner_id
    assert (record["task_id"], record["task_class"], record["outcome"]) == ("local-1", "sweep", "completed")
    assert record["started_at"] == "2026-08-29T00:00:00Z" and record["ended_at"].endswith("Z")
    assert mailbox.usage_outbox.pending() == []


def test_usage_is_posted_later_when_the_control_service_was_unreachable(tmp_path):
    client = FakeUsageClient(failures=1)
    mailbox = _mailbox(tmp_path, client)
    mailbox.record_local_usage(_task("a"), task_class="local", started_at="2026-08-29T00:00:00Z", outcome="failed")
    assert client.posted == []
    assert [r["task_id"] for r in mailbox.usage_outbox.pending()] == ["a"]

    mailbox.record_local_usage(_task("b"), task_class="local", started_at="2026-08-29T00:05:00Z", outcome="completed")
    assert [record["task_id"] for _, record in client.posted] == ["a", "b"]
    assert mailbox.usage_outbox.pending() == []


def test_a_missing_token_keeps_the_record_and_does_not_raise(tmp_path):
    def no_token():
        raise FleetClientError("RUNNER_TOKEN_MISSING", "set a token", 2)

    mailbox = RunnerMailbox(tmp_path / "journal.json", mode="live", client_factory=no_token)
    mailbox.record_local_usage(_task(), task_class="local", started_at="2026-08-29T00:00:00Z", outcome="completed")
    assert len(mailbox.usage_outbox.pending()) == 1


@pytest.mark.parametrize("mode", ["off", "shadow"])
def test_only_a_live_mailbox_posts_usage(tmp_path, mode):
    client = FakeUsageClient()
    mailbox = _mailbox(tmp_path, client, mode=mode)
    mailbox.record_local_usage(_task(), task_class="local", started_at="2026-08-29T00:00:00Z", outcome="completed")
    assert client.posted == []
    assert mailbox.usage_outbox.pending() == []


def test_runner_labels_a_hand_queued_task_local_and_a_scheduled_one_sweep(monkeypatch):
    runner = TaskRunner()
    queue = MagicMock()
    hand_queued, scheduled = object(), object()

    queue.get_next_task.return_value = hand_queued
    assert runner._choose_next(queue) is hand_queued
    assert runner._local_task_class == "local"

    monkeypatch.setattr(runner, "_schedule_next", lambda: None)
    queue.get_next_task.side_effect = [None, scheduled]
    assert runner._choose_next(queue) is scheduled
    assert runner._local_task_class == "sweep"

    queue.get_next_task.side_effect = [None, None]
    assert runner._choose_next(queue) is None
