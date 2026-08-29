import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from conductress import task_queue
from conductress.fleet_client import FleetClientError
from conductress.runner_mailbox import RunnerMailbox
from conductress.task_queue import TaskQueue

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests" / "fixtures" / "golden_tasks" / "PerfTaskData.json"


def inner_task():
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def claim_document():
    task = inner_task()
    task_id = "2026.08.29_00.00.00.123456"
    envelope = {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": "armbench",
        "task_class": "manual",
        "priority": 100,
        "submitted_at": "2026-08-29T00:00:00Z",
        "submitted_by": "rain",
        "canary_id": None,
        "task": task,
    }
    return {
        "schema_version": 1,
        "claim": {
            "task": {"task_id": task_id, "runner_id": "armbench", "envelope": envelope},
            "claim_token": "claim-token",
            "lease_expires": "2026-08-29T00:05:00Z",
        },
    }


class FakeRunnerClient:
    def __init__(self, *, accept_failures=0, outcome_failures=0):
        self.claims = [claim_document()]
        self.accept_failures = accept_failures
        self.outcome_failures = outcome_failures
        self.accepted = []
        self.outcomes = []
        self.statuses = []

    def claim_task(self, runner_id):
        assert runner_id == "armbench"
        return self.claims.pop(0) if self.claims else None

    def accept_task(self, task_id, claim_token):
        if self.accept_failures:
            self.accept_failures -= 1
            raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)
        self.accepted.append((task_id, claim_token))
        return {"schema_version": 1, "task": {"task_id": task_id}, "changed": True}

    def report_outcome(self, task_id, outcome):
        if self.outcome_failures:
            self.outcome_failures -= 1
            raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)
        self.outcomes.append((task_id, outcome))
        return {"schema_version": 1, "task": {"task_id": task_id}, "changed": True}

    def push_status(self, runner_id, status):
        self.statuses.append((runner_id, status))
        return {"schema_version": 1, "updated": True}


@pytest.fixture(autouse=True)
def mailbox_environment(monkeypatch, tmp_path):
    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
    monkeypatch.setattr(
        "conductress.runner_mailbox.get_runner_config",
        lambda: SimpleNamespace(runner_id="armbench"),
    )
    monkeypatch.setattr("conductress.runner_mailbox.CONDUCTRESS_OUTPUT", tmp_path / "output.jsonl")
    monkeypatch.setattr("conductress.runner_mailbox.CONDUCTRESS_FAILED_LOG", tmp_path / "failed.jsonl")


def make_mailbox(tmp_path, client, mode="live"):
    return RunnerMailbox(
        tmp_path / "delivery.json",
        mode=mode,
        client_factory=lambda: client,
    )


def test_live_claim_import_accept_and_complete(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")

    task = mailbox.poll(queue)
    assert task is not None
    assert queue.has_task(task.task_id)
    assert mailbox.journal.active["stage"] == "accepted"
    assert client.accepted == [(task.task_id, "claim-token")]

    result = {
        "task_id": task.task_id,
        "method": "perf-get",
        "score": 123,
        "commit_hash": "abc123",
        "end_time": "2026.08.29_00.01.00.000000",
    }
    mailbox.stage_success(task, result=result)
    queue.finish_task(task)
    assert mailbox.flush_pending_outcome() is True
    assert mailbox.journal.active is None
    assert client.outcomes[0][1]["state"] == "completed"
    assert mailbox.status()["imported_count_total"] == 1


def test_imported_task_never_executes_before_acceptance(tmp_path):
    client = FakeRunnerClient(accept_failures=1)
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")

    assert mailbox.poll(queue) is None
    assert mailbox.blocks_execution() is True
    assert queue.get_queue_length() == 1

    task = mailbox.poll(queue)
    assert task is not None
    assert mailbox.blocks_execution() is False
    assert mailbox.journal.active["stage"] == "accepted"


def test_pending_outcome_survives_failure_and_restart(tmp_path):
    client = FakeRunnerClient(outcome_failures=1)
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    mailbox.stage_failure(task, "boom")
    queue.finish_task(task)

    assert mailbox.flush_pending_outcome() is False
    assert mailbox.journal.active["stage"] == "outcome_pending"

    restarted = make_mailbox(tmp_path, client)
    assert restarted.reconcile(queue) is None
    assert restarted.journal.active is None
    assert client.outcomes[0][1]["state"] == "failed"


def test_restart_detects_existing_result_without_reexecution(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    output = tmp_path / "output.jsonl"
    output.write_text(json.dumps({"task_id": task.task_id, "method": "perf-get", "score": 1}) + "\n")

    restarted = make_mailbox(tmp_path, client)
    assert restarted.reconcile(queue) is None
    assert not queue.has_task(task.task_id)
    assert restarted.journal.active is None
    assert len(client.outcomes) == 1


def test_shadow_mode_pushes_status_but_never_claims(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client, mode="shadow")
    queue = TaskQueue(tmp_path / "queue")
    assert mailbox.poll(queue) is None
    assert len(client.claims) == 1
    assert mailbox.push_status({"schema_version": 1, "runner_id": "armbench"}) is True
    assert len(client.statuses) == 1


def test_irrecoverably_expired_claim_discards_local_import(tmp_path):
    client = FakeRunnerClient()

    def reject_accept(_task_id, _claim_token):
        raise FleetClientError("CLAIM_EXPIRED", "expired", 4)

    client.accept_task = reject_accept
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")

    assert mailbox.poll(queue) is None
    assert mailbox.journal.active is None
    assert queue.get_queue_length() == 0


def test_recovered_result_is_staged_before_queue_removal(tmp_path, monkeypatch):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    output = tmp_path / "output.jsonl"
    output.write_text(json.dumps({"task_id": task.task_id, "score": 1}) + "\n")

    restarted = make_mailbox(tmp_path, client)

    def fail_stage(*_args, **_kwargs):
        raise RuntimeError("crash while staging")

    monkeypatch.setattr(restarted, "stage_success", fail_stage)
    with pytest.raises(RuntimeError, match="crash while staging"):
        restarted.reconcile(queue)
    assert queue.has_task(task.task_id)
    assert restarted.journal.active["stage"] == "accepted"
