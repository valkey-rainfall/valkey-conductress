import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from conductress.fleet_client import FleetClient, FleetClientConfig
from conductress.runner_mailbox import RunnerMailbox
from conductress.task_queue import TaskQueue

from .helpers import task_envelope

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests" / "fixtures" / "golden_tasks" / "PerfTaskData.json"


@pytest.mark.asyncio
async def test_remote_submission_to_local_import_and_completion(
    api_client,
    operator_token,
    arm_runner_token,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("conductress.task_queue.config.REPO_NAMES", ["valkey"])
    base_url = str(api_client.make_url("/api/v1")).rstrip("/")
    operator = FleetClient(FleetClientConfig(base_url, operator_token))
    runner_client = FleetClient(FleetClientConfig(base_url, arm_runner_token))
    task_document = json.loads(GOLDEN.read_text(encoding="utf-8"))
    task_id = "2026.08.29_00.00.00.123456"
    envelope = task_envelope(task_id=task_id)
    envelope["task"] = task_document

    await asyncio.to_thread(operator.submit_task, envelope, f"armbench:{task_id}")

    queue = TaskQueue(tmp_path / "queue")
    output = tmp_path / "output.jsonl"
    with (
        patch(
            "conductress.runner_mailbox.get_runner_config",
            return_value=SimpleNamespace(runner_id="armbench"),
        ),
        patch("conductress.runner_mailbox.CONDUCTRESS_OUTPUT", output),
        patch("conductress.runner_mailbox.CONDUCTRESS_FAILED_LOG", tmp_path / "failed.jsonl"),
    ):
        mailbox = RunnerMailbox(
            tmp_path / "delivery.json",
            mode="live",
            client_factory=lambda: runner_client,
        )
        task = await asyncio.to_thread(mailbox.poll, queue)
        assert task.task_id == task_id
        assert queue.has_task(task_id)
        assert mailbox.journal.active["stage"] == "accepted"

        result = {"task_id": task_id, "method": "perf-get", "score": 123}
        output.write_text(json.dumps(result) + "\n", encoding="utf-8")
        mailbox.stage_success(task, result=result)
        queue.finish_task(task)
        assert await asyncio.to_thread(mailbox.flush_pending_outcome) is True

    remote = await asyncio.to_thread(operator.task, task_id)
    assert remote["task"]["state"] == "completed"
    assert remote["task"]["outcome"]["result"]["score"] == 123


@pytest.mark.asyncio
async def test_oversized_outcome_is_reduced_after_the_server_413(
    api_client,
    operator_token,
    arm_runner_token,
    tmp_path,
    monkeypatch,
):
    """The real app refuses a body over max_body_bytes (4096 in tests) with
    413 BODY_TOO_LARGE; the mailbox then delivers the reduced outcome."""
    monkeypatch.setattr("conductress.task_queue.config.REPO_NAMES", ["valkey"])
    base_url = str(api_client.make_url("/api/v1")).rstrip("/")
    operator = FleetClient(FleetClientConfig(base_url, operator_token))
    runner_client = FleetClient(FleetClientConfig(base_url, arm_runner_token))
    task_document = json.loads(GOLDEN.read_text(encoding="utf-8"))
    task_id = "2026.10.31_01.51.19.863762"
    envelope = task_envelope(task_id=task_id)
    envelope["task"] = task_document
    await asyncio.to_thread(operator.submit_task, envelope, f"armbench:{task_id}")

    queue = TaskQueue(tmp_path / "queue")
    output = tmp_path / "output.jsonl"
    series = {"count": 200, "t": [1000.0 + i for i in range(200)], "instances": {"6380": {"offset": list(range(200))}}}
    result = {
        "task_id": task_id,
        "method": "replica-read",
        "score": 4_230_000.0,
        "data": {"mean_rps": 4_230_000.0, "per_rep_results": [{"reader_rps": 4.2e6, "series": series}] * 3},
    }
    with (
        patch(
            "conductress.runner_mailbox.get_runner_config",
            return_value=SimpleNamespace(runner_id="armbench"),
        ),
        patch("conductress.runner_mailbox.CONDUCTRESS_OUTPUT", output),
        patch("conductress.runner_mailbox.CONDUCTRESS_FAILED_LOG", tmp_path / "failed.jsonl"),
    ):
        # A client budget above the server's limit, so the preflight does not
        # trim and the server is what says no.
        mailbox = RunnerMailbox(
            tmp_path / "delivery.json", mode="live", client_factory=lambda: runner_client, max_outcome_bytes=1 << 20
        )
        task = await asyncio.to_thread(mailbox.poll, queue)
        output.write_text(json.dumps(result) + "\n", encoding="utf-8")
        mailbox.stage_success(task, result=result)
        queue.finish_task(task)
        assert await asyncio.to_thread(mailbox.flush_pending_outcome) is True
        assert mailbox.journal.active is None
        downgrade = mailbox.status()["last_outcome_downgrade"]
        assert downgrade["stage"] == "reduced"
        assert downgrade["reason"].startswith("rejected: BODY_TOO_LARGE (HTTP 413), ")

    remote = await asyncio.to_thread(operator.task, task_id)
    assert remote["task"]["state"] == "completed"
    stored = remote["task"]["outcome"]["result"]
    assert stored["score"] == 4_230_000.0
    assert all("series" not in rep and rep["reader_rps"] == 4.2e6 for rep in stored["data"]["per_rep_results"])
    assert stored["outcome_delivery"]["full_result_path"] == str(output)
    assert len(stored["outcome_delivery"]["dropped"]) == 3
