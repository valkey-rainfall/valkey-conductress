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
