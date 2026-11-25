"""The cancel path of a running remote task, end to end: operator cancel -> runner sees it -> task stops -> outcome."""

import asyncio

import pytest

from conductress.fleet_client import FleetClient, FleetClientConfig, FleetClientError
from conductress.runner_mailbox import RunnerMailbox

from .conftest import ARM_TOKEN, G4_TOKEN, OPERATOR_TOKEN
from .helpers import task_envelope


def _bearer(token):
    return {"Authorization": f"Bearer {token}"}


async def _accepted_task(api_client, task_id="task-1"):
    """Submit a task, claim it as armbench and accept it, so it is running on that runner."""
    submitted = await api_client.post("/api/v1/tasks", json=task_envelope(task_id), headers=_bearer(OPERATOR_TOKEN))
    assert submitted.status == 201
    claim = await api_client.post("/api/v1/runners/armbench/claim", headers=_bearer(ARM_TOKEN))
    token = (await claim.json())["claim"]["claim_token"]
    accepted = await api_client.post(
        f"/api/v1/tasks/{task_id}/accept", json={"claim_token": token}, headers=_bearer(ARM_TOKEN)
    )
    assert accepted.status == 200
    return token


def _outcome(task_id, state="failed", error="TaskCancelled: cancelled by the operator"):
    return {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": "armbench",
        "state": state,
        "completed_at": "2026-08-29T00:10:00Z",
        "result": {},
        "error": error,
    }


@pytest.mark.asyncio
async def test_runner_token_reads_the_state_of_its_own_runners_task(api_client):
    await _accepted_task(api_client)
    response = await api_client.get("/api/v1/tasks/task-1", headers=_bearer(ARM_TOKEN))
    assert response.status == 200
    assert (await response.json())["task"]["state"] == "accepted"

    await api_client.delete("/api/v1/tasks/task-1", headers=_bearer(OPERATOR_TOKEN))
    response = await api_client.get("/api/v1/tasks/task-1", headers=_bearer(ARM_TOKEN))
    assert (await response.json())["task"]["state"] == "cancel-requested"


@pytest.mark.asyncio
async def test_runner_token_gets_only_the_state_not_the_envelope(api_client):
    await _accepted_task(api_client)
    task = (await (await api_client.get("/api/v1/tasks/task-1", headers=_bearer(ARM_TOKEN))).json())["task"]
    assert set(task) == {"task_id", "runner_id", "state"}


@pytest.mark.asyncio
async def test_runner_token_cannot_read_another_runners_or_an_unknown_task(api_client):
    await _accepted_task(api_client)
    other = await api_client.get("/api/v1/tasks/task-1", headers=_bearer(G4_TOKEN))
    assert (other.status, (await other.json())["code"]) == (404, "TASK_NOT_FOUND")
    unknown = await api_client.get("/api/v1/tasks/nope", headers=_bearer(ARM_TOKEN))
    assert (unknown.status, (await unknown.json())["code"]) == (404, "TASK_NOT_FOUND")


@pytest.mark.asyncio
async def test_runner_token_still_cannot_read_a_batch(api_client):
    response = await api_client.get("/api/v1/batches/batch-1", headers=_bearer(ARM_TOKEN))
    assert response.status == 403


@pytest.mark.asyncio
async def test_mailbox_sees_the_operator_cancel_of_its_running_task(api_client, tmp_path):
    """The real client and the real mailbox method, against the real service."""
    await _accepted_task(api_client)
    base_url = str(api_client.make_url("/api/v1")).rstrip("/")
    mailbox = RunnerMailbox(
        tmp_path / "journal.json",
        mode="live",
        client_factory=lambda: FleetClient(FleetClientConfig(base_url, ARM_TOKEN)),
    )
    mailbox.journal.set_active({"task_id": "task-1", "stage": "accepted"})

    assert await asyncio.to_thread(mailbox.cancel_requested, "task-1") is False
    await api_client.delete("/api/v1/tasks/task-1", headers=_bearer(OPERATOR_TOKEN))
    assert await asyncio.to_thread(mailbox.cancel_requested, "task-1") is True
    assert mailbox.journal.stats["control_reachable"] is True


@pytest.mark.asyncio
async def test_a_stopped_task_reports_its_outcome_and_ends_as_cancelled_running(api_client, control_env):
    await _accepted_task(api_client)
    await api_client.delete("/api/v1/tasks/task-1", headers=_bearer(OPERATOR_TOKEN))
    done = await api_client.post("/api/v1/tasks/task-1/fail", json=_outcome("task-1"), headers=_bearer(ARM_TOKEN))
    assert done.status == 200
    assert (await done.json())["task"]["state"] == "failed"
    with control_env["database"].read() as connection:
        rows = connection.execute("SELECT outcome FROM task_usage WHERE task_id='task-1'").fetchall()
    assert [row["outcome"] for row in rows] == ["cancelled-running"]


@pytest.mark.asyncio
async def test_a_task_cancelled_before_the_runner_accepts_it_ends_cancelled_and_accept_is_refused(api_client):
    submitted = await api_client.post("/api/v1/tasks", json=task_envelope(), headers=_bearer(OPERATOR_TOKEN))
    assert submitted.status == 201
    claim = await api_client.post("/api/v1/runners/armbench/claim", headers=_bearer(ARM_TOKEN))
    token = (await claim.json())["claim"]["claim_token"]

    cancelled = await api_client.delete("/api/v1/tasks/task-1", headers=_bearer(OPERATOR_TOKEN))
    assert (await cancelled.json())["tasks"][0]["state"] == "cancelled"

    accepted = await api_client.post(
        "/api/v1/tasks/task-1/accept", json={"claim_token": token}, headers=_bearer(ARM_TOKEN)
    )
    assert (accepted.status, (await accepted.json())["code"]) == (409, "TASK_NOT_CLAIMED")


@pytest.mark.asyncio
async def test_a_runner_that_cannot_reach_the_service_does_not_stop_its_task(tmp_path):
    def unreachable():
        raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)

    mailbox = RunnerMailbox(tmp_path / "journal.json", mode="live", client_factory=unreachable)
    mailbox.journal.set_active({"task_id": "task-1", "stage": "accepted"})
    assert mailbox.cancel_requested("task-1") is False
