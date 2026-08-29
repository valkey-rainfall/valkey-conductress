import asyncio

import pytest

from conductress.fleet_client import FleetClient, FleetClientConfig, FleetClientError

from .helpers import task_envelope


@pytest.mark.asyncio
async def test_stdlib_client_full_operator_lifecycle(api_client, operator_token):
    base_url = str(api_client.make_url("/api/v1")).rstrip("/")
    client = FleetClient(FleetClientConfig(base_url, operator_token))

    fleet = await asyncio.to_thread(client.fleet)
    assert {runner["runner_id"] for runner in fleet["runners"]} >= {"armbench", "g4bench"}

    submitted = await asyncio.to_thread(
        client.submit_task,
        task_envelope(),
        "e2e-request-1",
    )
    assert submitted["created"] is True
    listed = await asyncio.to_thread(client.list_tasks, runner_id="armbench", state="queued")
    assert [task["task_id"] for task in listed["tasks"]] == ["task-1"]
    shown = await asyncio.to_thread(client.task, "task-1")
    assert shown["task"]["envelope"]["task_id"] == "task-1"
    cancelled = await asyncio.to_thread(client.cancel_task, "task-1")
    assert cancelled["task"]["state"] == "cancelled"


@pytest.mark.asyncio
async def test_stdlib_client_maps_real_auth_error(api_client):
    base_url = str(api_client.make_url("/api/v1")).rstrip("/")
    client = FleetClient(FleetClientConfig(base_url, "wrong-token"))

    with pytest.raises(FleetClientError) as error:
        await asyncio.to_thread(client.fleet)
    assert error.value.code == "AUTH_INVALID"
    assert error.value.exit_code == 2
