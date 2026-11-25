"""Own-only cancel rules for non-operator tokens (multi-agent queue note, step 2)."""

import json

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from conductress.control.app import create_app
from conductress.control.auth import TokenStore, hash_token
from conductress.control.errors import AuthorizationError
from conductress.control.service import ControlService
from conductress.control.users import UserDirectory

from .conftest import OPERATOR_TOKEN
from .helpers import task_envelope

OWNER_TOKEN = "example-owner-secret"
AGENT_TOKEN = "example-agent-secret"


def _directory():
    return UserDirectory.from_dict(
        {
            "users": [
                {
                    "login": "example-owner",
                    "github": "example-owner",
                    "kind": "human",
                    "role": "owner",
                    "quota_runner_minutes_per_day": 0,
                },
                {
                    "login": "example-agent",
                    "github": "example-agent",
                    "kind": "human",
                    "role": "collaborator",
                    "quota_runner_minutes_per_day": 0,
                },
            ]
        }
    )


def _batch_envelope(task_id, batch_id):
    envelope = task_envelope(task_id)
    envelope["batch_id"] = batch_id
    return envelope


def _service(control_env):
    return ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=_directory(),
    )


def _submit(service, task_id, login, batch_id="batch-1"):
    service.submit_task(_batch_envelope(task_id, batch_id), actor=f"user:{login}", identity_login=login)


def _cancel_as(service, selector, login):
    return service.cancel_task(selector, actor=f"user:{login}", identity_login=login, is_operator=False)


def test_user_cancels_own_queued_task(control_env):
    service = _service(control_env)
    _submit(service, "task-1", "example-agent")
    tasks, changed, skipped = _cancel_as(service, "task-1", "example-agent")
    assert (changed, tasks[0]["state"]) == (1, "cancelled")
    assert skipped == {"not_yours": 0, "already_running": []}


def test_user_cannot_cancel_own_running_task(control_env):
    service = _service(control_env)
    _submit(service, "task-1", "example-agent")
    service.claim_task("armbench", actor="runner:armbench")
    tasks, changed, skipped = _cancel_as(service, "task-1", "example-agent")
    assert changed == 0
    assert tasks[0]["state"] == "claimed"
    assert skipped["already_running"] == ["task-1"]


def test_user_cannot_cancel_another_users_task(control_env):
    service = _service(control_env)
    _submit(service, "task-1", "example-owner")
    tasks, changed, skipped = _cancel_as(service, "task-1", "example-agent")
    assert (tasks, changed) == ([], 0)
    assert skipped["not_yours"] == 1
    with control_env["database"].read() as connection:
        assert connection.execute("SELECT state FROM tasks").fetchone()["state"] == "queued"


def test_owner_login_without_operator_token_cannot_cancel_another_logins_task(control_env):
    service = _service(control_env)
    _submit(service, "task-1", "example-agent")
    tasks, changed, skipped = _cancel_as(service, "task-1", "example-owner")
    assert (tasks, changed, skipped["not_yours"]) == ([], 0, 1)


def test_mixed_batch_gives_a_partial_result_with_reasons(control_env):
    service = _service(control_env)
    _submit(service, "own-running", "example-agent")
    service.claim_task("armbench", actor="runner:armbench")
    _submit(service, "own-queued", "example-agent")
    _submit(service, "other-queued", "example-owner")
    tasks, changed, skipped = _cancel_as(service, "batch-1", "example-agent")
    assert changed == 1
    assert {t["task_id"]: t["state"] for t in tasks} == {"own-running": "claimed", "own-queued": "cancelled"}
    assert skipped == {"not_yours": 1, "already_running": ["own-running"]}
    assert "other-queued" not in json.dumps([tasks, skipped])


def test_missing_login_is_refused_not_trusted(control_env):
    service = _service(control_env)
    _submit(service, "task-1", "example-agent")
    tasks, changed, skipped = service.cancel_task("task-1", actor="user:none", identity_login=None, is_operator=False)
    assert (tasks, changed, skipped["not_yours"]) == ([], 0, 1)


def test_operator_cancels_a_running_task_and_it_becomes_cancel_requested(control_env):
    service = _service(control_env)
    _submit(service, "task-1", "example-agent")
    claim = service.claim_task("armbench", actor="runner:armbench")
    service.accept_task("armbench", "task-1", claim["claim_token"], actor="runner:armbench")
    tasks, changed, _ = service.cancel_task("task-1", actor="operator:example-operator")
    assert (changed, tasks[0]["state"]) == (1, "cancel-requested")


def test_operator_cancel_of_a_claimed_task_ends_it_because_no_runner_will_report_it(control_env):
    service = _service(control_env)
    _submit(service, "task-1", "example-agent")
    service.claim_task("armbench", actor="runner:armbench")
    tasks, changed, _ = service.cancel_task("task-1", actor="operator:example-operator")
    assert (changed, tasks[0]["state"]) == (1, "cancelled")


@pytest.mark.parametrize("task_class", ["canary", "sweep"])
def test_user_token_cannot_submit_canary_or_sweep_class(control_env, task_class):
    service = _service(control_env)
    envelope = task_envelope("task-1", task_class=task_class)
    with pytest.raises(AuthorizationError) as refused:
        service.submit_task(envelope, actor="user:example-agent", identity_login="example-agent")
    assert refused.value.code == "TASK_CLASS_OPERATOR_ONLY"


def test_operator_token_may_submit_canary_class(control_env):
    service = _service(control_env)
    task, created = service.submit_task(task_envelope("task-1", task_class="canary"), actor="operator:example-operator")
    assert (created, task["task_class"]) == (True, "canary")


@pytest_asyncio.fixture
async def cancel_client(control_env, tmp_path):
    path = tmp_path / "cancel-tokens.json"
    records = [
        {"token_hash": hash_token(OPERATOR_TOKEN), "role": "operator", "label": "example-operator"},
        {"token_hash": hash_token(OWNER_TOKEN), "role": "user", "label": "owner", "login": "example-owner"},
        {"token_hash": hash_token(AGENT_TOKEN), "role": "user", "label": "agent", "login": "example-agent"},
    ]
    path.write_text(json.dumps({"schema_version": 1, "tokens": records}), encoding="utf-8")
    app = create_app(
        control_env["config"],
        database=control_env["database"],
        registry=control_env["registry"],
        token_store=TokenStore(path),
        canary_profiles=control_env["canary_profiles"],
        user_directory=_directory(),
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client
    await client.close()


def _bearer(token):
    return {"Authorization": f"Bearer {token}"}


async def _post_batch(client, token, task_ids):
    for task_id in task_ids:
        response = await client.post("/api/v1/tasks", json=_batch_envelope(task_id, "batch-1"), headers=_bearer(token))
        assert response.status == 201


@pytest.mark.asyncio
async def test_api_owner_login_cannot_cancel_a_task_it_does_not_own(cancel_client):
    await _post_batch(cancel_client, AGENT_TOKEN, ["task-1"])
    response = await cancel_client.delete("/api/v1/tasks/task-1", headers=_bearer(OWNER_TOKEN))
    body = await response.json()
    assert response.status == 200
    assert (body["changed"], body["tasks"], body["skipped"]["not_yours"]) == (0, [], 1)

    response = await cancel_client.delete("/api/v1/tasks/task-1", headers=_bearer(OPERATOR_TOKEN))
    assert (await response.json())["changed"] == 1


@pytest.mark.asyncio
async def test_api_batch_cancel_reports_skipped_and_hides_other_logins_tasks(cancel_client):
    await _post_batch(cancel_client, AGENT_TOKEN, ["task-1"])
    await _post_batch(cancel_client, OWNER_TOKEN, ["task-2"])
    response = await cancel_client.delete("/api/v1/tasks/batch-1", headers=_bearer(AGENT_TOKEN))
    body = await response.json()
    assert [t["task_id"] for t in body["tasks"]] == ["task-1"]
    assert body["skipped"] == {"not_yours": 1, "already_running": []}


@pytest.mark.asyncio
async def test_api_user_token_with_canary_class_gets_403(cancel_client):
    envelope = task_envelope("task-1", task_class="canary")
    response = await cancel_client.post("/api/v1/tasks", json=envelope, headers=_bearer(AGENT_TOKEN))
    assert response.status == 403
    assert (await response.json())["code"] == "TASK_CLASS_OPERATOR_ONLY"
