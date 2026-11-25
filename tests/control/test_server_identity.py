"""Server-set identity and token scope (multi-agent queue note, step 1)."""

import json

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from conductress.control.app import create_app
from conductress.control.auth import TokenStore, hash_token
from conductress.control.service import ControlService
from conductress.control.users import UserDirectory

from .conftest import OPERATOR_TOKEN
from .helpers import task_envelope

SUBMIT_TOKEN = "example-submit-secret"
READ_TOKEN = "example-read-secret"
DEFAULT_TOKEN = "example-default-secret"


def _directory():
    return UserDirectory.from_dict(
        {
            "users": [
                {
                    "login": "example-human",
                    "github": "example-human",
                    "kind": "human",
                    "role": "collaborator",
                    "quota_runner_minutes_per_day": 0,
                },
                {"login": "example-agent", "kind": "agent", "sponsor": "example-human"},
            ]
        }
    )


def _token_file(path, records):
    path.write_text(json.dumps({"schema_version": 1, "tokens": records}), encoding="utf-8")
    return TokenStore(path)


def _user_record(token, login, **extra):
    return {"token_hash": hash_token(token), "role": "user", "label": login, "login": login, **extra}


def _directory_service(control_env, directory=None):
    return ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=directory,
    )


def _forged_envelope(task_id="task-1"):
    envelope = task_envelope(task_id, priority=5000)
    envelope["submitted_by"] = "someone-else"
    envelope["submitter"] = {"login": "someone-else", "kind": "human", "sponsor": None}
    return envelope


def _audit_detail(control_env, task_id):
    with control_env["database"].read() as connection:
        row = connection.execute(
            "SELECT detail_json FROM audit_log WHERE action='task.submit' AND task_id=?", (task_id,)
        ).fetchone()
    return json.loads(row["detail_json"]) if row["detail_json"] else None


def test_user_token_overwrites_submitter_and_clamps_priority(control_env):
    service = _directory_service(control_env, _directory())
    task, created = service.submit_task(_forged_envelope(), actor="user:example-agent", identity_login="example-agent")
    assert created is True
    assert task["submitted_by"] == "example-agent"
    assert task["priority"] == 100
    assert task["envelope"]["priority"] == 100
    assert task["envelope"]["submitter"] == {"login": "example-agent", "kind": "agent", "sponsor": "example-human"}
    assert _audit_detail(control_env, "task-1") == {"client_submitted_by": "someone-else", "client_priority": 5000}


def test_matching_client_values_leave_no_audit_detail(control_env):
    service = _directory_service(control_env, _directory())
    envelope = task_envelope("task-1")
    envelope["submitted_by"] = "example-human"
    service.submit_task(envelope, actor="user:example-human", identity_login="example-human")
    assert _audit_detail(control_env, "task-1") is None


def test_operator_token_keeps_priority_and_is_named_by_label(control_env):
    service = _directory_service(control_env, _directory())
    envelope = {**_forged_envelope(), "submitter": None}
    task, _ = service.submit_task(envelope, actor="operator:example-operator")
    assert task["submitted_by"] == "operator:example-operator"
    assert task["priority"] == 5000
    assert _audit_detail(control_env, "task-1") == {"client_submitted_by": "someone-else"}


def test_operator_naming_a_directory_login_gets_the_directory_block(control_env):
    service = _directory_service(control_env, _directory())
    envelope = task_envelope("task-1")
    envelope["submitter"] = {"login": "example-agent", "kind": "human", "sponsor": None}
    task, _ = service.submit_task(envelope, actor="operator:example-operator")
    assert task["envelope"]["submitter"] == {"login": "example-agent", "kind": "agent", "sponsor": "example-human"}
    assert task["submitter_login"] == "example-agent"


def test_user_token_without_directory_entry_gets_no_submitter_block(control_env):
    service = _directory_service(control_env)
    task, _ = service.submit_task(_forged_envelope(), actor="user:example-agent", identity_login="example-agent")
    assert task["envelope"]["submitter"] is None
    assert task["submitted_by"] == "example-agent"


def test_replaying_a_forged_envelope_is_idempotent(control_env):
    service = _directory_service(control_env, _directory())
    kwargs = {"actor": "user:example-agent", "identity_login": "example-agent", "idempotency_key": "k1"}
    first, created = service.submit_task(_forged_envelope(), **kwargs)
    second, created_again = service.submit_task(_forged_envelope(), **kwargs)
    assert (created, created_again) == (True, False)
    assert first["task_id"] == second["task_id"]


def test_user_token_scope_defaults_to_submit(tmp_path):
    store = _token_file(
        tmp_path / "tokens.json",
        [_user_record("a", "example-human"), _user_record("b", "example-agent", scope="read")],
    )
    assert store.authenticate("Bearer a").scope == "submit"
    assert store.authenticate("Bearer b").scope == "read"


@pytest.mark.parametrize(
    "record, message",
    [
        ({"role": "operator", "label": "op", "scope": "submit"}, "only a user token may set scope"),
        ({"role": "runner", "label": "r", "runner_id": "armbench", "scope": "read"}, "only a user token may set scope"),
        ({"role": "user", "label": "u", "login": "example-human", "scope": "operator"}, "scope must be"),
    ],
)
def test_token_store_rejects_bad_scope(tmp_path, record, message):
    with pytest.raises(ValueError, match=message):
        _token_file(tmp_path / "tokens.json", [{"token_hash": hash_token("x"), **record}])


def test_check_logins_names_unknown_logins_without_token_data(tmp_path):
    store = _token_file(tmp_path / "tokens.json", [_user_record("secret-value", "ghost")])
    store.check_logins({"ghost"})
    with pytest.raises(ValueError, match="ghost") as error:
        store.check_logins({"example-human"})
    assert "secret-value" not in str(error.value)
    assert hash_token("secret-value") not in str(error.value)


def test_create_app_refuses_token_with_unknown_login(control_env, tmp_path):
    store = _token_file(tmp_path / "user-tokens.json", [_user_record("x", "ghost")])
    with pytest.raises(ValueError, match="ghost"):
        create_app(control_env["config"], token_store=store, user_directory=_directory())


@pytest_asyncio.fixture
async def scoped_client(control_env, tmp_path):
    store = _token_file(
        tmp_path / "scoped-tokens.json",
        [
            {"token_hash": hash_token(OPERATOR_TOKEN), "role": "operator", "label": "example-operator"},
            _user_record(SUBMIT_TOKEN, "example-human", scope="submit"),
            _user_record(READ_TOKEN, "example-agent", scope="read"),
            # No scope key: this is how every user token written before step 1 looks.
            _user_record(DEFAULT_TOKEN, "example-human"),
        ],
    )
    app = create_app(
        control_env["config"],
        database=control_env["database"],
        registry=control_env["registry"],
        token_store=store,
        canary_profiles=control_env["canary_profiles"],
        user_directory=_directory(),
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client
    await client.close()


def _bearer(token):
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_existing_user_token_without_scope_can_still_submit_and_cancel(scoped_client):
    response = await scoped_client.post("/api/v1/tasks", json=_forged_envelope(), headers=_bearer(DEFAULT_TOKEN))
    assert response.status == 201
    task = (await response.json())["task"]
    assert (task["submitted_by"], task["priority"]) == ("example-human", 100)

    cancelled = await scoped_client.delete("/api/v1/tasks/task-1", headers=_bearer(DEFAULT_TOKEN))
    assert cancelled.status == 200
    assert (await cancelled.json())["changed"] == 1


@pytest.mark.asyncio
async def test_read_scope_can_read_but_not_submit_or_cancel(scoped_client):
    await scoped_client.post("/api/v1/tasks", json=task_envelope(), headers=_bearer(SUBMIT_TOKEN))

    refused = await scoped_client.post("/api/v1/tasks", json=task_envelope("task-2"), headers=_bearer(READ_TOKEN))
    assert refused.status == 403
    assert (await refused.json())["code"] == "SCOPE_READ_ONLY"

    refused = await scoped_client.delete("/api/v1/tasks/task-1", headers=_bearer(READ_TOKEN))
    assert refused.status == 403
    assert (await refused.json())["code"] == "SCOPE_READ_ONLY"

    # A queued task of another login is not readable (see test_usage_report.py); the list of its own is.
    mine = await scoped_client.get("/api/v1/tasks/mine", headers=_bearer(READ_TOKEN))
    assert mine.status == 200


@pytest.mark.asyncio
async def test_read_scope_cannot_resolve_pending_tasks(scoped_client):
    for action in ("approve", "reject"):
        response = await scoped_client.post(f"/api/v1/tasks/task-1/{action}", headers=_bearer(READ_TOKEN))
        assert response.status == 403
        assert (await response.json())["code"] == "SCOPE_READ_ONLY"
