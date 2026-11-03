import json

import pytest

from .helpers import runner_status, task_envelope, task_outcome


@pytest.mark.asyncio
async def test_api_requires_authentication(api_client):
    response = await api_client.get("/api/v1/fleet")
    assert response.status == 401
    assert (await response.json())["code"] == "AUTH_REQUIRED"


@pytest.mark.asyncio
async def test_health_reports_the_deployed_body_limit(api_client, auth_headers, control_env):
    response = await api_client.get("/api/v1/health", headers=auth_headers["operator"])
    assert response.status == 200
    document = await response.json()
    assert document["status"] == "ok"
    assert document["limits"] == {"max_body_bytes": control_env["config"].max_body_bytes}
    # Runner tokens cannot read it; the limit is an operator's concern.
    response = await api_client.get("/api/v1/health", headers=auth_headers["arm"])
    assert response.status == 403


@pytest.mark.asyncio
async def test_public_dashboard_feed_is_get_only_and_sanitized(api_client, auth_headers):
    envelope = task_envelope()
    envelope["task"]["note"] = "visible description"
    await api_client.post("/api/v1/tasks", json=envelope, headers=auth_headers["operator"])

    response = await api_client.get("/api/v1/public/dashboard")
    assert response.status == 200
    assert response.headers["Access-Control-Allow-Origin"] == "*"
    assert response.headers["Access-Control-Allow-Methods"] == "GET, OPTIONS"
    assert response.headers["Access-Control-Allow-Headers"] == "Accept"
    assert response.headers["Cache-Control"] == "public, max-age=5"
    body = await response.json()
    arm = next(runner for runner in body["runners"] if runner["runner_id"] == "armbench")
    assert arm["total_count"] == 1
    assert arm["returned_count"] == 1
    assert arm["truncated"] is False
    assert arm["expected_duration_complete"] is True
    assert arm["remote_tasks"][0]["note"] == "visible description"
    serialized = json.dumps(body)
    assert "submitted_by" not in serialized
    assert "claim_token" not in serialized
    assert "outcome" not in serialized

    preflight = await api_client.options("/api/v1/public/dashboard")
    assert preflight.status == 204
    assert preflight.headers["Access-Control-Allow-Origin"] == "*"

    post = await api_client.post("/api/v1/public/dashboard")
    assert post.status == 401


@pytest.mark.asyncio
async def test_api_enforces_operator_and_runner_roles(api_client, auth_headers):
    response = await api_client.post("/api/v1/tasks", json=task_envelope(), headers=auth_headers["arm"])
    assert response.status == 403
    assert (await response.json())["code"] == "SUBMITTER_REQUIRED"

    response = await api_client.post("/api/v1/runners/armbench/claim", headers=auth_headers["operator"])
    assert response.status == 403
    assert (await response.json())["code"] == "RUNNER_REQUIRED"

    response = await api_client.post("/api/v1/runners/g4bench/claim", headers=auth_headers["arm"])
    assert response.status == 403
    assert (await response.json())["code"] == "RUNNER_SCOPE_MISMATCH"


@pytest.mark.asyncio
async def test_full_http_lifecycle_and_idempotency(api_client, auth_headers):
    submit = await api_client.post(
        "/api/v1/tasks",
        json=task_envelope(),
        headers={**auth_headers["operator"], "Idempotency-Key": "request-1"},
    )
    assert submit.status == 201
    assert (await submit.json())["created"] is True

    replay = await api_client.post(
        "/api/v1/tasks",
        json=task_envelope(),
        headers={**auth_headers["operator"], "Idempotency-Key": "request-1"},
    )
    assert replay.status == 200
    assert (await replay.json())["created"] is False

    claim_response = await api_client.post("/api/v1/runners/armbench/claim", headers=auth_headers["arm"])
    assert claim_response.status == 200
    claim = (await claim_response.json())["claim"]
    assert claim["task"]["task_id"] == "task-1"

    accept = await api_client.post(
        "/api/v1/tasks/task-1/accept",
        json={"claim_token": claim["claim_token"]},
        headers=auth_headers["arm"],
    )
    assert accept.status == 200
    assert (await accept.json())["task"]["state"] == "accepted"

    outcome = task_outcome()
    complete = await api_client.post(
        "/api/v1/tasks/task-1/complete",
        json=outcome,
        headers=auth_headers["arm"],
    )
    assert complete.status == 200
    body = await complete.json()
    assert body["changed"] is True
    assert body["task"]["state"] == "completed"

    complete_replay = await api_client.post(
        "/api/v1/tasks/task-1/complete",
        json=outcome,
        headers=auth_headers["arm"],
    )
    assert complete_replay.status == 200
    assert (await complete_replay.json())["changed"] is False


@pytest.mark.asyncio
async def test_list_show_cancel_and_fleet(api_client, auth_headers):
    await api_client.post("/api/v1/tasks", json=task_envelope(), headers=auth_headers["operator"])
    listed = await api_client.get("/api/v1/tasks?runner_id=armbench&state=queued", headers=auth_headers["operator"])
    assert [task["task_id"] for task in (await listed.json())["tasks"]] == ["task-1"]

    shown = await api_client.get("/api/v1/tasks/task-1", headers=auth_headers["operator"])
    assert (await shown.json())["task"]["envelope"]["task_id"] == "task-1"

    cancelled = await api_client.delete("/api/v1/tasks/task-1", headers=auth_headers["operator"])
    body = await cancelled.json()
    assert body["changed"] == 1
    assert body["tasks"][0]["state"] == "cancelled"

    fleet = await api_client.get("/api/v1/fleet", headers=auth_headers["operator"])
    runner_ids = {runner["runner_id"] for runner in (await fleet.json())["runners"]}
    assert runner_ids == {"armbench", "g4bench", "disabled"}


@pytest.mark.asyncio
async def test_status_push_and_runner_detail(api_client, auth_headers):
    response = await api_client.put(
        "/api/v1/runners/armbench/status",
        json=runner_status(),
        headers=auth_headers["arm"],
    )
    assert response.status == 200

    detail = await api_client.get("/api/v1/fleet/armbench", headers=auth_headers["operator"])
    assert (await detail.json())["runner"]["status"]["host"] == "host-a"


@pytest.mark.asyncio
async def test_content_type_schema_and_body_size_fail_closed(api_client, auth_headers):
    response = await api_client.post("/api/v1/tasks", data="not-json", headers=auth_headers["operator"])
    assert response.status == 415
    assert (await response.json())["code"] == "CONTENT_TYPE_INVALID"

    invalid = task_envelope()
    invalid["schema_version"] = 2
    response = await api_client.post("/api/v1/tasks", json=invalid, headers=auth_headers["operator"])
    assert response.status == 400
    assert (await response.json())["code"] == "SCHEMA_INVALID"

    response = await api_client.post(
        "/api/v1/tasks",
        data=json.dumps({"payload": "x" * 5000}),
        headers={**auth_headers["operator"], "Content-Type": "application/json"},
    )
    assert response.status == 413
    assert (await response.json())["code"] == "BODY_TOO_LARGE"


@pytest.mark.asyncio
async def test_batch_endpoint_returns_member_tasks(api_client, auth_headers):
    for task_id in ("b-1", "b-2"):
        envelope = task_envelope(task_id)
        envelope["batch_id"] = "batch-x"
        await api_client.post("/api/v1/tasks", json=envelope, headers=auth_headers["operator"])
    response = await api_client.get("/api/v1/batches/batch-x", headers=auth_headers["operator"])
    assert response.status == 200
    body = await response.json()
    assert body["batch"]["batch_id"] == "batch-x"
    assert {t["task_id"] for t in body["batch"]["tasks"]} == {"b-1", "b-2"}


@pytest.mark.asyncio
async def test_cancel_by_batch_marks_all_members(api_client, auth_headers):
    for task_id in ("c-1", "c-2"):
        envelope = task_envelope(task_id)
        envelope["batch_id"] = "batch-c"
        await api_client.post("/api/v1/tasks", json=envelope, headers=auth_headers["operator"])
    response = await api_client.delete("/api/v1/tasks/batch-c", headers=auth_headers["operator"])
    body = await response.json()
    assert body["changed"] == 2
    assert {t["state"] for t in body["tasks"]} == {"cancelled"}


@pytest.mark.asyncio
async def test_pending_requires_approver_role(api_client, auth_headers):
    # A runner token is neither operator nor approver.
    response = await api_client.get("/api/v1/tasks/pending", headers=auth_headers["arm"])
    assert response.status == 403
    assert (await response.json())["code"] == "APPROVER_REQUIRED"
    # An operator may list pending tasks.
    ok = await api_client.get("/api/v1/tasks/pending", headers=auth_headers["operator"])
    assert ok.status == 200
