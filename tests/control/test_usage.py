"""Usage rows written by the control service, and the runner usage route (rollout step 3)."""

import pytest

from conductress.control.errors import ControlError

from .helpers import now_text, task_envelope


def _usage(control_env, task_id=None):
    with control_env["database"].read() as connection:
        rows = connection.execute("SELECT * FROM task_usage ORDER BY task_id").fetchall()
    rows = [dict(row) for row in rows]
    return rows if task_id is None else next((r for r in rows if r["task_id"] == task_id), None)


def _outcome(task_id, state="completed", completed_at="2026-08-29T00:10:00Z", error=None):
    return {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": "armbench",
        "state": state,
        "completed_at": completed_at,
        "result": {},
        "error": error,
    }


def _accept(service, task_id="task-1"):
    service.submit_task(task_envelope(task_id), actor="operator:example-operator")
    claim = service.claim_task("armbench", actor="runner:armbench")
    service.accept_task("armbench", task_id, claim["claim_token"], actor="runner:armbench")


def test_completed_task_gets_one_usage_row(control_env):
    service = control_env["service"]
    _accept(service)
    assert _usage(control_env) == []
    with control_env["database"].transaction(immediate=True) as connection:
        connection.execute("UPDATE tasks SET accepted_at='2026-08-29T00:00:00Z' WHERE task_id='task-1'")
    service.record_outcome("armbench", "task-1", _outcome("task-1"), actor="runner:armbench")
    row = _usage(control_env, "task-1")
    assert (row["outcome"], row["runner_seconds"], row["task_class"]) == ("completed", 600.0, "manual")
    assert row["submitted_by"] == "operator:example-operator"
    assert row["platform"] == "arm64/c7g.metal/graviton3"
    assert row["started_at"] == "2026-08-29T00:00:00Z"
    assert row["ended_at"] == "2026-08-29T00:10:00Z"


def test_replayed_outcome_keeps_one_usage_row(control_env):
    service = control_env["service"]
    _accept(service)
    service.record_outcome("armbench", "task-1", _outcome("task-1"), actor="runner:armbench")
    service.record_outcome("armbench", "task-1", _outcome("task-1"), actor="runner:armbench")
    assert len(_usage(control_env)) == 1


def test_failed_task_gets_a_failed_row(control_env):
    service = control_env["service"]
    _accept(service)
    service.record_outcome("armbench", "task-1", _outcome("task-1", state="failed"), actor="runner:armbench")
    assert _usage(control_env, "task-1")["outcome"] == "failed"


def test_cancelled_queued_task_gets_a_zero_second_row(control_env):
    service = control_env["service"]
    service.submit_task(task_envelope("task-1"), actor="operator:example-operator")
    service.cancel_task("task-1", actor="operator:example-operator")
    row = _usage(control_env, "task-1")
    assert (row["outcome"], row["runner_seconds"], row["started_at"]) == ("cancelled-queued", 0.0, None)


def test_cancelled_running_task_is_recorded_when_the_runner_reports(control_env):
    service = control_env["service"]
    _accept(service)
    service.cancel_task("task-1", actor="operator:example-operator")
    assert _usage(control_env) == []
    cancelled = _outcome("task-1", state="failed", error="TaskCancelled: cancelled by the operator")
    service.record_outcome("armbench", "task-1", cancelled, actor="runner:armbench")
    assert _usage(control_env, "task-1")["outcome"] == "cancelled-running"


def test_cancel_that_loses_the_race_to_completion_is_completed(control_env):
    service = control_env["service"]
    _accept(service)
    service.cancel_task("task-1", actor="operator:example-operator")
    task, _ = service.record_outcome("armbench", "task-1", _outcome("task-1"), actor="runner:armbench")
    assert task["state"] == "completed"
    assert _usage(control_env, "task-1")["outcome"] == "completed"


def test_failure_after_cancel_request_for_another_reason_is_failed(control_env):
    service = control_env["service"]
    _accept(service)
    service.cancel_task("task-1", actor="operator:example-operator")
    failed = _outcome("task-1", state="failed", error="RuntimeError: server crashed")
    service.record_outcome("armbench", "task-1", failed, actor="runner:armbench")
    assert _usage(control_env, "task-1")["outcome"] == "failed"


def test_cancelled_error_without_a_cancel_request_is_failed(control_env):
    service = control_env["service"]
    _accept(service)
    cancelled = _outcome("task-1", state="failed", error="TaskCancelled: cancelled by the operator")
    service.record_outcome("armbench", "task-1", cancelled, actor="runner:armbench")
    assert _usage(control_env, "task-1")["outcome"] == "failed"


def test_cancel_after_the_outcome_arrived_changes_nothing(control_env):
    service = control_env["service"]
    _accept(service)
    service.record_outcome("armbench", "task-1", _outcome("task-1"), actor="runner:armbench")
    service.cancel_task("task-1", actor="operator:example-operator")
    rows = _usage(control_env)
    assert [(r["task_id"], r["outcome"]) for r in rows] == [("task-1", "completed")]


def test_cancel_while_claimed_is_cancelled_queued_and_the_runner_cannot_accept(control_env):
    service = control_env["service"]
    service.submit_task(task_envelope("task-1"), actor="operator:example-operator")
    claim = service.claim_task("armbench", actor="runner:armbench")
    service.cancel_task("task-1", actor="operator:example-operator")
    assert _usage(control_env, "task-1")["outcome"] == "cancelled-queued"
    with pytest.raises(ControlError):
        service.accept_task("armbench", "task-1", claim["claim_token"], actor="runner:armbench")
    assert len(_usage(control_env)) == 1


def test_runner_usage_is_stored_once_with_server_computed_seconds(control_env):
    service = control_env["service"]
    record = {
        "schema_version": 1,
        "task_id": "sweep-1",
        "task_class": "sweep",
        "started_at": "2026-08-29T00:00:00Z",
        "ended_at": "2026-08-29T00:02:30Z",
        "outcome": "completed",
    }
    assert service.record_runner_usage("armbench", record) is True
    assert service.record_runner_usage("armbench", record) is False
    rows = _usage(control_env)
    assert len(rows) == 1
    assert (rows[0]["submitted_by"], rows[0]["runner_seconds"], rows[0]["runner_id"]) == ("sweep", 150.0, "armbench")
    assert rows[0]["platform"] == "arm64/c7g.metal/graviton3"
    assert rows[0]["note"] is None and rows[0]["batch_id"] is None


@pytest.mark.parametrize(
    "change, code",
    [
        ({"task_class": "manual"}, "SCHEMA_INVALID"),
        ({"outcome": "cancelled-queued"}, "SCHEMA_INVALID"),
        ({"note": "free text"}, "SCHEMA_INVALID"),
        ({"ended_at": "2026-08-28T00:00:00Z"}, "USAGE_TIME_INVALID"),
        ({"ended_at": "2026-08-29T00:02:30"}, "SCHEMA_INVALID"),
    ],
)
def test_runner_usage_rejects_bad_records(control_env, change, code):
    record = {
        "schema_version": 1,
        "task_id": "local-1",
        "task_class": "local",
        "started_at": "2026-08-29T00:00:00Z",
        "ended_at": "2026-08-29T00:02:30Z",
        "outcome": "completed",
        **change,
    }
    with pytest.raises(ControlError) as refused:
        control_env["service"].record_runner_usage("armbench", record)
    assert refused.value.code == code
    assert _usage(control_env) == []


def test_fill_usage_platforms_sets_only_missing_platforms(control_env):
    service = control_env["service"]
    with control_env["database"].transaction(immediate=True) as connection:
        connection.execute(
            "INSERT INTO task_usage(task_id, submitted_by, runner_id, platform, task_class, ended_at, "
            "runner_seconds, outcome) VALUES ('old-1', 'example-agent', 'armbench', NULL, 'manual', "
            "'2026-08-29T00:00:00Z', 5, 'completed'), ('old-2', 'example-agent', 'armbench', 'kept', 'manual', "
            "'2026-08-29T00:00:00Z', 5, 'completed'), ('old-3', 'example-agent', 'gone', NULL, 'manual', "
            "'2026-08-29T00:00:00Z', 5, 'completed')"
        )
    assert service.fill_usage_platforms() == 1
    platforms = {r["task_id"]: r["platform"] for r in _usage(control_env)}
    assert platforms == {"old-1": "arm64/c7g.metal/graviton3", "old-2": "kept", "old-3": None}
    assert service.fill_usage_platforms() == 0


def _record(task_id="local-1"):
    return {
        "schema_version": 1,
        "task_id": task_id,
        "task_class": "local",
        "started_at": now_text(),
        "ended_at": now_text(),
        "outcome": "completed",
    }


@pytest.mark.asyncio
async def test_usage_route_accepts_its_own_runner_token_only(api_client, auth_headers):
    created = await api_client.post("/api/v1/runners/armbench/usage", json=_record(), headers=auth_headers["arm"])
    assert created.status == 201
    assert (await created.json())["stored"] is True
    repeated = await api_client.post("/api/v1/runners/armbench/usage", json=_record(), headers=auth_headers["arm"])
    assert (repeated.status, (await repeated.json())["stored"]) == (200, False)

    other = await api_client.post("/api/v1/runners/armbench/usage", json=_record("x"), headers=auth_headers["g4"])
    assert other.status == 403
    assert (await other.json())["code"] == "RUNNER_SCOPE_MISMATCH"
    operator = await api_client.post(
        "/api/v1/runners/armbench/usage", json=_record("y"), headers=auth_headers["operator"]
    )
    assert operator.status == 403
    assert (await operator.json())["code"] == "RUNNER_REQUIRED"
    anonymous = await api_client.post("/api/v1/runners/armbench/usage", json=_record("z"))
    assert anonymous.status == 401


@pytest.mark.asyncio
async def test_old_runner_flow_without_usage_posts_still_works(api_client, auth_headers):
    submitted = await api_client.post("/api/v1/tasks", json=task_envelope(), headers=auth_headers["operator"])
    assert submitted.status == 201
    claim = await api_client.post("/api/v1/runners/armbench/claim", headers=auth_headers["arm"])
    token = (await claim.json())["claim"]["claim_token"]
    accepted = await api_client.post(
        "/api/v1/tasks/task-1/accept", json={"claim_token": token}, headers=auth_headers["arm"]
    )
    assert accepted.status == 200
    done = await api_client.post("/api/v1/tasks/task-1/complete", json=_outcome("task-1"), headers=auth_headers["arm"])
    assert done.status == 200
