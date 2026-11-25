"""Usage report, public usage summary and task visibility (multi-agent queue note, rollout step 4)."""

import json
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from conductress import public_fields
from conductress.control.app import create_app
from conductress.control.auth import TokenStore, hash_token
from conductress.control.errors import ControlError
from conductress.control.service import ControlService
from conductress.control.users import UserDirectory

from .conftest import ARM_TOKEN, OPERATOR_TOKEN
from .helpers import now_text, task_envelope

AGENT_A_TOKEN = "example-agent-a-secret"
AGENT_B_TOKEN = "example-agent-b-secret"
READ_TOKEN = "example-reader-secret"
SECRET_NOTE = "private note for 198.51.100.7 only"
SECRET_BATCH = "batch-private-example"

_USERS = [
    {"login": "example-agent-a", "github": "agent-a-gh", "kind": "human", "role": "collaborator"},
    {"login": "example-agent-b", "github": "agent-b-gh", "kind": "human", "role": "collaborator"},
    {"login": "example-reader", "github": "reader-gh", "kind": "human", "role": "collaborator"},
]


def _directory():
    return UserDirectory.from_dict(
        {"users": [{**user, "quota_runner_minutes_per_day": 0} for user in _USERS]},
    )


def _insert_usage(
    connection, task_id, login, *, seconds=100.0, batch=None, note=None, platform="test/platform-a", **extra
):
    row = {
        "task_id": task_id,
        "submitted_by": login,
        "batch_id": batch,
        "note": note,
        "runner_id": "armbench",
        "platform": platform,
        "task_class": "manual",
        "queued_at": "2026-08-29T00:00:00Z",
        "started_at": "2026-08-29T00:01:00Z",
        "ended_at": "2026-08-29T00:10:00Z",
        "expected_seconds": None,
        "runner_seconds": seconds,
        "outcome": "completed",
        **extra,
    }
    connection.execute(
        "INSERT INTO task_usage VALUES (:task_id, :submitted_by, :batch_id, :note, :runner_id, :platform, "
        ":task_class, :queued_at, :started_at, :ended_at, :expected_seconds, :runner_seconds, :outcome)",
        row,
    )


@pytest.fixture
def usage_service(control_env, monkeypatch):
    service = ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=_directory(),
        published_canary_dir=control_env["config"].database_path.parent / "www",
    )
    monkeypatch.setattr("conductress.control.service.utc_now", lambda: datetime(2026, 8, 30, tzinfo=timezone.utc))
    with control_env["database"].transaction(immediate=True) as connection:
        _insert_usage(connection, "a-1", "example-agent-a", seconds=300.0, batch=SECRET_BATCH, note=SECRET_NOTE)
        _insert_usage(connection, "a-2", "example-agent-a", seconds=100.0, outcome="failed", batch="batch-a2")
        _insert_usage(connection, "b-1", "example-agent-b", seconds=500.0, batch="batch-b1")
        _insert_usage(
            connection,
            "sweep-1",
            "sweep",
            seconds=100.0,
            task_class="sweep",
            started_at="2026-08-29T00:05:00Z",
            queued_at=None,
        )
        _insert_usage(connection, "gone-1", "example-agent-b", seconds=0.0, outcome="cancelled-queued", started_at=None)
    return service


# ------------------------------------------------------------------ report


def test_operator_report_by_agent_shows_every_login_and_class(usage_service):
    report = usage_service.usage_report(by="agent", since="2026-08-01")
    rows = {row["key"]: row for row in report["rows"]}
    assert set(rows) == {"example-agent-a", "example-agent-b", "sweep"}
    assert rows["example-agent-a"]["runner_seconds"] == 400.0
    assert rows["example-agent-a"]["failed_count"] == 1
    assert rows["example-agent-a"]["mean_wait_seconds"] == 60.0
    assert rows["example-agent-b"]["cancelled_count"] == 1
    assert report["total_runner_seconds"] == 1000.0
    assert sum(row["share"] for row in report["rows"]) == pytest.approx(1.0, abs=0.001)


def test_operator_report_by_batch_and_platform(usage_service):
    by_batch = {row["key"] for row in usage_service.usage_report(by="batch", since="2026-08-01")["rows"]}
    assert {SECRET_BATCH, "batch-a2", "batch-b1", "-"} <= by_batch
    by_platform = usage_service.usage_report(by="platform", since="2026-08-01")["rows"]
    assert [row["key"] for row in by_platform] == ["test/platform-a"]


def test_user_report_shows_own_rows_and_folds_everyone_else_into_others(usage_service):
    report = usage_service.usage_report(by="agent", since="2026-08-01", viewer_login="example-agent-a")
    assert {row["key"] for row in report["rows"]} == {"example-agent-a", "others"}
    others = next(row for row in report["rows"] if row["key"] == "others")
    assert others["runner_seconds"] == 600.0
    text = json.dumps(report)
    for private in ("example-agent-b", "batch-b1", "sweep"):
        assert private not in text


@pytest.mark.parametrize("by", ["agent", "batch", "platform"])
def test_user_report_never_shows_another_login_batch_or_note(usage_service, by):
    report = usage_service.usage_report(by=by, since="2026-08-01", viewer_login="example-agent-b")
    text = json.dumps(report)
    for private in ("example-agent-a", SECRET_BATCH, "batch-a2", SECRET_NOTE, "198.51.100.7"):
        assert private not in text
    assert "others" in {row["key"] for row in report["rows"]}


def test_user_with_no_rows_sees_only_others(usage_service):
    report = usage_service.usage_report(by="batch", since="2026-08-01", viewer_login="example-reader")
    assert [row["key"] for row in report["rows"]] == ["others"]


def test_report_window_and_runner_filter(usage_service):
    assert usage_service.usage_report(by="agent", since="2026-08-29")["rows"]
    assert usage_service.usage_report(by="agent", since="2026-08-31")["rows"] == []
    assert usage_service.usage_report(by="agent", since="1h")["rows"] == []
    assert usage_service.usage_report(by="agent", since="7d", runner_id="g4bench")["rows"] == []
    assert usage_service.usage_report(by="agent", since="7d", runner_id="armbench")["rows"]


@pytest.mark.parametrize("since", ["", "7", "week", "2026-13-01", "7w"])
def test_report_refuses_a_bad_window(usage_service, since):
    with pytest.raises(ControlError) as refused:
        usage_service.usage_report(by="agent", since=since)
    assert refused.value.code == "SINCE_INVALID"


def test_report_refuses_an_unknown_grouping(usage_service):
    with pytest.raises(ControlError) as refused:
        usage_service.usage_report(by="login", since="7d")
    assert refused.value.code == "USAGE_GROUP_INVALID"


# ------------------------------------------------------------------ public summary

PRIVATE_VALUES = (
    "example-agent-a",
    "example-agent-b",
    SECRET_BATCH,
    "batch-a2",
    "batch-b1",
    SECRET_NOTE,
    "198.51.100.7",
)


def test_summary_has_only_allowlisted_fields_and_aggregates(usage_service):
    summary = usage_service.usage_summary()
    assert public_fields.unknown_usage_keys(summary) == []
    (platform,) = summary["platforms"]
    assert platform["platform"] == "test/platform-a"
    (day,) = platform["days"]
    assert day["date"] == "2026-08-29"
    assert day["runner_seconds"] == {"canary": 0.0, "local": 0.0, "manual": 900.0, "sweep": 100.0}
    # The cancelled-queued row used no runner time and is not counted.
    assert day["task_count"] == {"canary": 0, "local": 0, "manual": 3, "sweep": 1}
    assert day["median_wait_seconds"] == 60.0


def test_summary_carries_no_private_value(usage_service):
    text = json.dumps(usage_service.usage_summary())
    for private in PRIVATE_VALUES:
        assert private not in text
    for key in ("login", "submitted_by", "batch", "note", "runner_id"):
        assert key not in text


def test_summary_counts_the_queue_depth_per_platform(usage_service):
    usage_service.submit_task(task_envelope("queued-1"), actor="operator:example-operator")
    usage_service.submit_task(task_envelope("queued-2"), actor="operator:example-operator")
    depth = {p["platform"]: p["queue_depth"] for p in usage_service.usage_summary()["platforms"]}
    assert depth == {"test/platform-a": 0, "arm64/c7g.metal/graviton3": 2}


def test_unknown_usage_keys_flags_a_new_field():
    summary = {
        "schema_version": 1,
        "platforms": [
            {"platform": "p", "days": [{"date": "d", "runner_seconds": {"batch": 1}, "login": "x"}], "host": "h"}
        ],
        "who": "x",
    }
    assert sorted(public_fields.unknown_usage_keys(summary)) == [
        "day.login",
        "day.runner_seconds.batch",
        "platform.host",
        "summary.who",
    ]


def test_public_projection_drops_unlisted_keys_and_withholds_a_private_platform_label():
    document = {
        "schema_version": 1,
        "generated_at": "t",
        "window_days": 30,
        "owner": "example-agent-a",
        "platforms": [
            {
                "platform": "built on 198.51.100.7",
                "queue_depth": 1,
                "host": "198.51.100.7",
                "days": [
                    {
                        "date": "2026-08-29",
                        "runner_seconds": {"manual": 1.0, "example-agent-a": 5.0},
                        "task_count": {"manual": 1},
                        "median_wait_seconds": None,
                        "login": "example-agent-a",
                    }
                ],
            }
        ],
    }
    public = public_fields.public_usage_summary(document)
    assert public_fields.unknown_usage_keys(public) == []
    text = json.dumps(public)
    assert "example-agent-a" not in text and "198.51.100.7" not in text
    assert public["platforms"][0]["platform"] == "unknown"


def test_summary_file_is_written_under_the_published_directory(usage_service, control_env):
    usage_service.publish_usage_summary()
    path = control_env["config"].database_path.parent / "www" / "usage" / "summary.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    assert public_fields.unknown_usage_keys(document) == []
    text = path.read_text(encoding="utf-8")
    for private in PRIVATE_VALUES:
        assert private not in text
    assert oct(path.stat().st_mode & 0o777) == "0o644"


def test_ending_a_task_refreshes_the_summary_file_without_private_values(control_env):
    www = control_env["config"].database_path.parent / "www"
    service = ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=_directory(),
        published_canary_dir=www,
    )
    envelope = task_envelope("task-1")
    envelope["batch_id"] = SECRET_BATCH
    envelope["task"]["note"] = SECRET_NOTE
    service.submit_task(envelope, actor="user:example-agent-a", identity_login="example-agent-a")
    claim = service.claim_task("armbench", actor="runner:armbench")
    service.accept_task("armbench", "task-1", claim["claim_token"], actor="runner:armbench")
    outcome = {
        "schema_version": 1,
        "task_id": "task-1",
        "runner_id": "armbench",
        "state": "completed",
        "completed_at": now_text(),
        "result": {},
        "error": None,
    }
    service.record_outcome("armbench", "task-1", outcome, actor="runner:armbench")
    text = (www / "usage" / "summary.json").read_text(encoding="utf-8")
    assert json.loads(text)["platforms"][0]["days"][0]["task_count"]["manual"] == 1
    for private in ("example-agent-a", SECRET_BATCH, SECRET_NOTE, "198.51.100.7", "armbench"):
        assert private not in text


# ------------------------------------------------------------------ API and visibility


@pytest_asyncio.fixture
async def report_client(control_env, tmp_path):
    path = tmp_path / "report-tokens.json"
    records = [
        {"token_hash": hash_token(OPERATOR_TOKEN), "role": "operator", "label": "example-operator"},
        {"token_hash": hash_token(ARM_TOKEN), "role": "runner", "label": "armbench", "runner_id": "armbench"},
        {"token_hash": hash_token(AGENT_A_TOKEN), "role": "user", "label": "a", "login": "example-agent-a"},
        {"token_hash": hash_token(AGENT_B_TOKEN), "role": "user", "label": "b", "login": "example-agent-b"},
        {
            "token_hash": hash_token(READ_TOKEN),
            "role": "user",
            "label": "r",
            "login": "example-reader",
            "scope": "read",
        },
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


@pytest.mark.asyncio
async def test_usage_api_scopes_rows_by_token(report_client, control_env):
    with control_env["database"].transaction(immediate=True) as connection:
        _insert_usage(connection, "a-1", "example-agent-a", seconds=300.0, batch=SECRET_BATCH, note=SECRET_NOTE)
        _insert_usage(connection, "b-1", "example-agent-b", seconds=500.0, batch="batch-b1")
    url = "/api/v1/usage?by=agent&since=2026-08-01"

    operator = await (await report_client.get(url, headers=_bearer(OPERATOR_TOKEN))).json()
    assert {row["key"] for row in operator["rows"]} == {"example-agent-a", "example-agent-b"}

    mine = await report_client.get(url, headers=_bearer(AGENT_A_TOKEN))
    body = await mine.json()
    assert {row["key"] for row in body["rows"]} == {"example-agent-a", "others"}
    assert "example-agent-b" not in json.dumps(body)

    reader = await (await report_client.get(url, headers=_bearer(READ_TOKEN))).json()
    assert [row["key"] for row in reader["rows"]] == ["others"]

    refused = await report_client.get(url)
    assert refused.status == 401
    bad = await report_client.get("/api/v1/usage?by=login", headers=_bearer(OPERATOR_TOKEN))
    assert (bad.status, (await bad.json())["code"]) == (400, "USAGE_GROUP_INVALID")


@pytest.mark.asyncio
async def test_usage_api_refuses_a_runner_token(report_client):
    response = await report_client.get("/api/v1/usage", headers=_bearer(ARM_TOKEN))
    assert response.status == 403


async def _post(client, token, task_id, batch="batch-1"):
    envelope = task_envelope(task_id)
    envelope["batch_id"] = batch
    response = await client.post("/api/v1/tasks", json=envelope, headers=_bearer(token))
    assert response.status == 201


@pytest.mark.asyncio
async def test_a_user_token_cannot_read_another_logins_unfinished_task_or_batch(report_client):
    await _post(report_client, AGENT_A_TOKEN, "task-a")
    await _post(report_client, AGENT_B_TOKEN, "task-b")

    own = await report_client.get("/api/v1/tasks/task-a", headers=_bearer(AGENT_A_TOKEN))
    assert own.status == 200
    other = await report_client.get("/api/v1/tasks/task-a", headers=_bearer(AGENT_B_TOKEN))
    assert (other.status, (await other.json())["code"]) == (404, "TASK_NOT_FOUND")
    reader = await report_client.get("/api/v1/tasks/task-a", headers=_bearer(READ_TOKEN))
    assert reader.status == 404

    batch = await report_client.get("/api/v1/batches/batch-1", headers=_bearer(AGENT_A_TOKEN))
    assert [t["task_id"] for t in (await batch.json())["batch"]["tasks"]] == ["task-a"]
    empty = await report_client.get("/api/v1/batches/batch-1", headers=_bearer(READ_TOKEN))
    assert empty.status == 404

    operator = await report_client.get("/api/v1/batches/batch-1", headers=_bearer(OPERATOR_TOKEN))
    assert len((await operator.json())["batch"]["tasks"]) == 2


@pytest.mark.asyncio
async def test_a_finished_task_is_a_result_and_readable_by_any_user_token(report_client, control_env):
    await _post(report_client, AGENT_A_TOKEN, "task-a")
    with control_env["database"].transaction(immediate=True) as connection:
        connection.execute("UPDATE tasks SET state='completed' WHERE task_id='task-a'")
    shown = await report_client.get("/api/v1/tasks/task-a", headers=_bearer(READ_TOKEN))
    assert shown.status == 200
