"""Service-level tests for approval, batches, cancel, and mine."""

import pytest

from conductress.control.provenance import ProvenanceGate
from conductress.control.service import ControlService
from conductress.control.users import UserDirectory

from .helpers import task_envelope


def _directory():
    return UserDirectory.from_dict(
        {
            "users": [
                {
                    "login": "rain",
                    "github": "rainsupreme",
                    "kind": "human",
                    "role": "owner",
                    "quota_runner_minutes_per_day": 0,
                    "sources": ["valkey"],
                },
                {
                    "login": "dante",
                    "github": "xdk-amz",
                    "kind": "human",
                    "role": "collaborator",
                    "quota_runner_minutes_per_day": 0,
                },
            ]
        }
    )


def _service(control_env, *, directory=None, notifier=None, notification_url=None):
    return ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=directory,
        provenance_gate=None,
        notification_url=notification_url,
        notifier=notifier,
    )


def _batch_envelope(task_id, batch_id, submitter_login=None):
    envelope = task_envelope(task_id)
    envelope["batch_id"] = batch_id
    if submitter_login:
        envelope["submitter"] = {"login": submitter_login, "kind": "human", "sponsor": None}
    return envelope


def test_known_user_within_quota_is_queued(control_env):
    service = _service(control_env, directory=_directory())
    task, _ = service.submit_task(task_envelope("t1"), actor="user:dante", identity_login="dante")
    assert task["state"] == "queued"
    assert task["submitter_login"] == "dante"


def test_unknown_user_lands_in_pending_approval(control_env):
    service = _service(control_env, directory=_directory())
    task, _ = service.submit_task(task_envelope("t1"), actor="user:ghost", identity_login="ghost")
    assert task["state"] == "pending-approval"


def test_operator_without_directory_user_is_queued(control_env):
    # Operator token (no identity_login) with a directory present stays trusted.
    service = _service(control_env, directory=_directory())
    task, _ = service.submit_task(task_envelope("t1"), actor="operator:test")
    assert task["state"] == "queued"


def test_over_quota_lands_in_pending_approval(control_env):
    directory = UserDirectory.from_dict(
        {
            "users": [
                {
                    "login": "dante",
                    "github": "xdk-amz",
                    "kind": "human",
                    "role": "collaborator",
                    "quota_runner_minutes_per_day": 1,  # 60 seconds/day, tiny
                }
            ]
        }
    )
    service = _service(control_env, directory=directory)
    # First submission likely already exceeds a 1-minute daily budget for a
    # standard benchmark cell; if not the second surely does.
    service.submit_task(task_envelope("t1"), actor="user:dante", identity_login="dante")
    task2, _ = service.submit_task(task_envelope("t2"), actor="user:dante", identity_login="dante")
    assert task2["state"] == "pending-approval"


def test_approve_moves_pending_batch_to_queued(control_env):
    service = _service(control_env, directory=_directory())
    service.submit_task(_batch_envelope("t1", "b1"), actor="user:ghost", identity_login="ghost")
    service.submit_task(_batch_envelope("t2", "b1"), actor="user:ghost", identity_login="ghost")
    pending = service.pending_tasks()
    assert {t["task_id"] for t in pending} == {"t1", "t2"}

    tasks, changed = service.approve_tasks("b1", actor="approver:rain")
    assert changed == 2
    assert all(t["state"] == "queued" for t in tasks)
    assert service.pending_tasks() == []


def test_reject_cancels_pending_task(control_env):
    service = _service(control_env, directory=_directory())
    service.submit_task(task_envelope("t1"), actor="user:ghost", identity_login="ghost")
    tasks, changed = service.reject_tasks("t1", actor="approver:rain")
    assert changed == 1
    assert tasks[0]["state"] == "cancelled"


def test_user_batch_cancel_skips_own_running_task(control_env):
    service = _service(control_env, directory=_directory())
    service.submit_task(_batch_envelope("t1", "b1", "dante"), actor="user:dante", identity_login="dante")
    service.submit_task(_batch_envelope("t2", "b1", "dante"), actor="user:dante", identity_login="dante")
    # Claim one of them so it is in-flight.
    service.claim_task("armbench", actor="runner:armbench")
    tasks, changed, skipped = service.cancel_task("b1", actor="user:dante", identity_login="dante", is_operator=False)
    states = {t["task_id"]: t["state"] for t in tasks}
    assert changed == 1
    assert sorted(states.values()) == ["cancelled", "claimed"]
    assert skipped["already_running"] == [t["task_id"] for t in tasks if t["state"] == "claimed"]


def test_non_owner_cannot_cancel_another_users_task(control_env):
    service = _service(control_env, directory=_directory())
    service.submit_task(_batch_envelope("t1", "b1", "rain"), actor="user:rain", identity_login="rain")
    # dante (non-owner) attempts to cancel rain's task: nothing changes.
    tasks, changed, skipped = service.cancel_task("t1", actor="user:dante", identity_login="dante", is_operator=False)
    assert changed == 0
    assert tasks == []
    assert skipped == {"not_yours": 1, "already_running": []}


def test_list_mine_filters_by_submitter(control_env):
    service = _service(control_env, directory=_directory())
    service.submit_task(_batch_envelope("t1", "b1", "dante"), actor="user:dante", identity_login="dante")
    service.submit_task(_batch_envelope("t2", "b2", "rain"), actor="user:rain", identity_login="rain")
    mine = service.list_mine("dante")
    assert [t["task_id"] for t in mine] == ["t1"]


def test_get_batch_returns_member_tasks(control_env):
    service = _service(control_env, directory=_directory())
    service.submit_task(_batch_envelope("t1", "b1", "dante"), actor="user:dante", identity_login="dante")
    service.submit_task(_batch_envelope("t2", "b1", "dante"), actor="user:dante", identity_login="dante")
    batch = service.get_batch("b1")
    assert batch["batch_id"] == "b1"
    assert {t["task_id"] for t in batch["tasks"]} == {"t1", "t2"}


def test_pending_approval_fires_notification():
    captured = {}

    def notifier(request):
        captured["url"] = request.full_url
        captured["body"] = request.data

    # Build a service with a notifier and a URL via a lightweight harness.
    import json as _json
    import tempfile
    from pathlib import Path

    from conductress.control.db import ControlDatabase
    from conductress.control.fleet_registry import FleetRegistry

    tmp = Path(tempfile.mkdtemp())
    (tmp / "fleet.json").write_text(
        _json.dumps(
            {
                "schema_version": 1,
                "runners": [
                    {
                        "runner_id": "armbench",
                        "display_name": "arm",
                        "platform": "arm64/x",
                        "platform_aliases": ["arm64"],
                        "enabled": True,
                        "canary_profile": None,
                        "status_ttl_seconds": 900,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    database = ControlDatabase(tmp / "control.db", tmp / "audit.jsonl")
    database.initialize()
    registry = FleetRegistry.from_file(tmp / "fleet.json")
    directory = _directory()
    service = ControlService(
        database,
        registry,
        300,
        user_directory=directory,
        notification_url="https://hook.example/notify",
        notifier=notifier,
    )
    service.submit_task(task_envelope("t1"), actor="user:ghost", identity_login="ghost")
    assert captured["url"] == "https://hook.example/notify"
    payload = _json.loads(captured["body"].decode("utf-8"))
    assert payload["event"] == "task.pending_approval"
    assert payload["task_id"] == "t1"
