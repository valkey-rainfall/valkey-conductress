import threading

import pytest

from conductress.control.errors import ConflictError, ControlError

from .helpers import runner_status, task_envelope, task_outcome


def _accepted_task(service, task_id="task-1"):
    service.submit_task(task_envelope(task_id), actor="operator:test")
    claim = service.claim_task("armbench", actor="runner:armbench")
    service.accept_task("armbench", task_id, claim["claim_token"], actor="runner:armbench")
    return claim


def test_submit_is_idempotent_and_conflicts_on_changed_payload(control_env):
    service = control_env["service"]
    task, created = service.submit_task(task_envelope(), actor="operator:test", idempotency_key="request-1")
    replay, replay_created = service.submit_task(task_envelope(), actor="operator:test", idempotency_key="request-1")
    assert created is True
    assert replay_created is False
    assert replay["task_id"] == task["task_id"]

    changed = task_envelope(priority=999)
    with pytest.raises(ConflictError) as conflict:
        service.submit_task(changed, actor="operator:test", idempotency_key="request-1")
    assert conflict.value.code == "IDEMPOTENCY_CONFLICT"


def test_invalid_envelope_and_disabled_runner_fail_closed(control_env):
    service = control_env["service"]
    invalid = task_envelope()
    invalid["unexpected"] = True
    with pytest.raises(ControlError) as schema_error:
        service.submit_task(invalid, actor="operator:test")
    assert schema_error.value.code == "SCHEMA_INVALID"

    disabled = task_envelope(runner_id="disabled")
    with pytest.raises(ConflictError) as runner_error:
        service.submit_task(disabled, actor="operator:test")
    assert runner_error.value.code == "RUNNER_DISABLED"


def test_claim_uses_priority_then_fifo_and_is_replayable(control_env):
    service = control_env["service"]
    service.submit_task(task_envelope("low", priority=10), actor="operator:test")
    service.submit_task(task_envelope("high", priority=100), actor="operator:test")

    first = service.claim_task("armbench", actor="runner:armbench")
    replay = service.claim_task("armbench", actor="runner:armbench")

    assert first["task"]["task_id"] == "high"
    assert replay == first


def test_accept_and_complete_are_idempotent_and_never_reassigned(control_env):
    service = control_env["service"]
    claim = _accepted_task(service)
    task, changed = service.accept_task("armbench", "task-1", claim["claim_token"], actor="runner:armbench")
    assert changed is False
    assert task["state"] == "accepted"

    outcome = task_outcome()
    completed, completed_changed = service.record_outcome("armbench", "task-1", outcome, actor="runner:armbench")
    replay, replay_changed = service.record_outcome("armbench", "task-1", outcome, actor="runner:armbench")
    assert completed_changed is True
    assert replay_changed is False
    assert completed["state"] == replay["state"] == "completed"
    assert service.claim_task("armbench", actor="runner:armbench") is None

    with pytest.raises(ConflictError) as conflict:
        service.record_outcome(
            "armbench",
            "task-1",
            task_outcome(state="failed"),
            actor="runner:armbench",
        )
    assert conflict.value.code == "OUTCOME_CONFLICT"


def test_fail_outcome_and_wrong_runner_rejected(control_env):
    service = control_env["service"]
    claim = _accepted_task(service)
    with pytest.raises(ConflictError) as wrong_runner:
        service.accept_task("g4bench", "task-1", claim["claim_token"], actor="runner:g4bench")
    assert wrong_runner.value.code == "WRONG_RUNNER"

    failed, changed = service.record_outcome(
        "armbench",
        "task-1",
        task_outcome(state="failed"),
        actor="runner:armbench",
    )
    assert changed is True
    assert failed["state"] == "failed"


def test_cancel_only_before_claim(control_env):
    service = control_env["service"]
    service.submit_task(task_envelope(), actor="operator:test")
    cancelled, changed = service.cancel_task("task-1", actor="operator:test")
    replay, replay_changed = service.cancel_task("task-1", actor="operator:test")
    assert changed is True
    assert replay_changed is False
    assert cancelled["state"] == replay["state"] == "cancelled"

    service.submit_task(task_envelope("task-2"), actor="operator:test")
    service.claim_task("armbench", actor="runner:armbench")
    with pytest.raises(ConflictError) as conflict:
        service.cancel_task("task-2", actor="operator:test")
    assert conflict.value.code == "TASK_NOT_CANCELLABLE"


def test_expired_claim_requeues_but_accepted_task_never_does(control_env):
    service = control_env["service"]
    database = control_env["database"]
    service.submit_task(task_envelope(), actor="operator:test")
    claim = service.claim_task("armbench", actor="runner:armbench")
    with database.transaction(immediate=True) as connection:
        connection.execute("UPDATE tasks SET lease_expires='2000-01-01T00:00:00Z' WHERE task_id='task-1'")
    assert service.expire_stale_claims() == 1
    assert service.get_task("task-1")["state"] == "queued"

    claim = service.claim_task("armbench", actor="runner:armbench")
    service.accept_task("armbench", "task-1", claim["claim_token"], actor="runner:armbench")
    with database.transaction(immediate=True) as connection:
        connection.execute("UPDATE tasks SET lease_expires='2000-01-01T00:00:00Z' WHERE task_id='task-1'")
    assert service.expire_stale_claims() == 0
    assert service.get_task("task-1")["state"] == "accepted"


def test_status_and_fleet_counts(control_env):
    service = control_env["service"]
    service.push_status("armbench", runner_status(), actor="runner:armbench")
    service.submit_task(task_envelope(), actor="operator:test")

    arm = service.fleet_status("armbench")[0]
    assert arm["status"]["host"] == "host-a"
    assert arm["task_counts"] == {"queued": 1}


def test_concurrent_claims_produce_one_transition_and_one_token(control_env):
    service = control_env["service"]
    database = control_env["database"]
    service.submit_task(task_envelope(), actor="operator:test")
    barrier = threading.Barrier(4)
    claims = []
    errors = []

    def claim():
        try:
            barrier.wait()
            claims.append(service.claim_task("armbench", actor="runner:armbench"))
        except Exception as exc:  # surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=claim) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert {item["task"]["task_id"] for item in claims} == {"task-1"}
    assert len({item["claim_token"] for item in claims}) == 1
    with database.read() as connection:
        transitions = connection.execute("SELECT COUNT(*) FROM audit_log WHERE action='task.claim'").fetchone()[0]
    assert transitions == 1


def test_restart_preserves_queued_claimed_and_accepted_states(control_env):
    from conductress.control.service import ControlService

    service = control_env["service"]
    service.submit_task(task_envelope("queued"), actor="operator:test")
    service.submit_task(task_envelope("claimed", priority=200), actor="operator:test")
    claim = service.claim_task("armbench", actor="runner:armbench")
    assert claim["task"]["task_id"] == "claimed"

    restarted = ControlService(control_env["database"], control_env["registry"], claim_lease_seconds=300)
    replay = restarted.claim_task("armbench", actor="runner:armbench")
    assert replay["claim_token"] == claim["claim_token"]
    restarted.accept_task("armbench", "claimed", claim["claim_token"], actor="runner:armbench")

    restarted_again = ControlService(control_env["database"], control_env["registry"], claim_lease_seconds=300)
    assert restarted_again.get_task("queued")["state"] == "queued"
    assert restarted_again.get_task("claimed")["state"] == "accepted"
    assert restarted_again.expire_stale_claims() == 0


def test_invalid_datetime_format_fails_closed(control_env):
    service = control_env["service"]
    invalid = task_envelope()
    invalid["submitted_at"] = "not-a-date"
    with pytest.raises(ControlError) as error:
        service.submit_task(invalid, actor="operator:test")
    assert error.value.code == "SCHEMA_INVALID"
