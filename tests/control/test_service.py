import json
import threading

import pytest

from conductress.control.errors import AuthorizationError, ConflictError, ControlError
from conductress.control.provenance import ProvenanceGate
from conductress.control.service import ControlService
from conductress.control.users import UserDirectory

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


def test_cancel_unclaimed_and_claimed(control_env):
    service = control_env["service"]
    service.submit_task(task_envelope(), actor="operator:test")
    tasks, changed = service.cancel_task("task-1", actor="operator:test")
    replay, replay_changed = service.cancel_task("task-1", actor="operator:test")
    assert changed == 1
    assert replay_changed == 0  # already cancelled, nothing to change
    assert tasks[0]["state"] == "cancelled"
    assert replay[0]["state"] == "cancelled"

    # A claimed task is not cancelled outright: it is marked cancel-requested so
    # the runner stops at its next boundary.
    service.submit_task(task_envelope("task-2"), actor="operator:test")
    service.claim_task("armbench", actor="runner:armbench")
    tasks2, changed2 = service.cancel_task("task-2", actor="operator:test")
    assert changed2 == 1
    assert tasks2[0]["state"] == "cancel-requested"


def test_cancel_unknown_selector_raises(control_env):
    service = control_env["service"]
    with pytest.raises(Exception) as missing:
        service.cancel_task("nope", actor="operator:test")
    assert missing.value.code == "TASK_NOT_FOUND"


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


def test_dashboard_status_exposes_only_sanitized_nonterminal_tasks(control_env):
    service = control_env["service"]
    queued = task_envelope()
    queued["task"]["note"] = "descriptive benchmark note"
    queued["task"]["secret_field"] = "must not be public"
    service.submit_task(queued, actor="operator:test")
    service.submit_task(task_envelope("completed", runner_id="g4bench"), actor="operator:test")
    claim = service.claim_task("g4bench", actor="runner:g4bench")
    service.accept_task("g4bench", "completed", claim["claim_token"], actor="runner:g4bench")
    service.record_outcome("g4bench", "completed", task_outcome("completed", "g4bench"), actor="runner:g4bench")

    document = service.dashboard_status()
    assert "disabled" not in {runner["runner_id"] for runner in document["runners"]}
    arm = next(runner for runner in document["runners"] if runner["runner_id"] == "armbench")
    assert arm["total_count"] == 1
    assert arm["returned_count"] == 1
    assert arm["truncated"] is False
    assert arm["expected_duration_complete"] is True
    assert arm["expected_duration_sec"] == 78
    assert arm["remote_tasks"] == [
        {
            "id": "task-1",
            "type": "PerfTaskData",
            "note": "descriptive benchmark note",
            "source": "valkey",
            "specifier": "abc123",
            "state": "queued",
            "task_class": "manual",
            "priority": 100,
            "submitted_at": "2026-08-29T00:00:00Z",
            "expected_duration_sec": 78,
        }
    ]
    assert all(task["id"] != "completed" for runner in document["runners"] for task in runner["remote_tasks"])
    assert "submitted_by" not in arm["remote_tasks"][0]
    assert "envelope" not in arm["remote_tasks"][0]
    assert "outcome" not in arm["remote_tasks"][0]
    assert "secret_field" not in arm["remote_tasks"][0]


def test_dashboard_status_reports_full_count_when_task_details_are_capped(control_env):
    service = control_env["service"]
    for index in range(55):
        service.submit_task(task_envelope(f"task-{index:02d}"), actor="operator:test")

    document = service.dashboard_status()
    arm = next(runner for runner in document["runners"] if runner["runner_id"] == "armbench")
    assert arm["total_count"] == 55
    assert arm["returned_count"] == 50
    assert arm["truncated"] is True
    assert arm["expected_duration_complete"] is False
    assert len(arm["remote_tasks"]) == 50


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


class _FakeGitHub:
    def __init__(self, *, commits=None, open_pr_heads=None):
        self.commits = set(commits or set())
        self.open_pr_heads = dict(open_pr_heads or {})

    def commit_in_repo(self, repo, sha):
        return (repo, sha) in self.commits

    def open_pull_request_for_head(self, sha):
        return self.open_pr_heads.get(sha)


def _user_directory():
    return UserDirectory.from_dict(
        {
            "users": [
                {
                    "login": "rain",
                    "github": "rainsupreme",
                    "kind": "human",
                    "role": "owner",
                    "quota_runner_minutes_per_day": 1440,
                    "sources": ["valkey", "valkey-rainfall"],
                },
                {
                    "login": "dante",
                    "github": "xdk-amz",
                    "kind": "human",
                    "role": "collaborator",
                    "quota_runner_minutes_per_day": 240,
                },
            ]
        }
    )


def _gated_service(control_env, github, *, directory=None):
    return ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=directory,
        provenance_gate=ProvenanceGate(github),
    )


def _provenance_envelope(task_id="task-1", repo="valkey-io/valkey", sha="abc123", pr=None):
    envelope = task_envelope(task_id)
    envelope["provenance"] = {"repo": repo, "sha": sha, "recipe": None, "pr": pr}
    return envelope


def test_submit_accepts_allowlisted_provenance(control_env):
    service = _gated_service(control_env, _FakeGitHub(commits={("valkey-io/valkey", "abc123")}))
    task, created = service.submit_task(_provenance_envelope(), actor="operator:test")
    assert created is True
    assert task["envelope"]["provenance"]["repo"] == "valkey-io/valkey"


def test_submit_rejects_unverifiable_provenance(control_env):
    service = _gated_service(control_env, _FakeGitHub())
    with pytest.raises(AuthorizationError) as rejected:
        service.submit_task(_provenance_envelope(sha="ghost"), actor="operator:test")
    assert rejected.value.code == "PROVENANCE_REJECTED"


def test_submit_stores_resolved_pull_request(control_env):
    github = _FakeGitHub(open_pr_heads={"headsha": {"repo": "valkey-io/valkey", "number": 9, "head_sha": "headsha"}})
    service = _gated_service(control_env, github)
    envelope = _provenance_envelope(repo="fork/valkey", sha="headsha")
    task, _ = service.submit_task(envelope, actor="operator:test")
    assert task["envelope"]["provenance"]["pr"] == {"repo": "valkey-io/valkey", "number": 9, "head_sha": "headsha"}


def test_owner_may_bypass_provenance_gate(control_env):
    directory = _user_directory()
    service = _gated_service(control_env, _FakeGitHub(), directory=directory)
    envelope = _provenance_envelope(repo="random/repo", sha="unknown")
    # Owner login with explicit bypass is accepted despite unverifiable sha.
    task, created = service.submit_task(envelope, actor="user:rain", identity_login="rain", owner_bypass=True)
    assert created is True


def test_non_owner_cannot_bypass_provenance_gate(control_env):
    directory = _user_directory()
    service = _gated_service(control_env, _FakeGitHub(), directory=directory)
    envelope = _provenance_envelope(repo="random/repo", sha="unknown")
    with pytest.raises(AuthorizationError) as rejected:
        service.submit_task(envelope, actor="user:dante", identity_login="dante", owner_bypass=True)
    assert rejected.value.code == "PROVENANCE_REJECTED"


def test_user_fork_source_extends_allowlist(control_env):
    directory = _user_directory()
    # dante's fork is xdk-amz/valkey; a sha reachable there is accepted.
    github = _FakeGitHub(commits={("xdk-amz/valkey", "forksha")})
    service = _gated_service(control_env, github, directory=directory)
    envelope = _provenance_envelope(repo="xdk-amz/valkey", sha="forksha")
    task, created = service.submit_task(envelope, actor="user:dante", identity_login="dante")
    assert created is True


def test_submit_without_gate_ignores_provenance(control_env):
    # The stock control_env service has no provenance gate; provenance is stored
    # but not verified, so a deployment without GitHub reachability still works.
    service = control_env["service"]
    task, created = service.submit_task(_provenance_envelope(sha="whatever"), actor="operator:test")
    assert created is True


def test_record_outcome_publishes_results_jsonl_and_static_json(control_env, tmp_path):
    published_dir = tmp_path / "published"
    results_jsonl = tmp_path / "results.jsonl"
    service = ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        published_tasks_dir=published_dir,
        results_jsonl_path=results_jsonl,
    )
    service.submit_task(task_envelope("task-1"), actor="operator:test")
    claim = service.claim_task("armbench", actor="runner:armbench")
    service.accept_task("armbench", "task-1", claim["claim_token"], actor="runner:armbench")
    service.record_outcome("armbench", "task-1", task_outcome("task-1"), actor="runner:armbench")

    static = published_dir / "tasks" / "task-1.json"
    assert static.exists()
    published = json.loads(static.read_text(encoding="utf-8"))
    assert published["task_id"] == "task-1"
    assert published["outcome"]["result"]["score"] == 123.0
    lines = results_jsonl.read_text(encoding="utf-8").strip().splitlines()
    assert json.loads(lines[-1])["task_id"] == "task-1"


def test_canary_dir_alone_publishes_canary_files_but_no_task_records(control_env, tmp_path):
    # A deployment that sets only PUBLISHED_CANARY_DIR publishes the canary
    # status and nothing else: task records can carry private build and host
    # details, the canary status cannot.
    canary_root = tmp_path / "public"
    service = ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        published_canary_dir=canary_root,
    )
    service.submit_task(task_envelope("task-1"), actor="operator:test")
    claim = service.claim_task("armbench", actor="runner:armbench")
    service.accept_task("armbench", "task-1", claim["claim_token"], actor="runner:armbench")
    service.record_outcome("armbench", "task-1", task_outcome("task-1"), actor="runner:armbench")
    service.publish_canary_status()

    assert (canary_root / "canary" / "fleet.json").exists()
    assert (canary_root / "canary" / "armbench.json").exists()
    assert not (canary_root / "tasks").exists()
    assert not list(tmp_path.rglob("task-1.json"))


def test_published_canary_dir_falls_back_to_tasks_dir(monkeypatch, tmp_path):
    from conductress.control.config import ControlConfig

    monkeypatch.setenv("CONTROL_DB_PATH", str(tmp_path / "control.db"))
    for name in ("PUBLISHED_TASKS_DIR", "PUBLISHED_CANARY_DIR"):
        monkeypatch.delenv(name, raising=False)
    assert ControlConfig.from_env().published_canary_dir is None
    monkeypatch.setenv("PUBLISHED_TASKS_DIR", str(tmp_path / "www"))
    assert ControlConfig.from_env().published_canary_dir == tmp_path / "www"
    monkeypatch.setenv("PUBLISHED_CANARY_DIR", str(tmp_path / "canary-only"))
    config = ControlConfig.from_env()
    assert config.published_canary_dir == tmp_path / "canary-only"
    assert config.published_tasks_dir == tmp_path / "www"
