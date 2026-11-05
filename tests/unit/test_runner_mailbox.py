import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from conductress import task_queue
from conductress.fleet_client import FleetClientError
from conductress.runner_mailbox import RunnerMailbox
from conductress.task_queue import TaskQueue

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests" / "fixtures" / "golden_tasks" / "PerfTaskData.json"


def inner_task():
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def claim_document():
    task = inner_task()
    task_id = "2026.08.29_00.00.00.123456"
    envelope = {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": "armbench",
        "task_class": "manual",
        "priority": 100,
        "submitted_at": "2026-08-29T00:00:00Z",
        "submitted_by": "rain",
        "canary_id": None,
        "task": task,
    }
    return {
        "schema_version": 1,
        "claim": {
            "task": {"task_id": task_id, "runner_id": "armbench", "envelope": envelope},
            "claim_token": "claim-token",
            "lease_expires": "2026-08-29T00:05:00Z",
        },
    }


class FakeRunnerClient:
    def __init__(self, *, accept_failures=0, outcome_failures=0):
        self.claims = [claim_document()]
        self.accept_failures = accept_failures
        self.outcome_failures = outcome_failures
        self.accepted = []
        self.outcomes = []
        self.statuses = []

    def claim_task(self, runner_id):
        assert runner_id == "armbench"
        return self.claims.pop(0) if self.claims else None

    def accept_task(self, task_id, claim_token):
        if self.accept_failures:
            self.accept_failures -= 1
            raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)
        self.accepted.append((task_id, claim_token))
        return {"schema_version": 1, "task": {"task_id": task_id}, "changed": True}

    def report_outcome(self, task_id, outcome):
        if self.outcome_failures:
            self.outcome_failures -= 1
            raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)
        self.outcomes.append((task_id, outcome))
        return {"schema_version": 1, "task": {"task_id": task_id}, "changed": True}

    def push_status(self, runner_id, status):
        self.statuses.append((runner_id, status))
        return {"schema_version": 1, "updated": True}


@pytest.fixture(autouse=True)
def mailbox_environment(monkeypatch, tmp_path):
    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
    monkeypatch.setattr(
        "conductress.runner_mailbox.get_runner_config",
        lambda: SimpleNamespace(runner_id="armbench"),
    )
    monkeypatch.setattr("conductress.runner_mailbox.CONDUCTRESS_OUTPUT", tmp_path / "output.jsonl")
    monkeypatch.setattr("conductress.runner_mailbox.CONDUCTRESS_FAILED_LOG", tmp_path / "failed.jsonl")


def make_mailbox(tmp_path, client, mode="live"):
    return RunnerMailbox(
        tmp_path / "delivery.json",
        mode=mode,
        client_factory=lambda: client,
    )


def test_live_claim_import_accept_and_complete(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")

    task = mailbox.poll(queue)
    assert task is not None
    assert queue.has_task(task.task_id)
    assert mailbox.journal.active["stage"] == "accepted"
    assert client.accepted == [(task.task_id, "claim-token")]

    result = {
        "task_id": task.task_id,
        "method": "perf-get",
        "score": 123,
        "commit_hash": "abc123",
        "end_time": "2026.08.29_00.01.00.000000",
        "expected_duration_sec": 300,
        "observed_duration_sec": 275.5,
        "provenance_schema_version": 1,
        "runner_id": "armbench",
        "platform": "AWS Graviton 3",
        "environment": {"kernel_release": "6.1", "conductress_revision": "abc123"},
    }
    mailbox.stage_success(task, result=result)
    queue.finish_task(task)
    assert mailbox.flush_pending_outcome() is True
    assert mailbox.journal.active is None
    assert client.outcomes[0][1]["state"] == "completed"
    assert client.outcomes[0][1]["result"]["expected_duration_sec"] == 300
    assert client.outcomes[0][1]["result"]["observed_duration_sec"] == 275.5
    assert client.outcomes[0][1]["result"]["provenance_schema_version"] == 1
    assert client.outcomes[0][1]["result"]["runner_id"] == "armbench"
    assert client.outcomes[0][1]["result"]["platform"] == "AWS Graviton 3"
    assert client.outcomes[0][1]["result"]["environment"]["conductress_revision"] == "abc123"
    assert mailbox.status()["imported_count_total"] == 1


def test_imported_task_never_executes_before_acceptance(tmp_path):
    client = FakeRunnerClient(accept_failures=1)
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")

    assert mailbox.poll(queue) is None
    assert mailbox.blocks_execution() is True
    assert queue.get_queue_length() == 1

    task = mailbox.poll(queue)
    assert task is not None
    assert mailbox.blocks_execution() is False
    assert mailbox.journal.active["stage"] == "accepted"


def test_pending_outcome_survives_failure_and_restart(tmp_path):
    client = FakeRunnerClient(outcome_failures=1)
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    mailbox.stage_failure(task, "boom")
    queue.finish_task(task)

    assert mailbox.flush_pending_outcome() is False
    assert mailbox.journal.active["stage"] == "outcome_pending"

    restarted = make_mailbox(tmp_path, client)
    assert restarted.reconcile(queue) is None
    assert restarted.journal.active is None
    assert client.outcomes[0][1]["state"] == "failed"


def test_restart_detects_existing_result_without_reexecution(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    output = tmp_path / "output.jsonl"
    output.write_text(json.dumps({"task_id": task.task_id, "method": "perf-get", "score": 1}) + "\n")

    restarted = make_mailbox(tmp_path, client)
    assert restarted.reconcile(queue) is None
    assert not queue.has_task(task.task_id)
    assert restarted.journal.active is None
    assert len(client.outcomes) == 1


def test_shadow_mode_pushes_status_but_never_claims(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client, mode="shadow")
    queue = TaskQueue(tmp_path / "queue")
    assert mailbox.poll(queue) is None
    assert len(client.claims) == 1
    assert mailbox.push_status({"schema_version": 1, "runner_id": "armbench"}) is True
    assert len(client.statuses) == 1


def test_irrecoverably_expired_claim_discards_local_import(tmp_path):
    client = FakeRunnerClient()

    def reject_accept(_task_id, _claim_token):
        raise FleetClientError("CLAIM_EXPIRED", "expired", 4)

    client.accept_task = reject_accept
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")

    assert mailbox.poll(queue) is None
    assert mailbox.journal.active is None
    assert queue.get_queue_length() == 0


def test_recovered_result_is_staged_before_queue_removal(tmp_path, monkeypatch):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    output = tmp_path / "output.jsonl"
    output.write_text(json.dumps({"task_id": task.task_id, "score": 1}) + "\n")

    restarted = make_mailbox(tmp_path, client)

    def fail_stage(*_args, **_kwargs):
        raise RuntimeError("crash while staging")

    monkeypatch.setattr(restarted, "stage_success", fail_stage)
    with pytest.raises(RuntimeError, match="crash while staging"):
        restarted.reconcile(queue)
    assert queue.has_task(task.task_id)
    assert restarted.journal.active["stage"] == "accepted"


def _rich_result(task_id):
    """A result row with rich stats, a memory breakdown, and huge raw stacks."""
    return {
        "task_id": task_id,
        "method": "cachecannon",
        "score": 5_663_000,
        "commit_hash": "abc123",
        "end_time": "2026.09.21_00.01.00.000000",
        "cv": 0.0123,
        "reps": 5,
        "score_min": 5_600_000,
        "score_max": 5_720_000,
        "score_aggregate": 5_663_000,
        "runner_id": "armbench",
        "platform": "AWS Graviton 3",
        "data": {
            "per_run_rps": [5.6e6, 5.7e6, 5.66e6],
            "mean_rps": 5_663_000,
            "ci_95": [5.61e6, 5.71e6],
            "client_cpu": {"cores_busy_per_rep": [7.9], "saturated": False},
            "perf_counters": {"cycles_per_req": 12.3},
            "perf_counters_scope": "user+kernel",
            "io-threads": 7,
            "connections": 512,
            # A memory breakdown MUST travel; per-category totals are small.
            "results": [
                {
                    "breakdown": {"dict": 37.43, "robj_embval": 48.0},
                    # Resolved per-frame stacks are unbounded and MUST be dropped.
                    "raw_stacks": ["frame " * 100] * 5000,
                }
            ],
            # Top-level raw_stacks is dropped too.
            "raw_stacks": ["x" * 1000] * 1000,
            "cpu_stacks_main": [["main;processCommand", 10]] * 5000,
            "cpu_stacks_io": [["io;readQuery", 10]] * 5000,
            "memory": {"rss_hwm": 123456, "used_memory_peak": 654321},
            # Previously-excluded fields now travel as part of the full record.
            "toml_config": "save 900 1\nappendonly no\n",
            "topology": {"nodes": 2},
        },
    }


def test_success_summary_carries_full_record_without_raw_stacks(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)

    mailbox.stage_success(task, result=_rich_result(task.task_id))
    queue.finish_task(task)
    assert mailbox.flush_pending_outcome() is True

    published = client.outcomes[0][1]["result"]
    # Top-level stats present:
    assert published["cv"] == 0.0123
    assert published["score_aggregate"] == 5_663_000
    data = published["data"]
    # The full data record travels, including fields the old allowlist dropped.
    assert data["mean_rps"] == 5_663_000
    assert data["toml_config"].startswith("save")
    assert data["topology"] == {"nodes": 2}
    # Memory scalars and the jemalloc breakdown are kept.
    assert data["memory"]["rss_hwm"] == 123456
    assert data["results"][0]["breakdown"]["robj_embval"] == 48.0
    # Only raw_stacks is stripped, top level and per-run.
    assert "raw_stacks" not in data
    assert "raw_stacks" not in data["results"][0]
    # The unbounded CPU flamegraph stacks are stripped too.
    assert "cpu_stacks_main" not in data
    assert "cpu_stacks_io" not in data


def test_restart_finds_multi_megabyte_result_row(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)

    # A ~2.5 MB row dominated by raw stacks: no fixed window holds it whole.
    output = tmp_path / "output.jsonl"
    row = _rich_result(task.task_id)
    row["data"]["raw_stacks"] = ["x" * 2_500_000]
    output.write_text(json.dumps(row) + "\n")

    restarted = make_mailbox(tmp_path, client)
    assert restarted.reconcile(queue) is None
    assert not queue.has_task(task.task_id)
    published = client.outcomes[0][1]["result"]
    assert published["score"] == 5_663_000
    assert published["cv"] == 0.0123
    # The unbounded stacks are gone; the breakdown survives.
    assert "raw_stacks" not in published.get("data", {})
    assert published["data"]["results"][0]["breakdown"]["dict"] == 37.43


def test_find_failure_locates_large_failure_row(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)

    failed = tmp_path / "failed.jsonl"
    failed.write_text(json.dumps({"task_id": task.task_id, "error": "boom", "pad": "p" * 1_500_000}) + "\n")

    restarted = make_mailbox(tmp_path, client)
    assert restarted.reconcile(queue) is None
    assert not queue.has_task(task.task_id)
    assert client.outcomes[0][1]["state"] == "failed"
    assert client.outcomes[0][1]["error"] == "boom"


def test_unloadable_envelope_fails_task_without_crash_loop(tmp_path):
    """An envelope this runner cannot deserialize is failed, not raised.

    A task body carrying an unknown ``task_type`` cannot be turned into a
    runnable task. Reconcile must report it failed to the control plane, drop it
    from the local queue, and clear the active slot so the runner proceeds.
    """
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    assert task is not None

    # Corrupt the journalled envelope so from_dict raises on the next reconcile.
    active = mailbox.journal.active
    active["envelope"]["task"]["task_type"] = "NoSuchTaskType"
    mailbox.journal.set_active(active)

    restarted = make_mailbox(tmp_path, client)
    assert restarted.reconcile(queue) is None
    assert restarted.journal.active is None
    assert not queue.has_task(task.task_id)
    assert client.outcomes[0][1]["state"] == "failed"
    assert "NoSuchTaskType" in client.outcomes[0][1]["error"]


def test_unloadable_envelope_outcome_survives_offline_control(tmp_path):
    """If control is unreachable when failing an unloadable task, the outcome
    stays pending and flushes on a later boundary rather than being lost."""
    client = FakeRunnerClient(outcome_failures=1)
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    active = mailbox.journal.active
    active["envelope"]["task"]["task_type"] = "NoSuchTaskType"
    mailbox.journal.set_active(active)

    offline = make_mailbox(tmp_path, client)
    assert offline.reconcile(queue) is None
    # Flush failed; outcome held pending.
    assert offline.journal.active is not None
    assert offline.journal.active["stage"] == "outcome_pending"

    recovered = make_mailbox(tmp_path, client)
    assert recovered.reconcile(queue) is None
    assert recovered.journal.active is None
    assert client.outcomes[0][1]["state"] == "failed"


def test_cancel_requested_reads_control_task_state(tmp_path):
    """A runner honours a cancel-requested task it owns at a boundary."""
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)

    # Control reports the owned task as cancel-requested.
    client.task = lambda task_id: {"task": {"task_id": task_id, "state": "cancel-requested"}}
    assert mailbox.cancel_requested(task.task_id) is True

    # A different state is not a cancel request.
    client.task = lambda task_id: {"task": {"task_id": task_id, "state": "accepted"}}
    assert mailbox.cancel_requested(task.task_id) is False

    # A task the mailbox does not own is never cancel-requested.
    assert mailbox.cancel_requested("some-other-task") is False


def test_cancel_requested_false_when_control_unreachable(tmp_path):
    client = FakeRunnerClient()
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)

    def unreachable(_task_id):
        raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)

    client.task = unreachable
    # A transient control outage must not abort a running benchmark.
    assert mailbox.cancel_requested(task.task_id) is False


# ------------------------------------------------------------- outcome size


class RejectingClient(FakeRunnerClient):
    """Refuses outcome reports with the given (status, code) answers, in order, then accepts."""

    def __init__(self, answers):
        super().__init__()
        self.answers = list(answers)
        self.attempts = []

    def report_outcome(self, task_id, outcome):
        self.attempts.append(json.loads(json.dumps(outcome)))
        if self.answers:
            status, code = self.answers.pop(0)
            if status is None:
                raise FleetClientError("CONTROL_UNREACHABLE", "offline", 3)
            raise FleetClientError(code, "refused", 1, status)
        return super().report_outcome(task_id, outcome)


def _replica_read_row(task_id, reps=5, ticks=35):
    series = {
        "count": ticks,
        "t": [1000.0 + i for i in range(ticks)],
        "t_local": [1000.2 + i for i in range(ticks)],
        "instances": {
            "6379": {"master_repl_offset": [6000 * i for i in range(ticks)], "role": ["master"] * ticks},
            "6380": {"master_repl_offset": [6000 * i - 300 for i in range(ticks)], "role": ["slave"] * ticks},
        },
    }
    return {
        "task_id": task_id,
        "method": "replica-read",
        "score": 4_230_000.0,
        "runner_id": "armbench",
        "data": {
            "per_run_rps": [4.2e6] * reps,
            "mean_rps": 4_230_000.0,
            "bottleneck": {"verdict": "ambiguous", "valid": False},
            "per_rep_results": [
                {"reader_rps": 4.2e6, "lag": {"samples": 26, "mean_bytes": 10}, "series": dict(series)}
                for _ in range(reps)
            ],
            "lscpu": "Architecture: aarch64\n" * 50,
            "reader_toml": "[reader]\n" * 20,
            "writer_toml": "[writer]\n" * 20,
        },
    }


def _staged(tmp_path, client, **mailbox_kwargs):
    mailbox = RunnerMailbox(tmp_path / "delivery.json", mode="live", client_factory=lambda: client, **mailbox_kwargs)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    mailbox.stage_success(task, result=_replica_read_row(task.task_id))
    queue.finish_task(task)
    return mailbox, task


def test_max_outcome_bytes_defaults_below_server_limit_and_reads_env(tmp_path, monkeypatch):
    from conductress.control.config import DEFAULT_MAX_BODY_BYTES
    from conductress.outcome_size import DEFAULT_MAX_OUTCOME_BYTES

    mailbox = make_mailbox(tmp_path, FakeRunnerClient())
    assert mailbox.max_outcome_bytes == DEFAULT_MAX_OUTCOME_BYTES < DEFAULT_MAX_BODY_BYTES
    assert mailbox.status()["max_outcome_bytes"] == DEFAULT_MAX_OUTCOME_BYTES

    monkeypatch.setenv("CONDUCTRESS_MAX_OUTCOME_BYTES", "65536")
    assert make_mailbox(tmp_path, FakeRunnerClient()).max_outcome_bytes == 65536
    monkeypatch.setenv("CONDUCTRESS_MAX_OUTCOME_BYTES", "12")
    with pytest.raises(ValueError, match="at least 1024"):
        make_mailbox(tmp_path, FakeRunnerClient())
    monkeypatch.setenv("CONDUCTRESS_MAX_OUTCOME_BYTES", "lots")
    with pytest.raises(ValueError, match="integer"):
        make_mailbox(tmp_path, FakeRunnerClient())


def test_preflight_trims_an_oversized_outcome_to_the_budget(tmp_path):
    from conductress.outcome_size import wire_bytes

    client = RejectingClient([])
    mailbox, task = _staged(tmp_path, client, max_outcome_bytes=4096)

    staged = mailbox.journal.active["outcome"]
    assert wire_bytes(staged) <= 4096
    record = staged["result"]["outcome_delivery"]
    assert record["trimmed"] is True and record["reason"].startswith("preflight: ")
    assert record["original_bytes"] > 4096 >= record["sent_bytes"]
    assert record["full_result_path"].endswith("output.jsonl")
    assert record["dropped"] and all("series" in item for item in record["dropped"])
    # Scalars and per-rep aggregates are kept.
    data = staged["result"]["data"]
    assert data["mean_rps"] == 4_230_000.0 and data["per_rep_results"][0]["lag"]["samples"] == 26
    downgrade = mailbox.status()["last_outcome_downgrade"]
    assert downgrade["task_id"] == task.task_id and downgrade["stage"] == "trimmed"

    assert mailbox.flush_pending_outcome() is True
    assert client.attempts[0] == staged and mailbox.journal.active is None


def test_small_outcome_is_sent_whole_without_a_delivery_record(tmp_path):
    client = RejectingClient([])
    mailbox, _task = _staged(tmp_path, client)
    assert mailbox.flush_pending_outcome() is True
    sent = client.attempts[0]["result"]
    assert "outcome_delivery" not in sent
    assert all("series" in rep for rep in sent["data"]["per_rep_results"])
    assert mailbox.status()["last_outcome_downgrade"] is None


def test_413_sends_reduced_outcome_once_then_clears(tmp_path):
    # nginx answers 413 without a JSON body: the client sees HTTP_413.
    client = RejectingClient([(413, "HTTP_413")])
    mailbox, task = _staged(tmp_path, client)

    assert mailbox.flush_pending_outcome() is True
    assert mailbox.journal.active is None
    assert len(client.attempts) == 2
    full, reduced = client.attempts
    assert "outcome_delivery" not in full["result"]
    assert reduced["state"] == "completed"
    record = reduced["result"]["outcome_delivery"]
    assert record["reason"].startswith("rejected: HTTP_413 (HTTP 413), ")
    data = reduced["result"]["data"]
    assert all("series" not in rep for rep in data["per_rep_results"])
    assert "lscpu" not in data and "reader_toml" not in data
    assert data["mean_rps"] == 4_230_000.0 and data["per_rep_results"][0]["reader_rps"] == 4.2e6
    downgrade = mailbox.status()["last_outcome_downgrade"]
    assert downgrade["stage"] == "reduced" and downgrade["task_id"] == task.task_id
    assert downgrade["sent_bytes"] < downgrade["original_bytes"]
    # Delivery succeeded, so the contact stats say reachable with no error.
    assert mailbox.status()["last_error"] is None


def test_reduced_outcome_rejected_again_reports_failed_with_pointer(tmp_path):
    client = RejectingClient([(413, "BODY_TOO_LARGE"), (413, "BODY_TOO_LARGE")])
    mailbox, task = _staged(tmp_path, client)

    assert mailbox.flush_pending_outcome() is True
    assert mailbox.journal.active is None
    assert [a["state"] for a in client.attempts] == ["completed", "completed", "failed"]
    failed = client.attempts[2]
    assert failed["result"] is None
    assert failed["error"].startswith("outcome_rejected: BODY_TOO_LARGE (HTTP 413), ")
    assert "bytes; full result kept at " in failed["error"] and failed["error"].endswith("output.jsonl")
    assert failed["task_id"] == task.task_id and failed["runner_id"] == "armbench"
    assert mailbox.status()["last_outcome_downgrade"]["stage"] == "failed"
    # Nothing was removed from the local result row.
    assert mailbox.journal.active is None


def test_failed_report_rejected_too_clears_the_journal(tmp_path):
    client = RejectingClient([(400, "SCHEMA_INVALID"), (400, "SCHEMA_INVALID"), (400, "SCHEMA_INVALID")])
    mailbox, task = _staged(tmp_path, client)

    assert mailbox.flush_pending_outcome() is True
    assert mailbox.journal.active is None
    assert len(client.attempts) == 3 and client.outcomes == []
    downgrade = mailbox.status()["last_outcome_downgrade"]
    assert downgrade["stage"] == "dropped" and downgrade["task_id"] == task.task_id
    assert downgrade["reason"].startswith("dropped: SCHEMA_INVALID (HTTP 400)")


def test_transient_error_keeps_the_current_ladder_step_across_restart(tmp_path):
    # 413 on the full outcome, then the control plane goes away while the
    # reduced outcome is being sent: the reduced form waits in the journal.
    client = RejectingClient([(413, "HTTP_413"), (None, None)])
    mailbox, task = _staged(tmp_path, client)

    assert mailbox.flush_pending_outcome() is False
    active = mailbox.journal.active
    assert active["stage"] == "outcome_pending"
    assert len(active["rejections"]) == 1 and active["rejections"][0]["status"] == 413
    assert "outcome_delivery" in active["outcome"]["result"]

    restarted = RunnerMailbox(tmp_path / "delivery.json", mode="live", client_factory=lambda: client)
    assert restarted.reconcile(TaskQueue(tmp_path / "queue")) is None
    assert restarted.journal.active is None
    assert len(client.attempts) == 3
    assert client.attempts[2] == active["outcome"]  # the reduced form, not a new full send
    assert client.outcomes[0][1]["state"] == "completed"


def test_transient_and_state_errors_still_retry_forever(tmp_path):
    for status, code in ((503, "HTTP_503"), (409, "OUTCOME_CONFLICT"), (404, "TASK_NOT_FOUND"), (401, "AUTH_INVALID")):
        client = RejectingClient([(status, code)] * 3)
        mailbox, _task = _staged(tmp_path / code, client)
        assert mailbox.flush_pending_outcome() is False
        assert mailbox.flush_pending_outcome() is False
        assert mailbox.journal.active["stage"] == "outcome_pending"
        assert all("outcome_delivery" not in a["result"] for a in client.attempts)
        assert mailbox.status()["last_outcome_downgrade"] is None
        assert mailbox.status()["last_error"] == f"{code}: refused"


def test_stuck_journal_clears_itself_at_the_next_boundary(tmp_path):
    # The live case: a runner holds one full outcome the proxy refused with
    # 413 and retries it at every boundary. Whichever is deployed first fixes
    # the wedge on its own; no journal is edited by hand.
    # (a) nginx raised first: the retry of the full outcome is accepted.
    accepted = RejectingClient([])
    mailbox, task = _staged(tmp_path / "a", accepted)
    restarted = RunnerMailbox(tmp_path / "a" / "delivery.json", mode="live", client_factory=lambda: accepted)
    assert restarted.reconcile(TaskQueue(tmp_path / "a" / "queue")) is None
    assert restarted.journal.active is None
    assert "outcome_delivery" not in accepted.outcomes[0][1]["result"]
    # (b) runner code first, proxy still at the old limit: one more 413 steps
    # down to the reduced outcome, which the old limit accepts.
    refusing = RejectingClient([(413, "HTTP_413")])
    mailbox, task = _staged(tmp_path / "b", refusing)
    restarted = RunnerMailbox(tmp_path / "b" / "delivery.json", mode="live", client_factory=lambda: refusing)
    assert restarted.reconcile(TaskQueue(tmp_path / "b" / "queue")) is None
    assert restarted.journal.active is None
    delivered = refusing.outcomes[0][1]
    assert delivered["task_id"] == task.task_id and delivered["state"] == "completed"
    assert delivered["result"]["outcome_delivery"]["reason"].startswith("rejected: HTTP_413")


def test_rejected_failure_report_is_downgraded_with_original_error(tmp_path):
    client = RejectingClient([(413, "HTTP_413")])
    mailbox = make_mailbox(tmp_path, client)
    queue = TaskQueue(tmp_path / "queue")
    task = mailbox.poll(queue)
    mailbox.stage_failure(task, "Traceback: " + "x" * 5000)
    queue.finish_task(task)

    assert mailbox.flush_pending_outcome() is True
    assert len(client.attempts) == 2
    second = client.attempts[1]
    assert second["state"] == "failed"
    assert second["error"].startswith("outcome_rejected: HTTP_413 (HTTP 413), ")
    assert "original error: Traceback: xxx" in second["error"]
    assert second["error"].endswith("failed_tasks.jsonl") or "failed.jsonl" in second["error"]
    assert mailbox.journal.active is None
