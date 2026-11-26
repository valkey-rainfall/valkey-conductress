"""Fair-share claim order in the control service (multi-agent queue note, rollout step 5)."""

from datetime import datetime, timedelta, timezone

import pytest

from conductress.control.db import parse_utc, utc_text
from conductress.control.service import ControlService
from conductress.control.users import UserDirectory

from .helpers import task_envelope

BASE_TIME = datetime(2026, 8, 29, tzinfo=timezone.utc)


@pytest.fixture
def service(control_env):
    return ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
    )


def _submit(service, task_id, login, minute, *, runner_id="armbench", operator=False, **fields):
    """Submit a task that was submitted ``minute`` minutes after a fixed time (so the order is explicit)."""
    envelope = task_envelope(task_id, runner_id=runner_id, **fields)
    envelope["submitted_at"] = utc_text(BASE_TIME + timedelta(minutes=minute))
    if operator:
        service.submit_task(envelope, actor=f"operator:{login}")
    else:
        service.submit_task(envelope, actor=f"user:{login}", identity_login=login)


def _add_usage(database, login, seconds, *, runner_id="armbench", hours_ago=1.0, task_id=None, ended=None):
    ended = ended or datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "INSERT INTO task_usage(task_id, submitted_by, runner_id, task_class, ended_at, runner_seconds, outcome) "
            "VALUES (?, ?, ?, 'manual', ?, ?, 'completed')",
            (task_id or f"old-{login}-{runner_id}-{hours_ago}-{seconds}", login, runner_id, utc_text(ended), seconds),
        )


def _claim(service, runner_id="armbench"):
    claim = service.claim_task(runner_id, actor=f"runner:{runner_id}")
    return claim["task"]["task_id"] if claim else None


def _finish(service, task_id, seconds=600, runner_id="armbench"):
    """Claim already happened: accept the task and report it done after ``seconds`` of runner time."""
    row = service.get_task(task_id)
    token = _claim_token(service, task_id)
    accepted = service.accept_task(runner_id, task_id, token, actor=f"runner:{runner_id}")[0]
    completed_at = utc_text(parse_utc(accepted["accepted_at"]) + timedelta(seconds=seconds))
    outcome = {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": runner_id,
        "state": "completed",
        "completed_at": completed_at,
        "result": {},
        "error": None,
    }
    service.record_outcome(runner_id, task_id, outcome, actor=f"runner:{runner_id}")
    return row


def _claim_token(service, task_id):
    with service.database.read() as connection:
        return connection.execute("SELECT claim_token FROM tasks WHERE task_id=?", (task_id,)).fetchone()[0]


def _drain(service, runner_id="armbench", seconds=600):
    order = []
    while True:
        task_id = _claim(service, runner_id)
        if task_id is None:
            return order
        _finish(service, task_id, seconds, runner_id)
        order.append(task_id)


# --------------------------------------------------------------- single login: unchanged


def test_one_login_gets_first_in_first_out_whatever_the_usage(service, control_env):
    _add_usage(control_env["database"], "example-a", 100000)
    for index in (3, 1, 2):
        _submit(service, f"t{index}", "example-a", index)
    assert _drain(service) == ["t1", "t2", "t3"]


def test_priority_and_submit_time_still_decide_for_one_login(service):
    _submit(service, "late-high", "example-op", 5, operator=True, priority=200)
    _submit(service, "early-low", "example-op", 1, operator=True, priority=100)
    _submit(service, "early-high", "example-op", 2, operator=True, priority=200)
    assert _drain(service) == ["early-high", "late-high", "early-low"]


def test_a_database_with_no_usage_rows_keeps_the_plain_order_for_two_logins(service):
    _submit(service, "a1", "example-a", 1)
    _submit(service, "b1", "example-b", 2)
    _submit(service, "a2", "example-a", 3)
    # Nothing is recorded yet, so nothing is shared: oldest first, no error.
    assert _claim(service) == "a1"


# --------------------------------------------------------------- fairness


def test_the_light_login_is_claimed_before_the_heavy_one_that_queued_earlier(service, control_env):
    _add_usage(control_env["database"], "example-heavy", 20000)
    for index in range(1, 4):
        _submit(service, f"heavy-{index}", "example-heavy", index)
    _submit(service, "light-1", "example-light", 10)
    assert _claim(service) == "light-1"


def test_usage_on_another_runner_does_not_count(service, control_env):
    _add_usage(control_env["database"], "example-heavy", 20000, runner_id="armbench")
    _submit(service, "h1", "example-heavy", 1, runner_id="g4bench")
    _submit(service, "l1", "example-light", 2, runner_id="g4bench")
    assert _claim(service, "g4bench") == "h1"  # no usage on g4bench: plain order
    _submit(service, "h2", "example-heavy", 1, runner_id="armbench")
    _submit(service, "l2", "example-light", 2, runner_id="armbench")
    assert _claim(service, "armbench") == "l2"


def test_old_usage_counts_less_and_very_old_usage_not_at_all(service, control_env):
    database = control_env["database"]
    _add_usage(database, "example-a", 4000, hours_ago=24)  # weighs 2000
    _add_usage(database, "example-b", 100000, hours_ago=24 * 9)  # older than 8 half-lives: ignored
    _submit(service, "a1", "example-a", 1)
    _submit(service, "b1", "example-b", 2)
    # b has no recent usage, so b goes first although a queued earlier.
    assert _claim(service) == "b1"


def test_a_small_head_task_is_not_held_back_by_a_large_one(service, control_env):
    _add_usage(control_env["database"], "example-a", 1000)
    _add_usage(control_env["database"], "example-b", 1000)
    _submit(service, "a-short", "example-a", 5)
    envelope = task_envelope("b-long")
    envelope["submitted_at"] = utc_text(BASE_TIME + timedelta(minutes=1))
    envelope["task"].update({"repetitions": 10, "duration": 600, "warmup": 60})
    service.submit_task(envelope, actor="user:example-b", identity_login="example-b")
    # Both used 1000 s. b queued first, but its head task is much longer, so a goes first.
    assert _claim(service) == "a-short"


def test_ties_are_broken_the_same_way_every_time(service, control_env):
    ended = datetime.now(timezone.utc) - timedelta(hours=1)
    for login in ("example-a", "example-b"):
        _add_usage(control_env["database"], login, 500, task_id=f"u-{login}", ended=ended)
    _submit(service, "task-z", "example-a", 1)
    _submit(service, "task-a", "example-b", 1)  # same submit time, same duration, same usage
    assert _claim(service) == "task-a"  # the lower task id


def test_a_login_with_100_tasks_does_not_block_a_login_with_5(service):
    for index in range(100):
        _submit(service, f"heavy-{index:03d}", "example-heavy", index)
    for index in range(5):
        _submit(service, f"light-{index}", "example-light", 600 + index)
    order = _drain(service)
    assert len(order) == 105 and order[-1].startswith("heavy-")
    # The first heavy task runs (no usage yet). Then the light login, with no usage, is next,
    # and from then on the two alternate, so all 5 light tasks start within the first 11 claims.
    assert order[1] == "light-0"
    assert max(order.index(f"light-{index}") for index in range(5)) <= 10
    # First in first out inside one login.
    assert [task for task in order if task.startswith("heavy-")] == sorted(
        task for task in order if task.startswith("heavy-")
    )


# --------------------------------------------------------------- classes and priority unchanged


def test_operator_priority_beats_fair_share(service, control_env):
    _add_usage(control_env["database"], "operator:example-op", 100000)
    _submit(service, "a1", "example-a", 1)
    _submit(service, "op-high", "example-op", 9, operator=True, priority=500)
    assert _claim(service) == "op-high"


def test_class_order_is_unchanged_manual_then_canary_then_sweep(service, control_env):
    _add_usage(control_env["database"], "example-a", 100000)
    _submit(service, "sweep-1", "example-op", 1, operator=True, task_class="sweep")
    _submit(service, "canary-1", "example-op", 2, operator=True, task_class="canary")
    _submit(service, "manual-1", "example-a", 3)
    assert _drain(service) == ["manual-1", "canary-1", "sweep-1"]


def test_fair_share_only_chooses_inside_the_top_class_and_priority(service, control_env):
    _add_usage(control_env["database"], "example-heavy", 100000)
    _submit(service, "heavy-1", "example-heavy", 1, operator=False)
    _submit(service, "light-sweep", "example-light", 2, operator=True, task_class="sweep")
    # The light login only has a lower class task, so the manual task of the heavy login still goes first.
    assert _claim(service) == "heavy-1"


# --------------------------------------------------------------- switch off, quota


def test_half_life_zero_turns_fair_share_off(control_env):
    off = ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        fair_share_half_life_hours=0,
    )
    _add_usage(control_env["database"], "example-heavy", 100000)
    _submit(off, "heavy-1", "example-heavy", 1)
    _submit(off, "light-1", "example-light", 2)
    assert _claim(off) == "heavy-1"


def test_a_login_without_a_quota_is_never_held_however_much_it_used(control_env):
    directory = UserDirectory.from_dict(
        {"users": [{"login": "example-a", "kind": "human", "github": "example-a", "role": "collaborator"}]}
    )
    assert directory.quota_minutes("example-a") == 0
    service = ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=directory,
    )
    _add_usage(control_env["database"], "example-a", 10_000_000)
    for index in range(3):
        _submit(service, f"t{index}", "example-a", index)
    with control_env["database"].read() as connection:
        states = {row[0] for row in connection.execute("SELECT state FROM tasks")}
    assert states == {"queued"}


def test_a_login_with_a_quota_is_still_held_once_it_passes_it(control_env):
    directory = UserDirectory.from_dict(
        {
            "users": [
                {
                    "login": "example-a",
                    "kind": "human",
                    "github": "example-a",
                    "role": "collaborator",
                    "quota_runner_minutes_per_day": 1,
                }
            ]
        }
    )
    service = ControlService(
        control_env["database"],
        control_env["registry"],
        control_env["config"].claim_lease_seconds,
        canary_profiles=control_env["canary_profiles"],
        user_directory=directory,
    )
    for index in range(3):
        _submit(service, f"t{index}", "example-a", index)
    with control_env["database"].read() as connection:
        states = [row[0] for row in connection.execute("SELECT state FROM tasks ORDER BY task_id")]
    assert "pending-approval" in states


# --------------------------------------------------------------- what the claim reads


def test_the_claim_queries_use_indexes_and_never_scan_a_whole_table(service, control_env):
    database = control_env["database"]
    for login in ("example-a", "example-b"):
        _add_usage(database, login, 100)
        _submit(service, f"q-{login}", login, 1)
    statements = []
    connection = database.connect()
    try:
        connection.set_trace_callback(statements.append)
        service._next_queued_task(
            connection, "armbench", datetime.now(timezone.utc)
        )  # pylint: disable=protected-access
        reads = [s for s in statements if s.lstrip().upper().startswith("SELECT")]
        assert any("task_usage" in s for s in reads) and any("FROM tasks" in s for s in reads)
        plans = []
        for statement in reads:
            plans += [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + statement)]
    finally:
        connection.close()
    assert all(not plan.startswith("SCAN tasks") and not plan.startswith("SCAN task_usage") for plan in plans), plans
    assert any("idx_tasks_runner_state_priority" in plan for plan in plans)
    assert any("idx_task_usage_ended" in plan for plan in plans)


def test_more_competing_logins_than_the_limit_get_the_plain_order(service, control_env, monkeypatch):
    monkeypatch.setattr(ControlService, "_FAIR_SHARE_MAX_LOGINS", 1)
    _add_usage(control_env["database"], "example-heavy", 100000)
    _submit(service, "heavy-1", "example-heavy", 1)
    _submit(service, "light-1", "example-light", 2)
    assert _claim(service) == "heavy-1"
