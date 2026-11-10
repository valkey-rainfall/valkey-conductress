"""Confirmation sweep: triggers, pair picking, stop rules, priority, export privacy, switch off.

Everything runs on tmp_path stand-ins for the state file, the results file and
the task queue. Fixtures are synthetic: made-up commits, versions and ids.
"""

from __future__ import annotations

import json
import re
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from conductress import config
from conductress.sweep import confirmation as cf
from conductress.sweep.planner import BenchmarkPoint, PointStatus, TaskPriority
from conductress.task_queue import TaskQueue

UTC = timezone.utc
EVENT = datetime(2026, 11, 6, 16, 0, tzinfo=UTC)
NOW = datetime(2026, 11, 7, 9, 0, tzinfo=UTC)


def _task_time(moment: datetime) -> str:
    return moment.strftime("%Y.%m.%d_%H.%M.%S.%f")


def _env(**overrides):
    env = {
        "kernel_release": "6.1.0-1.x86_64",
        "glibc_version": "2.34",
        "cpu_microcode": "0x100",
        "cachecannon_commit": "a" * 40,
        "conductress_revision": "b" * 40,
        "instance_id": "i-0123456789abcdef0",
        "host_fingerprint": "0123456789abcdef",
        "boot_id": "boot-1",
        "packages": {"kernel": "6.1.0-1", "glibc": "2.34-1"},
    }
    env.update(overrides)
    return env


def _row(task_id: str, end: datetime, env: dict, score: float = 1.0, duration: float = 600.0, **extra) -> dict:
    row = {
        "task_id": task_id,
        "score": score,
        "end_time": _task_time(end),
        "observed_duration_sec": duration,
        "environment": env,
    }
    row.update(extra)
    return row


def _append_rows(path, rows):
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


# --------------------------------------------------------------------------- triggers


class TestHostChanges:
    def test_host_fields_trigger(self):
        old = cf.host_signature({"environment": _env()})
        for key, value in [
            ("kernel_release", "6.1.0-2.x86_64"),
            ("cpu_microcode", "0x200"),
            ("glibc_version", "2.35"),
            ("cachecannon_commit", "c" * 40),
            ("instance_id", "i-0fedcba9876543210"),
            ("host_fingerprint", "fedcba9876543210"),
        ]:
            new = cf.host_signature({"environment": _env(**{key: value})})
            assert [c.split(":")[0] for c in cf.host_changes(old, new)] == [key]

    def test_package_change_triggers(self):
        old = cf.host_signature({"environment": _env()})
        new = cf.host_signature({"environment": _env(packages={"kernel": "6.1.0-1", "glibc": "2.34-2"})})
        assert cf.host_changes(old, new) == ["packages.glibc: '2.34-1' -> '2.34-2'"]

    def test_harness_or_reboot_only_does_not_trigger(self):
        old = cf.host_signature({"environment": _env()})
        new = cf.host_signature({"environment": _env(conductress_revision="d" * 40, boot_id="boot-2")})
        assert cf.host_changes(old, new) == []

    def test_a_field_only_one_row_has_is_not_a_change(self):
        old = cf.host_signature({"environment": {"kernel_release": "k1"}})
        new = cf.host_signature({"environment": {"kernel_release": "k1", "cpu_microcode": "0x1"}})
        assert cf.host_changes(old, new) == []

    def test_server_host_fields_count_only_on_a_two_host_runner(self):
        def row(kernel, same_host):
            return {"environment": _env(), "server_environment": {"kernel_release": kernel, "same_host": same_host}}

        assert cf.host_changes(cf.host_signature(row("k1", False)), cf.host_signature(row("k2", False))) == [
            "server_kernel_release: 'k1' -> 'k2'"
        ]
        assert cf.host_changes(cf.host_signature(row("k1", True)), cf.host_signature(row("k2", True))) == []

    def test_host_change_around(self, tmp_path):
        results = tmp_path / "output.jsonl"
        assert cf.host_change_around(results, EVENT) is None
        _append_rows(
            results,
            [
                _row("t1", EVENT - timedelta(hours=1), _env()),
                _row("t2", EVENT + timedelta(hours=1), _env(kernel_release="k2")),
            ],
        )
        assert cf.host_change_around(results, EVENT) == ["kernel_release: '6.1.0-1.x86_64' -> 'k2'"]
        assert cf.host_change_around(results, EVENT + timedelta(hours=2)) is None  # no row after it yet


# --------------------------------------------------------------------------- pairs


def _point(commit, cv=0.5, reps=5, measured=EVENT - timedelta(days=1), **extra):
    return BenchmarkPoint(
        commit=commit,
        date="2026-10-01",
        value=1000.0,
        cv=cv,
        reps=reps,
        measured_at=_task_time(measured) if measured else None,
        status=PointStatus.COMPLETED,
        **extra,
    )


class _Source:
    """A stand-in for a v3 throughput coordinator: state, labels and a task factory."""

    metric_id = "throughput"
    _sweep_source = "valkey"

    def __init__(self, workload, points):
        from conductress.sweep.planner import SweepState

        self.workload_id = workload
        self.state = SweepState(points={p.commit: p for p in points})
        self.created = []

    def _create_task(self, sweep_task):
        from conductress.tasks.task_cachecannon import CachecannonTaskData
        from conductress.topology import TopologySpec

        self.created.append(sweep_task)
        return CachecannonTaskData(
            source="valkey",
            specifier=sweep_task.commit,
            make_args="",
            topology=TopologySpec.standalone(),
            note=f"[cachecannon-sweep-v3:valkey/{self.workload_id}] {sweep_task.reason}",
            requirements={},
        )


def _commit(i):
    return f"{i:040x}"


class TestPairs:
    def test_filters(self):
        source = _Source(
            "get-a",
            [
                _point(_commit(1)),
                _point(_commit(2), cv=1.2),  # too noisy
                _point(_commit(3), reps=4),  # too few reps
                _point(_commit(4), client_saturated=True),
                _point(_commit(5), task_id="excluded-task"),
                _point(_commit(6), measured=EVENT + timedelta(minutes=1)),  # after the event
                _point(_commit(7), measured=EVENT),  # at the event: not before it
                _point(_commit(8), measured=None),
                BenchmarkPoint(commit=_commit(9), date="", status=PointStatus.BUILD_FAILED),
            ],
        )
        found = cf.eligible_points([source], EVENT, frozenset({"excluded-task"}))
        assert [c.commit for c in found] == [_commit(1)]

    def test_one_per_workload_first_then_lowest_cv_at_most_two(self):
        sources = [
            _Source("a", [_point(_commit(1), cv=0.9), _point(_commit(2), cv=0.1), _point(_commit(3), cv=0.2)]),
            _Source("b", [_point(_commit(4), cv=0.8)]),
            _Source("c", [_point(_commit(5), cv=0.3), _point(_commit(6), cv=0.4)]),
        ]
        picked = cf.pick_pairs(cf.eligible_points(sources, EVENT, frozenset()), budget=8)
        assert [(c.workload, c.commit) for c in picked] == [
            ("a", _commit(2)),
            ("c", _commit(5)),
            ("b", _commit(4)),
            ("a", _commit(3)),
            ("c", _commit(6)),
        ]
        assert len(cf.pick_pairs(cf.eligible_points(sources, EVENT, frozenset()), budget=2)) == 2

    def test_release_points_are_preferred(self):
        source = _Source("a", [_point(_commit(1), cv=0.1), _point(_commit(2), cv=0.9, sample="release")])
        picked = cf.pick_pairs(cf.eligible_points([source], EVENT, frozenset()), budget=1)
        assert picked[0].commit == _commit(2) and picked[0].release


# --------------------------------------------------------------------------- stop rules


class TestStopRules:
    def test_confirmed_after_three_pairs(self):
        before = [1000.0, 2000.0, 1500.0]
        after = [1121.0, 2242.0, 1683.0]  # +12.1 .. +12.2%
        assert cf.evaluate(before[:2], after[:2], 0.005, "k")["status"] is None  # 2 pairs: keep measuring
        result = cf.evaluate(before, after, 0.005, "k")
        assert result["status"] == cf.STATUS_CONFIRMED
        assert result["effect"] == pytest.approx(0.121, abs=0.002) and result["ci_lo"] > 0

    def test_effect_below_the_floor_is_not_confirmed(self):
        before = [1000.0] * 4
        after = [1003.0, 1003.1, 1002.9, 1003.0]  # +0.3%, tight, but under a 0.5% floor
        result = cf.evaluate(before, after, 0.005, "k")
        assert result["status"] is None  # not confirmed, and 4 pairs are too few to clear

    def test_cleared_needs_five_pairs_inside_the_floor(self):
        before = [1000.0] * 5
        after = [1001.0, 999.0, 1000.5, 999.5, 1000.0]
        assert cf.evaluate(before[:4], after[:4], 0.005, "k")["status"] is None
        result = cf.evaluate(before, after, 0.005, "k")
        assert result["status"] == cf.STATUS_CLEARED
        assert -0.005 <= result["ci_lo"] and result["ci_hi"] <= 0.005

    def test_noisy_pairs_stay_open(self):
        before = [1000.0] * 6
        after = [1050.0, 960.0, 1030.0, 970.0, 1000.0, 1040.0]
        assert cf.evaluate(before, after, 0.005, "k")["status"] is None


# --------------------------------------------------------------------------- coordinator


@pytest.fixture
def rig(tmp_path, monkeypatch):
    from conductress import task_queue

    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
    sources = [
        _Source("get-a", [_point(_commit(1), cv=0.2), _point(_commit(2), cv=0.3)]),
        _Source("get-b", [_point(_commit(3), cv=0.4)]),
        _Source("get-c", [_point(_commit(4), cv=0.5), _point(_commit(5), cv=0.6)]),
    ]
    queue = TaskQueue(tmp_path / "queue")
    coordinator = cf.ConfirmationCoordinator(
        sources,
        state_path=tmp_path / "confirm_runs.json",
        results_file=tmp_path / "output.jsonl",
        floor_pct=0.5,
        queue=queue,
        clock=lambda: NOW,
    )
    return coordinator, queue, tmp_path


def _complete(coordinator, queue, rig_path, score_ratio, env=None):
    """Run the queued confirmation task: write its row, finish it, tell the coordinator."""
    (task,) = queue.get_all_tasks()
    state = cf.load_state(coordinator.state_path)
    run = cf.open_run(state)
    pair = next(p for p in run["pairs"] if p["task_id"] == task.task_id)
    end = NOW + timedelta(minutes=10 * (len(run["pairs"]) + 1))
    _append_rows(
        rig_path / "output.jsonl",
        [_row(task.task_id, end, env or _env(), score=pair["before"] * score_ratio, sample="confirm")],
    )
    queue.finish_task(task)
    coordinator.on_task_completed(task)
    return task


class TestCoordinator:
    def test_scheduler_interface(self, rig):
        coordinator, _, _ = rig
        assert coordinator.schedule_tier() == cf.CONFIRM_TIER == 2
        assert coordinator.schedule_key() == (TaskPriority.CONFIRM.value, 0.0, 0.0)
        assert not coordinator.has_nightly_task()
        assert TaskPriority.CONFIRM.value > TaskPriority.BACKFILL.value

    def test_automatic_trigger_opens_one_run(self, rig):
        coordinator, _, path = rig
        first = _row("t1", EVENT - timedelta(minutes=5), _env())
        _append_rows(path / "output.jsonl", [first])
        coordinator.on_task_completed(MagicMock(task_id="t1", confirm_run_id=""))
        assert cf.load_state(coordinator.state_path)["runs"] == []  # first row: nothing to compare with

        harness = _row("t2", EVENT, _env(conductress_revision="e" * 40, boot_id="boot-2"))
        _append_rows(path / "output.jsonl", [harness])
        coordinator.on_task_completed(MagicMock(task_id="t2", confirm_run_id=""))
        assert cf.load_state(coordinator.state_path)["runs"] == []  # harness and reboot only

        host = _row("t3", EVENT + timedelta(minutes=20), _env(kernel_release="6.1.0-2.x86_64", instance_id="i-0f"))
        _append_rows(path / "output.jsonl", [host])
        coordinator.on_task_completed(MagicMock(task_id="t3", confirm_run_id=""))
        (run,) = cf.load_state(coordinator.state_path)["runs"]
        assert run["trigger"] == cf.TRIGGER_AUTOMATIC and run["status"] == cf.STATUS_REQUESTED
        assert run["budget"] == cf.BUDGET_DEFAULT
        assert run["event_at"] == "2026-11-06T16:10:00Z"  # the start of the first new-host task
        assert "kernel_release: 6.1.0-1.x86_64 -> 6.1.0-2.x86_64" in run["label"]
        assert "host replaced" in run["label"] and "i-0" not in run["label"]

        again = _row("t4", EVENT + timedelta(hours=1), _env(kernel_release="6.1.0-3.x86_64"))
        _append_rows(path / "output.jsonl", [again])
        coordinator.on_task_completed(MagicMock(task_id="t4", confirm_run_id=""))
        assert len(cf.load_state(coordinator.state_path)["runs"]) == 1  # one open run per runner

    def test_run_confirms_and_stops_early(self, rig):
        coordinator, queue, path = rig
        cf.request_run(at=EVENT, label="kernel update", path=coordinator.state_path, now=NOW)
        coordinator.on_queue_empty()
        (task,) = queue.get_all_tasks()
        assert task.confirm_run_id.startswith("confirm-") and task.sweep_commit == ""
        assert task.note.startswith("[confirm-v3:valkey/get-a] ")
        assert coordinator.sources[0].created[0].priority == TaskPriority.CONFIRM
        coordinator.on_queue_empty()  # a cell is already queued: nothing more
        assert len(queue.get_all_tasks()) == 1

        for _ in range(3):
            _complete(coordinator, queue, path, 1.12)
            coordinator.on_queue_empty()
        (run,) = cf.load_state(coordinator.state_path)["runs"]
        assert run["status"] == cf.STATUS_CONFIRMED and run["ended_at"] == "2026-11-07T09:00:00Z"
        assert [p["state"] for p in run["pairs"]] == ["done", "done", "done", "pending", "pending"]
        assert run["effect"] == pytest.approx(0.12) and run["ci"][0] > 0
        assert queue.get_all_tasks() == []  # stopped: nothing queued after the verdict

    def test_run_stops_at_the_budget(self, rig):
        coordinator, queue, path = rig
        cf.request_run(at=EVENT, label="kernel update", budget=6, path=coordinator.state_path, now=NOW)
        coordinator.on_queue_empty()
        ratios = [1.01, 0.99, 1.008]
        for i in range(5):  # five eligible pairs, under the budget of six
            if i == 3:
                (task,) = queue.get_all_tasks()
                queue.finish_task(task)
                coordinator.on_task_failed(task)
            else:
                _complete(coordinator, queue, path, ratios[i % 3])
            coordinator.on_queue_empty()
        (run,) = cf.load_state(coordinator.state_path)["runs"]
        assert run["status"] == cf.STATUS_INSUFFICIENT
        assert sorted(p["state"] for p in run["pairs"]) == ["done"] * 4 + ["failed"]

    def test_no_eligible_point(self, rig):
        coordinator, queue, _ = rig
        cf.request_run(at=datetime(2026, 1, 1, tzinfo=UTC), label="early event", path=coordinator.state_path, now=NOW)
        coordinator.on_queue_empty()
        (run,) = cf.load_state(coordinator.state_path)["runs"]
        assert run["status"] == cf.STATUS_NO_PAIRS and queue.get_all_tasks() == []

    def test_confirmation_task_is_not_a_series_task(self, rig, tmp_path, monkeypatch):
        from conductress.sweep import coordinator_v3

        monkeypatch.setattr(coordinator_v3, "V3_STATE_DIR", tmp_path / "v3")
        series = coordinator_v3.CachecannonThroughputSweepCoordinatorV3(tmp_path / "repo")
        coordinator, queue, _ = rig
        coordinator.sources = [series]
        series.state.points[_commit(1)] = _point(_commit(1))
        cf.request_run(at=EVENT, label="kernel update", path=coordinator.state_path, now=NOW)
        coordinator.on_queue_empty()
        (task,) = queue.get_all_tasks()
        assert task.test == "get" and task.pipelining == series._pipelining
        assert not series._is_my_task(task)  # never recorded into the live series


class TestRequests:
    def test_one_open_run_and_budget_range(self, tmp_path):
        path = tmp_path / "runs.json"
        cf.request_run(at=EVENT, label="kernel update", path=path, now=NOW)
        with pytest.raises(ValueError, match="still open"):
            cf.request_run(at=EVENT, label="again", path=path, now=NOW + timedelta(seconds=1))
        for budget in (5, 13):
            with pytest.raises(ValueError, match="budget"):
                cf.request_run(at=EVENT, label="x", budget=budget, path=tmp_path / f"b{budget}.json", now=NOW)

    def test_label_must_be_public(self, tmp_path):
        with pytest.raises(ValueError, match="IPv4"):
            cf.request_run(at=EVENT, label="moved to 198.51.100.7", path=tmp_path / "r.json", now=NOW)

    def test_dry_run_writes_nothing(self, tmp_path):
        path = tmp_path / "r.json"
        cf.request_run(at=EVENT, label="kernel update", path=path, now=NOW, dry_run=True)
        assert not path.exists()

    def test_a_request_during_a_runner_write_is_not_lost(self, tmp_path):
        """The runner and the operator command take the same lock, so neither write is lost."""
        path = tmp_path / "runs.json"
        holding, release = threading.Event(), threading.Event()

        def runner_write() -> None:
            with cf.state_lock(path):
                state = cf.load_state(path)
                holding.set()
                release.wait(5)
                state["last_host"] = {"signature": {"kernel_release": "k2"}, "at": "2026-11-07T00:00:00Z"}
                cf.save_state(state, path)

        writer = threading.Thread(target=runner_write)
        writer.start()
        assert holding.wait(5)
        requester = threading.Thread(target=lambda: cf.request_run(at=EVENT, label="kernel update", path=path, now=NOW))
        requester.start()
        requester.join(0.3)
        assert requester.is_alive(), "the request must wait for the runner's write"
        release.set()
        writer.join(5)
        requester.join(5)
        state = cf.load_state(path)
        assert state["last_host"]["signature"] == {"kernel_release": "k2"}
        assert [r["label"] for r in state["runs"]] == ["kernel update"]


# --------------------------------------------------------------------------- scheduling


def test_confirm_never_queues_while_another_series_has_work(rig):
    from conductress.task_runner import TaskRunner

    coordinator, _, _ = rig
    coordinator.on_queue_empty = MagicMock()
    backfill = MagicMock(workload_id="get-a", retired=False)
    backfill.schedule_key.return_value = (TaskPriority.BACKFILL.value, -3.0, 0.0)
    backfill.has_nightly_task.return_value = False
    backfill.schedule_tier.return_value = 0
    memory = MagicMock(workload_id="memory-a", retired=False)
    memory.schedule_key.return_value = (TaskPriority.BACKFILL.value, -800.0, 0.0)
    memory.has_nightly_task.return_value = False
    memory.schedule_tier.return_value = 1  # deferred memory backfill

    runner = TaskRunner()
    runner._subscribers = [coordinator, memory, backfill]
    allowed = MagicMock()
    allowed.is_allowed.return_value = True
    with patch("conductress.task_runner.load_sweep_config", return_value=allowed):
        with patch("conductress.task_runner.TaskQueue") as queue_class:
            queue_class.return_value.get_all_tasks.return_value = [MagicMock()]  # backfill queues a task
            runner._schedule_next()
        backfill.on_queue_empty.assert_called_once()
        memory.on_queue_empty.assert_not_called()
        coordinator.on_queue_empty.assert_not_called()

        with patch("conductress.task_runner.TaskQueue") as queue_class:
            queue_class.return_value.get_all_tasks.return_value = []  # nobody else has work
            runner._schedule_next()
        assert memory.on_queue_empty.call_count == 1
        coordinator.on_queue_empty.assert_called_once()


# --------------------------------------------------------------------------- export and privacy


PRIVATE = {
    "an instance id": re.compile(r"\bi-[0-9a-f]{8,17}\b"),
    "an IPv4 address": re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"),
    "a UUID": re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"),
    "a path": re.compile(r"/(?:home|etc|opt|var|root|tmp|usr)/"),
}


def test_export_holds_only_public_fields(rig):
    coordinator, queue, path = rig
    cf.request_run(at=EVENT, label="kernel update", path=coordinator.state_path, now=NOW)
    coordinator.on_queue_empty()
    _complete(coordinator, queue, path, 1.1)
    # Private material in the private state must not reach the public file.
    state = cf.load_state(coordinator.state_path)
    state["last_host"]["signature"]["conductress_revision"] = "f" * 40
    state["runs"][0]["note"] = "boot 11111111-2222-3333-4444-555555555555 on 198.51.100.7 at /opt/runner/data"
    state["runs"].append(dict(state["runs"][0], run_id="confirm-old", status="cleared", label="host 198.51.100.9"))
    cf.save_state(state, coordinator.state_path)

    output = path / "www" / "confirm-intel.epoch-v3.json"
    assert coordinator.export(output, platform="x86_64/test-cpu") == 2
    text = output.read_text()
    document = json.loads(text)
    assert document["metadata"] == {
        "epoch": "v3",
        "platform": "x86_64/test-cpu",
        "generated_at": "2026-11-07T09:00:00Z",
    }
    run = document["runs"][0]
    assert set(run) == {
        "run_id",
        "event_at",
        "label",
        "trigger",
        "budget",
        "status",
        "floor_pct",
        "pairs_done",
        "effect_pct",
        "ci_pct",
        "started_at",
        "ended_at",
        "pairs",
    }
    assert set(run["pairs"][0]) == {"workload", "metric", "commit", "before", "after"}
    assert run["pairs_done"] == 1 and run["pairs"][0]["after"] == pytest.approx(run["pairs"][0]["before"] * 1.1)
    assert document["runs"][1]["label"].startswith("(text withheld")
    commits = {p["commit"] for r in document["runs"] for p in r["pairs"]}
    assert set(re.findall(r"\b[0-9a-f]{40}\b", text)) <= commits  # only public series commit ids
    for what, pattern in PRIVATE.items():
        assert not pattern.search(text), what
    assert "task_id" not in text and "before_cv" not in text


def test_row_of_a_confirmation_task_carries_sample_and_run_id(tmp_path, monkeypatch):
    from conductress import file_protocol as fp

    monkeypatch.setattr(fp, "CONDUCTRESS_OUTPUT", tmp_path / "output.jsonl")
    monkeypatch.setattr(fp, "CONDUCTRESS_QUEUE", tmp_path / "queue")
    monkeypatch.setattr(fp, "get_result_provenance", lambda binary=None: {})
    (tmp_path / "queue").mkdir()
    for task_id, document in [
        ("2026.11.08_07.00.00.000000", {"task_type": "CachecannonTaskData", "confirm_run_id": "confirm-x"}),
        ("2026.11.08_08.00.00.000000", {"task_type": "CachecannonTaskData", "confirm_run_id": ""}),
    ]:
        (tmp_path / "queue" / f"task_{task_id}.json").write_text(json.dumps(document))
        protocol = fp.FileProtocol(task_id, "client", base_dir=tmp_path / "tmp")
        protocol.write_results(
            fp.BenchmarkResults(
                method="cachecannon",
                source="valkey",
                specifier="x",
                commit_hash="x",
                score=1.0,
                end_time=datetime(2026, 11, 8, 7),
                data={},
                make_args="",
            )
        )
    confirm_row, series_row = [json.loads(line) for line in (tmp_path / "output.jsonl").read_text().splitlines()]
    assert confirm_row["sample"] == "confirm" and confirm_row["confirm_run_id"] == "confirm-x"
    assert "sample" not in series_row and "confirm_run_id" not in series_row


# --------------------------------------------------------------------------- switch off


def test_switch_is_off_by_default_and_builds_nothing(monkeypatch):
    monkeypatch.delenv("CONDUCTRESS_CONFIRM_SWEEP", raising=False)
    assert config._env_bool("CONDUCTRESS_CONFIRM_SWEEP", False) is False
    assert config.CONFIRM_SWEEP is False
    assert cf.create_confirmation_coordinator([MagicMock()]) is None


def test_switch_on_selects_the_default_engine_v3_throughput_series(monkeypatch, tmp_path):
    from conductress.sweep import coordinator_v3

    monkeypatch.setattr(coordinator_v3, "V3_STATE_DIR", tmp_path / "v3")
    monkeypatch.setattr(config, "CONFIRM_SWEEP", True)
    throughput = coordinator_v3.CachecannonThroughputSweepCoordinatorV3(tmp_path / "repo")
    latency = coordinator_v3.CachecannonLatencySweepCoordinatorV3(tmp_path / "repo")
    built = cf.create_confirmation_coordinator([throughput, latency, MagicMock()])
    assert built is not None and built.sources == [throughput]


def test_publisher_without_confirmation_writes_no_confirm_file(tmp_path):
    from conductress.publisher import DashboardPublisher

    publisher = DashboardPublisher.__new__(DashboardPublisher)
    publisher.coordinators = []
    publisher._export_dir = tmp_path
    publisher._platform_id = "intel"
    publisher._platform_label = "x86_64/test-cpu"
    publisher._rsync = MagicMock()
    publisher._publish()
    assert not list(tmp_path.glob("confirm-*"))
    publisher._rsync.assert_called_once()

    publisher.confirmation = MagicMock()
    publisher._publish()
    publisher.confirmation.export.assert_called_once_with(
        tmp_path / "confirm-intel.epoch-v3.json", platform="x86_64/test-cpu"
    )


def test_floor_must_be_a_positive_number(monkeypatch):
    monkeypatch.setenv("CONDUCTRESS_CONFIRM_FLOOR_PCT", "0")
    with pytest.raises(ValueError, match="greater than 0"):
        config._env_positive_float("CONDUCTRESS_CONFIRM_FLOOR_PCT", 0.5)
    monkeypatch.setenv("CONDUCTRESS_CONFIRM_FLOOR_PCT", "x")
    with pytest.raises(ValueError, match="number"):
        config._env_positive_float("CONDUCTRESS_CONFIRM_FLOOR_PCT", 0.5)
    monkeypatch.setenv("CONDUCTRESS_CONFIRM_FLOOR_PCT", "0.8")
    assert config._env_positive_float("CONDUCTRESS_CONFIRM_FLOOR_PCT", 0.5) == 0.8
