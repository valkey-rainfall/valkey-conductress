"""A cancelled remote task stops at a repetition boundary, is cleaned up, and is reported as failed."""

import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from conductress.config import ServerInfo
from conductress.file_protocol import FileProtocol
from conductress.task_queue import BaseTaskRunner, TaskCancelled
from conductress.task_runner import TaskRunner
from conductress.topology import TopologySpec

TASKS_DIR = Path(__file__).resolve().parents[2] / "src" / "conductress" / "tasks"


class FakeBenchmark(BaseTaskRunner):
    """Three repetitions, like the real runners: check at the top of each one."""

    def __init__(self, base_dir):
        super().__init__("cancel_test")
        self.file_protocol = FileProtocol("cancel_test", role_id="client", base_dir=base_dir)
        self.started_reps = []

    async def run(self) -> None:
        for rep in range(3):
            self.check_cancel()
            self.started_reps.append(rep)


def test_a_local_task_has_no_cancel_check_and_is_never_stopped(tmp_path):
    benchmark = FakeBenchmark(tmp_path)
    assert benchmark.cancel_check is None
    benchmark.check_cancel()


def test_check_cancel_raises_only_when_the_check_says_so(tmp_path):
    benchmark = FakeBenchmark(tmp_path)
    benchmark.cancel_check = lambda: False
    benchmark.check_cancel()
    benchmark.cancel_check = lambda: True
    with pytest.raises(TaskCancelled):
        benchmark.check_cancel()


def _task_data(runner):
    task_data = MagicMock(task_id="remote-task")
    task_data.topology = TopologySpec.standalone()
    task_data.prepare_task_runner.return_value = runner
    return task_data


@pytest.mark.asyncio
@patch("conductress.task_runner.get_servers", return_value=[ServerInfo(ip="127.0.0.1")])
async def test_a_remote_task_stops_after_the_repetition_that_saw_the_cancel(_servers, tmp_path):
    benchmark = FakeBenchmark(tmp_path)
    mailbox = MagicMock()
    mailbox.owns.return_value = True
    mailbox.cancel_requested.side_effect = [False, True]  # asked before rep 0 and before rep 1
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)

    with pytest.raises(TaskCancelled):
        await runner._TaskRunner__run_task(_task_data(benchmark))

    assert benchmark.started_reps == [0]
    assert mailbox.cancel_requested.call_args_list == [call("remote-task"), call("remote-task")]


@pytest.mark.asyncio
@patch("conductress.task_runner.get_servers", return_value=[ServerInfo(ip="127.0.0.1")])
async def test_a_cancelled_task_is_cleaned_up_like_a_failed_one(_servers, tmp_path):
    benchmark = FakeBenchmark(tmp_path)
    benchmark.file_protocol.cleanup = MagicMock()
    mailbox = MagicMock()
    mailbox.owns.return_value = True
    mailbox.cancel_requested.return_value = True
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)

    with patch("conductress.task_runner.Server") as server_class:
        server_class.return_value.kill_all_valkey_instances_on_host = AsyncMock()
        with pytest.raises(TaskCancelled):
            await runner._TaskRunner__run_task(_task_data(benchmark))

    benchmark.file_protocol.cleanup.assert_called_once()
    server_class.return_value.kill_all_valkey_instances_on_host.assert_awaited()


@pytest.mark.asyncio
@patch("conductress.task_runner.get_servers", return_value=[ServerInfo(ip="127.0.0.1")])
async def test_a_local_task_never_asks_the_control_service(_servers, tmp_path):
    benchmark = FakeBenchmark(tmp_path)
    mailbox = MagicMock()
    mailbox.owns.return_value = False
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)

    await runner._TaskRunner__run_task(_task_data(benchmark))

    assert benchmark.started_reps == [0, 1, 2]
    mailbox.cancel_requested.assert_not_called()


class ExitLoop(Exception):
    pass


@pytest.mark.asyncio
async def test_the_run_loop_reports_a_cancelled_task_as_failed_with_the_reason():
    mailbox = MagicMock()
    mailbox.journal.active = None
    mailbox.blocks_execution.return_value = False
    mailbox.status.return_value = {"mode": "live"}
    mailbox.owns.return_value = True
    task = MagicMock(task_id="remote-task", task_type="PerfTaskData", topology=TopologySpec.standalone())
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)
    runner._choose_next = MagicMock(side_effect=[task, ExitLoop()])
    runner._record_failure = MagicMock()

    async def stopped(_task):
        raise TaskCancelled("cancelled by the operator")

    with (
        patch("conductress.task_runner.TaskQueue"),
        patch("conductress.task_runner.FileProtocol.cleanup_orphaned_tasks", return_value=0),
        patch("conductress.task_runner.get_servers", return_value=[]),
        patch("conductress.task_runner.build_status", return_value={"schema_version": 1}),
        patch("conductress.task_runner.export_status"),
    ):
        runner._TaskRunner__run_task = stopped
        with pytest.raises(ExitLoop):
            await runner.run()

    (_task, error), _ = mailbox.stage_failure.call_args
    assert error.startswith("TaskCancelled:")
    mailbox.stage_success.assert_not_called()
    mailbox.flush_pending_outcome.assert_called_once()


@pytest.mark.parametrize("name", ["task_cachecannon", "task_perf_benchmark", "task_replica_read", "task_scenario"])
def test_every_repetition_loop_of_a_benchmark_checks_for_a_cancel(name):
    """A new repetition loop without the check would make an operator cancel silently do nothing."""
    source = (TASKS_DIR / f"{name}.py").read_text(encoding="utf-8")
    assert source.count("self.check_cancel()") == len(re.findall(r"for rep in range\(", source))
