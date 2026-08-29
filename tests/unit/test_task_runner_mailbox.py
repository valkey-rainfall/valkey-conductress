from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from conductress.task_runner import TaskRunner


class ExitLoop(Exception):
    pass


def mailbox_mock(active=None):
    mailbox = MagicMock()
    mailbox.journal.active = active
    mailbox.blocks_execution.return_value = False
    mailbox.status.return_value = {"mode": "live"}
    return mailbox


def test_choose_next_preserves_local_before_new_remote_claim():
    mailbox = mailbox_mock()
    queue = MagicMock()
    local = MagicMock(task_id="local")
    queue.get_next_task.return_value = local
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)

    assert runner._choose_next(queue) is local
    mailbox.poll.assert_not_called()


def test_choose_next_prefers_recovered_active_remote_and_blocks_unaccepted():
    mailbox = mailbox_mock(active={"task_id": "remote", "stage": "accepted"})
    queue = MagicMock()
    remote = MagicMock(task_id="remote")
    mailbox.poll.return_value = remote
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)
    assert runner._choose_next(queue) is remote
    queue.get_next_task.assert_not_called()

    mailbox.poll.return_value = None
    mailbox.blocks_execution.return_value = True
    assert runner._choose_next(queue) is None
    queue.get_next_task.assert_not_called()


def test_empty_local_queue_claims_remote_before_sweep():
    mailbox = mailbox_mock()
    queue = MagicMock()
    queue.get_next_task.return_value = None
    remote = MagicMock(task_id="remote")
    mailbox.poll.return_value = remote
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)
    runner._schedule_next = MagicMock()

    assert runner._choose_next(queue) is remote
    runner._schedule_next.assert_not_called()


def test_boundary_status_is_pushed_to_control_and_static_dashboard():
    mailbox = mailbox_mock()
    task = MagicMock(task_id="t1", task_type="PerfTaskData")
    runner = TaskRunner(
        mailbox=mailbox,
        publish_target="user@data:/var/www/data",
        management_settle_seconds=0,
    )
    with (
        patch("conductress.task_runner.build_status", side_effect=[{"version": 1}, {"version": 2}]) as build,
        patch("conductress.task_runner.export_status") as export,
    ):
        runner._publish_boundary("starting", task)

    mailbox.push_status.assert_called_once_with({"version": 1})
    export.assert_called_once_with(publish_target="user@data:/var/www/data", status={"version": 2})
    assert build.call_count == 2


@pytest.mark.asyncio
async def test_management_calls_surround_but_never_occur_inside_task_execution():
    mailbox = mailbox_mock()
    mailbox.owns.return_value = True
    task = MagicMock(
        task_id="remote-task",
        task_type="PerfTaskData",
        replicas=0,
    )
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)
    runner._choose_next = MagicMock(side_effect=[task, ExitLoop()])

    calls_at_execution = []

    async def execute(_task):
        calls_at_execution.extend(mailbox.method_calls)
        await AsyncMock()()
        assert mailbox.method_calls == calls_at_execution

    with (
        patch("conductress.task_runner.TaskQueue") as queue_class,
        patch("conductress.task_runner.FileProtocol.cleanup_orphaned_tasks", return_value=0),
        patch("conductress.task_runner.get_servers", return_value=[]),
        patch("conductress.task_runner.build_status", return_value={"schema_version": 1}),
        patch("conductress.task_runner.export_status"),
    ):
        queue = queue_class.return_value
        runner._TaskRunner__run_task = execute
        with pytest.raises(ExitLoop):
            await runner.run()

    assert mailbox.stage_success.call_count == 1
    assert mailbox.flush_pending_outcome.call_count == 1
    assert queue.finish_task.call_args == call(task)
    assert calls_at_execution


def test_pending_outcome_reconciles_before_local_task_selection():
    mailbox = mailbox_mock(active={"task_id": "remote", "stage": "outcome_pending"})
    queue = MagicMock()
    local = MagicMock(task_id="local")
    queue.get_next_task.return_value = local
    mailbox.poll.return_value = None
    mailbox.blocks_execution.return_value = False
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)

    assert runner._choose_next(queue) is local
    mailbox.poll.assert_called_once_with(queue)
