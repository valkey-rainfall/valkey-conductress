"""TaskRunner._choose_next with and without a sweep share (rollout step 6).

With no share (the default) the scheduler must behave exactly as before. ``_choose_next_before`` below
is the function of the previous version, copied unchanged; the tests run it and the new one on the
same inputs and compare what they return and every call they make.
"""

import itertools
from unittest.mock import MagicMock, patch

import pytest

from conductress.sweep_config import SweepConfig
from conductress.task_runner import TaskRunner


def _choose_next_before(runner, queue):
    """TaskRunner._choose_next as it was before the sweep share (copied from the previous version)."""
    if runner._mailbox and runner._mailbox.journal.active is not None:
        remote = runner._mailbox.poll(queue)
        if remote is not None:
            return remote
        if runner._mailbox.blocks_execution():
            return None

    local = queue.get_next_task()
    if local is not None:
        runner._local_task_class = "local"
        return local

    if runner._mailbox:
        remote = runner._mailbox.poll(queue)
        if remote is not None:
            return remote
        if runner._mailbox.blocks_execution():
            return None

    runner._schedule_next()
    scheduled = queue.get_next_task()
    if scheduled is not None:
        runner._local_task_class = "sweep"
    return scheduled


class Scenario:
    """One set of inputs, built fresh for each scheduler so the call logs are comparable."""

    def __init__(self, *, mailbox, active, remote, blocks, local, scheduled):
        self.mailbox, self.active, self.remote, self.blocks, self.local, self.scheduled = (
            mailbox,
            active,
            remote,
            blocks,
            local,
            scheduled,
        )

    def build(self):
        log = []
        queue = MagicMock()
        queued = []

        def next_task():
            log.append("queue.get_next_task")
            if queued:
                return queued[0]
            return "local-task" if self.local else None

        queue.get_next_task.side_effect = next_task
        mailbox = None
        if self.mailbox:
            mailbox = MagicMock()
            mailbox.journal.active = {"task_id": "x"} if self.active else None
            mailbox.blocks_execution.side_effect = lambda: log.append("mailbox.blocks") or self.blocks
            mailbox.poll.side_effect = lambda q: log.append("mailbox.poll") or ("remote-task" if self.remote else None)
        runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)

        def schedule():
            log.append("schedule_next")
            if self.scheduled:
                queued.append("sweep-task")

        runner._schedule_next = schedule
        # Any coordinator that is asked about need makes the test fail: with no share nobody may ask.
        coordinator = MagicMock(workload_id="w", epoch_id="v1", retired=False)
        coordinator.has_need.side_effect = AssertionError("has_need must not be asked when the share is off")
        runner._subscribers = [coordinator]
        return runner, queue, log


SCENARIOS = [
    Scenario(mailbox=m, active=a, remote=r, blocks=b, local=l, scheduled=s)
    for m, a, r, b, l, s in itertools.product([False, True], repeat=6)
    if m or not (a or r or b)  # a runner without a mailbox has none of those
]


@pytest.mark.parametrize("config", [SweepConfig(), SweepConfig(sweep_share=0.0)], ids=["no-file", "share-0"])
@pytest.mark.parametrize("index", range(len(SCENARIOS)))
def test_with_no_share_the_new_scheduler_matches_the_old_one_call_for_call(index, config):
    scenario = SCENARIOS[index]
    old_runner, old_queue, old_log = scenario.build()
    new_runner, new_queue, new_log = scenario.build()
    old_runner._local_task_class = new_runner._local_task_class = "local"

    with patch("conductress.task_runner.load_sweep_config", return_value=config):
        old_result = _choose_next_before(old_runner, old_queue)
        new_result = new_runner._choose_next(new_queue)

    assert new_result == old_result
    assert new_log == old_log
    assert new_runner._local_task_class == old_runner._local_task_class


def _runner(*, remote=True, scheduled=True, need=True, allowed=True, local=False, active=False):
    log = []
    queued = []
    queue = MagicMock()
    queue.get_next_task.side_effect = lambda: queued[0] if queued else ("local-task" if local else None)
    mailbox = MagicMock()
    mailbox.journal.active = {"task_id": "x"} if active else None
    mailbox.blocks_execution.return_value = False
    mailbox.poll.side_effect = lambda q: log.append("poll") or ("remote-task" if remote else None)
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)

    def schedule():
        log.append("schedule_next")
        if scheduled:
            queued.append("sweep-task")

    runner._schedule_next = schedule
    coordinator = MagicMock(workload_id="w", epoch_id="v1", retired=False)
    coordinator.has_need.side_effect = lambda: log.append("has_need") or need
    runner._subscribers = [coordinator]
    runner._clock = lambda: 1000.0
    return runner, queue, log, coordinator


def _config(share=25.0, mode="normal", paused=None):
    return SweepConfig(mode=mode, paused=paused, sweep_share=share)


def test_with_a_share_and_need_and_a_sweep_below_its_share_the_sweep_goes_before_the_remote_claim():
    runner, queue, log, _ = _runner()
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "sweep-task"
    assert log == ["has_need", "schedule_next"]  # no remote claim at all
    assert runner._local_task_class == "sweep"


def test_a_sweep_that_used_its_share_waits_behind_the_remote_task():
    runner, queue, log, _ = _runner()
    runner._share_window.add(300, is_sweep=True, now=1000.0)
    runner._share_window.add(900, is_sweep=False, now=1000.0)  # exactly 25 percent
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "remote-task"
    assert log == ["poll"]  # need is not even asked: the share is used up


def test_no_need_means_the_order_of_earlier_versions():
    runner, queue, log, _ = _runner(need=False)
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "remote-task"
    assert log == ["has_need", "poll"]


def test_a_paused_series_is_not_asked_and_cannot_claim_the_share():
    runner, queue, log, _ = _runner()
    with patch("conductress.task_runner.load_sweep_config", return_value=_config(mode="paused", paused=["w"])):
        assert runner._choose_next(queue) == "remote-task"
    assert "has_need" not in log


def test_a_retired_series_is_not_asked():
    runner, queue, log, coordinator = _runner()
    coordinator.retired = True
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "remote-task"
    assert "has_need" not in log


def test_when_the_sweep_has_need_but_queues_nothing_the_remote_task_is_claimed():
    runner, queue, log, _ = _runner(scheduled=False)
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "remote-task"
    assert log == ["has_need", "schedule_next", "poll"]


def test_a_task_queued_by_hand_still_goes_first():
    runner, queue, log, _ = _runner(local=True)
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "local-task"
    assert log == []


def test_a_claimed_task_that_the_runner_already_owns_is_never_displaced():
    runner, queue, log, _ = _runner(active=True)
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "remote-task"
    assert log == ["poll"]


def test_with_need_and_no_remote_task_the_sweep_runs_at_once_and_idle_time_is_not_held_back():
    runner, queue, log, _ = _runner(remote=False)
    runner._share_window.add(1000, is_sweep=True, now=1000.0)  # the sweep is over its share
    with patch("conductress.task_runner.load_sweep_config", return_value=_config()):
        assert runner._choose_next(queue) == "sweep-task"
    assert log == ["poll", "schedule_next"]  # the usual path: nothing to claim, so the sweep runs


def test_the_window_counts_the_time_of_every_task_it_is_given_so_a_long_sweep_uses_the_share_up():
    runner, _, _, _ = _runner()
    runner._share_window.add(600, is_sweep=True, now=1000.0)
    runner._share_window.add(1200, is_sweep=False, now=1000.0)
    assert runner._share_window.fraction(1000.0) == pytest.approx(1 / 3)
    with patch("conductress.task_runner.load_sweep_config", return_value=_config(share=25)):
        assert runner._sweep_owed_first() is False
    with patch("conductress.task_runner.load_sweep_config", return_value=_config(share=50)):
        assert runner._sweep_owed_first() is True


class _ExitLoop(Exception):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_class, owned_by_mailbox, sweep_seconds",
    [("sweep", False, 300.0), ("local", False, 0.0), ("sweep", True, 0.0)],
    ids=["sweep task", "hand-queued task", "remote task after a sweep task"],
)
async def test_the_run_loop_counts_each_finished_task_in_the_window(task_class, owned_by_mailbox, sweep_seconds):
    ticks = {"now": 0.0}
    mailbox = MagicMock()
    mailbox.journal.active = None
    mailbox.blocks_execution.return_value = False
    mailbox.status.return_value = {"mode": "live"}
    mailbox.owns.return_value = owned_by_mailbox
    task = MagicMock(task_id="t", task_type="PerfTaskData")
    runner = TaskRunner(mailbox=mailbox, management_settle_seconds=0)
    runner._clock = lambda: ticks["now"]
    runner._local_task_class = task_class
    runner._choose_next = MagicMock(side_effect=[task, _ExitLoop()])

    async def execute(_task):
        ticks["now"] += 300.0

    with (
        patch("conductress.task_runner.TaskQueue"),
        patch("conductress.task_runner.FileProtocol.cleanup_orphaned_tasks", return_value=0),
        patch("conductress.task_runner.get_servers", return_value=[]),
        patch("conductress.task_runner.build_status", return_value={"schema_version": 1}),
        patch("conductress.task_runner.export_status"),
    ):
        runner._TaskRunner__run_task = execute
        with pytest.raises(_ExitLoop):
            await runner.run()

    window = runner._share_window
    assert window.fraction(300.0) == pytest.approx(sweep_seconds / 300.0)
