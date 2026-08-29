"""Main entry point for Conductress, the benchmarking framework for Valkey.
This script runs tasks from a queue, executing performance and memory tests"""

import asyncio
import json
import logging
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Protocol

from conductress.sweep_config import load_sweep_config
from conductress.task_queue import BaseTaskRunner

from .config import (
    CONDUCTRESS_FAILED_DIR,
    CONDUCTRESS_FAILED_LOG,
    CONDUCTRESS_LOG,
    DELIVERY_JOURNAL_PATH,
    FLEET_IDLE_POLL_INTERVAL,
    MANAGEMENT_SETTLE_SECONDS,
    QUEUE_POLL_INTERVAL,
    get_servers,
)
from .file_protocol import FileProtocol
from .nudge_hook import NudgeHook
from .runner_mailbox import RunnerMailbox
from .server import Server
from .status_export import build_status, export_status
from .task_queue import BaseTaskData, TaskQueue

logger = logging.getLogger(__name__)


class TaskSubscriber(Protocol):
    """Protocol for task completion subscribers."""

    def on_task_completed(self, task: BaseTaskData) -> None: ...
    def on_task_failed(self, task: BaseTaskData) -> None: ...
    def on_queue_empty(self) -> None: ...


class TaskRunner:
    """Takes tasks from queue and runs them"""

    def __init__(
        self,
        sweep: bool = False,
        memory_sweep: bool = False,
        repo_path: Optional[Path] = None,
        publish_target: Optional[str] = None,
        nudge_url: Optional[str] = None,
        nudge_on: Optional[set[str]] = None,
        fleet_mode: str = "off",
        management_settle_seconds: Optional[float] = None,
        mailbox: Optional[RunnerMailbox] = None,
    ) -> None:
        self.task: Optional[BaseTaskData] = None
        self._subscribers: list[TaskSubscriber] = []
        self._publish_target = publish_target
        self._mailbox = mailbox
        if self._mailbox is None and fleet_mode != "off":
            self._mailbox = RunnerMailbox(DELIVERY_JOURNAL_PATH, mode=fleet_mode)
        self._management_settle_seconds = (
            MANAGEMENT_SETTLE_SECONDS if management_settle_seconds is None else management_settle_seconds
        )
        if self._management_settle_seconds < 0:
            raise ValueError("management_settle_seconds must not be negative")
        if sweep:
            from conductress.config import SWEEP_IO_THREADS, SWEEP_PIPELINING, SWEEP_THROUGHPUT_WORKLOADS
            from conductress.platform import get_local_platform_tag
            from conductress.sweep.coordinator import SweepCoordinator

            if repo_path is None:
                repo_path = Path.home() / "valkey"
            coordinator = SweepCoordinator(repo_path)
            coordinator.initialize()
            self._subscribers.append(coordinator)

            # Additional throughput workloads (e.g. 64B values, platform-optimal configs)
            local_platform = get_local_platform_tag()
            for wl in SWEEP_THROUGHPUT_WORKLOADS:
                platforms = wl.get("platforms")
                if platforms and local_platform not in platforms:
                    continue
                extra = SweepCoordinator(
                    repo_path,
                    val_size=wl["val_size"],
                    test=wl.get("test", "get"),
                    io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                    pipelining=wl.get("pipelining", SWEEP_PIPELINING),
                )
                extra.initialize()
                self._subscribers.append(extra)

            # Latency sweep runs independently (flat rate, no throughput dependency)
            from conductress.sweep.latency_coordinator import LatencySweepCoordinator

            latency_coordinator = LatencySweepCoordinator(repo_path)
            latency_coordinator.initialize()
            self._subscribers.append(latency_coordinator)

            # Memory sweep runs alongside throughput
            from conductress.sweep.memory_coordinator import create_memory_coordinators

            for mem_coordinator in create_memory_coordinators(repo_path):
                mem_coordinator.initialize()
                self._subscribers.append(mem_coordinator)

            # Additional engines (e.g. Redis) -- throughput + memory sweep
            from conductress.config import SWEEP_ENGINES

            for engine in SWEEP_ENGINES:
                if engine.source == "valkey":
                    continue  # Already handled above
                engine_repo = Path.home() / engine.source
                if not engine_repo.exists():
                    logger.info("Skipping engine %s: repo %s not found", engine.source, engine_repo)
                    continue
                engine_coord = SweepCoordinator(engine_repo, engine=engine)
                engine_coord.initialize()
                self._subscribers.append(engine_coord)
                for wl in SWEEP_THROUGHPUT_WORKLOADS:
                    platforms = wl.get("platforms")
                    if platforms and local_platform not in platforms:
                        continue
                    extra = SweepCoordinator(
                        engine_repo,
                        val_size=wl["val_size"],
                        test=wl.get("test", "get"),
                        io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                        pipelining=wl.get("pipelining", SWEEP_PIPELINING),
                        engine=engine,
                    )
                    extra.initialize()
                    self._subscribers.append(extra)
                # Memory sweep for this engine
                for mem_coordinator in create_memory_coordinators(engine_repo, engine=engine):
                    mem_coordinator.initialize()
                    self._subscribers.append(mem_coordinator)
        if memory_sweep and not sweep:
            # Standalone memory sweep (backward compat, rarely used)
            from conductress.sweep.memory_coordinator import create_memory_coordinators

            if repo_path is None:
                repo_path = Path.home() / "valkey"
            for mem_coordinator in create_memory_coordinators(repo_path):
                mem_coordinator.initialize()
                self._subscribers.append(mem_coordinator)
        if publish_target:
            from conductress.publisher import DashboardPublisher
            from conductress.sweep.coordinator import BaseSweepCoordinator

            coordinators = [s for s in self._subscribers if isinstance(s, BaseSweepCoordinator)]
            publisher = DashboardPublisher(publish_target, coordinators)
            self._subscribers.append(publisher)
        if nudge_url:
            self._subscribers.append(NudgeHook(nudge_url, events=nudge_on))

    async def __run_task(self, task_data: BaseTaskData) -> None:
        """Run a task, ensuring CPU allocations are released on failure."""
        servers = get_servers()
        server_count = task_data.replicas + 1 if task_data.replicas > 0 else 1
        if len(servers) < server_count:
            raise RuntimeError(f"Not enough servers for {task_data.replicas} replicas. Found {len(servers)} servers.")

        task_runner: BaseTaskRunner = task_data.prepare_task_runner(servers[:server_count])
        try:
            await task_runner.run()
            task_runner.file_protocol.mark_completed_and_cleanup()
        except Exception:
            task_runner.file_protocol.cleanup()
            # Release any leaked CPU allocations (server may have started but not stopped)
            try:
                await asyncio.gather(
                    *[Server(s.ip).kill_all_valkey_instances_on_host() for s in servers[:server_count]]
                )
            except Exception as cleanup_err:
                logger.warning("Failed to release CPU allocations: %s", cleanup_err)
            raise

    def _publish_boundary(self, state: str, task: Optional[BaseTaskData]) -> None:
        if self._mailbox is None and not self._publish_target:
            return
        boundary = {
            "state": state,
            "task_id": task.task_id if task else None,
            "task_type": task.task_type if task else None,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        fleet_control = self._mailbox.status() if self._mailbox else None
        status = build_status(fleet_control=fleet_control, boundary=boundary)
        if self._mailbox:
            self._mailbox.push_status(status)
            status = build_status(fleet_control=self._mailbox.status(), boundary=boundary)
        export_status(publish_target=self._publish_target or "", status=status)

    def _choose_next(self, queue: TaskQueue) -> Optional[BaseTaskData]:
        if self._mailbox and self._mailbox.journal.active is not None:
            remote = self._mailbox.poll(queue)
            if remote is not None:
                return remote
            if self._mailbox.blocks_execution():
                return None

        local = queue.get_next_task()
        if local is not None:
            return local

        if self._mailbox:
            remote = self._mailbox.poll(queue)
            if remote is not None:
                return remote
            if self._mailbox.blocks_execution():
                return None

        self._schedule_next()
        return queue.get_next_task()

    def _settle_management(self) -> None:
        if (self._mailbox is not None or self._publish_target) and self._management_settle_seconds:
            time.sleep(self._management_settle_seconds)

    async def run(self):
        """Execute local and remote tasks with management only at boundaries."""
        cleaned_count = FileProtocol.cleanup_orphaned_tasks()
        if cleaned_count > 0:
            logger.info("Cleaned up %d orphaned benchmark directories on startup", cleaned_count)

        await asyncio.gather(*[Server(server.ip).kill_all_valkey_instances_on_host() for server in get_servers()])

        queue = TaskQueue()
        self.task = self._choose_next(queue)
        while True:
            if self.task is None:
                self._publish_boundary("idle", None)
                logger.debug("waiting for new jobs in queue")
                next_fleet_poll = time.monotonic() + FLEET_IDLE_POLL_INTERVAL
                while self.task is None:
                    time.sleep(QUEUE_POLL_INTERVAL)
                    if self._mailbox and self._mailbox.journal.active is not None:
                        self.task = self._choose_next(queue)
                        continue
                    local = queue.get_next_task()
                    if local is not None:
                        self.task = local
                        continue
                    if self._mailbox is None or time.monotonic() >= next_fleet_poll:
                        self.task = self._choose_next(queue)
                        next_fleet_poll = time.monotonic() + FLEET_IDLE_POLL_INTERVAL
                continue

            task = self.task
            self._publish_boundary("starting", task)
            self._settle_management()
            try:
                await self.__run_task(task)
            except Exception as exc:
                self._record_failure(task, exc)
                for sub in self._subscribers:
                    sub.on_task_failed(task)
                if self._mailbox and self._mailbox.owns(task.task_id):
                    self._mailbox.stage_failure(task, f"{type(exc).__name__}: {exc}")
                queue.finish_task(task)
                if self._mailbox and self._mailbox.owns(task.task_id):
                    self._mailbox.flush_pending_outcome()
                self._publish_boundary("failed", task)
            else:
                for sub in self._subscribers:
                    sub.on_task_completed(task)
                if self._mailbox and self._mailbox.owns(task.task_id):
                    self._mailbox.stage_success(task)
                queue.finish_task(task)
                if self._mailbox and self._mailbox.owns(task.task_id):
                    self._mailbox.flush_pending_outcome()
                self._publish_boundary("completed", task)

            self.task = self._choose_next(queue)

    def _schedule_next(self) -> None:
        """Pick the highest-priority coordinator and let it queue a task.

        Called after every task completion/failure. Each coordinator proposes
        its next task via urgency scoring. NIGHTLY (untested HEAD) gets
        absolute priority over all urgency scores.
        """
        if not self._subscribers:
            return

        config = load_sweep_config()

        # Absolute priority: any coordinator with a NIGHTLY task goes first
        for sub in self._subscribers:
            wid = getattr(sub, "workload_id", None)
            if wid and not config.is_allowed(wid):
                continue
            if getattr(sub, "has_nightly_task", lambda: False)():
                sub.on_queue_empty()
                queue = TaskQueue()
                if queue.get_all_tasks():
                    return

        # Score each subscriber that supports urgency scoring
        candidates = []
        for sub in self._subscribers:
            wid = getattr(sub, "workload_id", None)
            if wid and not config.is_allowed(wid):
                continue
            score = getattr(sub, "get_urgency_score", lambda: 0.0)()
            candidates.append((score, sub))
        # Sort by urgency (highest first) and let the winner queue
        candidates.sort(key=lambda x: x[0], reverse=True)
        for _score, sub in candidates:
            sub.on_queue_empty()
            queue = TaskQueue()
            if queue.get_all_tasks():
                return

    def _record_failure(self, task: BaseTaskData, exc: Exception) -> None:
        """Log a task failure to failed_tasks.jsonl and move task file to failed/."""
        tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
        error_msg = f"{type(exc).__name__}: {exc}"

        logger.error("Task failed (note=%s, task_id=%s): %s", task.note, task.task_id, error_msg)

        entry = {
            "task_id": task.task_id,
            "note": task.note,
            "source": task.source,
            "specifier": task.specifier,
            "error": error_msg,
            "traceback": "".join(tb),
            "timestamp": datetime.now().isoformat(),
        }
        with open(CONDUCTRESS_FAILED_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")

        CONDUCTRESS_FAILED_DIR.mkdir(exist_ok=True)
        task_file = CONDUCTRESS_FAILED_DIR / f"task_{task.task_id}.json"
        task.save_to_file(task_file)


if __name__ == "__main__":
    logging.basicConfig(
        filename=CONDUCTRESS_LOG,
        encoding="utf-8",
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("asyncssh").setLevel(logging.WARNING)
    runner = TaskRunner()
    asyncio.run(runner.run())
