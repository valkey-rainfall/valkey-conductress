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

from conductress.task_queue import BaseTaskRunner

from .config import CONDUCTRESS_FAILED_DIR, CONDUCTRESS_FAILED_LOG, CONDUCTRESS_LOG, QUEUE_POLL_INTERVAL, get_servers
from .file_protocol import FileProtocol
from .server import Server
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
    ) -> None:
        self.task: Optional[BaseTaskData] = None
        self._subscribers: list[TaskSubscriber] = []
        if sweep:
            from conductress.sweep.coordinator import SweepCoordinator

            if repo_path is None:
                repo_path = Path.home() / "valkey"
            coordinator = SweepCoordinator(repo_path)
            coordinator.initialize()
            self._subscribers.append(coordinator)
        if memory_sweep:
            from conductress.sweep.memory_coordinator import MemorySweepCoordinator

            if repo_path is None:
                repo_path = Path.home() / "valkey"
            mem_coordinator = MemorySweepCoordinator(repo_path)
            mem_coordinator.initialize()
            self._subscribers.append(mem_coordinator)

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

    async def run(self):
        """Main function - execute tasks from the queue."""
        cleaned_count = FileProtocol.cleanup_orphaned_tasks()
        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} orphaned benchmark directories on startup")
            logger.info(f"Cleaned up {cleaned_count} orphaned benchmark directories on startup")

        await asyncio.gather(*[Server(server.ip).kill_all_valkey_instances_on_host() for server in get_servers()])

        # Notify subscribers on startup if queue is empty (e.g. sweep queues its first task)
        queue = TaskQueue()
        self.task = queue.get_next_task()
        if not self.task:
            self._notify_queue_empty()
        while True:
            while self.task:
                try:
                    await self.__run_task(self.task)
                except Exception as exc:
                    self._record_failure(self.task, exc)
                    # Release any leaked CPU allocations from the failed task
                    await asyncio.gather(
                        *[Server(s.ip).kill_all_valkey_instances_on_host() for s in get_servers()]
                    )
                    for sub in self._subscribers:
                        sub.on_task_failed(self.task)
                    queue.finish_task(self.task)
                    self.task = queue.get_next_task()
                    continue

                for sub in self._subscribers:
                    sub.on_task_completed(self.task)
                queue.finish_task(self.task)
                self.task = queue.get_next_task()

            # Queue empty — notify subscribers (they may queue new work)
            self._notify_queue_empty()
            self.task = queue.get_next_task()
            if self.task:
                continue

            logger.debug("waiting for new jobs in queue")
            while not self.task:
                time.sleep(QUEUE_POLL_INTERVAL)
                self.task = queue.get_next_task()
                if not self.task:
                    self._notify_queue_empty()
                    self.task = queue.get_next_task()

    def _notify_queue_empty(self) -> None:
        for sub in self._subscribers:
            sub.on_queue_empty()

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
