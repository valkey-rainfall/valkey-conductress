"""Non-blocking CLI status command for Conductress."""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import CONDUCTRESS_TMP, PROJECT_ROOT
from .file_protocol import FileProtocol
from .task_queue import TaskQueue


def _find_runner_pid() -> Optional[int]:
    """Find the PID of a running task runner process."""
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            cmdline = (proc_dir / "cmdline").read_bytes().decode(errors="ignore")
            if "src" in cmdline and "run" in cmdline and "python" in cmdline:
                return int(proc_dir.name)
        except (PermissionError, FileNotFoundError, ProcessLookupError):
            continue
    return None


def _format_elapsed(seconds: float) -> str:
    """Format seconds into human-readable elapsed time."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.0f}m {seconds % 60:.0f}s"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"


def print_status() -> int:
    """Print runner status, active tasks, and queue depth. Returns exit code."""
    # Runner process
    runner_pid = _find_runner_pid()
    if runner_pid:
        print(f"Runner: alive (PID {runner_pid})")
    else:
        print("Runner: NOT RUNNING")

    # Last crash
    crash_file = PROJECT_ROOT / "last_crash.json"
    if crash_file.exists():
        try:
            crash = json.loads(crash_file.read_text())
            ts = crash.get("timestamp", "unknown")
            task = crash.get("task", "unknown")
            tb_lines = crash.get("traceback", "").strip().splitlines()
            last_line = tb_lines[-1] if tb_lines else "unknown"
            print(f"Last crash: {ts}")
            print(f"  Error: {last_line}")
            if task:
                print(f"  Task: {task}")
        except (json.JSONDecodeError, KeyError):
            pass

    # Active tasks
    active_tasks = FileProtocol.get_active_task_ids(CONDUCTRESS_TMP)
    if active_tasks:
        print(f"\nActive tasks: {len(active_tasks)}")
        for task_id, status in active_tasks.items():
            elapsed = ""
            if status.start_time:
                elapsed = f" ({_format_elapsed(time.time() - status.start_time)})"
            progress = ""
            if status.steps_total > 0:
                progress = f" [{status.steps_completed}/{status.steps_total}]"
            state = status.state or "unknown"
            print(f"  {task_id}: {status.task_type} {state}{progress}{elapsed}")
    else:
        print("\nActive tasks: none")

    # Queue
    queue = TaskQueue()
    tasks = queue.get_all_tasks()
    print(f"Queued tasks: {len(tasks)}")
    for task in tasks[:5]:
        desc = f"{task.source}:{task.specifier}"
        note = f" ({task.note})" if hasattr(task, "note") and task.note else ""
        print(f"  {task.task_id}: {desc}{note}")
    if len(tasks) > 5:
        print(f"  ... and {len(tasks) - 5} more")

    return 0
