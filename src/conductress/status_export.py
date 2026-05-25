"""Status JSON exporter for remote monitoring.

Writes a machine-readable status.json containing runner state, current task,
queue contents, and recent results. Designed to be pulled via SSH by an
aggregator on benchdev.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .config import CONDUCTRESS_OUTPUT, CONDUCTRESS_TMP, PROJECT_ROOT
from .file_protocol import FileProtocol
from .status import _find_runner_pid, _format_elapsed
from .task_queue import TaskQueue

logger = logging.getLogger(__name__)

STATUS_EXPORT_DIR = PROJECT_ROOT / "status"
STATUS_EXPORT_FILE = STATUS_EXPORT_DIR / "status.json"

# How many recent results to include
RECENT_RESULTS_COUNT = 5

# Average task duration for ETA estimation (seconds). Updated dynamically from recent results.
DEFAULT_TASK_DURATION = 150  # 2.5 min (build + benchmark on AMD)


def export_status(publish_target: str = "") -> Path:
    """Export current runner status to status.json. Returns the output path."""
    status: dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "host": _get_hostname(),
        "runner": _get_runner_info(),
        "current_task": _get_current_task(),
        "queue": _get_queue_info(),
        "recent_results": _get_recent_results(),
    }

    # Estimate time to complete queue
    avg_duration = _estimate_task_duration()
    queue_depth = status["queue"]["depth"]
    current_remaining = _estimate_current_remaining(status["current_task"])
    status["eta_minutes"] = round((current_remaining + queue_depth * avg_duration) / 60, 1)

    STATUS_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_EXPORT_FILE.write_text(json.dumps(status, indent=2))

    if publish_target:
        _publish_status(publish_target)

    return STATUS_EXPORT_FILE


def _publish_status(target: str) -> None:
    """Rsync status.json to the dashboard data server."""
    import subprocess

    from conductress.publisher import detect_platform

    platform_id, _ = detect_platform()
    # Map platform to status filename expected by dashboard
    name_map = {"arm64": "arm", "amd64": "x86", "intel": "intel"}
    filename = name_map.get(platform_id, platform_id)

    ssh_key = Path.home() / "conductress" / "server-keyfile.pem"
    if not ssh_key.exists():
        ssh_key = Path.home() / ".ssh" / "openssh-ec2-pair.pem"
    ssh_cmd = f"ssh -i {ssh_key} -F /dev/null -o StrictHostKeyChecking=no -o ConnectTimeout=10"
    dest = f"{target}/status/{filename}.json"
    result = subprocess.run(
        ["rsync", "-az", "--chmod=D755,F644", "-e", ssh_cmd, str(STATUS_EXPORT_FILE), dest],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0:
        logger.warning("Status publish failed: %s", result.stderr.strip())


def _get_hostname() -> str:
    """Get a short hostname for identification."""
    import socket

    return socket.gethostname().split(".")[0]


def _get_runner_info() -> dict[str, Any]:
    pid = _find_runner_pid()
    if pid:
        # Get uptime from /proc
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().split()
            # Approximate: use process start time
            start_time = float(stat[21]) / 100  # clock ticks to seconds
            system_uptime = float(Path("/proc/uptime").read_text().split()[0])
            uptime_sec = time.time() - (system_uptime - start_time / 100)
        except Exception:
            uptime_sec = None
        return {"pid": pid, "state": "running", "uptime_hours": round(uptime_sec / 3600, 1) if uptime_sec else None}
    else:
        return {"pid": None, "state": "stopped", "uptime_hours": None}


def _get_current_task() -> Optional[dict[str, Any]]:
    active = FileProtocol.get_active_task_ids(CONDUCTRESS_TMP)
    if not active:
        return None

    task_id, status = next(iter(active.items()))
    elapsed = time.time() - status.start_time if status.start_time else 0
    progress_pct = (status.steps_completed / status.steps_total * 100) if status.steps_total > 0 else 0

    return {
        "id": task_id,
        "type": status.task_type,
        "state": status.state,
        "progress_pct": round(progress_pct, 1),
        "elapsed_sec": round(elapsed),
        "steps": f"{status.steps_completed}/{status.steps_total}",
    }


def _get_queue_info() -> dict[str, Any]:
    queue = TaskQueue()
    tasks = queue.get_all_tasks()
    return {
        "depth": len(tasks),
        "tasks": [
            {"id": t.task_id, "type": t.task_type, "note": t.note or "", "source": t.source, "specifier": t.specifier}
            for t in tasks[:10]
        ],
    }


def _get_recent_results() -> list[dict[str, Any]]:
    """Read the last N results from output.jsonl."""
    if not CONDUCTRESS_OUTPUT.exists():
        return []

    lines = CONDUCTRESS_OUTPUT.read_text().strip().splitlines()
    results: list[dict[str, Any]] = []
    for line in reversed(lines[-RECENT_RESULTS_COUNT * 2 :]):  # read extra in case of parse errors
        if len(results) >= RECENT_RESULTS_COUNT:
            break
        try:
            entry = json.loads(line)
            results.append(
                {
                    "task_id": entry.get("task_id", ""),
                    "method": entry.get("method", ""),
                    "score": entry.get("score"),
                    "commit": entry.get("commit_hash", "")[:8],
                    "source": entry.get("source", ""),
                    "specifier": entry.get("specifier", ""),
                    "note": entry.get("note", ""),
                    "completed": entry.get("end_time", ""),
                }
            )
        except (json.JSONDecodeError, KeyError):
            continue
    return results


def _estimate_task_duration() -> float:
    """Estimate average task duration from recent results."""
    # Simple heuristic: use DEFAULT_TASK_DURATION
    # Could be improved by reading timestamps from output.jsonl
    return DEFAULT_TASK_DURATION


def _estimate_current_remaining(current_task: Optional[dict]) -> float:
    """Estimate seconds remaining for current task."""
    if not current_task:
        return 0
    progress = current_task.get("progress_pct", 0)
    elapsed = current_task.get("elapsed_sec", 0)
    if progress > 5 and elapsed > 0:
        total_estimated = elapsed / (progress / 100)
        return max(0, total_estimated - elapsed)
    return DEFAULT_TASK_DURATION  # fallback
