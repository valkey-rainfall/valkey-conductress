"""Factories for valid fleet control-plane test documents."""

from datetime import datetime, timezone


def now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def fleet_manifest():
    return {
        "schema_version": 1,
        "runners": [
            {
                "runner_id": "armbench",
                "display_name": "Graviton 3",
                "platform": "arm64/c7g.metal/graviton3",
                "platform_aliases": ["graviton3", "arm64"],
                "enabled": True,
                "canary_profile": "throughput-get-v1",
                "status_ttl_seconds": 900,
            },
            {
                "runner_id": "g4bench",
                "display_name": "Graviton 4",
                "platform": "arm64/c8g.metal/graviton4",
                "platform_aliases": ["graviton4"],
                "enabled": True,
                "canary_profile": "throughput-get-v1",
                "status_ttl_seconds": 900,
            },
            {
                "runner_id": "disabled",
                "display_name": "Disabled",
                "platform": "test/disabled",
                "platform_aliases": ["disabled"],
                "enabled": False,
                "canary_profile": None,
                "status_ttl_seconds": 900,
            },
        ],
    }


def task_envelope(task_id="task-1", runner_id="armbench", priority=100, task_class="manual"):
    return {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": runner_id,
        "task_class": task_class,
        "priority": priority,
        "submitted_at": "2026-08-29T00:00:00Z",
        "submitted_by": "rain",
        "canary_id": None,
        "task": {
            "task_type": "PerfTaskData",
            "source": "valkey",
            "specifier": "abc123",
            "timestamp": "2026-08-29T00:00:00.000000",
        },
    }


def runner_status(runner_id="armbench"):
    return {
        "schema_version": 1,
        "timestamp": now_text(),
        "runner_id": runner_id,
        "platform": "arm64/c7g.metal/graviton3",
        "identity": {},
        "host": "host-a",
        "runner": {"state": "running"},
        "current_task": None,
        "queue": {"depth": 0, "tasks": []},
        "recent_results": [],
        "disk": {},
        "eta_minutes": 0,
    }


def task_outcome(task_id="task-1", runner_id="armbench", state="completed"):
    return {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": runner_id,
        "state": state,
        "completed_at": now_text(),
        "result": {"score": 123.0} if state == "completed" else None,
        "error": "benchmark failed" if state == "failed" else None,
    }
