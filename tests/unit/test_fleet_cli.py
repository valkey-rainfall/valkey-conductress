import json
from unittest.mock import MagicMock, patch

import pytest

from conductress import cli, task_queue
from conductress.fleet_cli import main, resolve_runner
from conductress.fleet_client import FleetClientError


@pytest.fixture(autouse=True)
def valid_sources(monkeypatch):
    monkeypatch.setattr(cli.config, "REPO_NAMES", ["valkey", "valkey-rainfall"])
    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey", "valkey-rainfall"])


class FakeClient:
    def __init__(self):
        self.submitted = []
        self.cancelled = []

    def fleet(self):
        return {
            "schema_version": 1,
            "runners": [
                {
                    "runner_id": "armbench",
                    "display_name": "Graviton 3",
                    "platform": "arm64/c7g.metal/graviton3",
                    "platform_aliases": ["graviton3", "arm64"],
                    "enabled": True,
                    "status_ttl_seconds": 900,
                    "status": None,
                    "status_updated_at": None,
                    "task_counts": {"queued": 2},
                },
                {
                    "runner_id": "disabled",
                    "display_name": "Disabled",
                    "platform": "test/disabled",
                    "platform_aliases": ["disabled"],
                    "enabled": False,
                    "status_ttl_seconds": 900,
                    "status": None,
                    "status_updated_at": None,
                    "task_counts": {},
                },
            ],
        }

    def runner(self, runner_id):
        runner = next(r for r in self.fleet()["runners"] if r["runner_id"] == runner_id)
        return {"schema_version": 1, "runner": runner}

    def list_tasks(self, **_filters):
        return {
            "schema_version": 1,
            "tasks": [
                {
                    "task_id": "task-1",
                    "runner_id": "armbench",
                    "task_class": "manual",
                    "state": "queued",
                    "priority": 100,
                    "submitted_at": "2026-08-29T00:00:00Z",
                }
            ],
        }

    def task(self, task_id):
        return {
            "schema_version": 1,
            "task": {
                "task_id": task_id,
                "runner_id": "armbench",
                "task_class": "manual",
                "state": "queued",
                "priority": 100,
                "submitted_at": "2026-08-29T00:00:00Z",
                "submitted_by": "rain",
                "envelope": {"task": {"task_type": "PerfTaskData"}},
                "outcome": None,
            },
        }

    def cancel_task(self, task_id):
        self.cancelled.append(task_id)
        return {
            "schema_version": 1,
            "task": {"task_id": task_id, "state": "cancelled"},
            "changed": True,
        }

    def submit_task(self, envelope, idempotency_key):
        self.submitted.append((envelope, idempotency_key))
        return {
            "schema_version": 1,
            "task": {
                "task_id": envelope["task_id"],
                "runner_id": envelope["runner_id"],
                "state": "queued",
            },
            "created": True,
        }


def test_fleet_list_human_and_json(capsys):
    assert main(["fleet", "list"], client=FakeClient()) == 0
    human = capsys.readouterr().out
    assert "armbench" in human
    assert "graviton3" in human

    assert main(["fleet", "list", "--json"], client=FakeClient()) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["command"] == "fleet.list"
    assert document["data"]["runners"][0]["runner_id"] == "armbench"


def test_fleet_status_show_and_remote_commands(capsys):
    client = FakeClient()
    assert main(["fleet", "status"], client=client) == 0
    assert "offline" in capsys.readouterr().out
    assert main(["fleet", "show", "armbench"], client=client) == 0
    assert "Graviton 3" in capsys.readouterr().out
    assert main(["remote", "list"], client=client) == 0
    assert "task-1" in capsys.readouterr().out
    assert main(["remote", "show", "task-1"], client=client) == 0
    assert "PerfTaskData" in capsys.readouterr().out
    assert main(["remote", "cancel", "task-1"], client=client) == 0
    assert client.cancelled == ["task-1"]


def test_platform_resolution_unique_disabled_and_ambiguous():
    client = FakeClient()
    assert resolve_runner(client, runner_id=None, platform="graviton3") == "armbench"
    with pytest.raises(FleetClientError) as disabled:
        resolve_runner(client, runner_id="disabled", platform=None)
    assert disabled.value.exit_code == 4

    original = client.fleet
    client.fleet = lambda: {
        **original(),
        "runners": original()["runners"] + [{**original()["runners"][0], "runner_id": "armbench-2"}],
    }
    with pytest.raises(FleetClientError) as ambiguous:
        resolve_runner(client, runner_id=None, platform="graviton3")
    assert ambiguous.value.code == "PLATFORM_AMBIGUOUS"


def test_json_errors_are_stable(capsys):
    class BrokenClient(FakeClient):
        def fleet(self):
            raise FleetClientError("AUTH_INVALID", "bad token", 2, 401)

    assert main(["fleet", "list", "--json"], client=BrokenClient()) == 2
    document = json.loads(capsys.readouterr().out)
    assert document == {
        "schema_version": 1,
        "error": True,
        "code": "AUTH_INVALID",
        "message": "bad token",
        "exit_code": 2,
    }


@pytest.mark.parametrize(
    "argv",
    [
        ["queue", "add", "--tests", "get", "--runner", "armbench"],
        ["queue", "add-memory", "--runner", "armbench"],
        ["queue", "add-mixed", "--set-ratio", "20", "--runner", "armbench"],
        ["queue", "add-scenario", "--scenario", "eval-storm", "--runner", "armbench"],
        ["queue", "add-latency", "valkey", "abc123", "100000", "--runner", "armbench"],
        ["queue", "add-cachecannon", "--runner", "armbench"],
    ],
)
def test_every_task_command_accepts_remote_routing(argv):
    args = cli.build_parser().parse_args(argv)
    assert args.runner == "armbench"
    assert args.platform is None
    assert args.priority == 100


def test_queue_add_remote_submits_envelope_without_touching_local_queue(capsys):
    client = FakeClient()
    local_queue = MagicMock()
    with (
        patch("conductress.fleet_client.FleetClient.from_env", return_value=client),
        patch("conductress.cli.TaskQueue", return_value=local_queue),
    ):
        result = cli.main(
            [
                "queue",
                "add",
                "--tests",
                "get",
                "--platform",
                "graviton3",
                "--json",
            ]
        )

    assert result == 0
    local_queue.submit_task.assert_not_called()
    assert len(client.submitted) == 1
    envelope, key = client.submitted[0]
    assert envelope["runner_id"] == "armbench"
    assert key == f"armbench:{envelope['task_id']}"
    output = json.loads(capsys.readouterr().out)
    assert output["data"]["destination"] == "remote"


def test_queue_add_without_routing_remains_local(capsys):
    local_queue = MagicMock()
    with (
        patch("conductress.cli.TaskQueue", return_value=local_queue),
        patch("conductress.fleet_client.FleetClient.from_env", side_effect=AssertionError("must not contact control")),
    ):
        result = cli.main(["queue", "add", "--tests", "get"])

    assert result == 0
    local_queue.submit_task.assert_called_once()
    assert "Queued 1 task" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv",
    [
        ["queue", "add", "--tests", "get"],
        ["queue", "add-memory", "--types", "set"],
        ["queue", "add-mixed", "--set-ratio", "20"],
        ["queue", "add-scenario", "--scenario", "eval-storm"],
        ["queue", "add-latency", "valkey", "abc123", "100000"],
        ["queue", "add-cachecannon"],
    ],
)
def test_all_task_handlers_submit_remotely(argv, capsys):
    client = FakeClient()
    with patch("conductress.fleet_client.FleetClient.from_env", return_value=client):
        result = cli.main([*argv, "--runner", "armbench", "--json"])

    assert result == 0
    assert client.submitted
    document = json.loads(capsys.readouterr().out)
    assert document["data"]["destination"] == "remote"
    assert document["data"]["runner_id"] == "armbench"


def test_partial_remote_batch_failure_reports_submitted_ids(capsys):
    class FlakyClient(FakeClient):
        def submit_task(self, envelope, idempotency_key):
            if self.submitted:
                raise FleetClientError("CONTROL_UNREACHABLE", "connection lost", 3)
            return super().submit_task(envelope, idempotency_key)

    client = FlakyClient()
    with patch("conductress.fleet_client.FleetClient.from_env", return_value=client):
        result = cli.main(
            [
                "queue",
                "add",
                "--tests",
                "get",
                "--sizes",
                "16,32",
                "--runner",
                "armbench",
                "--json",
            ]
        )

    assert result == 3
    document = json.loads(capsys.readouterr().out)
    assert document["code"] == "CONTROL_UNREACHABLE"
    assert document["details"]["runner_id"] == "armbench"
    assert len(document["details"]["submitted"]) == 1
    assert document["details"]["submitted"][0]["task_id"] in document["message"]
