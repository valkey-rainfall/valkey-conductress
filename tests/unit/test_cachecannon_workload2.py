"""Second cachecannon workload in one task: cpu split, per-workload counts, task data, CLI and runner wiring.

The runner test fakes the topology, the local client host and the cachecannon
processes. The fake generator answers with a canned JSON result chosen from the
TOML it was launched with (SET-only -> writer numbers, otherwise reader
numbers), so the test checks the launch order, the TOML each process got and
the shape of the recorded row.
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from conductress import cli, config, task_queue
from conductress.cachecannon import achieved_set_pct, expand_cpu_list, split_cpu_list, workload_summary
from conductress.file_protocol import FileProtocol
from conductress.tasks import task_cachecannon as module
from conductress.tasks.task_cachecannon import CachecannonTaskData, generate_toml_config
from conductress.topology import TopologySpec


@pytest.fixture(autouse=True)
def _repo_names(monkeypatch):
    monkeypatch.setattr(cli.config, "REPO_NAMES", ["valkey", "valkey-rainfall"])
    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey", "valkey-rainfall"])


def _task(**overrides):
    base = dict(
        source=config.REPO_NAMES[0],
        specifier="unstable",
        make_args="",
        topology=TopologySpec.standalone(),
        note="",
        requirements={},
        test="get",
        val_size=16,
        pipelining=1,
        connections=360,
        threads=4,
        warmup=2,
        duration=10,
        repetitions=1,
        keyspace_count=3_000_000,
    )
    base.update(overrides)
    return CachecannonTaskData(**base)


# --------------------------------------------------------------------------- helpers


def test_split_cpu_list_exact_by_threads():
    assert split_cpu_list("8-12", [4, 1]) == ["8-11", "12"]


def test_split_cpu_list_proportional_keeps_one_cpu_each():
    slices = split_cpu_list("0-3", [30, 1])
    assert slices == ["0-2", "3"]
    cpus = [c for s in slices for c in expand_cpu_list(s)]
    assert cpus == [0, 1, 2, 3]


def test_split_cpu_list_empty_and_errors():
    assert split_cpu_list("", [4, 1]) == ["", ""]
    with pytest.raises(ValueError):
        split_cpu_list("0", [1, 1])
    with pytest.raises(ValueError):
        split_cpu_list("0-3", [0, 1])


def test_workload_summary_and_achieved_mix():
    reader = {
        "throughput_rps": 90.0,
        "error_pct": 0.0,
        "requests": 900,
        "latency_get": {"count": 900, "p99_ms": 0.1},
        "latency_set": None,
    }
    writer = {
        "throughput_rps": 10.0,
        "error_pct": 0.0,
        "requests": 100,
        "latency_get": None,
        "latency_set": {"count": 100, "p99_ms": 0.2},
    }
    summaries = [workload_summary(reader), workload_summary(writer)]
    assert [s["get_count"] for s in summaries] == [900, 0]
    assert [s["set_count"] for s in summaries] == [0, 100]
    assert achieved_set_pct(summaries) == pytest.approx(10.0)
    assert achieved_set_pct([workload_summary(dict(reader, latency_get=None))]) is None


# --------------------------------------------------------------------------- task data and CLI


def test_off_by_default_and_label_unchanged():
    task = _task()
    assert task.workload2_connections == 0
    assert task.workload_label() == "get"
    assert "w2" not in task.short_description()


def test_label_description_and_runner_fields():
    task = _task(workload2_connections=40, workload2_threads=1, workload2_set_ratio=100, workload2_rate_limit=5000)
    assert task.workload_label() == "get w2s100"
    assert "w2 40c 1t s100 5000 req/s" in task.short_description()
    runner = task.prepare_task_runner([])
    assert runner.workload == "get w2s100"
    assert (runner.workload2_connections, runner.workload2_threads) == (40, 1)
    assert (runner.workload2_set_ratio, runner.workload2_rate_limit) == (100, 5000)


@pytest.mark.parametrize(
    "overrides",
    [
        dict(workload2_connections=-1),
        dict(workload2_connections=4, workload2_threads=0),
        dict(workload2_connections=2, workload2_threads=4),
        dict(workload2_connections=4, workload2_set_ratio=101),
        dict(workload2_connections=4, workload2_rate_limit=-1),
    ],
)
def test_invalid_workload2_rejected(overrides):
    with pytest.raises(ValueError, match="workload2"):
        _task(**overrides)


def test_task_round_trips_through_serialization():
    from conductress.task_envelope import serialize_task
    from conductress.task_queue import BaseTaskData

    task = _task(workload2_connections=40, workload2_threads=2, workload2_set_ratio=0, workload2_rate_limit=7)
    restored = BaseTaskData.from_dict(serialize_task(task))
    assert isinstance(restored, CachecannonTaskData)
    assert (restored.workload2_connections, restored.workload2_threads) == (40, 2)
    assert (restored.workload2_set_ratio, restored.workload2_rate_limit) == (0, 7)


def _fake_client():
    client = MagicMock()
    client.fleet.return_value = {
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
                "task_counts": {"queued": 0},
            }
        ],
    }
    client.submitted = []

    def submit_task(envelope, idempotency_key):
        client.submitted.append(envelope)
        return {
            "schema_version": 1,
            "task": {"task_id": envelope["task_id"], "runner_id": envelope["runner_id"], "state": "queued"},
            "created": True,
        }

    client.submit_task.side_effect = submit_task
    return client


def test_cli_flags_land_in_remote_envelope():
    client = _fake_client()
    argv = ["queue", "add-cachecannon", "--runner", "armbench", "--json"]
    argv += ["--workload2-connections", "40", "--workload2-threads", "2"]
    argv += ["--workload2-set-ratio", "100", "--workload2-rate", "5000"]
    with patch("conductress.fleet_client.FleetClient.from_env", return_value=client):
        assert cli.main(argv) == 0
    payload = client.submitted[0]["task"]
    assert payload["workload2_connections"] == 40
    assert payload["workload2_threads"] == 2
    assert payload["workload2_set_ratio"] == 100
    assert payload["workload2_rate_limit"] == 5000


def test_cli_defaults_leave_workload2_off():
    client = _fake_client()
    with patch("conductress.fleet_client.FleetClient.from_env", return_value=client):
        assert cli.main(["queue", "add-cachecannon", "--runner", "armbench", "--json"]) == 0
    assert client.submitted[0]["task"]["workload2_connections"] == 0


def test_cli_rejects_bad_workload2_set_ratio(capsys):
    client = _fake_client()
    argv = ["queue", "add-cachecannon", "--runner", "armbench"]
    argv += ["--workload2-connections", "40", "--workload2-set-ratio", "150"]
    with patch("conductress.fleet_client.FleetClient.from_env", return_value=client):
        assert cli.main(argv) == 1
    assert "--workload2-set-ratio" in capsys.readouterr().err
    assert not client.submitted


# --------------------------------------------------------------------------- runner


def _cc_result(command: str, count: int) -> str:
    latency = {"p50_us": 100, "p90_us": 200, "p99_us": 400, "p999_us": 800, "p9999_us": 1600, "max_us": 3000}
    hits = count if command == "get" else 0
    return json.dumps(
        {
            "type": "result",
            "throughput": count / 10,
            "err_pct": 0.0,
            "hits": hits,
            "misses": 0,
            "hit_pct": 100.0 if hits else 0.0,
            "requests": count,
            "responses": count,
            "errors": 0,
            "duration_secs": 10,
            command: dict(latency, count=count),
        }
    )


class FakeCommand:
    """cachecannon stand-in: finishes at once with a result chosen from its TOML."""

    launched: list = []  # (toml_name, toml_text) in launch order

    def __init__(self, command: str):
        toml_path = command.split()[-1]
        with open(toml_path) as f:
            self.toml = f.read()
        FakeCommand.launched.append((toml_path.rsplit("/", 1)[-1], self.toml))
        self.p = None
        self._sent = False

    def start(self):
        self.p = MagicMock(pid=4_194_311, returncode=0)

    def is_running(self):
        return False

    def poll_output(self):
        if self._sent:
            return "", ""
        self._sent = True
        if "set = 100" in self.toml:
            return _cc_result("set", 100_000), ""
        return _cc_result("get", 900_000), ""


class FakeServer:
    ip = "127.0.0.1"
    port = 6379
    server_cpus = "0-3"

    def get_build_hash(self):
        return "0123456789ab"

    async def run_host_command(self, cmd, check=True):
        return "lscpu-output", ""


class FakeGroup:
    def __init__(self):
        self.primary = FakeServer()
        self.replicas = []

    async def stop_all_servers(self):
        pass

    async def kill_all_valkey_instances(self):
        pass

    async def start(self):
        pass

    async def begin_replication(self):
        pass

    async def wait_for_repl_sync(self):
        pass


class FakeClientHost:
    def __init__(self, ip):
        self.ip = ip

    async def ensure_host_cpu_allocation(self):
        pass


async def _no_memory(*args, **kwargs):
    return {}


def _run(task, tmp_path, monkeypatch):
    FakeCommand.launched = []
    monkeypatch.setattr(module, "RealtimeCommand", FakeCommand)
    monkeypatch.setattr(module, "Server", FakeClientHost)
    monkeypatch.setattr(module.TopologyGroup, "for_task", staticmethod(lambda *a, **k: FakeGroup()))
    monkeypatch.setattr(module, "sample_process_tree_cpu", lambda pid: None)
    monkeypatch.setattr(module, "reset_memory_peaks", _no_memory)
    monkeypatch.setattr(module, "collect_memory_after", _no_memory)
    monkeypatch.setattr(module, "fold_memory_records", lambda records: None)
    monkeypatch.setattr(module, "MemorySampler", MagicMock())
    monkeypatch.setattr(module.time, "sleep", lambda secs: None)
    runner = task.prepare_task_runner([config.ServerInfo(ip="127.0.0.1", username="operator")])
    runner.file_protocol = FileProtocol(runner.task_name, role_id="client", base_dir=tmp_path)
    runner.file_protocol.write_results = MagicMock()
    asyncio.run(runner.run())
    return runner.file_protocol.write_results.call_args.args[0]


def test_runner_without_workload2_is_unchanged(tmp_path, monkeypatch):
    task = _task(benchmark_cpu_override="8-11")
    row = _run(task, tmp_path, monkeypatch)
    assert [name for name, _ in FakeCommand.launched] == ["cachecannon_rep1.toml"]
    expected = generate_toml_config(
        duration=10,
        warmup=2,
        threads=4,
        cpu_list="8-11",
        endpoint="127.0.0.1:6379",
        connections=360,
        pipeline_depth=1,
        keyspace_count=3_000_000,
        val_size=16,
    )
    assert FakeCommand.launched[0][1] == expected
    assert row.method == "cachecannon-get"
    assert "workload2" not in row.data and "workloads" not in row.data
    assert "workloads" not in row.data["per_rep_results"][0]


def test_runner_shared_prefill_then_both_workloads(tmp_path, monkeypatch):
    task = _task(
        benchmark_cpu_override="8-12",
        workload2_connections=40,
        workload2_threads=1,
        workload2_set_ratio=100,
        workload2_rate_limit=5000,
    )
    row = _run(task, tmp_path, monkeypatch)
    names = [name for name, _ in FakeCommand.launched]
    assert names == ["cachecannon_rep1_prefill.toml", "cachecannon_rep1.toml", "cachecannon_rep1_w2.toml"]
    prefill, main, w2 = (text for _, text in FakeCommand.launched)

    # One prefill run seeds the keyspace; both measured workloads skip prefill.
    assert "prefill = true" in prefill and 'duration = "1s"' in prefill and 'warmup = "0s"' in prefill
    assert "prefill = false" in main and "prefill = false" in w2
    # Own connections, cpus, command mix and rate per workload; same keyspace and timing.
    assert "connections = 360" in main and 'cpu_list = "8-11"' in main and "get = 100\nset = 0" in main
    assert "connections = 40" in w2 and 'cpu_list = "12"' in w2 and "get = 0\nset = 100" in w2
    assert "rate_limit = 5000" in w2 and "rate_limit" not in main
    for text in (main, w2):
        assert 'duration = "10s"' in text and 'warmup = "2s"' in text and "count = 3000000" in text

    data = row.data
    assert row.method == "cachecannon-get-w2s100"
    assert data["per_run_rps"] == [100_000.0]  # merged total: 90k GET/s + 10k SET/s
    assert data["workloads"]["main"]["get_count"] == 900_000
    assert data["workloads"]["workload2"]["set_count"] == 100_000
    assert data["achieved_set_pct"] == pytest.approx(10.0)
    assert data["workload2"]["connections"] == 40
    assert data["workload2"]["toml_config"] == w2
    assert data["per_rep_results"][0]["achieved_set_pct"] == pytest.approx(10.0)
