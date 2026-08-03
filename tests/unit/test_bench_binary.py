"""Unit tests for the --bench-binary expert override (generator A/Bs)."""

from unittest.mock import MagicMock

from conductress.config import PROJECT_ROOT, VALKEY_BENCHMARK
from conductress.tasks.task_perf_benchmark import PerfTaskRunner


def _make_task(**overrides):
    task = MagicMock(spec=PerfTaskRunner)
    task.valsize = 16
    task.pipelining = 10
    task.bench_clients = 1200
    task.bench_threads = 16
    task.test_command = "-t get"
    task.benchmark_cpu_override = ""
    task.client_netns = ""
    task.bench_binary = overrides.get("bench_binary", "")
    task.test = MagicMock()
    task.test.keyspace = None
    task._is_local_benchmark = lambda ip: ip in {"127.0.0.1", "localhost", "::1"}
    task._build_benchmark_command = PerfTaskRunner._build_benchmark_command.__get__(task)
    return task


def _make_client():
    client = MagicMock()
    client.ip = "127.0.0.1"
    client._cpu_allocator.get_net_interface_numa.return_value = 0
    client._cpu_allocator.get_allocation.return_value = [1, 2, 3]
    return client


def test_default_uses_repo_binary():
    task = _make_task()
    cmd = task._build_benchmark_command(_make_client(), "127.0.0.1", None)
    assert str(PROJECT_ROOT / VALKEY_BENCHMARK) in cmd


def test_override_replaces_binary():
    task = _make_task(bench_binary="/tmp/bench-patched")
    cmd = task._build_benchmark_command(_make_client(), "127.0.0.1", None)
    assert "/tmp/bench-patched" in cmd
    assert str(PROJECT_ROOT / VALKEY_BENCHMARK) not in cmd


def test_override_applies_to_cpu_override_branch():
    task = _make_task(bench_binary="/tmp/bench-patched")
    task.benchmark_cpu_override = "16,17"
    cmd = task._build_benchmark_command(_make_client(), "127.0.0.1", None)
    assert "/tmp/bench-patched" in cmd
    assert "--physcpubind=16,17" in cmd
