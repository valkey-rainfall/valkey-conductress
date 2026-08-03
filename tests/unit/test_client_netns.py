"""Tests for the --client-netns dual-ENI hairpin option (docs/real-nic-hairpin.md)."""

from unittest.mock import MagicMock, patch

from conductress.tasks.task_perf_benchmark import PerfTaskRunner


def _make_task(**overrides):
    task = MagicMock(spec=PerfTaskRunner)
    task.valsize = 16
    task.pipelining = 10
    task.bench_clients = 1200
    task.bench_threads = 16
    task.test_command = "-t get"
    task.benchmark_cpu_override = ""
    task.client_netns = overrides.get("client_netns", "")
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


def test_default_no_netns_prefix_and_localhost_target():
    task = _make_task()
    cmd = task._build_benchmark_command(_make_client(), "127.0.0.1", None)
    assert "ip netns exec" not in cmd
    assert "-h 127.0.0.1" in cmd


@patch("conductress.tasks.task_perf_benchmark.get_primary_interface_ip", return_value="172.31.34.114")
def test_netns_prefixes_command_and_rewrites_local_target(mock_ip):
    task = _make_task(client_netns="loadgen")
    cmd = task._build_benchmark_command(_make_client(), "127.0.0.1", None)
    assert cmd.startswith("sudo ip netns exec loadgen numactl")
    assert "-h 172.31.34.114" in cmd
    assert "-h 127.0.0.1" not in cmd
    mock_ip.assert_called_once()


@patch("conductress.tasks.task_perf_benchmark.get_primary_interface_ip", return_value="172.31.34.114")
def test_netns_keeps_explicit_remote_target(mock_ip):
    """A non-local target is used as-is — only localhost needs rewriting."""
    task = _make_task(client_netns="loadgen")
    cmd = task._build_benchmark_command(_make_client(), "10.0.0.42", None)
    assert cmd.startswith("sudo ip netns exec loadgen ")
    assert "-h 10.0.0.42" in cmd
    mock_ip.assert_not_called()


@patch("conductress.tasks.task_perf_benchmark.get_primary_interface_ip", return_value="172.31.34.114")
def test_netns_applies_to_cpu_override_branch(mock_ip):
    task = _make_task(client_netns="loadgen")
    task.benchmark_cpu_override = "4,5,6"
    client = _make_client()
    client._cpu_allocator.get_numa_nodes_for_cpus.return_value = [0]
    cmd = task._build_benchmark_command(client, "127.0.0.1", None)
    assert cmd.startswith("sudo ip netns exec loadgen numactl --physcpubind=4,5,6")
    assert "-h 172.31.34.114" in cmd


def test_netns_preflight_message_mentions_setup_paths():
    """The preflight failure must point at the fix (script + docs), since the
    namespace silently vanishes on reboot."""
    import inspect

    from conductress.tasks.task_perf_benchmark import PerfTaskRunner

    src = inspect.getsource(PerfTaskRunner)
    assert "does not exist on this host" in src
    assert "setup-loadgen-netns.sh" in src
    assert "real-nic-hairpin.md" in src
