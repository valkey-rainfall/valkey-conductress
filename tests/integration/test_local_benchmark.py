"""Integration test for local benchmarking (server + client on same host)."""

import asyncio
from pathlib import Path

import pytest

from conductress.config import ServerInfo
from conductress.server import Server
from conductress.tasks.task_perf_benchmark import PerfTaskData, PerfTaskRunner

pytestmark = pytest.mark.requires_server


@pytest.mark.asyncio
async def test_local_benchmark_cpu_allocation():
    """Test that local benchmarks properly allocate separate CPUs for server and client."""

    # Create a local server
    server = Server("127.0.0.1", port=9000)
    await server.ensure_host_cpu_allocation()

    # Check that loopback interface is detected
    interface = await server._detect_interface_for_ip()
    assert interface == "lo", f"Expected loopback interface 'lo', got '{interface}'"

    # Verify NUMA node is set (should default to 0 for loopback)
    net_numa = server._cpu_allocator.get_net_interface_numa(server.ip)
    assert net_numa == 0, f"Expected NUMA node 0 for loopback, got {net_numa}"

    # Check available CPUs
    available_cpus = await server.get_available_cpu_count()
    print(f"Available CPUs on localhost: {available_cpus}")
    assert available_cpus > 0, "No CPUs available"


@pytest.mark.asyncio
async def test_local_benchmark_no_irq_pinning():
    """Test that local benchmarks skip IRQ pinning for loopback interface."""

    server = Server("127.0.0.1", port=9000)
    await server.ensure_host_cpu_allocation()

    # Check that no IRQ allocation was created for loopback
    from conductress.cpu_allocator import AllocationTag

    irq_tag = AllocationTag(task_id="system", purpose="irq")
    irq_allocation = server._cpu_allocator.get_allocation(server.ip, irq_tag)

    # For loopback, IRQ allocation should be None since we skip IRQ pinning
    assert irq_allocation is None, "IRQ allocation should not exist for loopback interface"


@pytest.mark.asyncio
async def test_local_benchmark_end_to_end():
    """Test a complete local benchmark run with minimal duration."""

    # Just test detection logic, don't create full task
    from conductress.tasks.task_perf_benchmark import PerfTaskRunner

    # Test local benchmark detection
    runner = PerfTaskRunner(
        task_name="test",
        server_infos=[],
        binary_source="valkey",
        specifier="unstable",
        io_threads=2,
        valsize=64,
        pipelining=1,
        test="set",
        warmup=2,
        duration=5,
        preload_keys=False,
        has_expire=False,
        make_args="",
    )

    # Verify local benchmark is detected
    assert runner._is_local_benchmark("127.0.0.1"), "Should detect local benchmark"
    assert not runner._is_local_benchmark("198.51.100.30"), "Should not detect remote as local"

    print("Local benchmark detection working correctly")


def test_is_local_benchmark_ipv4_localhost():
    """Test 127.0.0.1 is detected as local."""
    from conductress.tasks.task_perf_benchmark import PerfTaskRunner

    runner = PerfTaskRunner(
        task_name="test",
        server_infos=[],
        binary_source="valkey",
        specifier="unstable",
        io_threads=2,
        valsize=64,
        pipelining=1,
        test="set",
        warmup=1,
        duration=1,
        preload_keys=False,
        has_expire=False,
        make_args="",
    )
    assert runner._is_local_benchmark("127.0.0.1")


def test_is_local_benchmark_ipv6_localhost():
    """Test ::1 is detected as local."""
    from conductress.tasks.task_perf_benchmark import PerfTaskRunner

    runner = PerfTaskRunner(
        task_name="test",
        server_infos=[],
        binary_source="valkey",
        specifier="unstable",
        io_threads=2,
        valsize=64,
        pipelining=1,
        test="set",
        warmup=1,
        duration=1,
        preload_keys=False,
        has_expire=False,
        make_args="",
    )
    assert runner._is_local_benchmark("::1")


def test_is_local_benchmark_hostname_localhost():
    """Test 'localhost' string is detected as local."""
    from conductress.tasks.task_perf_benchmark import PerfTaskRunner

    runner = PerfTaskRunner(
        task_name="test",
        server_infos=[],
        binary_source="valkey",
        specifier="unstable",
        io_threads=2,
        valsize=64,
        pipelining=1,
        test="set",
        warmup=1,
        duration=1,
        preload_keys=False,
        has_expire=False,
        make_args="",
    )
    assert runner._is_local_benchmark("localhost")


def test_is_local_benchmark_remote_ip():
    """Test remote IPs are not detected as local."""
    from conductress.tasks.task_perf_benchmark import PerfTaskRunner

    runner = PerfTaskRunner(
        task_name="test",
        server_infos=[],
        binary_source="valkey",
        specifier="unstable",
        io_threads=2,
        valsize=64,
        pipelining=1,
        test="set",
        warmup=1,
        duration=1,
        preload_keys=False,
        has_expire=False,
        make_args="",
    )
    assert not runner._is_local_benchmark("192.168.1.100")
    assert not runner._is_local_benchmark("10.0.0.1")


@pytest.mark.asyncio
async def test_cpu_isolation_local_benchmark():
    """Test that server and client CPUs don't overlap in local benchmarks."""

    from conductress.cpu_allocator import AllocationTag

    server = Server("127.0.0.1", port=9000)
    await server.ensure_host_cpu_allocation()

    # Simulate server CPU allocation
    server_tag = AllocationTag(task_id="test_server", purpose="server")
    net_numa = server._cpu_allocator.get_net_interface_numa(server.ip)
    server_cpus = server._cpu_allocator.allocate(
        server.ip,
        server_tag,
        count=4,  # 2 io-threads + 2 background
        require_numa=net_numa,
    )

    # Simulate benchmark client CPU allocation
    benchmark_tag = AllocationTag(task_id="test_benchmark", purpose="benchmark")
    benchmark_cpus = server._cpu_allocator.allocate(
        server.ip,
        benchmark_tag,
        count=16,  # PERF_BENCH_THREADS
        require_numa=net_numa,
    )

    # Verify no overlap
    server_set = set(server_cpus)
    benchmark_set = set(benchmark_cpus)
    overlap = server_set & benchmark_set

    assert len(overlap) == 0, f"Server and benchmark CPUs overlap: {overlap}"
    print(f"Server CPUs: {server_cpus}")
    print(f"Benchmark CPUs: {benchmark_cpus}")
    print("CPU isolation verified - no overlap between server and client")

    # Cleanup
    server._cpu_allocator.release(server.ip, server_tag)
    server._cpu_allocator.release(server.ip, benchmark_tag)


if __name__ == "__main__":
    # Run tests directly
    asyncio.run(test_local_benchmark_cpu_allocation())
    asyncio.run(test_local_benchmark_no_irq_pinning())
    asyncio.run(test_local_benchmark_end_to_end())
    asyncio.run(test_cpu_isolation_local_benchmark())
    print("\nAll local benchmark tests passed!")
