"""The memory task result carries a memory record, and degrades when the engine reports little."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conductress.server import Server
from conductress.tasks.task_mem_efficiency import MemTaskRunner

PROFILING_OFF = "  config.prof: true\n  opt.prof: false\n"
PROFILING_ON = "  config.prof: true\n  opt.prof: true\n"


def proc_text(memory_calls):
    """Made-up /proc/<pid>/status: the server holds more resident memory after the fill."""
    kb = 100 if memory_calls <= 1 else 4000
    return f"VmRSS:\t {kb} kB\nRssAnon:\t {kb - 10} kB\n"


@pytest.fixture
def mock_server():
    server = MagicMock(spec=Server)
    server.ip = "127.0.0.1"
    server.port = 6379
    server.get_build_hash = MagicMock(return_value="abc123")
    server.count_items_expires = AsyncMock(return_value=(5000000, 0))
    server.stop = AsyncMock()
    server.valkey_pid = 4242
    return server


@pytest.fixture
def runner():
    return MemTaskRunner(
        task_name="test_task",
        server_ip="127.0.0.1",
        source="valkey",
        specifier="unstable",
        test="set",
        val_sizes=[64],
        has_expire=False,
        make_args="",
        note="test note",
        key_size=16,
        user_data_bytes=80,
    )


def reporting_server(server, *, malloc_stats, with_info_rss=True):
    """Make ``server`` answer the way an engine that reports everything does."""
    before = {"used_memory": "1000000"}
    after = {"used_memory": "6000000"}
    if with_info_rss:
        before["used_memory_rss"] = "2000000"
        after["used_memory_rss"] = "9000000"
    calls = {"memory": 0}

    async def info(section):
        if section == "server":
            return {"server_name": "valkey", "valkey_version": "9.1.0", "executable": "/h/valkey-server"}
        calls["memory"] += 1
        return before if calls["memory"] == 1 else after

    async def valkey_command(command):
        if command == "MEMORY MALLOC-STATS":
            return malloc_stats
        if command == "CONFIG GET hz":
            return "hz\n10"
        return None

    async def host_command(command, check=True, pin_cpus=""):
        if command.startswith("cat /proc/"):
            return proc_text(calls["memory"]), ""
        if command.startswith("sha256sum"):
            return "d" * 64 + "  /mock/binary\n", ""
        return "Example v=9.1.0\n", ""

    server.info = AsyncMock(side_effect=info)
    server.run_valkey_command = AsyncMock(side_effect=valkey_command)
    server.run_host_command = AsyncMock(side_effect=host_command)


@patch("conductress.tasks.task_mem_efficiency.populate")
@patch("conductress.tasks.task_mem_efficiency.Server")
@pytest.mark.asyncio
async def test_result_carries_the_memory_record(mock_server_class, mock_populate, runner, mock_server):
    mock_server_class.with_path = AsyncMock(return_value=mock_server)
    reporting_server(mock_server, malloc_stats=PROFILING_OFF)
    runner.cached_binary_path = Path("/mock/binary")

    result = await runner.test_single_size_overhead(64, 6379, asyncio.Semaphore(1))

    record = result["memory_record"]
    assert record["engine"]["server_name"] == "valkey"
    assert record["engine"]["version"] == "9.1.0"
    assert record["engine"]["executable"] == "valkey-server"
    assert record["binary"] == {"sha256": "d" * 64, "version_text": "Example v=9.1.0"}
    assert record["profiling"] == {"requested": False, "active": False, "profiled": False}
    assert record["item_count"] == 5_000_000
    assert record["config"] == {"hz": "10"}
    assert record["total_bytes_per_item"] == pytest.approx(result["per_key_size"])
    # RSS comes from /proc (3900 kB more than before), not from INFO used_memory_rss.
    assert record["rss_source"] == "proc_vmrss"
    assert record["rss_bytes_per_item"] == pytest.approx(3900 * 1024 / 5_000_000)
    assert set(record["samples"]) == {"before_fill", "after_fill", "after_settle"}


@patch("conductress.tasks.task_mem_efficiency.populate")
@patch("conductress.tasks.task_mem_efficiency.Server")
@pytest.mark.asyncio
async def test_rss_works_for_an_engine_without_used_memory_rss(mock_server_class, mock_populate, runner, mock_server):
    mock_server_class.with_path = AsyncMock(return_value=mock_server)
    reporting_server(mock_server, malloc_stats=None, with_info_rss=False)
    runner.cached_binary_path = Path("/mock/binary")

    record = (await runner.test_single_size_overhead(64, 6379, asyncio.Semaphore(1)))["memory_record"]

    assert record["profiling"]["active"] is None  # this engine cannot say, and none was requested
    assert record["profiling"]["profiled"] is False
    assert record["samples"]["after_settle"]["used_memory_rss"] is None
    assert record["rss_source"] == "proc_vmrss"
    assert record["rss_bytes_per_item"] == pytest.approx(3900 * 1024 / 5_000_000)


@patch("conductress.tasks.task_mem_efficiency.populate")
@patch("conductress.tasks.task_mem_efficiency.Server")
@pytest.mark.asyncio
async def test_profiled_run_has_total_but_no_rss(mock_server_class, mock_populate, runner, mock_server):
    mock_server_class.with_path = AsyncMock(return_value=mock_server)
    reporting_server(mock_server, malloc_stats=PROFILING_ON)
    runner.cached_binary_path = Path("/mock/binary")
    runner.enable_profiling = True

    with (
        patch("conductress.tasks.task_mem_efficiency.cleanup_heap_dumps", AsyncMock()),
        patch("conductress.tasks.task_mem_efficiency.collect_heap_profile", AsyncMock(return_value=None)),
    ):
        result = await runner.test_single_size_overhead(64, 6379, asyncio.Semaphore(1))

    record = result["memory_record"]
    assert record["profiling"]["profiled"] is True
    assert record["rss_bytes_per_item"] is None  # profiler metadata would inflate it
    assert record["rss_source"] is None
    assert record["total_bytes_per_item"] == pytest.approx(result["per_key_size"])  # used_memory stays valid


@patch("conductress.tasks.task_mem_efficiency.populate")
@patch("conductress.tasks.task_mem_efficiency.Server")
@pytest.mark.asyncio
async def test_engine_that_reports_nothing_still_completes(mock_server_class, mock_populate, runner, mock_server):
    """No INFO server, no malloc stats, no /proc, no sha256sum: every optional field is None."""
    mock_server_class.with_path = AsyncMock(return_value=mock_server)
    calls = {"memory": 0}

    async def info(section):
        if section != "memory":
            raise RuntimeError("no such section")
        calls["memory"] += 1
        return {"used_memory": "1000000"} if calls["memory"] == 1 else {"used_memory": "6000000"}

    mock_server.info = AsyncMock(side_effect=info)
    mock_server.run_valkey_command = AsyncMock(side_effect=RuntimeError("no such command"))
    mock_server.run_host_command = AsyncMock(side_effect=RuntimeError("ssh failed"))
    runner.cached_binary_path = Path("/mock/binary")

    result = await runner.test_single_size_overhead(64, 6379, asyncio.Semaphore(1))

    record = result["memory_record"]
    assert result["per_item_overhead"] == pytest.approx(1.0 - 80)  # 5 MB over 5M items, minus user data
    assert record["engine"]["server_name"] is None
    assert record["binary"] == {"sha256": None, "version_text": None}
    assert record["profiling"]["active"] is None
    assert record["rss_bytes_per_item"] is None
    assert record["total_bytes_per_item"] == pytest.approx(result["per_key_size"])
