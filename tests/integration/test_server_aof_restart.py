"""Loopback test: a rep-2 restart of an ``--appendonly yes`` server starts from an empty dataset.

Runs the real ``valkey-server`` binary on this machine through ``Server.start``
and ``Server.stop`` exactly as the perf task's rep loop does (stop the rep-1
instance, build a new ``Server`` handle, start it with the same server args).
Host commands run in a local shell instead of over SSH, and the CPU and sudo
preparation (``__pre_start``) is skipped, so no root, SSH or benchmark
client is needed.

Skipped unless a server binary is found: ``CONDUCTRESS_TEST_VALKEY_SERVER``
(and ``CONDUCTRESS_TEST_VALKEY_CLI``) or ``valkey-server`` / ``valkey-cli``
on PATH.
"""

import asyncio
import os
import shutil
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from conductress import config
from conductress.server import Server

SERVER_BIN = os.environ.get("CONDUCTRESS_TEST_VALKEY_SERVER") or shutil.which("valkey-server")
CLI_BIN = os.environ.get("CONDUCTRESS_TEST_VALKEY_CLI") or shutil.which("valkey-cli")
PORT = 7392
KEYS = 60_000
# Microseconds of artificial delay per loaded key (valkey ``key-load-delay``):
# 60k keys x 100 us = 6 s of LOADING, well past the 1.5 s probe budget below.
KEY_LOAD_DELAY_US = 100

pytestmark = pytest.mark.skipif(
    not SERVER_BIN or not CLI_BIN, reason="needs valkey-server and valkey-cli (CONDUCTRESS_TEST_VALKEY_SERVER/_CLI)"
)


async def _local_shell(cmd: str, check: bool = True, pin_cpus: str = "") -> tuple[str, str]:
    process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await process.communicate()
    if check and process.returncode != 0:
        raise RuntimeError(f"{cmd!r} failed ({process.returncode}): {err.decode()}")
    return out.decode(), err.decode()


def _local_server(instance_dir: Path) -> Server:
    """A ``Server`` for 127.0.0.1 whose host side is this machine's shell."""
    server = Server("127.0.0.1", PORT, instance_dir=instance_dir)
    server._Server__pre_start = AsyncMock()  # sudo/CPU preparation: not needed on loopback
    server._allocate_server_cpus = AsyncMock(return_value=list(range(4)))
    server._pin_valkey_threads = AsyncMock()
    server._profiling.cleanup = AsyncMock()
    server.run_host_command = _local_shell

    async def cli(command: str):
        out, _ = await _local_shell(f"{CLI_BIN} -p {PORT} {command}", check=False)
        return out.strip() if out else None

    server.run_valkey_command = cli
    return server


@pytest.fixture
def aof_rig(tmp_path, monkeypatch):
    """Short readiness budget and no files outside tmp_path (log file included)."""
    monkeypatch.setattr(Server, "path_root", tmp_path)
    monkeypatch.setattr(config, "SERVER_READY_MAX_RETRIES", 3)
    monkeypatch.setattr(config, "SERVER_READY_RETRY_DELAY", 0.5)
    instance_dir = tmp_path / "inst"
    args = "--appendonly yes --appenddirname aof-rep-test"
    started = []

    async def rep1():
        server = _local_server(instance_dir)
        await server.start(Path(SERVER_BIN), 1, server_args=args)
        started.append(server)
        gen = f"python3 -c 'import sys\nfor i in range({KEYS}): sys.stdout.write(\"SET k%d {'x' * 16}\\r\\n\" % i)'"
        await _local_shell(f"{gen} | {CLI_BIN} -p {PORT} --pipe > /dev/null")
        assert await server.run_valkey_command("DBSIZE") == str(KEYS)
        await asyncio.sleep(1.5)  # let appendfsync everysec flush the AOF
        await server.stop()  # as TopologyGroup.stop_all_servers does between reps
        return server

    yield instance_dir, args, rep1, started
    for server in started:
        try:
            asyncio.get_event_loop().run_until_complete(server.stop())
        except Exception:  # pylint: disable=broad-except
            pass
    asyncio.get_event_loop().run_until_complete(_local_shell(f"pkill -f 'valkey-server .*--port {PORT} '", check=False))


@pytest.mark.asyncio
async def test_rep2_restart_starts_empty_after_an_aof_rep1(aof_rig):
    instance_dir, args, rep1, started = aof_rig
    await rep1()
    aof_dir = instance_dir / "aof-rep-test"
    assert aof_dir.is_dir() and sum(p.stat().st_size for p in aof_dir.iterdir()) > 1_000_000

    # Rep 2: a new handle, the same server args; the restarted server must not load rep 1's AOF.
    rep2 = _local_server(instance_dir)
    started.append(rep2)
    await rep2.start(Path(SERVER_BIN), 1, server_args=f"{args} --key-load-delay {KEY_LOAD_DELAY_US}")
    assert await rep2.run_valkey_command("PING") == "PONG"
    assert await rep2.run_valkey_command("DBSIZE") == "0"
    # The AOF directory was recreated empty by the new server, not inherited.
    assert sum(p.stat().st_size for p in aof_dir.iterdir()) < 1_000
    await rep2.stop()


@pytest.mark.asyncio
async def test_without_the_cleanup_rep2_is_loading_and_the_reason_says_so(aof_rig, monkeypatch):
    instance_dir, args, rep1, started = aof_rig
    await rep1()

    monkeypatch.setattr(Server, "remove_persistence_files", AsyncMock())  # the pre-fix behaviour
    rep2 = _local_server(instance_dir)
    started.append(rep2)
    with pytest.raises(RuntimeError, match=r"still LOADING after 1\.5 s"):
        await rep2.start(Path(SERVER_BIN), 1, server_args=f"{args} --key-load-delay {KEY_LOAD_DELAY_US}")
