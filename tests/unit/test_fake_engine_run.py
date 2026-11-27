"""A whole memory task against a fake engine process: generic launch, fill, neutral settle, stop.

The engine is tests/fixtures/fake_engine.py, a tiny script that speaks the Redis commands the task uses. The
"host" is this machine: host commands run in a local shell, so the launch command string is really executed.
"""

import asyncio
import os
import shlex
import socket
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import valkey

from conductress import config
from conductress.engines import ENGINES, EngineProfile
from conductress.server import Server
from conductress.tasks import task_mem_efficiency as tme

FAKE_ENGINE = Path(__file__).resolve().parent.parent / "fixtures" / "fake_engine.py"

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="uses /proc, taskset and a local shell")


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _cli(port, command):
    """What valkey-cli would print for ``command``: raw text, or the error text."""
    client = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=5)
    try:
        reply = client.execute_command(*shlex.split(command))
    except valkey.ResponseError as exc:
        return str(exc) if str(exc).startswith("ERR") else f"ERR {exc}"
    except valkey.ConnectionError:  # not listening yet: valkey-cli prints nothing useful, the caller retries
        return ""
    finally:
        client.close()
    if command.split()[0].upper() == "PING":
        return "PONG" if reply else ""
    if isinstance(reply, dict):  # valkey-py parses INFO into a dict; valkey-cli prints name:value lines
        return "\n".join(
            f"{name}:" + (",".join(f"{k}={v}" for k, v in value.items()) if isinstance(value, dict) else str(value))
            for name, value in reply.items()
        )
    if isinstance(reply, bytes):
        return reply.decode().replace("\r\n", "\n").strip()
    if isinstance(reply, list):
        return "\n".join(str(item) for item in reply)
    return str(reply)


def _engine_pid(port):
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                argv = (entry / "cmdline").read_bytes().split(b"\x00")
            except OSError:
                continue
            if b"fake_engine.py" in b" ".join(argv) and f"--port\x00{port}".encode() in b"\x00".join(argv):
                return int(entry.name)
    return None


async def _local_host(port, command, check=True):
    """Run a host command in a local shell, answering the few that need the real host's tools."""
    if command.startswith("lsof"):
        return f"{_engine_pid(port) or ''}\n", ""
    if "SHUTDOWN NOSAVE" in command:  # the cleanup would call valkey-cli; do the SHUTDOWN here
        try:
            valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=2).execute_command("SHUTDOWN", "NOSAVE")
        except Exception:
            pass
        return "", ""
    process = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, stdin=asyncio.subprocess.DEVNULL
    )
    out, err = await process.communicate()
    return out.decode(), err.decode()


@pytest.mark.asyncio
async def test_a_memory_task_runs_against_a_fake_engine(monkeypatch):
    port = _free_port()
    monkeypatch.setitem(
        ENGINES,
        "fakeengine",
        EngineProfile(name="fakeengine", rehash_check=False, process_names=None, thread_flag="--threads"),
    )

    async def fast_sleep(_):
        return None

    monkeypatch.setitem(tme.quiesce_until_stable.__kwdefaults__, "sleep_fn", fast_sleep)
    monkeypatch.setitem(tme.check_memory_stability.__kwdefaults__, "sleep_fn", fast_sleep)
    monkeypatch.setattr(config, "SERVER_READY_RETRY_DELAY", 0.1)
    cpu = min(os.sched_getaffinity(0))

    async def with_path(ip, server_port, binary, io_threads, env_prefix="", server_args="", engine="valkey"):
        server = Server(ip, server_port, engine=engine)
        server._host.run_host_command = lambda command, check=True: _local_host(server_port, command, check)
        await server.start(binary, io_threads, env_prefix=env_prefix, server_args=server_args)
        return server

    async def run_cli(self, command):
        return await asyncio.to_thread(_cli, self.port, command)

    runner = tme.MemTaskRunner(
        task_name="t",
        server_ip="127.0.0.1",
        source=config.MANUALLY_UPLOADED,
        specifier=str(FAKE_ENGINE),
        test="zadd",
        val_sizes=[16],
        has_expire=False,
        make_args="",
        note="",
        user_data_bytes=24,
        settle=True,
        item_count=2000,
        engine="fakeengine",
    )
    runner.cached_binary_path = FAKE_ENGINE
    try:
        with (
            patch.object(tme.Server, "with_path", AsyncMock(side_effect=with_path)),
            patch.object(Server, "run_valkey_command", run_cli),
            patch.object(Server, "_allocate_server_cpus", AsyncMock(return_value=[cpu])),
            patch.object(Server, "_Server__pre_start", AsyncMock()),
            patch.object(Server, "remove_persistence_files", AsyncMock()),
            patch.object(tme, "conductress_revision", lambda: "rev0"),
        ):
            result = await runner.test_single_size_overhead(16, port, asyncio.Semaphore(1))
    finally:
        pid = _engine_pid(port)
        if pid:
            os.kill(pid, 9)

    assert result["engine"] == "fakeengine" and result["rehash_check"] is False
    assert result["item_count"] == 2000
    assert result["per_key_size"] == 40.0  # the fake engine's memory model: 40 bytes per item
    assert result["settled"] is True and result["rehash_passes"] == 1
    assert result["settle_key_check"] == "not_applicable"
    assert result["settle_stability"]["verdict"] == "stable"
    record = result["memory_record"]
    assert record["settle"]["rehash_check"] is False
    assert record["engine"]["server_name"] == "dragonfly" and record["engine"]["version"] == "df-fake"
    assert record["total_bytes_per_item"] == 40.0
    assert record["rss_bytes_per_item"] is not None
    assert _engine_pid(port) is None  # stop() ended it
