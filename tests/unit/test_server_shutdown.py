"""Tests for Server.stop() graceful shutdown and env_prefix support."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conductress.server import Server


@pytest.mark.asyncio
class TestServerStop:
    async def test_stop_uses_shutdown_nosave(self):
        """Verify stop() sends SHUTDOWN NOSAVE (required for jemalloc prof_final)."""
        server = Server.__new__(Server)
        server.port = 9000
        server.ip = "127.0.0.1"
        server.valkey_pid = 12345
        server.server_cpus = None
        server._profiling = MagicMock()
        server._profiling.cleanup = AsyncMock()

        commands_run = []

        async def mock_run(cmd, check=True):
            commands_run.append(cmd)
            if "ps -p" in cmd:
                return ("12345 ? valkey-server\n", "")
            return ("", "")

        server.run_host_command = mock_run

        await server.stop()

        assert server.valkey_pid == -1
        # Verify SHUTDOWN NOSAVE is in the command
        shutdown_cmd = [c for c in commands_run if "SHUTDOWN" in c]
        assert len(shutdown_cmd) == 1
        assert "SHUTDOWN NOSAVE" in shutdown_cmd[0]
        assert "kill -9" in shutdown_cmd[0]  # fallback present

    async def test_stop_clears_pid(self):
        """After stop(), valkey_pid is reset to -1."""
        server = Server.__new__(Server)
        server.port = 9000
        server.ip = "127.0.0.1"
        server.valkey_pid = 99999
        server.server_cpus = None
        server._profiling = MagicMock()
        server._profiling.cleanup = AsyncMock()

        async def mock_run(cmd, check=True):
            if "ps -p" in cmd:
                return ("99999 ? valkey-server\n", "")
            return ("", "")

        server.run_host_command = mock_run
        await server.stop()
        assert server.valkey_pid == -1

    async def test_stop_skips_when_no_pid(self):
        """stop() is a no-op when valkey_pid is -1."""
        server = Server.__new__(Server)
        server.port = 9000
        server.valkey_pid = -1
        server.server_cpus = None
        server.run_host_command = AsyncMock()

        await server.stop()
        # Should not call any commands
        server.run_host_command.assert_not_called()


async def _stop_command(ip: str) -> str:
    server = Server.__new__(Server)
    server.port = 9000
    server.ip = ip
    server.valkey_pid = 12345
    server.server_cpus = None
    server.instance_dir = None
    server._profiling = MagicMock()
    server._profiling.cleanup = AsyncMock()
    commands = []

    async def mock_run(cmd, check=True):
        commands.append(cmd)
        return ("12345 ? valkey-server\n", "") if "ps -p" in cmd else ("", "")

    server.run_host_command = mock_run
    await server.stop()
    return next(c for c in commands if "SHUTDOWN" in c)


@pytest.mark.asyncio
class TestStopValkeyCliPath:
    async def test_local_host_uses_this_checkouts_valkey_cli(self):
        from conductress import config

        for ip in ("127.0.0.1", "localhost"):
            command = await _stop_command(ip)
            assert command.startswith(f"{config.PROJECT_ROOT / config.VALKEY_CLI} -p 9000 SHUTDOWN NOSAVE")

    async def test_remote_host_uses_the_home_relative_path(self):
        command = await _stop_command("198.51.100.7")
        assert command.startswith("~/conductress/valkey-cli -p 9000 SHUTDOWN NOSAVE")
        assert "/home/" not in command.split(" -p ")[0]


@pytest.mark.asyncio
class TestServerEnvPrefix:
    async def test_start_includes_env_prefix_in_command(self):
        """Verify env_prefix is prepended to the server start command."""
        server = Server.__new__(Server)
        server.port = 9000
        server.ip = "127.0.0.1"
        server.threads = 1
        server.server_cpus = None
        server.valkey_pid = -1
        server.args = []

        commands_run = []

        async def capture_run(cmd, check=True):
            commands_run.append(cmd)
            if "lsof" in cmd:
                return ("12345", "")
            return ("", "")

        server.run_host_command = capture_run
        server._Server__pre_start = AsyncMock()
        server._allocate_server_cpus = AsyncMock(return_value=[0, 1])
        server.wait_until_ready = AsyncMock()
        server._pin_valkey_threads = AsyncMock()

        import conductress.config as config

        with patch.object(
            config,
            "FEATURE_STATES",
            {
                config.Features.PIN_VALKEY_THREADS: False,
                config.Features.BIND_NUMA_MEMORY: False,
                config.Features.ENABLE_CPU_CONSISTENCY_MODE: False,
            },
        ):
            from pathlib import Path

            await server.start(Path("/usr/bin/valkey-server"), io_threads=1, env_prefix='JE_MALLOC_CONF="prof:true"')

        # The start command should begin with the env prefix
        start_cmd = next(c for c in commands_run if "/usr/bin/valkey-server" in c)
        assert start_cmd.startswith('JE_MALLOC_CONF="prof:true"')
        assert "/usr/bin/valkey-server" in start_cmd

    async def test_start_without_env_prefix(self):
        """Without env_prefix, command starts with the loader path then the binary.

        LD_LIBRARY_PATH always points at the cached build dir so module-era
        valkey binaries can dlopen their cached libvalkeylua.so.
        """
        server = Server.__new__(Server)
        server.port = 9000
        server.ip = "127.0.0.1"
        server.threads = 1
        server.server_cpus = None
        server.valkey_pid = -1
        server.args = []

        commands_run = []

        async def capture_run(cmd, check=True):
            commands_run.append(cmd)
            if "lsof" in cmd:
                return ("12345", "")
            return ("", "")

        server.run_host_command = capture_run
        server._Server__pre_start = AsyncMock()
        server._allocate_server_cpus = AsyncMock(return_value=[0, 1])
        server.wait_until_ready = AsyncMock()
        server._pin_valkey_threads = AsyncMock()

        import conductress.config as config

        with patch.object(
            config,
            "FEATURE_STATES",
            {
                config.Features.PIN_VALKEY_THREADS: False,
                config.Features.BIND_NUMA_MEMORY: False,
                config.Features.ENABLE_CPU_CONSISTENCY_MODE: False,
            },
        ):
            from pathlib import Path

            await server.start(Path("/usr/bin/valkey-server"), io_threads=1)

        start_cmd = next(c for c in commands_run if "/usr/bin/valkey-server" in c)
        assert start_cmd.startswith("LD_LIBRARY_PATH=/usr/bin /usr/bin/valkey-server")
