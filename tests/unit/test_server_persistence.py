"""Persistence-file cleanup before a server start, and the LOADING readiness message."""

import logging
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from conductress import config
from conductress.server import Server


def _server(instance_dir=None):
    server = Server.__new__(Server)
    server.port = 9000
    server.ip = "192.168.1.1"
    server.instance_dir = instance_dir
    server.valkey_pid = -1
    server.server_cpus = []
    server.logger = logging.getLogger("test-server")
    return server


class TestPersistencePaths:
    def test_legacy_layout_lists_home_and_project_root(self):
        paths = _server().persistence_paths()
        assert paths == [
            (Path("~/dump.rdb"), Path("~/appendonlydir")),
            (config.PROJECT_ROOT / "dump.rdb", config.PROJECT_ROOT / "appendonlydir"),
        ]

    def test_instance_dir_is_the_only_working_dir(self):
        paths = _server(Path("~/conductress-instances/9000")).persistence_paths()
        assert paths == [
            (Path("~/conductress-instances/9000/dump.rdb"), Path("~/conductress-instances/9000/appendonlydir"))
        ]

    def test_server_args_override_dir_and_file_names(self):
        server = _server(Path("~/conductress-instances/9000"))
        paths = server.persistence_paths(
            "--appendonly yes --dir /data/p21 --dbfilename cell.rdb --appenddirname aof-cell"
        )
        assert paths == [(Path("/data/p21/cell.rdb"), Path("/data/p21/aof-cell"))]

    def test_appenddirname_alone_keeps_the_instance_dir(self):
        server = _server(Path("~/conductress-instances/9000"))
        assert server.persistence_paths("--appendonly yes --appenddirname aof-task-7") == [
            (Path("~/conductress-instances/9000/dump.rdb"), Path("~/conductress-instances/9000/aof-task-7"))
        ]

    def test_quoted_values_and_last_one_wins(self):
        paths = _server().persistence_paths("--dir '/data/with space' --dir /data/final --io-threads 4")
        assert paths == [(Path("/data/final/dump.rdb"), Path("/data/final/appendonlydir"))]

    def test_trailing_flag_without_value_is_ignored(self):
        assert Server._server_arg_value("--appendonly yes --dir", "dir") is None
        assert Server._server_arg_value("", "dir") is None


@pytest.mark.asyncio
class TestRemovePersistenceFiles:
    async def test_removes_exactly_the_servers_own_paths(self):
        server = _server(Path("~/conductress-instances/9000"))
        commands = []

        async def record(cmd, check=True):
            commands.append((cmd, check))
            return ("", "")

        server.run_host_command = record
        await server.remove_persistence_files("--appendonly yes --appenddirname aof-task-7")

        assert commands == [
            ("rm -f ~/conductress-instances/9000/dump.rdb; rm -rf ~/conductress-instances/9000/aof-task-7", False)
        ]
        # Never a wildcard, never the bare working directory or the home directory.
        assert all("*" not in cmd for cmd, _ in commands)
        assert all(not cmd.endswith("/9000") and " ~ " not in cmd and not cmd.endswith(" ~") for cmd, _ in commands)

    async def test_legacy_layout_cleans_both_candidate_dirs(self):
        server = _server()
        commands = []

        async def record(cmd, check=True):
            commands.append(cmd)
            return ("", "")

        server.run_host_command = record
        await server.remove_persistence_files()
        assert commands == [
            "rm -f ~/dump.rdb; rm -rf ~/appendonlydir",
            f"rm -f {config.PROJECT_ROOT}/dump.rdb; rm -rf {config.PROJECT_ROOT}/appendonlydir",
        ]

    async def test_start_cleans_after_pre_start_and_before_launch(self):
        server = Server("192.168.1.1", 9000, instance_dir=Path("~/conductress-instances/9000"))
        events = []

        async def pre_start():
            events.append("pre_start")

        server._Server__pre_start = pre_start
        server._allocate_server_cpus = AsyncMock(return_value=list(range(6)))
        server.wait_until_ready = AsyncMock()
        server._pin_valkey_threads = AsyncMock()

        async def mock_run(cmd, check=True):
            if cmd.startswith("rm -f"):
                events.append(cmd)
            elif "valkey-server" in cmd:
                events.append("launch")
            if cmd.startswith("lsof"):
                return ("12345", "")
            return ("", "")

        server.run_host_command = mock_run
        await server.start(Path("/opt/cache/valkey-server"), 2, server_args="--appendonly yes --appenddirname aof-x")

        assert events == [
            "pre_start",
            "rm -f ~/conductress-instances/9000/dump.rdb; rm -rf ~/conductress-instances/9000/aof-x",
            "launch",
        ]


@pytest.mark.asyncio
class TestWaitUntilReady:
    @pytest.fixture(autouse=True)
    def fast_probe(self, monkeypatch):
        monkeypatch.setattr(config, "SERVER_READY_MAX_RETRIES", 3)
        monkeypatch.setattr(config, "SERVER_READY_RETRY_DELAY", 0.5)
        monkeypatch.setattr("conductress.server.asyncio.sleep", AsyncMock())

    async def test_loading_reply_is_named_in_the_failure(self):
        server = _server()
        server.run_valkey_command = AsyncMock(return_value="LOADING Valkey is loading the dataset in memory")
        with pytest.raises(RuntimeError, match=r"port 9000 still LOADING after 1\.5 s .*RDB or AOF"):
            await server.wait_until_ready()
        assert server.run_valkey_command.await_count == 3

    async def test_no_reply_is_a_start_failure(self):
        server = _server()
        server.run_valkey_command = AsyncMock(return_value=None)
        with pytest.raises(RuntimeError, match=r"did not start successfully on port 9000 after 1\.5 s"):
            await server.wait_until_ready()

    async def test_loading_then_pong_returns(self):
        server = _server()
        server.run_valkey_command = AsyncMock(side_effect=["LOADING Valkey is loading the dataset in memory", "PONG"])
        await server.wait_until_ready()
        assert server.run_valkey_command.await_count == 2
