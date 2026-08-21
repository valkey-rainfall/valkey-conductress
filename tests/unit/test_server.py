"""Unit tests for Server class pure logic (no SSH/network required)."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from conductress.binary_manager import BinaryManager
from conductress.server import Server


class TestGetCachedBuildPath:
    """Tests for BinaryManager.get_cached_build_path() — build cache path construction."""

    def _make_manager(self, source="valkey", hash_val="abc123", make_args=""):
        host = MagicMock()
        host.ip = "127.0.0.1"
        mgr = BinaryManager(host)
        mgr.source = source
        mgr.hash = hash_val
        mgr.make_args = make_args
        return mgr

    def test_basic_path_construction(self):
        mgr = self._make_manager(source="valkey", hash_val="abc123def456", make_args="OPTIMIZATION=-O2")
        path = mgr.get_cached_build_path()

        assert path.parts[-3] == "valkey"
        assert path.parts[-2] == "abc123def456"
        assert len(path.parts[-1]) == 16

    def test_different_make_args_different_path(self):
        mgr = self._make_manager(hash_val="abc123")

        mgr.make_args = "OPTIMIZATION=-O2"
        path1 = mgr.get_cached_build_path()

        mgr.make_args = ""
        path2 = mgr.get_cached_build_path()

        assert path1 != path2

    def test_empty_make_args_valid(self):
        mgr = self._make_manager(hash_val="abc123", make_args="")
        path = mgr.get_cached_build_path()
        assert len(path.parts[-1]) == 16

    def test_raises_when_source_none(self):
        mgr = self._make_manager()
        mgr.source = None

        with pytest.raises(RuntimeError, match="source and hash must be set"):
            mgr.get_cached_build_path()

    def test_raises_when_hash_none(self):
        mgr = self._make_manager()
        mgr.hash = None

        with pytest.raises(RuntimeError, match="source and hash must be set"):
            mgr.get_cached_build_path()


class TestGetSourceBinaryPath:
    """Tests for BinaryManager.get_source_binary_path()."""

    def test_basic_path(self):
        host = MagicMock()
        host.ip = "127.0.0.1"
        mgr = BinaryManager(host)
        mgr.source = "valkey"

        path = mgr.get_source_binary_path()
        assert str(path).endswith("valkey/src")

    def test_raises_when_source_none(self):
        host = MagicMock()
        host.ip = "127.0.0.1"
        mgr = BinaryManager(host)
        mgr.source = None

        with pytest.raises(RuntimeError, match="source must be set"):
            mgr.get_source_binary_path()


class TestGetBuildHash:
    """Tests for Server.get_build_hash()."""

    def test_returns_hash_when_set(self):
        server = Server("127.0.0.1")
        server.hash = "deadbeef1234"
        assert server.get_build_hash() == "deadbeef1234"

    def test_returns_none_when_not_set(self):
        server = Server("127.0.0.1")
        assert server.get_build_hash() is None


class TestReleaseServerCpus:
    """Tests for Server._release_server_cpus()."""

    def test_release_clears_state(self):
        from conductress.cpu_allocator import AllocationTag, CpuAllocator

        allocator = CpuAllocator()
        allocator.register_host("127.0.0.1", all_cpus=list(range(16)))
        Server._cpu_allocator = allocator

        server = Server("127.0.0.1")
        tag = AllocationTag(task_id="server_127.0.0.1_6379", purpose="server")
        server._allocation_tag = tag
        server.server_cpus = allocator.allocate("127.0.0.1", tag, count=4)

        assert allocator.get_available_count("127.0.0.1") == 12

        server._release_server_cpus()

        assert server.server_cpus == []
        assert server._allocation_tag is None
        assert allocator.get_available_count("127.0.0.1") == 16

    def test_release_noop_when_no_cpus(self):
        """Release is safe to call when no CPUs are allocated."""
        server = Server("127.0.0.1")
        server.server_cpus = []
        server._allocation_tag = None
        # Should not raise
        server._release_server_cpus()


class TestServerInit:
    """Tests for Server initialization defaults."""

    def test_default_port(self):
        server = Server("10.0.0.1")
        assert server.port == 6379

    def test_custom_port(self):
        server = Server("10.0.0.1", port=7777)
        assert server.port == 7777

    def test_initial_state(self):
        server = Server("10.0.0.1")
        assert server.valkey_pid == -1
        assert server.server_cpus == []
        assert server.source is None
        assert server.hash is None
        assert server.ssh is None


class TestStartCommandLoaderPath:
    """Regression test for the libvalkeylua.so boot-crash fix (launch side).

    Module-era valkey binaries dlopen libvalkeylua.so; the module is cached
    next to the binary, and the launch command must point the dynamic loader
    at the cache dir so the per-commit copy resolves.
    """

    @pytest.mark.asyncio
    async def test_ld_library_path_points_at_cache_dir(self, monkeypatch):
        from unittest.mock import AsyncMock

        from conductress import config

        server = Server("127.0.0.1", port=7777)
        commands: list[str] = []

        async def fake_run(cmd, check=True):
            commands.append(cmd)
            return ("123\n", "")

        monkeypatch.setattr(server, "run_host_command", fake_run)
        monkeypatch.setattr(server, "_Server__pre_start", AsyncMock())
        monkeypatch.setattr(server, "_allocate_server_cpus", AsyncMock(return_value=[0, 1, 2, 3]))
        monkeypatch.setattr(server, "wait_until_ready", AsyncMock())
        monkeypatch.setattr(config, "check_feature", lambda _feature: False)

        cached_binary = Path("~/build_cache/valkey/abc123/deadbeef/valkey-server")
        await server.start(cached_binary, io_threads=1)

        launch_cmds = [c for c in commands if "valkey-server" in c and "--port" in c]
        assert launch_cmds, f"no launch command captured: {commands}"
        assert f"LD_LIBRARY_PATH={cached_binary.parent}" in launch_cmds[0], (
            "launch command must set LD_LIBRARY_PATH to the cache dir so the "
            "cached libvalkeylua.so resolves for module-era binaries"
        )
