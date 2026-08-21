"""Tests for BinaryManager build caching and git operations."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import asyncssh
import pytest

from conductress.binary_manager import BinaryManager


@pytest.fixture
def mock_host():
    host = MagicMock()
    host.ip = "127.0.0.1"
    host.run_host_command = AsyncMock(return_value=("", ""))
    host.check_file_exists = AsyncMock(return_value=False)
    host.put_remote_file = AsyncMock()
    return host


@pytest.fixture
def manager(mock_host):
    mgr = BinaryManager(mock_host)
    mgr.source = "valkey"
    mgr.specifier = "unstable"
    mgr.make_args = "OPTIMIZATION=-O2"
    return mgr


class TestCacheHitSkipsBuild:
    """When binary is already cached, no build should be triggered."""

    @pytest.mark.asyncio
    async def test_cache_hit_returns_path_without_building(self, manager, mock_host):
        mock_host.check_file_exists = AsyncMock(return_value=True)
        mock_host.run_host_command = AsyncMock(
            side_effect=[
                ("", ""),  # git fetch
                ("refs/remotes/origin/unstable\n", ""),  # rev-parse
                ("", ""),  # git reset --hard
                ("abc123def456\n", ""),  # git rev-parse HEAD
            ]
        )

        result = await manager._ensure_build_cached()

        # Should NOT have called make
        commands = [call[0][0] for call in mock_host.run_host_command.call_args_list]
        assert not any("make" in cmd for cmd in commands)
        assert "abc123def456" in str(result)

    @pytest.mark.asyncio
    async def test_cache_miss_triggers_build(self, manager, mock_host):
        mock_host.check_file_exists = AsyncMock(return_value=False)
        mock_host.run_host_command = AsyncMock(
            side_effect=[
                ("", ""),  # git fetch
                ("refs/remotes/origin/unstable\n", ""),  # rev-parse
                ("", ""),  # git reset --hard
                ("abc123\n", ""),  # git rev-parse HEAD
                ("", ""),  # make distclean && make -j
                ("", ""),  # mkdir -p
                ("", ""),  # cp binary
            ]
        )

        await manager._ensure_build_cached()

        commands = [call[0][0] for call in mock_host.run_host_command.call_args_list]
        assert any("make" in cmd for cmd in commands)
        assert any("mkdir" in cmd for cmd in commands)
        assert any("cp" in cmd for cmd in commands)


class TestBuildFailureHandling:
    @pytest.mark.asyncio
    async def test_build_failure_propagates_exception(self, manager, mock_host):
        mock_host.check_file_exists = AsyncMock(return_value=False)

        error = asyncssh.ProcessError(None, "make", None, 1, None, 1, "", "compilation error")
        mock_host.run_host_command = AsyncMock(
            side_effect=[
                ("", ""),  # git fetch
                ("--\n", ""),  # rev-parse (commit hash)
                ("", ""),  # git reset --hard
                ("abc123\n", ""),  # git rev-parse HEAD
                error,  # make fails
            ]
        )

        with pytest.raises(asyncssh.ProcessError):
            await manager._ensure_build_cached()


class TestSpecifierNormalization:
    @pytest.mark.asyncio
    async def test_branch_name_prefixed_with_origin(self, manager, mock_host):
        mock_host.run_host_command = AsyncMock(
            side_effect=[
                ("", ""),  # git fetch
                ("refs/remotes/origin/unstable\n", ""),  # rev-parse
            ]
        )

        result = await manager._normalize_specifier("unstable")
        assert result == "origin/unstable"

    @pytest.mark.asyncio
    async def test_commit_hash_used_as_is(self, manager, mock_host):
        mock_host.run_host_command = AsyncMock(
            side_effect=[
                ("", ""),  # git fetch
                ("--\n", ""),  # rev-parse returns -- for raw hashes
            ]
        )

        result = await manager._normalize_specifier("abc123def")
        assert result == "abc123def"

    @pytest.mark.asyncio
    async def test_invalid_specifier_raises(self, manager, mock_host):
        mock_host.run_host_command = AsyncMock(
            side_effect=[
                ("", ""),  # git fetch
                ("\n", ""),  # empty result
            ]
        )

        with pytest.raises(ValueError, match="invalid specifier"):
            await manager._normalize_specifier("nonexistent")


class TestEnsureBinaryCached:
    @pytest.mark.asyncio
    async def test_unknown_source_raises(self, manager, mock_host):
        manager.source = "unknown_repo"

        with pytest.raises(ValueError, match="Unknown source"):
            await manager.ensure_binary_cached()

    @pytest.mark.asyncio
    async def test_updates_state_from_args(self, mock_host, monkeypatch):
        import conductress.config as config

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "rainsupreme"])

        mgr = BinaryManager(mock_host)

        mock_host.check_file_exists = AsyncMock(return_value=True)
        mock_host.run_host_command = AsyncMock(
            side_effect=[
                ("", ""),  # git fetch
                ("refs/remotes/origin/main\n", ""),  # rev-parse
                ("", ""),  # git reset
                ("deadbeef\n", ""),  # rev-parse HEAD
            ]
        )

        await mgr.ensure_binary_cached(source="valkey", specifier="main", make_args="")

        assert mgr.source == "valkey"
        assert mgr.specifier == "main"
        assert mgr.make_args == ""
        assert mgr.hash == "deadbeef"


class TestLuaModuleCaching:
    """Regression tests for the missing-libvalkeylua.so build-cache bug.

    Module-era valkey commits (valkey-io#2858..#3392) dlopen libvalkeylua.so
    at startup and SIGABRT if it is missing. The cache used to store only the
    server binary, so every cached module-era commit boot-crashed in the
    perf-sweep (280 coredumps, Jul-Aug 2026). Worse, the binary's DT_RPATH
    points into the shared source tree, so a leftover tree .so from a
    different commit would load silently.

    Pre-existing half-cached entries are cleaned up eagerly at deploy time
    (one-shot sweep); there is no lazy self-heal. Correctness going forward
    relies on write ordering: runtime artifacts are cached before the binary,
    whose presence is the cache-hit marker.
    """

    @pytest.mark.asyncio
    async def test_module_artifact_cached_and_removed_from_tree(self, manager, mock_host):
        """After a build that produced libvalkeylua.so, the module must be copied
        into the cache dir and deleted from the shared source tree."""

        async def check(path):
            s = str(path)
            if s.endswith("modules/lua/libvalkeylua.so"):
                return True  # build produced the module in the tree
            if "build_cache" in s:
                return False  # nothing cached yet -> build
            return True  # tree build output exists

        mock_host.check_file_exists = AsyncMock(side_effect=check)
        mock_host.run_host_command = AsyncMock(return_value=("abc123\n", ""))

        await manager._ensure_build_cached()

        commands = [call[0][0] for call in mock_host.run_host_command.call_args_list]
        module_cmds = [cmd for cmd in commands if "libvalkeylua.so" in cmd and "cp " in cmd]
        assert module_cmds, f"module artifact was not cached; commands: {commands}"
        assert any("rm -f" in cmd for cmd in module_cmds), (
            "tree copy of libvalkeylua.so must be removed after caching "
            "(DT_RPATH would silently load a wrong-commit module otherwise)"
        )

    @pytest.mark.asyncio
    async def test_module_cached_before_binary(self, manager, mock_host):
        """The binary must be the LAST artifact written to the cache entry.

        Its presence is the cache-hit marker: writing it last guarantees an
        interrupted build reads as a cache miss, never as a binary without
        its module (which would boot-crash on every future cache hit)."""

        async def check(path):
            s = str(path)
            if s.endswith("modules/lua/libvalkeylua.so"):
                return True
            if "build_cache" in s:
                return False
            return True

        mock_host.check_file_exists = AsyncMock(side_effect=check)
        mock_host.run_host_command = AsyncMock(return_value=("abc123\n", ""))

        await manager._ensure_build_cached()

        commands = [call[0][0] for call in mock_host.run_host_command.call_args_list]
        module_idx = next(i for i, c in enumerate(commands) if "libvalkeylua.so" in c and "cp " in c)
        binary_idx = next(i for i, c in enumerate(commands) if c.startswith("cp ") and "libvalkeylua.so" not in c)
        assert module_idx < binary_idx, (
            f"module must be cached before the binary (module at {module_idx}, " f"binary at {binary_idx}): {commands}"
        )


class TestMakeArgsAffectCacheKey:
    def test_different_make_args_produce_different_paths(self, mock_host):
        mgr1 = BinaryManager(mock_host)
        mgr1.source = "valkey"
        mgr1.hash = "abc123"
        mgr1.make_args = "OPTIMIZATION=-O2"

        mgr2 = BinaryManager(mock_host)
        mgr2.source = "valkey"
        mgr2.hash = "abc123"
        mgr2.make_args = ""

        assert mgr1.get_cached_build_path() != mgr2.get_cached_build_path()

    def test_empty_make_args_is_valid_cache_key(self, mock_host):
        mgr = BinaryManager(mock_host)
        mgr.source = "valkey"
        mgr.hash = "abc123"
        mgr.make_args = ""

        # Should not raise
        path = mgr.get_cached_build_path()
        assert "abc123" in str(path)
