"""The cache use marker: written on every hit, never fails a lookup, and read by build-cache prune."""

import asyncio
import hashlib
import logging
import os
import stat
from pathlib import Path

import pytest

from conductress import binary_manager as bm
from conductress import build_cache as bc
from conductress import config
from conductress.binary_manager import BinaryManager

from .test_build_cache import DAY, NOW, env, make_entry, run, sha  # noqa: F401  (env is a fixture)

MARKER = config.BUILD_CACHE_USE_MARKER


class LocalHost:
    """The host commands BinaryManager uses, run in a local shell with HOME set by the test."""

    ip = "127.0.0.1"

    def __init__(self):
        self.commands = []

    async def run_host_command(self, command, check=True):
        self.commands.append(command)
        process = await asyncio.create_subprocess_shell(
            command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, executable="/bin/bash"
        )
        out, err = await process.communicate()
        return out.decode(), err.decode()

    async def check_file_exists(self, path):
        out, _ = await self.run_host_command(f"[[ -f {path} ]] && echo 1 || echo 0;")
        return out.strip() == "1"


@pytest.fixture(autouse=True)
def _quiet_marker_warning():
    """The 'warn once' flag is per process; give every test a fresh one."""
    bm._marker_warned = False
    yield
    bm._marker_warned = False


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """A manager for source valkey at commit ``sha(1)``, with HOME pointing at a temporary directory."""
    monkeypatch.setenv("HOME", str(tmp_path))
    manager = BinaryManager(LocalHost())
    manager.source, manager.hash = "valkey", sha(1)
    entry = Path(os.path.expanduser(str(manager.get_cached_build_path())))
    return manager, entry


def _write_binary(entry: Path, age_days: float = 100):
    entry.mkdir(parents=True)
    binary = entry / "valkey-server"
    binary.write_bytes(b"#!/bin/sh\n")
    binary.chmod(0o755)
    old = NOW - age_days * DAY
    os.utime(binary, (old, old))
    return binary


@pytest.mark.asyncio
async def test_a_hit_writes_the_marker_and_leaves_the_binary_alone(cache):
    manager, entry = cache
    binary = _write_binary(entry)
    before = binary.stat()
    mode_before = entry.stat().st_mode
    assert await manager._is_binary_cached() is True
    assert (entry / MARKER).is_file() and (entry / MARKER).stat().st_size == 0
    after = binary.stat()
    assert (after.st_mtime, after.st_mode, after.st_size) == (before.st_mtime, before.st_mode, before.st_size)
    assert entry.stat().st_mode == mode_before
    assert sorted(p.name for p in entry.iterdir()) == [MARKER, "valkey-server"]


@pytest.mark.asyncio
async def test_a_later_hit_moves_the_marker_forward(cache):
    manager, entry = cache
    _write_binary(entry)
    await manager._is_binary_cached()
    os.utime(entry / MARKER, (1_000_000, 1_000_000))
    await manager._is_binary_cached()
    assert (entry / MARKER).stat().st_mtime > 1_000_000_000


@pytest.mark.asyncio
async def test_a_miss_writes_nothing(cache):
    manager, entry = cache
    assert await manager._is_binary_cached() is False
    assert not entry.exists()
    entry.mkdir(parents=True)  # an entry directory without its binary is a miss too
    assert await manager._is_binary_cached() is False
    assert list(entry.iterdir()) == []


@pytest.mark.asyncio
async def test_a_hit_that_cannot_touch_the_marker_still_succeeds_and_warns_once(cache, caplog):
    manager, entry = cache
    _write_binary(entry)

    real = manager._host.run_host_command

    async def refuse(command, check=True):
        if command.startswith("touch"):
            raise RuntimeError("connection lost")
        return await real(command, check)

    manager._host.run_host_command = refuse
    with caplog.at_level(logging.WARNING):
        assert await manager._is_binary_cached() is True
        assert await manager._is_binary_cached() is True
    warnings = [r for r in caplog.records if "could not record the use" in r.getMessage()]
    assert len(warnings) == 1 and "connection lost" in warnings[0].getMessage()
    assert not (entry / MARKER).exists()


@pytest.mark.asyncio
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
async def test_a_read_only_entry_gives_a_hit_and_one_warning(cache, caplog):
    manager, entry = cache
    _write_binary(entry)
    entry.chmod(0o555)
    try:
        with caplog.at_level(logging.WARNING):
            assert await manager._is_binary_cached() is True
            assert await manager._is_binary_cached() is True
    finally:
        entry.chmod(0o755)
    assert len([r for r in caplog.records if "could not record the use" in r.getMessage()]) == 1
    assert not (entry / MARKER).exists()


@pytest.mark.asyncio
async def test_the_marker_is_written_in_the_same_place_for_a_manual_upload(tmp_path, monkeypatch):
    """manually_uploaded entries live in the build cache like any other and get the same marker."""
    monkeypatch.setenv("HOME", str(tmp_path))
    uploaded = tmp_path / "engine-binary"
    uploaded.write_bytes(b"x")
    digest = hashlib.sha1(b"x").hexdigest()
    manager = BinaryManager(LocalHost())
    manager.source, manager.hash = config.MANUALLY_UPLOADED, digest
    entry = Path(os.path.expanduser(str(manager.get_cached_build_path())))
    _write_binary(entry)
    path = await manager.ensure_binary_cached(source=config.MANUALLY_UPLOADED, specifier=str(uploaded))
    assert path == manager.get_cached_build_path() / "valkey-server"  # a hit: nothing was uploaded
    assert (entry / MARKER).is_file()


@pytest.mark.asyncio
async def test_a_prebuilt_engine_is_not_in_the_cache_and_gets_no_marker(tmp_path):
    """An installed engine is verified on every use and lives under ~/engines, outside the build cache."""
    from .test_prebuilt_engine import Install

    install = Install(tmp_path)
    host = LocalHost()
    manager = BinaryManager(host)
    from conductress import config as cfg

    cfg_root = cfg.PREBUILT_ENGINE_ROOT
    cfg.PREBUILT_ENGINE_ROOT = str(install.root)
    try:
        await manager.ensure_binary_cached(source=cfg.PREBUILT_ENGINE, specifier="dragonfly:v0")
    finally:
        cfg.PREBUILT_ENGINE_ROOT = cfg_root
    assert not any(MARKER in command or "touch" in command for command in host.commands)
    assert sorted(p.name for p in install.dir.iterdir()) == ["MANIFEST", "dragonfly", "lib"]


# --- the prune reads the marker ---------------------------------------------------------------------------


def _mark(leaf: Path, age_days: float):
    marker = leaf / MARKER
    marker.write_text("")
    stamp = NOW - age_days * DAY
    os.utime(marker, (stamp, stamp))
    os.utime(leaf, (NOW - 400 * DAY, NOW - 400 * DAY))  # creating the marker changed the directory time


def test_an_old_binary_with_a_fresh_marker_is_kept(env):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=200)
    _mark(leaf, age_days=3)
    code, text = run(env, "--apply")
    assert leaf.exists()
    assert "Last use taken from: marker 1," in text


def test_a_fresh_binary_without_a_marker_is_kept(env):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=2)
    run(env, "--apply")
    assert leaf.exists()


def test_old_entries_are_removed_with_an_old_marker_and_without_one(env):
    without = make_entry(env["root"], "valkey", sha(1), age_days=200)
    old_marker = make_entry(env["root"], "valkey", sha(2), age_days=200)
    _mark(old_marker, age_days=150)
    _, text = run(env, "--apply")
    assert not without.exists() and not old_marker.exists()
    assert "Last use taken from: marker 1," in text


def test_the_dry_run_says_where_each_last_use_came_from(env):
    _mark(make_entry(env["root"], "valkey", sha(1), age_days=200), age_days=3)
    make_entry(env["root"], "valkey", sha(2), age_days=200)
    make_entry(env["root"], "valkey", sha(3), age_days=200)
    _, text = run(env)
    assert "Last use taken from: marker 1, access time 0, build time 2 entries." in text
    assert "A marker is written when a task uses an entry" in text or "a marker is written when a task uses" in text


def test_the_note_about_the_marker_is_left_out_when_every_entry_has_one(env):
    _mark(make_entry(env["root"], "valkey", sha(1), age_days=200), age_days=3)
    _, text = run(env)
    assert "Last use taken from: marker 1, access time 0, build time 0 entries." in text
    assert "few weeks" not in text


def test_an_access_time_newer_than_the_files_is_reported_as_such(env):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=200)
    binary = leaf / "valkey-server"
    os.utime(binary, (NOW - 5 * DAY, NOW - 200 * DAY))
    (entry,) = bc.scan_cache(env["root"], ["valkey"])[0]
    assert entry.use_source == bc.USE_ACCESS_TIME
    assert entry.last_use == pytest.approx(NOW - 5 * DAY)


def test_the_marker_counts_as_a_file_of_the_entry(env):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=200)
    _mark(leaf, age_days=1)
    (entry,) = bc.scan_cache(env["root"], ["valkey"])[0]
    assert entry.use_source == bc.USE_MARKER and entry.last_use == pytest.approx(NOW - DAY)
    assert entry.apparent == sum(p.stat().st_size for p in leaf.iterdir())


def test_a_marker_in_a_sub_directory_is_not_a_use_marker(env):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=200)
    (leaf / "sub").mkdir()
    (leaf / "sub" / MARKER).write_text("")
    os.utime(leaf / "sub" / MARKER, (NOW, NOW))
    (entry,) = bc.scan_cache(env["root"], ["valkey"])[0]
    assert entry.use_source != bc.USE_MARKER


def test_the_marker_survives_apply_checks_for_use_since_the_scan(env):
    """apply_plan re-measures an entry before removing it; a marker written after the scan keeps it."""
    import io

    leaf = make_entry(env["root"], "valkey", sha(1), age_days=200)
    entries, skipped = bc.scan_cache(env["root"], ["valkey"])
    plan = bc.make_plan(entries, skipped, now=NOW, min_age_days=60, series=set(), pinned=set())
    (leaf / MARKER).write_text("")
    os.utime(leaf / MARKER, (NOW, NOW))  # a task hit the entry after the scan
    removed, _, _ = bc.apply_plan(
        plan,
        env["root"],
        log_path=env["tmp"] / "p.log",
        is_busy=lambda: [],
        now=NOW,
        min_age_days=60,
        out=io.StringIO(),
    )
    assert removed == 0 and leaf.exists()
    assert stat.S_ISREG((leaf / MARKER).stat().st_mode)
