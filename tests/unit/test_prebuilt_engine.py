"""The prebuilt_engine source: a hand-installed engine is used only if every file matches its MANIFEST."""

import asyncio
import hashlib
import json
import os
import re
import socket
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress import config
from conductress import prebuilt_engine as pe
from conductress.binary_manager import BinaryManager
from conductress.cli import main
from conductress.engines import ENGINES, EngineProfile
from conductress.prebuilt_engine import ManifestError, parse_manifest, parse_specifier, verify_prebuilt_engine
from conductress.tasks.task_mem_efficiency import MemTaskData
from conductress.topology import TopologySpec


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class LocalHost:
    """Runs host commands in a local shell and records them (the real head, find, test, id and sha256sum)."""

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


class Install:
    """A hand-installed engine under tmp_path/engines/<engine>/<tag> with a MANIFEST that matches."""

    def __init__(self, tmp_path, files=None, engine="dragonfly", tag="v0", entrypoint="dragonfly"):
        self.root = tmp_path / "engines"
        self.engine, self.tag, self.entrypoint = engine, tag, entrypoint
        self.dir = self.root / engine / tag
        self.dir.mkdir(parents=True)
        self.marker = tmp_path / "RAN"
        self.files = files or {
            entrypoint: f"#!/bin/sh\ntouch {self.marker}\n".encode(),
            "lib/libexample.so": b"library bytes",
        }
        for name, data in self.files.items():
            path = self.dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o755 if name == entrypoint else 0o644)
        self.write_manifest()

    def manifest(self):
        return {
            "schema": 1,
            "engine": self.engine,
            "tag": self.tag,
            "entrypoint": self.entrypoint,
            "tarball": {"name": "engine-aarch64.tar.gz", "sha256": "ab" * 32},
            "files": {name: sha(data) for name, data in self.files.items()},
        }

    def write_manifest(self, document=None):
        path = self.dir / "MANIFEST"
        path.write_text(json.dumps(document if document is not None else self.manifest()))
        path.chmod(0o644)

    @property
    def specifier(self):
        return f"{self.engine}:{self.tag}"

    async def verify(self, host=None):
        return await verify_prebuilt_engine(host or LocalHost(), self.specifier, root=str(self.root))


@pytest.fixture
def install(tmp_path):
    return Install(tmp_path)


# --- a good installation ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_matching_installation_is_accepted(install):
    verified = await install.verify()
    assert verified.entrypoint == str(install.dir / "dragonfly")
    assert verified.entrypoint_sha256 == sha(install.files["dragonfly"])
    info = verified.info()
    assert info["tag"] == "v0" and info["files"] == 2 and info["tarball_sha256"] == "ab" * 32


@pytest.mark.asyncio
async def test_checking_never_runs_anything_from_the_directory(install):
    host = LocalHost()
    await install.verify(host)
    assert not install.marker.exists()  # the entrypoint is a script that would leave this file
    for command in host.commands:
        assert command.startswith(("(", "head ", "find ", "id ", "cd ")), command
        assert "--version" not in command
    hashing = [c for c in host.commands if "sha256sum" in c]
    assert hashing == [f"cd {install.dir} && sha256sum -- dragonfly lib/libexample.so"]


@pytest.mark.asyncio
async def test_a_wrong_sha256_is_refused_and_nothing_ran(install):
    (install.dir / "dragonfly").write_bytes(b"#!/bin/sh\ntouch " + str(install.marker).encode() + b"\n# changed\n")
    with pytest.raises(ManifestError, match="sha256 does not match for: dragonfly"):
        await install.verify()
    assert not install.marker.exists()


@pytest.mark.asyncio
async def test_many_files_are_hashed_in_chunks(tmp_path):
    files = {"engine": b"#!/bin/sh\n", **{f"data/f{i:03d}.bin": f"{i}".encode() for i in range(120)}}
    host = LocalHost()
    await Install(tmp_path, files=files, entrypoint="engine").verify(host)
    assert len([c for c in host.commands if "sha256sum" in c]) == 3


# --- refusals ---------------------------------------------------------------------------------------------


def _extra_file(i):
    (i.dir / "extra.sh").write_text("x")


def _missing_file(i):
    (i.dir / "lib" / "libexample.so").unlink()


def _symlink_to_outside(i):
    (i.dir / "lib" / "libexample.so").unlink()
    (i.dir / "lib" / "libexample.so").symlink_to("/etc/hostname")


def _symlink_unlisted(i):
    (i.dir / "link").symlink_to(i.dir / "dragonfly")


def _symlink_entrypoint(i):
    (i.dir / "real").write_bytes(i.files["dragonfly"])
    (i.dir / "dragonfly").unlink()
    (i.dir / "dragonfly").symlink_to("real")


def _fifo(i):
    os.mkfifo(i.dir / "pipe")


def _setuid(i):
    (i.dir / "lib" / "libexample.so").chmod(0o4755)


def _group_writable(i):
    (i.dir / "lib" / "libexample.so").chmod(0o664)


def _world_writable(i):
    (i.dir / "lib" / "libexample.so").chmod(0o666)


def _world_writable_dir(i):
    (i.dir / "lib").chmod(0o777)


def _not_executable(i):
    (i.dir / "dragonfly").chmod(0o644)


def _no_manifest(i):
    (i.dir / "MANIFEST").unlink()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate,message",
    [
        (_extra_file, "extra.sh: not listed"),
        (_missing_file, "libexample.so: listed in the MANIFEST but missing"),
        (_symlink_to_outside, "not a regular file or directory"),
        (_symlink_unlisted, "link: not a regular file or directory"),
        (_symlink_entrypoint, "not a regular file or directory"),
        (_fifo, "pipe: not a regular file or directory"),
        (_setuid, "setuid, setgid or sticky"),
        (_group_writable, "writable by group or others"),
        (_world_writable, "writable by group or others"),
        (_world_writable_dir, "lib: writable by group or others"),
        (_not_executable, "entrypoint is not executable"),
        (_no_manifest, "MANIFEST is missing or empty"),
    ],
)
async def test_an_installation_that_differs_from_its_manifest_is_refused(install, mutate, message):
    mutate(install)
    with pytest.raises(ManifestError, match=re.escape(message)):
        await install.verify()
    assert not install.marker.exists()


@pytest.mark.asyncio
async def test_a_symlinked_tag_directory_is_refused(tmp_path):
    install = Install(tmp_path)
    real = tmp_path / "elsewhere"
    install.dir.rename(real)
    install.dir.symlink_to(real)
    with pytest.raises(ManifestError, match="missing or is a symlink"):
        await install.verify()


@pytest.mark.asyncio
async def test_a_symlinked_engine_directory_is_refused(tmp_path):
    install = Install(tmp_path)
    real = tmp_path / "elsewhere"
    (install.root / "dragonfly").rename(real)
    (install.root / "dragonfly").symlink_to(real)
    with pytest.raises(ManifestError, match="missing or is a symlink"):
        await install.verify()


@pytest.mark.asyncio
async def test_a_missing_installation_is_refused(tmp_path):
    with pytest.raises(ManifestError, match="missing or is a symlink"):
        await verify_prebuilt_engine(LocalHost(), "dragonfly:v9", root=str(tmp_path / "engines"))


def test_an_entry_owned_by_another_user_is_refused(install):
    manifest = parse_manifest(json.dumps(install.manifest()), "dragonfly", "v0")
    inventory = "f 755 1000 dragonfly\nf 644 1000 lib/libexample.so\nf 644 1000 MANIFEST\n"
    pe.check_inventory(manifest, inventory, uid=1000)
    with pytest.raises(ManifestError, match="owned by another user"):
        pe.check_inventory(manifest, inventory, uid=1001)


# --- the MANIFEST itself ------------------------------------------------------------------------------------


def _doc(**changes):
    document = {
        "schema": 1,
        "engine": "dragonfly",
        "tag": "v0",
        "entrypoint": "dragonfly",
        "tarball": {"name": "t.tar.gz", "sha256": "0" * 64},
        "files": {"dragonfly": "1" * 64},
    }
    document.update(changes)
    return json.dumps(document)


def test_a_minimal_manifest_parses():
    assert parse_manifest(_doc(), "dragonfly", "v0").entrypoint == "dragonfly"


@pytest.mark.parametrize(
    "text",
    [
        "not json",
        "[]",
        _doc(schema=2),
        _doc(schema=True),
        _doc(engine="garnet"),
        _doc(tag="v1"),
        _doc(extra=1),
        _doc(entrypoint="missing"),
        _doc(entrypoint="/bin/sh"),
        _doc(entrypoint="../dragonfly"),
        _doc(files={"../dragonfly": "1" * 64}, entrypoint="../dragonfly"),
        _doc(files={"/abs/dragonfly": "1" * 64}, entrypoint="/abs/dragonfly"),
        _doc(files={"a/./b": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={"a//b": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={"a/../b": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={"with space": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={"semi;colon": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={"new\nline": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={".hidden": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={"MANIFEST": "1" * 64, "dragonfly": "1" * 64}),
        _doc(files={"dragonfly": "XYZ"}),
        _doc(files={"dragonfly": "A" * 64}),
        _doc(files={}),
        _doc(tarball={"name": "t", "sha256": "short"}),
        _doc(tarball={"name": "../t", "sha256": "0" * 64}),
        _doc(tarball={"name": "t", "sha256": "0" * 64, "url": "x"}),
    ],
)
def test_a_manifest_that_is_not_acceptable_is_refused(text):
    with pytest.raises(ManifestError):
        parse_manifest(text, "dragonfly", "v0")


def test_too_many_files_are_refused():
    files = {f"f{i}": "1" * 64 for i in range(pe.MAX_FILES + 1)}
    with pytest.raises(ManifestError, match="1 to"):
        parse_manifest(_doc(files=files, entrypoint="f0"), "dragonfly", "v0")


@pytest.mark.parametrize("specifier", ["dragonfly:v2.1", "garnet:v2.2.14", "dragonfly:2.1.0-rc1"])
def test_good_specifiers(specifier):
    assert ":".join(parse_specifier(specifier)) == specifier


@pytest.mark.parametrize(
    "specifier",
    [
        None,
        "",
        "dragonfly",
        "dragonfly:",
        ":v1",
        "valkey:v1",
        "nope:v1",
        "dragonfly:../x",
        "dragonfly:a/b",
        "dragonfly:v1;ls",
        "dragonfly:-v1",
        "Dragonfly:v1",
        "dragonfly:v1 ",
    ],
)
def test_bad_specifiers_are_refused(specifier):
    with pytest.raises(ManifestError):
        parse_specifier(specifier)


def test_a_new_profile_can_be_installed(monkeypatch):
    monkeypatch.setitem(ENGINES, "fakeengine", EngineProfile(name="fakeengine", rehash_check=False))
    assert parse_specifier("fakeengine:v1") == ("fakeengine", "v1")


# --- no network ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_checking_opens_no_network_connection(install, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("network use")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    await install.verify()


def test_the_module_has_no_network_code():
    source = Path(pe.__file__).read_text().lower()
    for word in ("urllib", "requests", "http", "curl", "wget", "socket", "asyncssh", "download", "subprocess"):
        assert word not in source, word


# --- the source in BinaryManager, task data and the CLI ------------------------------------------------------


@pytest.mark.asyncio
async def test_binary_manager_returns_the_verified_entrypoint(install, monkeypatch):
    monkeypatch.setattr(config, "PREBUILT_ENGINE_ROOT", str(install.root))
    manager = BinaryManager(LocalHost())
    path = await manager.ensure_binary_cached(source=config.PREBUILT_ENGINE, specifier="dragonfly:v0")
    assert path == install.dir / "dragonfly"
    assert manager.get_build_hash() == sha(install.files["dragonfly"])
    assert manager.prebuilt_info()["tag"] == "v0"
    assert not install.marker.exists()


@pytest.mark.asyncio
async def test_binary_manager_refuses_a_mismatch_and_returns_no_path(install, monkeypatch):
    monkeypatch.setattr(config, "PREBUILT_ENGINE_ROOT", str(install.root))
    (install.dir / "lib" / "libexample.so").write_bytes(b"changed")
    manager = BinaryManager(LocalHost())
    with pytest.raises(ManifestError):
        await manager.ensure_binary_cached(source=config.PREBUILT_ENGINE, specifier="dragonfly:v0")
    assert manager.prebuilt_info() is None and manager.get_build_hash() is None


def _task(**kwargs):
    base = dict(
        source=config.PREBUILT_ENGINE,
        specifier="dragonfly:v0",
        topology=TopologySpec.standalone(),
        note="n",
        requirements={},
        make_args="",
        type="zadd",
        val_sizes=[16],
        has_expire=False,
        engine="dragonfly",
    )
    base.update(kwargs)
    return MemTaskData(**base)


def test_a_memory_task_may_use_the_source():
    assert _task().source == config.PREBUILT_ENGINE


@pytest.mark.parametrize(
    "kwargs,message",
    [
        ({"engine": "valkey"}, "the task engine is valkey"),
        ({"specifier": "garnet:v1"}, "names garnet"),
        ({"specifier": "dragonfly"}, "<engine>:<tag>"),
        ({"specifier": "valkey:v1"}, "not a prebuilt engine"),
        ({"specifier": "dragonfly:../x"}, "<engine>:<tag>"),
    ],
)
def test_a_memory_task_with_a_wrong_specifier_or_engine_is_refused(kwargs, message):
    with pytest.raises(ValueError, match=message):
        _task(**kwargs)


def test_other_task_types_do_not_accept_the_source():
    from conductress.task_queue import BaseTaskData
    from conductress.tasks.task_perf_benchmark import PerfTaskData

    assert BaseTaskData._extra_sources(MagicMock()) == ()
    assert PerfTaskData._extra_sources(MagicMock()) == ()


class TestCli:
    ARGS = ["queue", "add-memory", "--source", "prebuilt_engine", "--types", "zadd"]

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_with_a_prebuilt_engine(self, queue_cls):
        queue = MagicMock()
        queue_cls.return_value = queue
        assert main(self.ARGS + ["--specifier", "dragonfly:v0", "--engine", "dragonfly"]) == 0
        task = queue.submit_task.call_args[0][0]
        assert (task.source, task.specifier, task.engine) == ("prebuilt_engine", "dragonfly:v0", "dragonfly")

    @patch("conductress.cli.TaskQueue")
    def test_engine_must_match_the_specifier(self, queue_cls, capsys):
        queue_cls.return_value = MagicMock()
        assert main(self.ARGS + ["--specifier", "garnet:v1", "--engine", "dragonfly"]) == 1
        assert "pass --engine garnet" in capsys.readouterr().err

    @patch("conductress.cli.TaskQueue")
    def test_a_bad_specifier_is_reported(self, queue_cls, capsys):
        queue_cls.return_value = MagicMock()
        assert main(self.ARGS + ["--specifier", "unstable", "--engine", "dragonfly"]) == 1
        assert "<engine>:<tag>" in capsys.readouterr().err
        queue_cls.return_value.submit_task.assert_not_called()

    def test_other_commands_do_not_accept_the_source(self, capsys):
        from conductress.cli import _source_is_valid, validate_source

        assert not validate_source(config.PREBUILT_ENGINE)
        assert not _source_is_valid(config.PREBUILT_ENGINE)
        assert "Invalid source" in capsys.readouterr().err
        assert _source_is_valid(config.PREBUILT_ENGINE, extra=(config.PREBUILT_ENGINE,))


@pytest.mark.asyncio
async def test_the_task_result_records_the_verified_installation(monkeypatch):
    from .mem_settle_harness import ScriptedServer, make_runner, run_single

    runner = make_runner(engine="dragonfly")
    runner.prebuilt_info = {"engine": "dragonfly", "tag": "v0", "entrypoint_sha256": "1" * 64}
    result = await run_single(ScriptedServer(debug=False), runner, monkeypatch)
    assert result["prebuilt_engine"] == {"engine": "dragonfly", "tag": "v0", "entrypoint_sha256": "1" * 64}
    plain = await run_single(ScriptedServer(debug=False), make_runner(engine="dragonfly"), monkeypatch)
    assert "prebuilt_engine" not in plain


def test_build_cache_prune_never_touches_the_prebuilt_source(tmp_path):
    """Installed engines live under ~/engines, outside the build cache. A stray prebuilt_engine directory inside the
    cache is not a cache source, so the prune lists it as skipped and leaves it."""
    from conductress.build_cache import scan_cache

    (tmp_path / "prebuilt_engine" / ("0" * 40) / ("0" * 16)).mkdir(parents=True)
    entries, skipped = scan_cache(tmp_path, ["valkey", config.MANUALLY_UPLOADED])
    assert entries == [] and skipped == ["prebuilt_engine"]
