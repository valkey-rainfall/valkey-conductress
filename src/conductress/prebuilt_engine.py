"""The prebuilt_engine source: an engine installed on the host by hand, checked against its MANIFEST before use.

Some engines are not built from a git commit (a vendor release unpacked into a directory, possibly many files).
A person installs one under ``~/engines/<engine>/<tag>/`` and writes a ``MANIFEST`` next to the files. Conductress
only checks and uses it:

* It never fetches anything. This module has no network code.
* It never runs a file from the directory while checking. The checks use ``cat``, ``find``, ``test`` and
  ``sha256sum``. The caller launches the entrypoint only after ``verify_prebuilt_engine`` returned.
* It refuses anything the MANIFEST does not list: an unlisted file, a listed file with another sha256, a missing
  file, a symlink, a special file, a setuid, setgid or group- or world-writable entry, an entry owned by another
  user, a path with ``..`` or an absolute path, and a root or parent directory that is a symlink.

MANIFEST (JSON)::

    {"schema": 1, "engine": "dragonfly", "tag": "v2.1", "entrypoint": "dragonfly",
     "tarball": {"name": "dragonfly-aarch64.tar.gz", "sha256": "<64 hex>"},
     "files": {"dragonfly": "<64 hex>", "lib/libexample.so": "<64 hex>"}}

``tarball.sha256`` is the vendor archive's digest, checked by the person who installed it (the archive is not kept);
it is recorded in the task result. Every file under the directory except MANIFEST must be listed in ``files``.

shortcut: the check and the launch are two steps; a file replaced in between would not be seen. The directory is
installed read-only and owned by the runner user, which is the same trust as the runner's own checkout. Upgrade to
copying the verified files into a per-task directory if that is not enough.
"""

import json
import re
import shlex
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Optional

from . import config
from .engines import ENGINES
from .memory_record import shell_path

MANIFEST_NAME = "MANIFEST"
MANIFEST_MAX_BYTES = 1 << 20
MAX_FILES = 5000
MAX_DEPTH = 8
HASH_CHUNK = 50  # files per sha256sum command

_SPECIFIER_RE = re.compile(r"^([a-z][a-z0-9_-]{0,31}):([A-Za-z0-9][A-Za-z0-9._-]{0,63})$")
_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_INVENTORY_RE = re.compile(r"^([a-zA-Z]) ([0-7]{3,5}) (\d+) (.+)$")
_HASH_LINE_RE = re.compile(r"^([0-9a-f]{64})  (.+)$")
_TARBALL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,199}$")
_MANIFEST_KEYS = {"schema", "engine", "tag", "entrypoint", "tarball", "files"}


class ManifestError(Exception):
    """The installed engine does not match its MANIFEST, or the MANIFEST is not acceptable. Nothing was run."""


@dataclass(frozen=True)
class Manifest:
    engine: str
    tag: str
    entrypoint: str
    tarball_name: str
    tarball_sha256: str
    files: dict  # relative path -> sha256


@dataclass(frozen=True)
class VerifiedEngine:
    """An installed engine whose files all matched. ``entrypoint`` is the path to launch (it may start with ~/)."""

    entrypoint: str
    entrypoint_sha256: str
    manifest: Manifest

    def info(self) -> dict:
        """What a task result records about the installed engine."""
        return {
            "engine": self.manifest.engine,
            "tag": self.manifest.tag,
            "entrypoint": self.manifest.entrypoint,
            "entrypoint_sha256": self.entrypoint_sha256,
            "tarball_name": self.manifest.tarball_name,
            "tarball_sha256": self.manifest.tarball_sha256,
            "files": len(self.manifest.files),
        }


def parse_specifier(specifier: Optional[str]) -> tuple[str, str]:
    """``<engine>:<tag>`` -> (engine, tag). The engine must be a non-Valkey profile; the tag is one safe name."""
    match = _SPECIFIER_RE.match(specifier or "")
    if match is None:
        raise ManifestError(f"specifier must look like <engine>:<tag>, got {specifier!r}")
    engine, tag = match.groups()
    profile = ENGINES.get(engine)
    if profile is None or profile.valkey_launch:
        others = sorted(name for name, p in ENGINES.items() if not p.valkey_launch)
        raise ManifestError(f"engine {engine!r} is not a prebuilt engine; valid: {', '.join(others)}")
    return engine, tag


def check_relative_path(path: Any, what: str) -> str:
    """A plain relative path of safe components: no absolute path, no empty, ``.`` or ``..`` part, no odd characters."""
    if not isinstance(path, str) or not path or len(path) > 512:
        raise ManifestError(f"{what}: not a usable path")
    if path.startswith("/"):
        raise ManifestError(f"{what}: {path!r} is an absolute path")
    parts = path.split("/")
    if len(parts) > MAX_DEPTH or any(_COMPONENT_RE.match(part) is None or part in (".", "..") for part in parts):
        raise ManifestError(f"{what}: {path!r} is not a plain relative path")
    return path


def parse_manifest(text: str, engine: str, tag: str) -> Manifest:
    """The MANIFEST as a Manifest, or ManifestError. Strict: unknown keys, odd types and bad paths are refused."""
    try:
        document = json.loads(text)
    except ValueError as exc:
        raise ManifestError(f"MANIFEST is not JSON: {exc}") from exc
    if not isinstance(document, dict) or set(document) != _MANIFEST_KEYS:
        raise ManifestError(f"MANIFEST must be an object with exactly the keys {sorted(_MANIFEST_KEYS)}")
    if document["schema"] != 1 or isinstance(document["schema"], bool):
        raise ManifestError("MANIFEST schema must be 1")
    if document["engine"] != engine or document["tag"] != tag:
        raise ManifestError(
            f"MANIFEST is for {document['engine']!r} {document['tag']!r}, the task asks for {engine!r} {tag!r}"
        )
    tarball = document["tarball"]
    if (
        not isinstance(tarball, dict)
        or set(tarball) != {"name", "sha256"}
        or not isinstance(tarball["name"], str)
        or _TARBALL_NAME_RE.match(tarball["name"]) is None
        or not isinstance(tarball["sha256"], str)
        or _SHA256_RE.match(tarball["sha256"]) is None
    ):
        raise ManifestError("MANIFEST tarball must be {name, sha256} with a plain name and a 64-hex sha256")
    files = document["files"]
    if not isinstance(files, dict) or not 0 < len(files) <= MAX_FILES:
        raise ManifestError(f"MANIFEST files must list 1 to {MAX_FILES} files")
    for path, digest in files.items():
        check_relative_path(path, "MANIFEST files")
        if path == MANIFEST_NAME:
            raise ManifestError("MANIFEST must not list itself")
        if not isinstance(digest, str) or _SHA256_RE.match(digest) is None:
            raise ManifestError(f"MANIFEST files: {path!r} has no 64-hex sha256")
    entrypoint = check_relative_path(document["entrypoint"], "MANIFEST entrypoint")
    if entrypoint not in files:
        raise ManifestError(f"MANIFEST entrypoint {entrypoint!r} is not listed in files")
    return Manifest(engine, tag, entrypoint, tarball["name"], tarball["sha256"], dict(files))


def check_inventory(manifest: Manifest, inventory: str, uid: int) -> None:
    """Compare ``find -printf '%y %m %U %P'`` output with the MANIFEST. Raises ManifestError listing the problems."""
    problems: list[str] = []
    seen_files: set[str] = set()
    for line in inventory.splitlines():
        if not line.strip():
            continue
        match = _INVENTORY_RE.match(line)
        if match is None:
            problems.append(f"unreadable entry {line[:80]!r}")
            continue
        kind, mode_text, owner, path = match.groups()
        mode = int(mode_text, 8)
        if kind not in ("f", "d"):
            problems.append(f"{path}: not a regular file or directory (type {kind})")
            continue
        if mode & 0o7000:
            problems.append(f"{path}: setuid, setgid or sticky bit set")
        if mode & 0o022:
            problems.append(f"{path}: writable by group or others")
        if int(owner) != uid:
            problems.append(f"{path}: owned by another user")
        if kind == "d":
            continue
        seen_files.add(path)
        if path != MANIFEST_NAME and path not in manifest.files:
            problems.append(f"{path}: not listed in the MANIFEST")
        if path == manifest.entrypoint and not mode & 0o100:
            problems.append(f"{path}: the entrypoint is not executable")
    for path in manifest.files:
        if path not in seen_files:
            problems.append(f"{path}: listed in the MANIFEST but missing")
    if MANIFEST_NAME not in seen_files:
        problems.append("MANIFEST is missing")
    if problems:
        raise ManifestError(
            "; ".join(problems[:20]) + (f" (and {len(problems) - 20} more)" if len(problems) > 20 else "")
        )


def check_hashes(manifest: Manifest, outputs: list[str]) -> None:
    """Compare ``sha256sum`` output lines with the MANIFEST. Every listed file must appear once with its digest."""
    found: dict[str, str] = {}
    for line in outputs:
        match = _HASH_LINE_RE.match(line.rstrip("\n"))
        if match is None:
            raise ManifestError(f"unreadable sha256sum line {line[:80]!r}")
        found[match.group(2)] = match.group(1)
    bad = [path for path, digest in manifest.files.items() if found.get(path) != digest]
    extra = sorted(set(found) - set(manifest.files))
    if bad or extra:
        raise ManifestError("sha256 does not match for: " + ", ".join(sorted(bad)[:10] + extra[:10]))


def _quote(path: str) -> str:
    return shlex.quote(path)


async def verify_prebuilt_engine(host: Any, specifier: str, root: Optional[str] = None) -> VerifiedEngine:
    """Check ``<root>/<engine>/<tag>`` on ``host`` against its MANIFEST and return the entrypoint to launch.

    ``host`` needs ``run_host_command(command, check=True) -> (stdout, stderr)``. Only cat, find, test, id and
    sha256sum run here; nothing from the installed directory is executed.
    """
    engine, tag = parse_specifier(specifier)
    base = root or config.PREBUILT_ENGINE_ROOT
    chain = [base, f"{base}/{engine}", f"{base}/{engine}/{tag}"]
    directory = chain[-1]

    # The directory and its parents must be real directories, not symlinks.
    tests = " && ".join(f"[ -d {shell_path_str(p)} ] && [ ! -L {shell_path_str(p)} ]" for p in chain)
    out, _ = await host.run_host_command(f"({tests}) && echo OK || echo BAD", check=False)
    if out.strip() != "OK":
        raise ManifestError(f"{directory} (or a parent) is missing or is a symlink")

    text, _ = await host.run_host_command(
        f"head -c {MANIFEST_MAX_BYTES + 1} {shell_path_str(directory + '/' + MANIFEST_NAME)}", check=False
    )
    if not text.strip():
        raise ManifestError(f"{directory}/{MANIFEST_NAME} is missing or empty")
    if len(text.encode()) > MANIFEST_MAX_BYTES:
        raise ManifestError("MANIFEST is too large")
    manifest = parse_manifest(text, engine, tag)

    inventory, _ = await host.run_host_command(
        f"find {shell_path_str(directory)} -mindepth 1 -printf '%y %m %U %P\\n'", check=False
    )
    uid_text, _ = await host.run_host_command("id -u", check=False)
    check_inventory(manifest, inventory, int(uid_text.strip()))

    paths = sorted(manifest.files)
    outputs: list[str] = []
    for start in range(0, len(paths), HASH_CHUNK):
        names = " ".join(_quote(path) for path in paths[start : start + HASH_CHUNK])
        out, _ = await host.run_host_command(f"cd {shell_path_str(directory)} && sha256sum -- {names}", check=False)
        outputs.extend(line for line in out.splitlines() if line.strip())
    check_hashes(manifest, outputs)

    entrypoint = str(PurePosixPath(directory) / manifest.entrypoint)
    return VerifiedEngine(entrypoint, manifest.files[manifest.entrypoint], manifest)


def shell_path_str(path: str) -> str:
    """A path as a shell word; a leading ~/ stays unquoted so the host's shell expands it."""
    return shell_path(Path(path))
