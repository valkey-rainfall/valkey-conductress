"""Stable runner identity and additive result provenance.

All collection is local-only: this module never contacts instance metadata or any
other network endpoint. Values are cached because result/status writers call these
helpers repeatedly during a runner's lifetime.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import socket
import subprocess
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from .config import PROJECT_ROOT, RUNNER_CONFIG_PATH
from .platform import get_local_platform_info

RUNNER_INFO_SCHEMA_VERSION = 1
# 2: ``environment`` carries glibc_version, cpu_microcode, boot_id,
# cachecannon_commit and cachecannon_dirty (each may be None), and a result
# row may carry ``server_environment`` for the host the server ran on.
# Schema 3 adds ``packages``: the installed version-release of a short fixed
# list of packages (``rpm -q``), None where rpm is missing. The running libc
# reports ``2.34`` before and after a glibc update; the package release moves.
PROVENANCE_SCHEMA_VERSION = 3
PACKAGE_NAMES = ("kernel", "glibc", "microcode_ctl", "tuned")
_RPM_QUERY = ["rpm", "-q", "--qf", "%{NAME}=%{VERSION}-%{RELEASE}\n", *PACKAGE_NAMES]
_RPM_TIMEOUT_SECONDS = 2.0
_RUNNER_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_ALLOWED_CONFIG_KEYS = {"schema_version", "runner_id", "display_name"}
_UNKNOWN_REVISION = "unknown"

# Environment fields that can move a benchmark result without a code change.
# Together they form the key of an environment PERIOD (sweep/exporter.py);
# ``boot_id`` is listed apart because a reboot is only a possible change and
# is marked more lightly than a package or kernel change.
PERIOD_FIELDS = ("kernel_release", "glibc_version", "cpu_microcode", "conductress_revision", "cachecannon_commit")
BOOT_FIELDS = ("boot_id",)
# Host-level fields read on the server host as well (``server_environment``).
HOST_FIELDS = (
    "host_fingerprint",
    "instance_id",
    "kernel_release",
    "machine",
    "cpu_model",
    "glibc_version",
    "cpu_microcode",
    "boot_id",
    "packages",
)

# One shell line that prints the server host's environment, one ``key=value``
# per line, parsed by :func:`parse_remote_environment`. Every read is a local
# file, getconf or rpm; a missing file or a host without rpm prints an empty
# value. The packages line holds ``name=version-release`` items joined by
# commas; rpm's "package X is not installed" items are skipped by the parser.
REMOTE_ENVIRONMENT_COMMAND = (
    "echo packages=$(rpm -q --qf '%{NAME}=%{VERSION}-%{RELEASE},' " + " ".join(PACKAGE_NAMES) + " 2>/dev/null"
    " | tr '\\n' ','); "
    "echo kernel_release=$(uname -r); echo machine=$(uname -m); "
    "echo boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null); "
    "echo machine_id=$(cat /etc/machine-id 2>/dev/null || cat /var/lib/dbus/machine-id 2>/dev/null); "
    "echo hostname=$(hostname -s 2>/dev/null); "
    "echo glibc_version=$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}'); "
    "echo cpu_model=$(grep -m1 -E '^(model name|CPU part)' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//'); "
    "echo cpu_microcode=$(grep -m1 '^microcode' /proc/cpuinfo | cut -d: -f2 | tr -d ' '); "
    "echo instance_id=$(cat /sys/devices/virtual/dmi/id/board_asset_tag 2>/dev/null "
    "|| cat /sys/devices/virtual/dmi/id/product_uuid 2>/dev/null || cat /sys/hypervisor/uuid 2>/dev/null)"
)


@dataclass(frozen=True)
class RunnerConfig:
    """Locally configured stable identity for one Conductress runner."""

    runner_id: str
    display_name: str


def _short_hostname() -> str:
    return socket.gethostname().split(".", 1)[0].lower()


def _validate_runner_id(value: str) -> str:
    if not _RUNNER_ID_RE.fullmatch(value):
        raise ValueError(
            "runner_id must be 1-63 lowercase ASCII letters, digits, or hyphens "
            "and must start with a letter or digit"
        )
    return value


def load_runner_config(path: Optional[Path] = None) -> RunnerConfig:
    """Load runner.json, with an environment/hostname compatibility fallback.

    ``CONDUCTRESS_RUNNER_ID`` is useful for immutable deployments. When neither
    it nor runner.json is present, the short hostname becomes the runner ID so
    existing installations continue to work before explicit configuration.
    """

    env_runner_id = os.environ.get("CONDUCTRESS_RUNNER_ID")
    if env_runner_id:
        runner_id = _validate_runner_id(env_runner_id)
        return RunnerConfig(runner_id=runner_id, display_name=os.environ.get("CONDUCTRESS_RUNNER_NAME", runner_id))

    config_path = path or Path(os.environ.get("CONDUCTRESS_RUNNER_CONFIG", RUNNER_CONFIG_PATH))
    if not config_path.exists():
        runner_id = _validate_runner_id(_short_hostname())
        return RunnerConfig(runner_id=runner_id, display_name=runner_id)

    data = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"runner config must be a JSON object: {config_path}")
    unknown = set(data) - _ALLOWED_CONFIG_KEYS
    if unknown:
        raise ValueError(f"unknown runner config fields: {', '.join(sorted(unknown))}")
    if data.get("schema_version", RUNNER_INFO_SCHEMA_VERSION) != RUNNER_INFO_SCHEMA_VERSION:
        raise ValueError(f"unsupported runner config schema_version in {config_path}")
    if "runner_id" not in data:
        raise ValueError(f"runner config is missing runner_id: {config_path}")

    runner_id = _validate_runner_id(data["runner_id"])
    display_name = data.get("display_name") or runner_id
    if not isinstance(display_name, str):
        raise ValueError("display_name must be a string")
    return RunnerConfig(runner_id=runner_id, display_name=display_name)


@lru_cache(maxsize=1)
def get_runner_config() -> RunnerConfig:
    """Return the process-stable runner configuration."""

    return load_runner_config()


def _read_first(paths: list[Path]) -> Optional[str]:
    for path in paths:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value and value.lower() not in {"none", "not specified", "unknown"}:
            return value
    return None


def _instance_id() -> Optional[str]:
    """Read a cloud/hypervisor instance identifier without network access."""

    return _read_first(
        [
            Path("/sys/devices/virtual/dmi/id/board_asset_tag"),
            Path("/sys/devices/virtual/dmi/id/product_uuid"),
            Path("/sys/hypervisor/uuid"),
        ]
    )


def _cpuinfo_fields(path: Path = Path("/proc/cpuinfo")) -> dict[str, str]:
    """The first processor's ``/proc/cpuinfo`` fields, lower-cased keys."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    fields: dict[str, str] = {}
    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields.setdefault(key.strip().lower(), value.strip())
    return fields


def _cpu_model(fields: Optional[dict[str, str]] = None) -> str:
    fields = _cpuinfo_fields() if fields is None else fields
    return fields.get("model name") or fields.get("cpu part") or fields.get("processor") or "unknown"


def _cpu_microcode(fields: Optional[dict[str, str]] = None) -> Optional[str]:
    """The CPU microcode revision (``0x2b000685``), or None where the platform does not expose it.

    x86 prints it in ``/proc/cpuinfo``; Graviton guests have no ``microcode``
    line and no sysfs node, so None there means "not available", not a value
    (a diff treats None -> None as no change).
    """
    fields = _cpuinfo_fields() if fields is None else fields
    value = fields.get("microcode")
    if value:
        return value
    return _read_first([Path("/sys/devices/system/cpu/cpu0/microcode/version")])


def _glibc_version() -> Optional[str]:
    """The C library version this process runs with (``2.34``).

    ``gnu_get_libc_version`` is asked directly through ctypes: the process is
    already linked against the host glibc, so no library lookup or subprocess
    is needed. A non-glibc libc falls back to ``platform.libc_ver``; None when
    neither knows.
    """
    try:
        import ctypes

        function = ctypes.CDLL(None).gnu_get_libc_version
        function.restype = ctypes.c_char_p
        value = function()
        if value:
            return value.decode("ascii", "replace")
    except (AttributeError, OSError, ValueError):
        pass
    return platform.libc_ver()[1] or None


def _boot_id(path: Path = Path("/proc/sys/kernel/random/boot_id")) -> Optional[str]:
    """The kernel's boot id (a new UUID at every boot); None off Linux."""
    return _read_first([path])


def parse_package_versions(items: list[str], kernel_release: Optional[str] = None) -> dict[str, Optional[str]]:
    """``{name: version-release}`` for :data:`PACKAGE_NAMES` from ``name=version-release`` items.

    Items that are not of that form (rpm's ``package X is not installed``) are
    skipped; a package without an item is None. Several installed kernels are
    normal (the previous one stays until it is removed); the one whose
    version-release is the prefix of the running ``kernel_release`` wins, else
    the last listed.
    """
    found: dict[str, list[str]] = {name: [] for name in PACKAGE_NAMES}
    for item in items:
        name, sep, version = item.strip().partition("=")
        if sep and name in found and version:
            found[name].append(version)
    packages: dict[str, Optional[str]] = {}
    for name in PACKAGE_NAMES:
        versions = found[name]
        chosen = versions[-1] if versions else None
        if name == "kernel" and kernel_release:
            running = [v for v in versions if kernel_release.startswith(v)]
            if running:
                chosen = running[0]
        packages[name] = chosen
    return packages


def _installed_packages(kernel_release: Optional[str] = None) -> Optional[dict[str, Optional[str]]]:
    """The installed version-release of :data:`PACKAGE_NAMES` via rpm; None where rpm is missing or fails."""
    try:
        result = subprocess.run(_RPM_QUERY, capture_output=True, text=True, timeout=_RPM_TIMEOUT_SECONDS, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    # rpm exits non-zero when any package is not installed; its stdout still
    # lists the installed ones, one per line.
    if not result.stdout.strip():
        return None
    return parse_package_versions(result.stdout.splitlines(), kernel_release)


@lru_cache(maxsize=8)
def _cachecannon_identity(binary: str, mtime_ns: int) -> dict[str, Any]:
    """Commit and dirty flag of the cachecannon checkout a binary was built from.

    The binary has no commit in ``--version``, so the checkout above
    ``target/release/`` is asked. ``mtime_ns`` is part of the cache key so a
    rebuilt binary at the same path is read again. A binary outside a git
    checkout gets a content hash instead, so two copies can still be told
    apart.
    """
    path = Path(binary).expanduser()
    checkout = path.parent.parent.parent if path.parent.name in ("release", "debug") else path.parent
    result: dict[str, Any] = {"cachecannon_commit": None, "cachecannon_dirty": None}
    try:
        head = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"], check=False, capture_output=True, text=True, timeout=2
        )
        if head.returncode == 0 and head.stdout.strip():
            result["cachecannon_commit"] = head.stdout.strip()
            status = subprocess.run(
                ["git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=no"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            result["cachecannon_dirty"] = bool(status.stdout.strip()) if status.returncode == 0 else None
            return result
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        result["cachecannon_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except OSError:
        pass
    return result


def cachecannon_identity(binary: Optional[str]) -> dict[str, Any]:
    """Per-task cachecannon provenance for the binary a task names (None: no cachecannon)."""
    if not binary:
        return {"cachecannon_commit": None, "cachecannon_dirty": None}
    try:
        mtime_ns = Path(binary).expanduser().stat().st_mtime_ns
    except OSError:
        return {"cachecannon_commit": None, "cachecannon_dirty": None}
    return dict(_cachecannon_identity(str(binary), mtime_ns))


def parse_remote_environment(text: str) -> dict[str, Any]:
    """Parse the output of :data:`REMOTE_ENVIRONMENT_COMMAND` into a host environment block.

    Pure, so it is testable without a host. Empty values become None; the
    fingerprint is derived the same way as the local one (instance id,
    machine id, hostname, platform label is unknown remotely so the kernel
    machine string stands in). A truncated output yields None fields, never
    an exception.
    """
    raw: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        raw[key.strip()] = value.strip()

    def field(name: str) -> Optional[str]:
        return raw.get(name) or None

    env: dict[str, Any] = {
        "host_fingerprint": _host_fingerprint(
            field("instance_id"), field("machine") or "", field("hostname") or "", machine_id=field("machine_id") or ""
        ),
        "instance_id": field("instance_id"),
        "kernel_release": field("kernel_release"),
        "machine": field("machine"),
        "cpu_model": field("cpu_model") or "unknown",
        "glibc_version": field("glibc_version"),
        "cpu_microcode": field("cpu_microcode"),
        "boot_id": field("boot_id"),
        "packages": None,
    }
    packages_field = field("packages")
    if packages_field:
        env["packages"] = parse_package_versions(packages_field.split(","), env["kernel_release"])
    return env


def environment_for_period(row: dict[str, Any]) -> dict[str, Any]:
    """The reduced environment of a result row that an environment PERIOD is keyed on.

    Client-host fields under their own names, the server host's under
    ``server_`` when the row carries a different server host, plus the
    provenance schema version (a field a row's schema did not have is not a
    change) and ``cachecannon_dirty`` (label material, not key material).
    """
    environment = row.get("environment") or {}
    reduced: dict[str, Any] = {"provenance_schema_version": row.get("provenance_schema_version")}
    for key in PERIOD_FIELDS + BOOT_FIELDS:
        reduced[key] = environment.get(key)
    reduced["cachecannon_dirty"] = environment.get("cachecannon_dirty")
    if "packages" in environment:
        reduced["packages"] = environment.get("packages")
    server = row.get("server_environment")
    if isinstance(server, dict) and not server.get("same_host", False):
        for key in ("kernel_release", "glibc_version", "cpu_microcode", "boot_id"):
            reduced[f"server_{key}"] = server.get(key)
        if "packages" in server:
            reduced["server_packages"] = server.get("packages")
    return reduced


def _conductress_revision() -> str:
    configured = os.environ.get("CONDUCTRESS_REVISION")
    if configured:
        return configured
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return _UNKNOWN_REVISION
    revision = result.stdout.strip()
    return revision if result.returncode == 0 and revision else _UNKNOWN_REVISION


def _host_fingerprint(
    instance_id: Optional[str], platform_label: str, hostname: str, machine_id: Optional[str] = None
) -> str:
    if machine_id is None:
        machine_id = _read_first([Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")]) or ""
    material = "\0".join((instance_id or "", machine_id, hostname, platform_label))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


@lru_cache(maxsize=1)
def get_runner_info() -> dict[str, Any]:
    """Return versioned identity/environment information for CLI and status."""

    config = get_runner_config()
    platform_id, platform_label, aliases = get_local_platform_info()
    hostname = _short_hostname()
    instance_id = _instance_id()
    cpuinfo = _cpuinfo_fields()
    # Cached for the process lifetime on purpose: kernel, boot id, glibc and
    # microcode cannot change under a running runner (a reboot ends the
    # process, a package update takes effect for new processes only), so the
    # cached values are exactly what this runner process runs with.
    return {
        "schema_version": RUNNER_INFO_SCHEMA_VERSION,
        "runner_id": config.runner_id,
        "display_name": config.display_name,
        "hostname": hostname,
        "platform": {
            "id": platform_id,
            "label": platform_label,
            "aliases": aliases,
        },
        "environment": {
            "host_fingerprint": _host_fingerprint(instance_id, platform_label, hostname),
            "instance_id": instance_id,
            "kernel_release": platform.release(),
            "machine": platform.machine(),
            "cpu_model": _cpu_model(cpuinfo),
            "conductress_revision": _conductress_revision(),
            "glibc_version": _glibc_version(),
            "cpu_microcode": _cpu_microcode(cpuinfo),
            "boot_id": _boot_id(),
            "packages": _installed_packages(platform.release()),
        },
    }


def get_result_provenance(cachecannon_binary: Optional[str] = None) -> dict[str, Any]:
    """Return additive top-level fields shared by every result writer.

    The host part is cached; the cachecannon part depends on the binary the
    task names (``cachecannon_binary``), so it is resolved per call and cached
    per binary path and mtime.
    """

    info = get_runner_info()
    environment = dict(info["environment"])
    environment.update(cachecannon_identity(cachecannon_binary))
    return {
        "provenance_schema_version": PROVENANCE_SCHEMA_VERSION,
        "runner_id": info["runner_id"],
        "platform": info["platform"]["label"],
        "environment": environment,
    }


def clear_identity_caches() -> None:
    """Clear process caches; intended for tests and explicit config reloads."""

    get_runner_config.cache_clear()
    get_runner_info.cache_clear()
    _cachecannon_identity.cache_clear()
