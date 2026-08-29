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
PROVENANCE_SCHEMA_VERSION = 1
_RUNNER_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_ALLOWED_CONFIG_KEYS = {"schema_version", "runner_id", "display_name"}
_UNKNOWN_REVISION = "unknown"


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


def _cpu_model() -> str:
    try:
        lines = Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return "unknown"

    fields: dict[str, str] = {}
    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields.setdefault(key.strip().lower(), value.strip())
    return fields.get("model name") or fields.get("cpu part") or fields.get("processor") or "unknown"


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


def _host_fingerprint(instance_id: Optional[str], platform_label: str, hostname: str) -> str:
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
            "cpu_model": _cpu_model(),
            "conductress_revision": _conductress_revision(),
        },
    }


@lru_cache(maxsize=1)
def get_result_provenance() -> dict[str, Any]:
    """Return additive top-level fields shared by every result writer."""

    info = get_runner_info()
    return {
        "provenance_schema_version": PROVENANCE_SCHEMA_VERSION,
        "runner_id": info["runner_id"],
        "platform": info["platform"]["label"],
        "environment": dict(info["environment"]),
    }


def clear_identity_caches() -> None:
    """Clear process caches; intended for tests and explicit config reloads."""

    get_runner_config.cache_clear()
    get_runner_info.cache_clear()
    get_result_provenance.cache_clear()
