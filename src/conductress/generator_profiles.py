"""Generator profile registry and bootstrap for benchmark client binaries.

A generator profile pins a specific benchmark-client binary (valkey-benchmark)
built from a known repo + commit with known build args, identified by a SHA-256
content hash.  Profiles are the SINGLE source of truth for which binary runs
benchmarks; tasks declare a profile name rather than an ad-hoc binary path.

The legacy (v1) binary is the stock valkey-benchmark pinned at
VALKEY_BENCHMARK_COMMIT in config.py.  The scalable (v2) binary carries the
generator contention patch.  Custom ``bench_binary`` overrides still work and
take absolute precedence over any profile.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Optional

from conductress.config import PROJECT_ROOT, VALKEY_BENCHMARK_COMMIT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Profile dataclass and registry
# ---------------------------------------------------------------------------

GENERATOR_DIR = PROJECT_ROOT / "generators"


@dataclass(frozen=True)
class GeneratorProfile:
    """Immutable descriptor of a benchmark-client binary."""

    name: str
    repo_url: str
    commit_sha: str  # full 40-hex SHA
    build_args: str = ""  # extra ``make`` arguments (after ``make -j``)
    binary_name: str = "valkey-benchmark"  # produced binary filename
    features: tuple[str, ...] = ()  # generator capabilities a test command may need (see FEATURE_RANGE)

    @property
    def install_dir(self) -> Path:
        """Per-profile directory under GENERATOR_DIR."""
        return GENERATOR_DIR / self.name

    @property
    def binary_path(self) -> Path:
        return self.install_dir / self.binary_name

    @property
    def manifest_path(self) -> Path:
        return self.install_dir / "manifest.json"


# ---------------------------------------------------------------------------
# Canonical profiles
# ---------------------------------------------------------------------------

LEGACY_V1 = GeneratorProfile(
    name="legacy-v1",
    repo_url="https://github.com/valkey-io/valkey.git",
    commit_sha=VALKEY_BENCHMARK_COMMIT,
    build_args="",
    binary_name="valkey-benchmark",
)

SCALABLE_V2 = GeneratorProfile(
    name="scalable-v2",
    repo_url="https://github.com/valkey-rainfall/valkey.git",
    commit_sha="026288e2aaedc757c3dd8d347c237e669086a948",
    build_args="",
    binary_name="valkey-benchmark",
)

# Range mode: --count and the __rand_beg__/__rand_end__ placeholders (rank or score windows
# for ZRANGE, ZRANGEBYSCORE and similar). Neither legacy-v1 nor scalable-v2 has them: a test
# command that uses them makes those generators exit without a result.
FEATURE_RANGE = "range"

RANGE_V1 = GeneratorProfile(
    name="range-v1",
    repo_url="https://github.com/valkey-rainfall/valkey.git",
    commit_sha="cbc0ae1bfe9410db0abb94f0c7893c5d783ec6af",
    build_args="",
    binary_name="valkey-benchmark",
    features=(FEATURE_RANGE,),
)

# Registry: name -> profile
PROFILES: Dict[str, GeneratorProfile] = {
    LEGACY_V1.name: LEGACY_V1,
    SCALABLE_V2.name: SCALABLE_V2,
    RANGE_V1.name: RANGE_V1,
}

# The profile a test with range placeholders gets when the task names none. Use one profile
# for every engine in a comparison: the generator is part of the workload.
DEFAULT_RANGE_PROFILE = RANGE_V1.name

_RANGE_MARKERS = ("__rand_beg__", "__rand_end__")


def required_generator_features(command: str) -> frozenset[str]:
    """Generator capabilities a valkey-benchmark test command needs."""
    needs = set()
    if any(marker in command for marker in _RANGE_MARKERS) or "--count" in command.split():
        needs.add(FEATURE_RANGE)
    return frozenset(needs)


def effective_generator_profile(command: str, requested: str) -> str:
    """Return the generator profile to run ``command`` with.

    A command without special needs keeps ``requested`` (empty = the legacy default). A command
    that needs a capability gets the default profile that has it when none was requested, and is
    refused when the requested profile lacks it: running it would only fail at once with
    "No results recorded".
    """
    needs = required_generator_features(command)
    if not needs:
        return requested
    if not requested:
        return DEFAULT_RANGE_PROFILE
    try:
        profile = get_profile(requested)
    except KeyError as exc:
        raise ValueError(str(exc.args[0])) from exc
    missing = sorted(needs - set(profile.features))
    if missing:
        have = sorted(name for name, p in PROFILES.items() if set(needs) <= set(p.features))
        raise ValueError(
            f"generator profile {requested!r} lacks {', '.join(missing)} support needed by the test command; "
            f"use one of: {', '.join(have)}"
        )
    return requested


DEFAULT_PROFILE = LEGACY_V1.name


def get_profile(name: str) -> GeneratorProfile:
    """Look up a profile by name.  Raises KeyError if unknown."""
    if name not in PROFILES:
        raise KeyError(f"Unknown generator profile '{name}'. Known: {sorted(PROFILES)}")
    return PROFILES[name]


# ---------------------------------------------------------------------------
# Manifest: persisted build provenance
# ---------------------------------------------------------------------------


@dataclass
class GeneratorManifest:
    """On-disk record of a built generator binary."""

    profile_name: str
    repo_url: str
    commit_sha: str
    build_args: str
    binary_sha256: str

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(self), indent=2) + "\n")
        tmp.rename(path)

    @classmethod
    def load(cls, path: Path) -> Optional["GeneratorManifest"]:
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            return cls(**data)
        except Exception:
            return None


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Bootstrap: build a profile binary if missing or mismatched
# ---------------------------------------------------------------------------


def bootstrap_profile(profile: GeneratorProfile) -> Path:
    """Ensure the profile binary is built and its manifest matches.

    If the binary does not exist or the manifest disagrees with the profile
    definition, a fresh build is performed atomically (build in a temp dir,
    then move).

    Returns the absolute path to the ready binary.
    """
    manifest = GeneratorManifest.load(profile.manifest_path)

    # Fast path: binary exists and manifest matches profile
    if (
        manifest is not None
        and profile.binary_path.exists()
        and manifest.profile_name == profile.name
        and manifest.commit_sha == profile.commit_sha
        and manifest.build_args == profile.build_args
        and manifest.repo_url == profile.repo_url
        and manifest.binary_sha256 == _sha256_file(profile.binary_path)
    ):
        logger.info("Generator %s: cached binary OK (sha256=%s…)", profile.name, manifest.binary_sha256[:12])
        return profile.binary_path

    logger.info("Generator %s: bootstrap build from %s@%s", profile.name, profile.repo_url, profile.commit_sha[:12])

    install_dir = profile.install_dir
    src_dir = install_dir / "src"

    # Clone or reset the source
    if (src_dir / ".git").is_dir():
        _run(["git", "fetch", "--quiet", "origin"], cwd=src_dir)
    else:
        install_dir.mkdir(parents=True, exist_ok=True)
        _run(["git", "clone", "--quiet", profile.repo_url, str(src_dir)])

    _run(["git", "reset", "--hard", profile.commit_sha], cwd=src_dir)

    # Build
    make_cmd = "make distclean && cd src && MAKEFLAGS= make -j valkey-benchmark"
    if profile.build_args:
        make_cmd += f" {profile.build_args}"
    _run(["bash", "-c", make_cmd], cwd=src_dir)

    built_binary = src_dir / "src" / profile.binary_name
    if not built_binary.exists():
        raise FileNotFoundError(f"Build did not produce {built_binary}")

    # Atomic install: copy to final location
    profile.binary_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_binary = profile.binary_path.with_suffix(".tmp")
    _run(["cp", str(built_binary), str(tmp_binary)])
    os.chmod(str(tmp_binary), 0o755)
    tmp_binary.rename(profile.binary_path)

    # Write manifest
    binary_hash = _sha256_file(profile.binary_path)
    new_manifest = GeneratorManifest(
        profile_name=profile.name,
        repo_url=profile.repo_url,
        commit_sha=profile.commit_sha,
        build_args=profile.build_args,
        binary_sha256=binary_hash,
    )
    new_manifest.save(profile.manifest_path)
    logger.info("Generator %s: built OK (sha256=%s…)", profile.name, binary_hash[:12])

    return profile.binary_path


# ---------------------------------------------------------------------------
# Resolution: task -> binary path
# ---------------------------------------------------------------------------


def resolve_bench_binary(
    generator_profile: str = DEFAULT_PROFILE,
    bench_binary_override: str = "",
) -> tuple[str, dict]:
    """Resolve the benchmark binary path and provenance metadata.

    Returns (binary_path, provenance_dict).

    ``bench_binary_override`` takes absolute precedence (custom override).
    Otherwise the named profile is bootstrapped and its binary returned.
    """
    if bench_binary_override:
        provenance = {
            "generator_profile": "custom-override",
            "bench_binary": bench_binary_override,
        }
        return bench_binary_override, provenance

    profile = get_profile(generator_profile)
    binary_path = bootstrap_profile(profile)
    manifest = GeneratorManifest.load(profile.manifest_path)

    provenance = {
        "generator_profile": profile.name,
        "generator_repo": profile.repo_url,
        "generator_commit": profile.commit_sha,
        "generator_build_args": profile.build_args,
        "generator_binary_sha256": manifest.binary_sha256 if manifest else "",
    }
    return str(binary_path), provenance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(cmd: list[str], cwd: Optional[Path] = None) -> str:
    """Run a subprocess, raising on failure."""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr}")
    return result.stdout
