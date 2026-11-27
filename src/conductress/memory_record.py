"""What a memory task records about the server it measured, and how the exporter reads it.

The memory task measures used_memory per item. A number is only comparable between
engines, versions and runs if the record also says WHAT was measured: which server
binary answered, whether the allocator was profiling (which inflates RSS about 100x),
whether the structures had settled, and how much memory the process held from the
operating system's point of view.

Everything here is black box: it reads what a server reports (INFO, MEMORY MALLOC-STATS,
CONFIG GET, OBJECT ENCODING), what the binary prints (--version, a file hash) and what
the kernel says about the process (/proc/<pid>/status). A field an engine cannot answer
is recorded as None. A missing optional field never fails a task.

Units: every byte count is an int of bytes; "per item" values are floats.
"""

from __future__ import annotations

import logging
import re
import shlex
from functools import partial
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

RECORD_SCHEMA = 1

# /proc/<pid>/status lines (value in kB) -> record key. VmRSS is the resident set the kernel
# reports for the process; RssAnon is its anonymous part (heap), which excludes the binary's
# own file-backed pages.
PROC_STATUS_FIELDS = {"VmRSS": "vm_rss", "RssAnon": "rss_anon", "RssFile": "rss_file", "RssShmem": "rss_shmem"}

# CONFIG GET patterns for settings that change how much memory a structure needs.
CONFIG_PATTERNS = (
    "hash-max-listpack-*",
    "hash-max-ziplist-*",
    "zset-max-*",
    "set-max-*",
    "list-max-*",
    "activerehashing",
    "hz",
    "lazyfree-*",
    "maxmemory*",
)

# Where the per-item RSS comes from, recorded with the number.
RSS_SOURCE_PROC = "proc_vmrss"
RSS_SOURCE_INFO = "info_used_memory_rss"

COLLECTION_KEYS = {"sadd": "myset", "hset": "myhash", "zadd": "myzset"}


def parse_proc_status(text: Optional[str]) -> dict[str, int]:
    """Byte counts from the text of /proc/<pid>/status; absent or garbled lines are skipped."""
    fields: dict[str, int] = {}
    for line in (text if isinstance(text, str) else "").splitlines():
        name, sep, rest = line.partition(":")
        key = PROC_STATUS_FIELDS.get(name.strip())
        if not sep or key is None:
            continue
        parts = rest.split()
        if len(parts) >= 1 and parts[0].isdigit():
            unit = parts[1].lower() if len(parts) > 1 else "b"
            if unit == "kb":
                fields[key] = int(parts[0]) * 1024
            elif unit == "b":
                fields[key] = int(parts[0])
    return fields


def engine_identity(info_server: Optional[dict[str, str]]) -> dict[str, Optional[str]]:
    """Server name, version, executable name and git sha from an INFO server section.

    The version is valkey_version when the server reports one (a Valkey server still shows a
    compatibility redis_version), otherwise redis_version. The executable is reduced to its
    file name so no host path is stored. Missing fields are None; nothing is inferred from
    the binary name.
    """
    info = info_server if isinstance(info_server, dict) else {}
    executable = info.get("executable")
    return {
        "server_name": info.get("server_name") or None,
        "version": info.get("valkey_version") or info.get("redis_version") or None,
        "executable": Path(executable).name if executable else None,
        "git_sha1": info.get("redis_git_sha1") or info.get("valkey_git_sha1") or None,
    }


def parse_version_text(text: Optional[str]) -> Optional[str]:
    """The first non-empty line of `<binary> --version`, capped at 200 characters."""
    for line in (text if isinstance(text, str) else "").splitlines():
        line = line.strip()
        if line:
            return line[:200]
    return None


_OPT_PROF = re.compile(r"opt\.prof:\s*(true|false)")
_CONFIG_PROF = re.compile(r"config\.prof:\s*(true|false)")


def profiling_active(malloc_stats: Optional[str]) -> Optional[bool]:
    """Whether the allocator was profiling at runtime, from MEMORY MALLOC-STATS text.

    True or False when the text states it (jemalloc prints opt.prof); None ("unknown") for
    an engine or allocator that does not say. A build without profiling support
    (config.prof false) is False.
    """
    text = malloc_stats if isinstance(malloc_stats, str) else ""
    match = _OPT_PROF.search(text)
    if match:
        return match.group(1) == "true"
    match = _CONFIG_PROF.search(text)
    if match and match.group(1) == "false":
        return False
    return None


def parse_config_get(text: Optional[str]) -> dict[str, str]:
    """name -> value from the raw (alternating lines) output of CONFIG GET."""
    lines = [line.strip() for line in (text if isinstance(text, str) else "").splitlines() if line.strip()]
    return {lines[i]: lines[i + 1] for i in range(0, len(lines) - 1, 2)}


def per_item(after: Optional[int], before: Optional[int], item_count: int) -> Optional[float]:
    """(after - before) / item_count, or None when a reading is missing."""
    if after is None or before is None or item_count <= 0:
        return None
    return (after - before) / item_count


def sample_values(info_memory: Optional[dict[str, str]], proc: Optional[dict[str, int]]) -> dict[str, Optional[int]]:
    """One snapshot: used_memory and used_memory_rss from INFO, VmRSS and RssAnon from /proc."""
    info = info_memory if isinstance(info_memory, dict) else {}
    proc = proc if isinstance(proc, dict) else {}

    def _int(value: Optional[str]) -> Optional[int]:
        try:
            return int(value) if value is not None else None
        except ValueError:
            return None

    return {
        "used_memory": _int(info.get("used_memory")),
        "used_memory_rss": _int(info.get("used_memory_rss")),
        "vm_rss": proc.get("vm_rss"),
        "rss_anon": proc.get("rss_anon"),
    }


def rss_per_item(
    before: dict[str, Optional[int]],
    after: dict[str, Optional[int]],
    item_count: int,
    profiled: Optional[bool],
) -> tuple[Optional[float], Optional[str]]:
    """(RSS bytes per item, source) from two snapshots; (None, None) when it is not valid.

    /proc VmRSS is preferred because every engine has it. INFO used_memory_rss is the fallback
    for a /proc that could not be read. A run with the allocator profiling (or not known to be
    without it) has an RSS inflated by profiler metadata: no value.
    """
    if profiled is not False:
        return None, None
    value = per_item(after.get("vm_rss"), before.get("vm_rss"), item_count)
    if value is not None:
        return value, RSS_SOURCE_PROC
    value = per_item(after.get("used_memory_rss"), before.get("used_memory_rss"), item_count)
    if value is not None:
        return value, RSS_SOURCE_INFO
    return None, None


def build_memory_record(
    *,
    engine: dict[str, Optional[str]],
    binary_sha256: Optional[str],
    version_text: Optional[str],
    profiling_requested: bool,
    profiling_active_at_runtime: Optional[bool],
    settle_requested: bool,
    settle_passes: Optional[int],
    settle_key_check: Optional[str],
    settle_stability: Optional[str],
    encoding: Optional[str],
    config: dict[str, str],
    item_count: int,
    samples: dict[str, dict[str, Optional[int]]],
    conductress_revision: Optional[str],
    settle_rehash_check: bool = True,
) -> dict[str, Any]:
    """The per-task record stored in the task result under ``memory_record``.

    ``samples`` has ``before_fill``, ``after_fill`` and ``after_settle`` (the last equals
    after_fill when settle was off). Totals and RSS are taken between before_fill and the last
    sample. A run counts as profiled when the allocator reported profiling, or, when it could
    not say, when profiling was requested. ``settle_rehash_check`` is False for an engine settled without a
    DEBUG HTSTATS rehash assertion; the record then carries ``settle.rehash_check: false`` (absent means it ran).
    """
    last = samples.get("after_settle") or samples.get("after_fill") or {}
    before = samples.get("before_fill") or {}
    profiled = profiling_active_at_runtime if profiling_active_at_runtime is not None else profiling_requested
    rss, rss_source = rss_per_item(before, last, item_count, profiled)
    settle = {
        "requested": settle_requested,
        "settled": bool(settle_requested),
        "passes": settle_passes,
        "key_check": settle_key_check,
        "stability": settle_stability,
    }
    if not settle_rehash_check:
        settle["rehash_check"] = False
    return {
        "schema": RECORD_SCHEMA,
        "engine": engine,
        "binary": {"sha256": binary_sha256, "version_text": version_text},
        "profiling": {"requested": profiling_requested, "active": profiling_active_at_runtime, "profiled": profiled},
        "settle": settle,
        "encoding": encoding,
        "config": config,
        "item_count": item_count,
        "samples": samples,
        "total_bytes_per_item": per_item(last.get("used_memory"), before.get("used_memory"), item_count),
        "rss_bytes_per_item": rss,
        "rss_source": rss_source,
        "conductress_revision": conductress_revision,
    }


# --- Collectors (talk to a server handle; every step is optional) -----------------------------

_Step = Callable[[], Awaitable[Any]]


async def _optional(label: str, step: _Step) -> Any:
    """Run one collection step; any failure is logged and gives None."""
    try:
        return await step()
    except Exception as exc:  # a missing optional field never fails the task
        logger.warning("memory record: %s not recorded (%s)", label, exc)
        return None


async def read_proc_status(server: Any) -> dict[str, int]:
    """VmRSS and friends of the server process, read on the server host. {} if unreadable."""
    pid = getattr(server, "valkey_pid", -1)
    if not isinstance(pid, int) or pid <= 0:
        return {}

    async def _read() -> dict[str, int]:
        out, _ = await server.run_host_command(f"cat /proc/{pid}/status", check=False)
        return parse_proc_status(out)

    return (await _optional("proc status", _read)) or {}


async def take_sample(server: Any) -> dict[str, Optional[int]]:
    """INFO memory plus /proc, as one snapshot."""
    info = await _optional("info memory", lambda: server.info("memory"))
    proc = await read_proc_status(server)
    return sample_values(info, proc)


def shell_path(path: Path) -> str:
    """A remote path as a shell word. A leading "~/" stays unquoted so the remote shell expands it.

    The binary cache lives under "~/build_cache/...". shlex.quote alone wraps the whole path in single quotes, the
    shell then does not expand "~", and the command fails (every binary sha256 and --version came back null).
    """
    text = str(path)
    if text == "~":
        return "~"
    if text.startswith("~/"):
        return "~/" + shlex.quote(text[2:])
    return shlex.quote(text)


async def collect_identity(server: Any, binary_path: Path) -> dict[str, Any]:
    """Engine identity from INFO server, the binary's sha256 and its --version text."""
    info = await _optional("info server", lambda: server.info("server"))

    async def _sha() -> Optional[str]:
        out, _ = await server.run_host_command(f"sha256sum {shell_path(binary_path)}", check=False)
        token = (out if isinstance(out, str) else "").split()
        return token[0] if token and re.fullmatch(r"[0-9a-f]{64}", token[0]) else None

    async def _version() -> Optional[str]:
        out, _ = await server.run_host_command(
            f"LD_LIBRARY_PATH={shell_path(binary_path.parent)} {shell_path(binary_path)} --version", check=False
        )
        return parse_version_text(out)

    return {
        "engine": engine_identity(info),
        "binary_sha256": await _optional("binary sha256", _sha),
        "version_text": await _optional("binary version", _version),
    }


async def collect_profiling_state(server: Any) -> Optional[bool]:
    """Whether the allocator was profiling at runtime; None when the engine cannot say."""
    text = await _optional("malloc stats", lambda: server.run_valkey_command("MEMORY MALLOC-STATS"))
    return profiling_active(text)


async def collect_config(server: Any) -> dict[str, str]:
    """CONFIG GET for the memory-relevant settings the engine knows. {} when unavailable."""
    config: dict[str, str] = {}
    for pattern in CONFIG_PATTERNS:
        command = f"CONFIG GET {pattern}"
        text = await _optional(f"config {pattern}", partial(server.run_valkey_command, command))
        config.update(parse_config_get(text))
    return config


async def collect_encoding(server: Any, test: str) -> Optional[str]:
    """OBJECT ENCODING of the collection key (None for SET, which has no collection key)."""
    key = COLLECTION_KEYS.get(test)
    if key is None:
        return None
    text = await _optional("object encoding", lambda: server.run_valkey_command(f"OBJECT ENCODING {key}"))
    if not isinstance(text, str) or not text:
        return None
    name = text.strip().strip('"').strip().lower()
    return None if not name or name.startswith("err") or " " in name else name


def conductress_revision() -> Optional[str]:
    """The harness revision of this runner; None when it cannot be determined."""
    try:
        from conductress.runner_identity import get_runner_info

        revision = get_runner_info().get("environment", {}).get("conductress_revision")
    except Exception:
        return None
    return revision if isinstance(revision, str) and revision and revision != "unknown" else None


# --- Exporter side ------------------------------------------------------------------------------

REVISION_PUBLIC_CHARS = 12


def export_memory_fields(
    record: Optional[dict[str, Any]], overhead: Optional[float], user_data_bytes: int
) -> dict[str, Any]:
    """The point-level ``memory`` object of a published memory series.

    A point recorded before the record existed has every identity field None and settled None
    (unknown); its total is rebuilt from the published overhead and the series' user data, which
    is the definition of the overhead. The harness revision is published as a 12-character prefix
    only; the full value stays in the sweep state.
    """
    rec = record or {}
    engine = rec.get("engine") or {}
    binary = rec.get("binary") or {}
    settle = rec.get("settle") or {}
    profiling = rec.get("profiling") or {}
    revision = rec.get("conductress_revision")
    total = rec.get("total_bytes_per_item")
    if total is None and overhead is not None:
        total = overhead + user_data_bytes
    return {
        "total_bytes_per_item": total,
        "rss_bytes_per_item": rec.get("rss_bytes_per_item"),
        "rss_source": rec.get("rss_source"),
        "n": rec.get("item_count"),
        "settled": settle.get("settled") if record else None,
        "settle_passes": settle.get("passes"),
        "profiled": profiling.get("profiled"),
        "engine_server_name": engine.get("server_name"),
        "engine_version": engine.get("version"),
        "binary_sha256": binary.get("sha256"),
        "conductress_revision": revision[:REVISION_PUBLIC_CHARS] if isinstance(revision, str) else None,
    }
