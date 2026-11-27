"""Tests memory efficiency of the specified type. Result is bytes of overhead per item."""

import asyncio
import datetime
import logging
import re
import time
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Optional, Sequence

import plotext as plt

from conductress import config
from conductress.config import (
    MEM_TEST_ITEM_COUNT,
    MEM_TEST_KEY_SIZE,
    MEM_TEST_MAX_CONCURRENT,
    ServerInfo,
    get_sweep_engine,
    should_profile_internals,
)
from conductress.engines import DEFAULT_ENGINE, get_engine, non_valkey_sources
from conductress.file_protocol import BenchmarkResults, BenchmarkStatus
from conductress.heap_profiler import (
    JEMALLOC_PROF_CONFIGURE_OPTS,
    JEMALLOC_PROF_ENV,
    HeapProfileResult,
    cleanup_heap_dumps,
    collect_heap_profile,
)
from conductress.memory_record import (
    build_memory_record,
    collect_config,
    collect_encoding,
    collect_identity,
    collect_profiling_state,
    conductress_revision,
    read_proc_status,
    sample_values,
    take_sample,
)
from conductress.prebuilt_engine import ManifestError, parse_specifier
from conductress.server import Server
from conductress.sweep.populator import populate, touch
from conductress.task_queue import BaseTaskData, BaseTaskRunner
from conductress.utility import HumanByte, port_generator, print_pretty_header

logger = logging.getLogger(__name__)


# --- Quiesce-until-stable (opt-in) -------------------------------------------
# Some features reclaim memory asynchronously in the background (e.g. zset
# load-factor compaction, active-defrag). Sampling used_memory the instant a
# populate finishes measures a transient, not the steady state. When enabled,
# the memory task polls used_memory until it plateaus before sampling.
SETTLE_POLL_INTERVAL_S = 1.0
SETTLE_STABLE_SAMPLES = 3
SETTLE_REL_EPS = 0.001  # 0.1%: consecutive samples this close count as "stable"
SETTLE_MAX_SECONDS = 180.0


def _window_stable(samples: "Sequence[float]", rel_eps: float) -> bool:
    """True if the spread (max-min) across `samples` is within `rel_eps` of the mean.

    A windowed criterion (rather than pairwise consecutive deltas) absorbs bounded
    jitter *and* catches slow monotonic drift: a steady creep smaller than the
    per-poll threshold still accumulates past `rel_eps` across the window.
    """
    if not samples:
        return False
    spread = max(samples) - min(samples)
    mean = sum(samples) / len(samples)
    return spread <= max(1.0, mean) * rel_eps


async def quiesce_until_stable(
    sample_fn: Callable[[], Awaitable[int]],
    *,
    interval: float = SETTLE_POLL_INTERVAL_S,
    stable_samples: int = SETTLE_STABLE_SAMPLES,
    rel_eps: float = SETTLE_REL_EPS,
    max_seconds: float = SETTLE_MAX_SECONDS,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[int, float]:
    """Poll `sample_fn` until its value plateaus, returning (final_value, elapsed_s).

    Stops once the spread (max-min) across the last `stable_samples + 1` readings is
    within `rel_eps` of their mean, or when `max_seconds` elapses (whichever first).
    The windowed spread test absorbs bounded noise while still catching a slow drift
    that a pairwise consecutive-delta test would miss. Injecting `sample_fn`/`sleep_fn`
    keeps this pure and unit-testable without a server.
    """
    window: "deque[int]" = deque(maxlen=stable_samples + 1)
    value = await sample_fn()
    window.append(value)
    elapsed = 0.0
    while elapsed < max_seconds:
        await sleep_fn(interval)
        elapsed += interval
        value = await sample_fn()
        window.append(value)
        if len(window) == window.maxlen and _window_stable(list(window), rel_eps):
            break
    return value, elapsed


# --- Rehash settle-and-assert (part of `settle`) ------------------------------
# A bulk fill leaves a hash table mid-rehash: both tables allocated, plus the
# collision entries of the crowded old table. The keyspace finishes its rehash in
# the background (activerehashing), but a collection's own table only advances on
# access, so a memory sample taken right after the fill measures a load-order
# artifact rather than the structure's steady-state size. When `settle` is on, the
# task reads every item once (one rehash step each) and asserts via DEBUG HTSTATS
# that no rehash is still in progress before sampling.
SETTLE_REHASH_MAX_PASSES = 3
REHASHING_MARKER = "rehashing target"  # printed by dict.c and hashtable.c stats only mid-rehash
COLLECTION_KEYS = {"sadd": "myset", "hset": "myhash", "zadd": "myzset"}


def htstats_command(test: str) -> str:
    """DEBUG command whose output names the table(s) the workload fills."""
    if test in COLLECTION_KEYS:
        return f"DEBUG HTSTATS-KEY {COLLECTION_KEYS[test]}"
    return "DEBUG HTSTATS 0"  # keyspace (and expires) of db 0


def is_rehashing(stats: Optional[str]) -> bool:
    """True if a DEBUG HTSTATS / HTSTATS-KEY response shows a rehash in progress."""
    if stats is None:
        raise RuntimeError("DEBUG HTSTATS returned nothing; is enable-debug-command set on the test server?")
    if stats.startswith("ERR"):
        raise RuntimeError(f"DEBUG HTSTATS failed: {stats}")
    return REHASHING_MARKER in stats


async def settle_rehash(
    stats_fn: Callable[[], Awaitable[Optional[str]]],
    touch_fn: Callable[[], Awaitable[int]],
    *,
    max_passes: int = SETTLE_REHASH_MAX_PASSES,
) -> int:
    """Drive any in-progress rehash to completion; return the number of read passes used.

    Checks first, so an already-settled table costs one DEBUG call. Raises if the
    table is still rehashing after `max_passes` full read passes, so a partial state
    is never recorded as settled.
    """
    passes = 0
    while is_rehashing(await stats_fn()):
        if passes >= max_passes:
            raise RuntimeError(f"hash table still rehashing after {passes} full read passes")
        await touch_fn()
        passes += 1
    return passes


# Error text of DEBUG HTSTATS-KEY when the key's value is not a hash table (a different
# encoding, such as a tree or a compact list). Seen in the wild: "not represented using
# hash table" and "not represented using an hash table". Matched case-insensitively on the
# stable part, with or without an article; every other error still fails the task.
NOT_HASHTABLE_RE = re.compile(r"not represented using\s+(?:an?\s+|the\s+)?hash\s*table", re.IGNORECASE)
KEY_CHECK_HASHTABLE = "hashtable"
KEY_CHECK_NOT_HASHTABLE = "not_hashtable"
KEY_CHECK_NOT_APPLICABLE = "not_applicable"  # neutral settle: the engine has no key-level table check

# OBJECT ENCODING answers, asked before DEBUG HTSTATS-KEY. Only these are decided up front:
# a hash table (a skiplist set keeps one) is checked, a compact or tree encoding has no
# key-level table to wait for. An encoding in neither list is not guessed: the key-level
# check runs and its error reply decides.
HASHTABLE_ENCODINGS = frozenset({"hashtable", "skiplist"})
NON_HASHTABLE_ENCODINGS = frozenset({"listpack", "listpackex", "intset", "btree", "quicklist"})


def is_not_hashtable(stats: Optional[str]) -> bool:
    """True if DEBUG HTSTATS-KEY answered that the key is not stored as a hash table."""
    return stats is not None and stats.startswith("ERR") and NOT_HASHTABLE_RE.search(stats) is not None


def normalize_encoding(reply: Optional[str]) -> Optional[str]:
    """The encoding name from an OBJECT ENCODING reply, or None if the reply is not a name."""
    if reply is None:
        return None
    name = reply.strip().strip('"').strip().lower()
    if not name or name.startswith("err") or name.startswith("(") or " " in name:
        return None
    return name


async def settle_rehash_for_key(
    key_stats_fn: Callable[[], Awaitable[Optional[str]]],
    keyspace_stats_fn: Callable[[], Awaitable[Optional[str]]],
    touch_fn: Callable[[], Awaitable[int]],
    *,
    encoding_fn: Optional[Callable[[], Awaitable[Optional[str]]]] = None,
    max_passes: int = SETTLE_REHASH_MAX_PASSES,
) -> tuple[int, str, Optional[str]]:
    """Settle the rehash of a collection key; return (read passes, key check, key encoding).

    First ask OBJECT ENCODING (if `encoding_fn` is given). An encoding that is not hash table
    based (listpack, btree, ...) has no key-level rehash to wait for. Otherwise run DEBUG
    HTSTATS-KEY: a hash table is settled with `settle_rehash` (key check "hashtable"), and the
    reply "not represented using a hash table" gives the same result as a skipped key. When the
    key has no key-level table the key check is "not_hashtable" and only the keyspace table
    (DEBUG HTSTATS 0) is settled. Any other DEBUG error still raises.
    """
    encoding = normalize_encoding(await encoding_fn()) if encoding_fn else None
    if encoding in NON_HASHTABLE_ENCODINGS:
        passes = await settle_rehash(keyspace_stats_fn, touch_fn, max_passes=max_passes)
        return passes, KEY_CHECK_NOT_HASHTABLE, encoding
    first = await key_stats_fn()
    if is_not_hashtable(first):
        return (
            await settle_rehash(keyspace_stats_fn, touch_fn, max_passes=max_passes),
            KEY_CHECK_NOT_HASHTABLE,
            encoding,
        )
    unread = [first]  # reuse the reading we already have, so a settled key still costs one DEBUG call

    async def _stats() -> Optional[str]:
        return unread.pop() if unread else await key_stats_fn()

    return await settle_rehash(_stats, touch_fn, max_passes=max_passes), KEY_CHECK_HASHTABLE, encoding


# --- Engine-neutral memory stability check (part of `settle`) -------------------
# DEBUG HTSTATS only exists for hash tables, so it cannot tell that a different encoding
# (or a background reclaim) has finished. After the read pass, poll the numbers every engine
# reports -- INFO used_memory, plus any MEMORY STATS field about hash table or rehash overhead
# the engine has -- until they stop moving. Defaults keep a task short: one sample a second,
# 3 consecutive stable readings (4 samples within 0.1% of each other), give up after 60 s.
SETTLE_STABILITY_INTERVAL_S = 1.0
SETTLE_STABILITY_SAMPLES = 3
SETTLE_STABILITY_REL_EPS = 0.001
SETTLE_STABILITY_MAX_SECONDS = 60.0
STABILITY_STABLE = "stable"
STABILITY_TIMEOUT = "timeout"


def parse_memory_stats_rehash_fields(text: Optional[str]) -> dict[str, float]:
    """Numeric MEMORY STATS fields about hash table or rehash overhead, from valkey-cli raw output.

    The output is name and value on alternating lines, with nested maps (per database) flattened
    in between, so a line counts as a name only when the next line is a number. Engines without
    such fields give an empty dict.
    """
    fields: dict[str, float] = {}
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    i = 0
    while i + 1 < len(lines):
        name, value = lines[i], lines[i + 1]
        try:
            number = float(value)
        except ValueError:
            i += 1
            continue
        lowered = name.lower()
        if "rehash" in lowered or lowered.startswith("overhead.hashtable"):
            fields[name] = number
        i += 2
    return fields


async def check_memory_stability(
    sample_fn: Callable[[], Awaitable[dict[str, float]]],
    *,
    interval: float = SETTLE_STABILITY_INTERVAL_S,
    stable_samples: int = SETTLE_STABILITY_SAMPLES,
    rel_eps: float = SETTLE_STABILITY_REL_EPS,
    max_seconds: float = SETTLE_STABILITY_MAX_SECONDS,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    """Poll `sample_fn` (name -> number) until every series is stable; return a record.

    Stable means the last `stable_samples + 1` samples of each series (every series the samples
    share) stay within `rel_eps` of their mean. The record has the verdict ("stable" or
    "timeout"), every sample, the series names and the elapsed seconds. A timeout is recorded,
    not raised: the caller decides what an unsettled measurement means.
    """
    samples: list[dict[str, float]] = [await sample_fn()]
    elapsed = 0.0
    verdict = STABILITY_TIMEOUT
    window = stable_samples + 1
    while True:
        recent = samples[-window:]
        if len(recent) == window:
            names = set(recent[0]).intersection(*recent[1:])
            if names and all(_window_stable([r[n] for r in recent], rel_eps) for n in names):
                verdict = STABILITY_STABLE
                break
        if elapsed >= max_seconds:
            break
        await sleep_fn(interval)
        elapsed += interval
        samples.append(await sample_fn())
    return {
        "verdict": verdict,
        "elapsed_s": elapsed,
        "series": sorted(set().union(*samples)),
        "samples": samples,
        "interval_s": interval,
        "stable_samples": stable_samples,
        "rel_eps": rel_eps,
        "max_seconds": max_seconds,
    }


@dataclass
class MemTaskData(BaseTaskData):
    """data class for memory efficiency task"""

    type: str
    val_sizes: list[int]
    has_expire: bool
    sweep_commit: str = ""  # non-empty marks this as a sweep task
    enable_profiling: bool = False  # enable jemalloc heap profiling for per-struct breakdown
    key_size: int = 0  # key size in bytes (SET) or 0 (single-key commands); set by coordinator
    field_size: int = 0  # field name size (HSET only)
    user_data_bytes: int = 0  # per-item user data for overhead calculation
    populate_mode: str = "random"  # zadd insertion pattern: random | sequential | churn
    settle: bool = False  # opt-in: quiesce until used_memory plateaus before sampling
    # Items to fill. Only a manual task may change it; a sweep task (sweep_commit set) keeps the default.
    item_count: int = MEM_TEST_ITEM_COUNT
    # Engine profile (conductress.engines): how the server is started and settled. Left out of the saved
    # document while it is the default, so a Valkey or Redis task is serialized exactly as before.
    engine: str = DEFAULT_ENGINE
    _OMIT_WHEN_DEFAULT: ClassVar[tuple[str, ...]] = ("engine",)

    def _extra_sources(self) -> tuple[str, ...]:
        return (config.PREBUILT_ENGINE,)

    def __post_init__(self):
        super().__post_init__()
        get_engine(self.engine)  # ValueError for an unknown name
        if self.source == config.PREBUILT_ENGINE:
            try:
                named_engine, _ = parse_specifier(self.specifier)
            except ManifestError as exc:
                raise ValueError(str(exc)) from exc
            if named_engine != self.engine:
                raise ValueError(f"specifier {self.specifier!r} names {named_engine}, the task engine is {self.engine}")
        if self.engine != DEFAULT_ENGINE:
            if self.source not in non_valkey_sources():
                raise ValueError(
                    f"engine {self.engine} needs a source in {list(non_valkey_sources())}, got {self.source}"
                )
            if self.sweep_commit:
                raise ValueError("a sweep task runs the default engine")

    def short_description(self) -> str:
        description = self.type
        if len(self.val_sizes) > 1:
            description += f" {len(self.val_sizes)} sizes"
        else:
            description += f" {HumanByte.to_human(self.val_sizes[0])}"
        if self.has_expire:
            description += " with expiration"
        return description

    def prepare_task_runner(self, server_infos: list[ServerInfo]) -> "MemTaskRunner":
        """Return the task runner for this task."""
        if self.sweep_commit and self.item_count != MEM_TEST_ITEM_COUNT:
            raise ValueError(f"a sweep task keeps the default item count {MEM_TEST_ITEM_COUNT}, got {self.item_count}")
        return MemTaskRunner(
            self.task_id,
            server_infos[0].ip,
            self.source,
            self.specifier,
            self.type,
            self.val_sizes,
            self.has_expire,
            self.make_args,
            self.note,
            self.enable_profiling,
            self.key_size,
            self.field_size,
            self.user_data_bytes,
            self.populate_mode,
            self.settle,
            self.item_count,
            rehash_check=get_engine(self.engine).rehash_check,
            engine=self.engine,
        )


class MemTaskRunner(BaseTaskRunner):
    """Tests memory efficiency of the specified type. Result is bytes of overhead per item."""

    tests = ["set", "sadd", "zadd", "hset"]

    def __init__(
        self,
        task_name: str,
        server_ip: str,
        source: str,
        specifier: str,
        test: str,
        val_sizes: list[int],
        has_expire: bool,
        make_args: str,
        note: str,
        enable_profiling: bool = False,
        key_size: int = 0,
        field_size: int = 0,
        user_data_bytes: int = 0,
        populate_mode: str = "random",
        settle: bool = False,
        item_count: Optional[int] = None,
        rehash_check: Optional[bool] = None,
        engine: str = DEFAULT_ENGINE,
    ):
        super().__init__(task_name)

        self.logger = logging.getLogger(self.__class__.__name__ + "." + test)

        self.title = f"{test} memory efficiency, {source}:{specifier}, has_expire={has_expire}"

        if test not in self.tests:
            raise ValueError(f"Test {test} is not supported. Supported tests: {self.tests}")

        # settings
        self.server_ip = server_ip
        self.source = source
        self.specifier = specifier
        self.test = test
        self.val_sizes = val_sizes
        self.has_expire = has_expire
        self.note = note
        self.make_args = make_args
        self.enable_profiling = enable_profiling
        self.key_size = key_size
        self.field_size = field_size
        self.user_data_bytes = user_data_bytes
        self.populate_mode = populate_mode
        self.settle = settle
        self.engine = engine
        self._profile = get_engine(engine)
        # False for an engine without DEBUG HTSTATS: the settle then makes no rehash assertion (see below).
        self.rehash_check = self._profile.rehash_check if rehash_check is None else rehash_check
        self.item_count = item_count if item_count is not None else MEM_TEST_ITEM_COUNT

        # test data
        self.prebuilt_info: Optional[dict] = None  # set when the source is prebuilt_engine
        self.commit_hash: Optional[str] = None
        self.cached_binary_path: Optional[Path] = None

        # Initialize status
        self.status = BenchmarkStatus(
            steps_total=len(self.val_sizes) + 2, task_type=f"mem-{test}"
        )  # setup + sizes + results

    async def run(self) -> None:
        """Run the memory efficiency test for each value size."""
        print_pretty_header(self.title)
        self.logger.info(f"Val Sizes: {', '.join(HumanByte.to_human(size) for size in self.val_sizes)}")

        # Write initial status
        self.file_protocol.write_status(self.status)

        cpus_per_server = Server.get_num_cpus(1) if self._profile.valkey_launch else self._profile.cpus_needed(1)
        num_servers: int = await self._host().get_available_cpu_count() // cpus_per_server
        num_servers = MEM_TEST_MAX_CONCURRENT if num_servers > MEM_TEST_MAX_CONCURRENT else num_servers

        self.logger.info("Ensuring binary is ready")
        await self._host().kill_all_valkey_instances_on_host()
        host = self._host()
        self.cached_binary_path = await host.ensure_binary_cached(
            source=self.source, specifier=self.specifier, make_args=self.make_args
        )
        if self.source == config.PREBUILT_ENGINE:
            self.prebuilt_info = host.prebuilt_info()
        self.logger.info(f"Binary ready! Testing with up to {num_servers} instances on server")

        # Update progress: setup complete
        self.status.state = "running"
        self.status.steps_completed = 1
        self.file_protocol.write_status(self.status)

        port_gen = port_generator()
        semaphore = asyncio.Semaphore(num_servers)
        result_futures: list[Coroutine] = []
        for size in self.val_sizes:
            port = next(port_gen)
            result_futures.append(self.test_single_size_overhead(size, port, semaphore))

        efficiency_map: dict[int, float] = {}
        results = []

        completed_sizes = 0
        for future in asyncio.as_completed(result_futures):
            point_result: dict[str, float] = await future
            results.append(point_result)
            val_size = int(point_result["val_size"])
            per_item_overhead = point_result["per_item_overhead"]
            efficiency_map[val_size] = per_item_overhead
            self.plot(efficiency_map)

            # Log metric data point
            from conductress.file_protocol import MetricData

            metric = MetricData(
                metrics={
                    "val_size": float(val_size),
                    "per_item_overhead": per_item_overhead,
                }
            )
            self.file_protocol.append_metric(metric)

            # Update progress for each completed size
            completed_sizes += 1
            self.status.steps_completed = 1 + completed_sizes
            self.file_protocol.write_status(self.status)

        # output result
        results.sort(key=lambda x: x["val_size"])
        score: float = -1.0
        if len(results) == 1:
            score = results[0]["per_item_overhead"]
        # Write results to file protocol (replaces record_task_result)
        detailed_results = {
            "results": results,
            "management_cpus": self.management_cpus,
        }
        completion_time = datetime.datetime.now()
        results_data = BenchmarkResults(
            method=f"mem-{self.test}",
            source=self.source,
            specifier=self.specifier,
            commit_hash=self.commit_hash or "",
            score=score,
            end_time=completion_time,
            data=detailed_results,
            make_args=self.make_args,
            note=self.note,
        )
        self.file_protocol.write_results(results_data)

        # Update progress: results complete
        self.status.state = "completed"
        self.status.steps_completed = self.status.steps_total
        self.status.end_time = time.time()
        self.file_protocol.write_status(self.status)

        await self._host().kill_all_valkey_instances_on_host()

    def _host(self) -> Server:
        """A Server handle for host-wide commands (kill, binary cache). The Valkey call is the one it always was."""
        return Server(self.server_ip) if self._profile.valkey_launch else Server(self.server_ip, engine=self.engine)

    async def test_single_size_overhead(self, val_size: int, port: int, semaphore) -> dict[str, Any]:
        """Test memory efficiency for a single item size."""
        async with semaphore:
            if self.cached_binary_path is None:
                raise RuntimeError("cached_binary_path must be set before calling test_single_size_overhead")

            # Set up profiling env if enabled
            env_prefix = ""
            if self.enable_profiling:
                ssh_host = Server(self.server_ip)
                await cleanup_heap_dumps(ssh_host)
                env_prefix = JEMALLOC_PROF_ENV

            # Settling asserts rehash completion through DEBUG HTSTATS, which the
            # server only answers when the debug command is enabled at startup.
            # The engine is passed only when it is not the default, so a Valkey call is the one it always was.
            engine_kwargs = {} if self._profile.valkey_launch else {"engine": self.engine}
            valkey = await Server.with_path(
                self.server_ip,
                port,
                self.cached_binary_path,
                io_threads=1,
                env_prefix=env_prefix,
                server_args="--enable-debug-command yes" if self.settle and self.rehash_check else "",
                **engine_kwargs,
            )
            self.commit_hash = valkey.get_build_hash()

            before_memory: dict[str, str] = await valkey.info("memory")
            sample_before = sample_values(before_memory, await read_proc_status(valkey))

            # What this run measures: the engine and binary that answered, and whether the
            # allocator was profiling. Every step is optional; None means "not reported".
            identity = await collect_identity(valkey, self.cached_binary_path)
            profiling_state = await collect_profiling_state(valkey)

            count = self.item_count
            from conductress.sweep.memory_coordinator import MemoryWorkload  # noqa: E402 (circular import)

            workload = MemoryWorkload(
                command=self.test,
                key_size=self.key_size,
                value_size=val_size,
                field_size=self.field_size,
                has_expire=self.has_expire,
                label=f"{self.test}-populator",
                item_count=count,
                user_data_bytes=self.user_data_bytes,
                populate_mode=self.populate_mode,
            )
            populate(valkey.ip, valkey.port, workload)
            sample_filled = await take_sample(valkey)

            # Opt-in: let background memory reclamation (e.g. zset compaction) settle
            # so we sample steady-state memory rather than the post-populate transient.
            rehash_passes: Optional[int] = None
            settle_key_check: Optional[str] = None
            settle_key_encoding: Optional[str] = None
            settle_stability: Optional[dict[str, Any]] = None
            if self.settle:

                async def _sample_used_memory() -> int:
                    info = await valkey.info("memory")
                    return int(info["used_memory"])

                settled_bytes, settle_secs = await quiesce_until_stable(_sample_used_memory)
                self.logger.info(
                    "Quiesce: used_memory settled at %d bytes after %.0fs (port %d)",
                    settled_bytes,
                    settle_secs,
                    port,
                )

                if self.rehash_check:
                    # Finish any rehash the fill left behind, then assert none remains.
                    async def _stats() -> Optional[str]:
                        return await valkey.run_valkey_command(htstats_command(self.test))

                    async def _keyspace_stats() -> Optional[str]:
                        return await valkey.run_valkey_command("DEBUG HTSTATS 0")

                    async def _touch() -> int:
                        return await asyncio.to_thread(touch, valkey.ip, valkey.port, workload)

                    async def _encoding() -> Optional[str]:
                        return await valkey.run_valkey_command(f"OBJECT ENCODING {COLLECTION_KEYS[self.test]}")

                    rehash_passes, settle_key_check, settle_key_encoding = await settle_rehash_for_key(
                        _stats,
                        _keyspace_stats,
                        _touch,
                        encoding_fn=_encoding if self.test in COLLECTION_KEYS else None,
                    )
                    self.logger.info(
                        "Rehash settled after %d read pass(es), key check %s, encoding %s (port %d)",
                        rehash_passes,
                        settle_key_check,
                        settle_key_encoding,
                        port,
                    )
                    if rehash_passes:
                        # Completing a rehash frees the old table; let the sample catch up.
                        settled_bytes, settle_secs = await quiesce_until_stable(_sample_used_memory)
                        self.logger.info(
                            "Post-rehash quiesce: used_memory settled at %d bytes after %.0fs (port %d)",
                            settled_bytes,
                            settle_secs,
                            port,
                        )
                else:
                    # An engine without DEBUG HTSTATS: read every item once, as
                    # the Valkey settle does, but make no rehash assertion. The record says so.
                    async def _touch_once() -> int:
                        return await asyncio.to_thread(touch, valkey.ip, valkey.port, workload)

                    await _touch_once()
                    rehash_passes, settle_key_check = 1, KEY_CHECK_NOT_APPLICABLE
                    self.logger.info("Neutral settle: one read pass, no rehash check (port %d)", port)
                    settled_bytes, settle_secs = await quiesce_until_stable(_sample_used_memory)
                    self.logger.info(
                        "Post-read quiesce: used_memory settled at %d bytes after %.0fs (port %d)",
                        settled_bytes,
                        settle_secs,
                        port,
                    )

            if self.settle:
                # Engine-neutral last check: used_memory (and any hash table overhead fields
                # MEMORY STATS reports) must have stopped moving.
                memory_stats_available = True

                async def _sample_stability() -> dict[str, float]:
                    nonlocal memory_stats_available
                    info = await valkey.info("memory")
                    sample = {"used_memory": float(info["used_memory"])}
                    if memory_stats_available:
                        try:
                            fields = parse_memory_stats_rehash_fields(await valkey.run_valkey_command("MEMORY STATS"))
                        except Exception:  # an engine without MEMORY STATS: skip quietly from now on
                            fields = {}
                        if fields:
                            sample.update(fields)
                        else:
                            memory_stats_available = False
                    return sample

                settle_stability = await check_memory_stability(_sample_stability)
                self.logger.info(
                    "Memory stability: %s after %.0fs, %d sample(s) (port %d)",
                    settle_stability["verdict"],
                    settle_stability["elapsed_s"],
                    len(settle_stability["samples"]),
                    port,
                )
                if settle_stability["verdict"] != STABILITY_STABLE:
                    self.logger.warning("used_memory was still moving at the timeout (port %d)", port)

            after_memory: dict[str, str] = await valkey.info("memory")
            sample_after = sample_values(after_memory, await read_proc_status(valkey))

            key_count, expire_count = await valkey.count_items_expires()
            if self.test == "set":
                # SET creates one key per item
                item_count = key_count
            else:
                # SADD/ZADD/HSET create a single collection with N members
                if key_count != 1:
                    raise RuntimeError(f"Expected 1 collection key but found {key_count} on port {port}")
                if self.test == "zadd":
                    cardinality_cmd, key_name = "ZCARD", "myzset"
                elif self.test == "hset":
                    cardinality_cmd, key_name = "HLEN", "myhash"
                else:
                    cardinality_cmd, key_name = "SCARD", "myset"
                result_str = await valkey.run_valkey_command(f"{cardinality_cmd} {key_name}")
                if result_str is None:
                    raise RuntimeError(f"{cardinality_cmd} returned None on port {port}")
                item_count = int(result_str)
            # churn ends near `count` (random walk), not exactly; sequential/random are exact.
            # The queried item_count is the source of truth for per-item math.
            if self.populate_mode == "churn":
                if not (0 < item_count < 2 * count):
                    raise RuntimeError(f"churn produced {item_count} items, expected ~{count} on port {port}")
            elif item_count != count:
                raise RuntimeError(f"Expected {count} items but found {item_count} on port {port}")
            if self.has_expire:
                if expire_count != count:
                    raise RuntimeError(f"Expected {count} expires but found {expire_count} on port {port}")
            else:
                if expire_count != 0:
                    raise RuntimeError(f"Expected 0 expires but found {expire_count} on port {port}")

            # Facts about the finished structure, read while the server is still up.
            key_encoding = await collect_encoding(valkey, self.test)
            memory_config = await collect_config(valkey)

            await valkey.stop()  # required cleanup, release CPU allocations, etc

            # Collect heap profile if profiling is enabled (dump created on shutdown by prof_final).
            # Skipped for engines that opt out of internal profiling (Redis): allocation stacks
            # expose the binary's symbols. Total memory below is still recorded regardless.
            breakdown: Optional[dict[str, float]] = None
            raw_stacks: Optional[list[list]] = None
            if self.enable_profiling and should_profile_internals(get_sweep_engine(self.source)):
                profile_result = await collect_heap_profile(valkey, str(self.cached_binary_path), item_count)
                if profile_result:
                    breakdown = profile_result.breakdown
                    raw_stacks = profile_result.raw_stacks
                    self.logger.info("Memory breakdown collected: %s", breakdown)
                else:
                    self.logger.warning(
                        "Profiling was enabled but no heap breakdown was collected for %s v=%d; "
                        "recording breakdown=None. Most likely the binary was built without "
                        "%s (make_args=%r).",
                        self.test,
                        val_size,
                        JEMALLOC_PROF_CONFIGURE_OPTS,
                        self.make_args,
                    )
                await cleanup_heap_dumps(valkey)

            # User data per item from workload config
            user_data_per_item = self.user_data_bytes

            before_usage = int(before_memory["used_memory"])
            after_usage = int(after_memory["used_memory"])
            total_usage = after_usage - before_usage
            per_key = float(total_usage) / item_count  # queried cardinality (exact for all modes)
            per_item_overhead = per_key - user_data_per_item

            memory_record = build_memory_record(
                engine=identity["engine"],
                binary_sha256=identity["binary_sha256"],
                version_text=identity["version_text"],
                profiling_requested=self.enable_profiling,
                profiling_active_at_runtime=profiling_state,
                settle_requested=self.settle,
                settle_passes=rehash_passes,
                settle_key_check=settle_key_check,
                settle_stability=settle_stability["verdict"] if settle_stability else None,
                settle_rehash_check=self.rehash_check,
                encoding=key_encoding or settle_key_encoding,
                config=memory_config,
                item_count=item_count,
                samples={"before_fill": sample_before, "after_fill": sample_filled, "after_settle": sample_after},
                conductress_revision=conductress_revision(),
            )

            result = {
                "memory_record": memory_record,
                "item_count": item_count,  # reached; the queried cardinality
                "item_count_requested": count,
                "before_memory": before_memory,
                "after_memory": after_memory,
                "has_expire": self.has_expire,
                "user_data_per_item": user_data_per_item,
                "val_size": val_size,
                "per_key_size": per_key,
                "per_item_overhead": per_item_overhead,
                "settled": self.settle,
                "rehash_passes": rehash_passes,
                "settle_key_check": settle_key_check,
                "settle_key_encoding": settle_key_encoding,
                "settle_stability": settle_stability,
                "breakdown": breakdown,
                "raw_stacks": raw_stacks,
            }
            if not self.rehash_check:  # absent means the rehash check ran, as in every record before it existed
                result["rehash_check"] = False
            if not self._profile.valkey_launch:
                result["engine"] = self.engine
            if self.prebuilt_info is not None:
                result["prebuilt_engine"] = self.prebuilt_info
            return result

    def plot(self, efficiency_map: dict[int, float]) -> None:
        """Plot the memory efficiency results."""
        plt.clear_terminal()
        plt.clear_figure()
        plt.canvas_color("black")
        plt.axes_color("black")
        plt.ticks_color("orange")

        plt.title(self.title)
        plt.frame(False)
        plt.xlabel("Val+Key Size (bytes)")
        plt.ylabel("Overhead (bytes)")

        keysize = MEM_TEST_KEY_SIZE
        sizes = [valsize + keysize for valsize in self.val_sizes]
        overheads = [efficiency_map.get(valsize, None) for valsize in self.val_sizes]

        plt.plot(sizes, overheads, marker="braille", color="orange+")
        plt.xticks(sizes, [HumanByte.to_human(size) for size in sizes])
        plt.ylim(lower=0)

        plt.show()
