"""Configuration for the Conductress benchmark framework"""

import json
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

# TODO fix paths for remote hosts?
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

PERF_BENCH_KEYSPACE = 3_000_000
PERF_BENCH_CLIENTS = 1200
PERF_BENCH_THREADS = 16  # 75 connections per thread

# Default compiler arguments for Valkey builds.
# Bare make already gives O3+LTO+frame-pointer, so no extra flags are needed.
# (USE_FAST_FLOAT was a no-op — no such variable exists in the Valkey Makefile.)
DEFAULT_MAKE_ARGS = ""

# Benchmark defaults (single source of truth for the CLI and the runner)
DEFAULT_IO_THREADS = 9
DEFAULT_PIPELINING = 10
DEFAULT_WARMUP = 5  # seconds
DEFAULT_DURATION = 30  # seconds
DEFAULT_REPETITIONS = 3

# Minimum repetitions before adaptive early-stop may fire. With fewer reps, a
# bimodal between-restart distribution (observed on Intel Xeon, suspected on
# AMD EPYC) can place every sample on one mode, producing a deceptively low CV
# and a false-certainty early stop. At n=3 the probability of all samples
# landing on the same mode is 25%; at n=5 it drops to 6.25%. Five is the
# conservative floor: enough to expose inter-mode variance with high
# probability, cheap enough to not meaningfully extend well-behaved cells.
SWEEP_MIN_REPS = 5
DEFAULT_VAL_SIZE = 512  # bytes
DEFAULT_KEY_SIZE = 0  # 0 = standard keys

# Dashboard data server (rsync target for --publish). Override without a deploy
# via CONDUCTRESS_PUBLISH_TARGET; the default is the live dashboard endpoint.
PUBLISH_TARGET = os.environ.get("CONDUCTRESS_PUBLISH_TARGET", "ec2-user@data.conductress.rainsupreme.net:/var/www/data")

# memtier_benchmark load generator. Benchmark command strings run on the REMOTE
# benchmark host over SSH, where "~" expands to that host's home directory, so
# the remote form must stay a literal "~/..." string (never a local absolute
# path). REMOTE_MEMTIER_BENCHMARK is the single source of truth for those
# command strings; MEMTIER_BENCHMARK is the local-path form for code that
# resolves the binary under this checkout's PROJECT_ROOT. bootstrap.ensure_memtier
# installs it at ~/conductress/memtier_benchmark on each host.
REMOTE_MEMTIER_BENCHMARK = "~/conductress/memtier_benchmark"
MEMTIER_BENCHMARK = PROJECT_ROOT / "memtier_benchmark"

# cachecannon load generator binary on benchmark hosts. Like memtier, the task
# command strings run on the remote host, so this default is the absolute path
# cachecannon builds to there (~/cachecannon/target/release/cachecannon as
# ec2-user). Override without a deploy via CONDUCTRESS_CACHECANNON_BINARY.
CACHECANNON_BINARY = os.environ.get(
    "CONDUCTRESS_CACHECANNON_BINARY", "/home/ec2-user/cachecannon/target/release/cachecannon"
)

# Stable identity for this Conductress runner. runner.json is local deployment
# configuration and is intentionally not committed.
RUNNER_CONFIG_PATH = PROJECT_ROOT / "runner.json"


class Features(Enum):
    PIN_VALKEY_THREADS = "pin_valkey_threads"
    ENABLE_CPU_CONSISTENCY_MODE = "cpu_consistency_mode"
    BIND_NUMA_MEMORY = "bind_numa_memory"


FEATURE_STATES = {
    Features.PIN_VALKEY_THREADS: True,
    Features.ENABLE_CPU_CONSISTENCY_MODE: True,
    Features.BIND_NUMA_MEMORY: False,
}


def check_feature(feature: Features) -> bool:
    """Get the state of a specific feature flag."""
    return FEATURE_STATES.get(feature, False)


def get_all_features() -> dict[Features, bool]:
    """Get all feature flags and their current values."""
    return FEATURE_STATES.copy()


# Memory efficiency test configuration
MEM_TEST_ITEM_COUNT = 5_000_000  # 5 million items for memory tests
MEM_TEST_KEY_SIZE = 16  # Size of "key:__rand_int__" pattern
MEM_TEST_MEMBER_SIZE = 20  # Size of "element:__rand_int__" pattern (used by sadd/zadd)
MEM_TEST_SCORE_SIZE = 8  # Size of a double score (used by zadd)
MEM_TEST_MAX_CONCURRENT = 9  # Max concurrent server instances # TODO max session limit typically 10 by default
MEM_TEST_EXPIRE_SECONDS = 7 * 24 * 60 * 60  # 7 days expiration

# =============================================================================
# RUNTIME CONSTANTS
# =============================================================================

# Task runner polls the local queue at this interval when idle (seconds)
QUEUE_POLL_INTERVAL = 4

# Fleet mailbox management happens only between tasks. The runner sleeps after
# the final boundary contact before starting benchmark work.
FLEET_IDLE_POLL_INTERVAL = 30
MANAGEMENT_SETTLE_SECONDS = 2.0

# CPUs the runner process confines itself to while a task runs, reserved through
# the allocator from the tail of a non-network NUMA node (see runner_affinity).
# 0 disables pinning. One core covers the runner's ~0.35-core steady load.
RUNNER_MANAGEMENT_CPUS = 1
DELIVERY_JOURNAL_PATH = PROJECT_ROOT / "fleet_delivery.json"

# How often sweep fetches new commits from origin (seconds).
# Runs between jobs, not during benchmarks.
SWEEP_FETCH_INTERVAL = 3600

# =============================================================================
# Sweep configuration: throughput
# =============================================================================
SWEEP_SOURCE = "valkey"
SWEEP_REF = "origin/unstable"
SWEEP_STATE_DIR = PROJECT_ROOT / "sweep_data"
SWEEP_STATE_FILE = SWEEP_STATE_DIR / "state.json"
SWEEP_TEST = "get"
SWEEP_KEY_SIZE = 16
SWEEP_VAL_SIZE = 16
SWEEP_IO_THREADS = 7
SWEEP_PIPELINING = 10
SWEEP_WARMUP = 5
SWEEP_DURATION = 30
# Adaptive repetitions: run at least SWEEP_REPETITIONS reps, stop early once the
# 95% CI half-width of the mean is <= SWEEP_TARGET_CV (% of mean), up to
# SWEEP_MAX_REPS. Min is 5 (not 3) because Intel shows a bimodal
# between-restart distribution (docs/benchmark-precision-guide.md): with 3 reps
# there is a ~25% chance all land on one mode, freezing in a mean several % off
# with a deceptively tight interval.
SWEEP_REPETITIONS = 5
SWEEP_MAX_REPS = 10
SWEEP_TARGET_CV = 0.5
# Minimum |delta| to annotate a pinpointed change as "notable" in exported data.
# Set above binary layout noise floor (~3-4%) to reduce false positives.
ANNOTATION_THRESHOLD = 0.04
SWEEP_MAKE_ARGS = ""

# Versioned sweep epoch. Disabled by default until overlap validation is
# complete; enabling it runs additive v2 GET + 80:20 mixed coordinators while
# leaving every v1 coordinator/state file active and unchanged.
_TRUE_ENV_VALUES = {"1", "true", "yes", "on"}
_FALSE_ENV_VALUES = {"0", "false", "no", "off"}


def _env_bool(name: str, default: bool = False) -> bool:
    """Read a strict boolean environment variable.

    Unknown values fail startup rather than silently selecting the wrong sweep
    epoch. This keeps fleet enablement explicit and rollback-friendly.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_ENV_VALUES:
        return True
    if value in _FALSE_ENV_VALUES:
        return False
    allowed = ", ".join(sorted(_TRUE_ENV_VALUES | _FALSE_ENV_VALUES))
    raise ValueError(f"{name} must be one of: {allowed}; got {raw!r}")


SWEEP_V3_ENABLED = _env_bool("CONDUCTRESS_SWEEP_V3_ENABLED", False)
SWEEP_V3_EPOCH_ID = "v3"


def _env_positive_float(name: str, default: float) -> float:
    """Read a positive number from the environment; a bad value fails startup."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number; got {raw!r}") from exc
    if not value > 0:
        raise ValueError(f"{name} must be greater than 0; got {raw!r}")
    return value


# Confirmation sweep (sweep/confirmation.py): after a host change, re-measure a
# few old (commit, workload) points of the live v3 generation at the lowest
# priority. OFF by default: when off, the runner builds no confirmation
# coordinator, queues no confirmation task and publishes no confirm file.
CONFIRM_SWEEP = _env_bool("CONDUCTRESS_CONFIRM_SWEEP", False)
# The smallest effect (percent) a confirmation run calls confirmed, and the
# half-width of the band a cleared run's interval must stay inside.
CONFIRM_FLOOR_PCT = _env_positive_float("CONDUCTRESS_CONFIRM_FLOOR_PCT", 0.5)


def _env_epoch_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Read a comma-separated epoch precedence list from the environment.

    An explicitly set but empty/blank value is an error rather than a silent
    fallback, matching ``_env_bool``: mis-set scheduling policy should fail
    startup, not quietly restore the default ordering.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    items = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not items:
        raise ValueError(f"{name} must be a comma-separated epoch list (e.g. 'v3,v1'); got {raw!r}")
    duplicates = {e for e in items if items.count(e) > 1}
    if duplicates:
        raise ValueError(f"{name} lists duplicate epochs: {sorted(duplicates)}")
    return items


# Archived measurement epochs: their history keeps rendering on the dashboard,
# but the runner builds no coordinator for them, loads none of their state, and
# never schedules them.  v1 is archived: its state files are 1.2-1.5 GB each
# (points embed perf stacks) and loading all of them at every runner start cost
# 6-7 GB of resident memory for data that never changes again.  The already
# exported v1 dashboard files persist in the publish export dir (see
# PUBLISH_EXPORT_DIR) and are re-synced as-is, and the publisher advertises an
# archived epoch in every manifest so the dashboard's epoch selector keeps
# offering v1 history.  A one-time `conductress sweep export-archived --epoch v1`
# regenerates those files on a runner whose export dir was wiped, without the
# service ever paying the load cost.  Override with CONDUCTRESS_SWEEP_ARCHIVED_EPOCHS.
SWEEP_ARCHIVED_EPOCHS = _env_epoch_list("CONDUCTRESS_SWEEP_ARCHIVED_EPOCHS", ("v1",))


def epoch_is_archived(epoch_id: str) -> bool:
    """Whether an epoch is archived (renders history, never builds/loads/schedules)."""
    return epoch_id in SWEEP_ARCHIVED_EPOCHS


# Scheduling precedence between LIVE measurement epochs, highest priority first.
#
# Before this existed, a newly added epoch outranked (or was outranked by) the
# legacy coordinators purely as a side effect of the order in which TaskRunner
# happened to append subscribers.  That was invisible in the scheduler and easy
# to break by moving a registration block.  Precedence is now explicit.
#
# It matters because NIGHTLY (untested HEAD) has absolute priority and returns on
# the FIRST matching coordinator, so whichever epoch is scanned first measures
# each new HEAD first.  v3 is the only live epoch, so it measures every HEAD; v1
# is archived and no longer scheduled at all.
#
# Epochs absent from this list are scanned after every listed epoch, preserving
# their existing relative order.  Override without a deploy via
# CONDUCTRESS_SWEEP_EPOCH_PRECEDENCE.  An archived epoch listed here is dropped
# with a log line rather than scheduled -- an archived epoch has no coordinators
# to order, so a stale "v3,v1" override cannot resurrect v1 scheduling.
def _resolve_epoch_precedence(raw: tuple[str, ...]) -> tuple[str, ...]:
    """Drop archived epochs from a precedence list, logging any that were dropped."""
    live = tuple(e for e in raw if not epoch_is_archived(e))
    dropped = tuple(e for e in raw if epoch_is_archived(e))
    if dropped:
        import logging

        logging.getLogger(__name__).info(
            "Ignoring archived epoch(s) %s in sweep precedence; archived epochs are not scheduled",
            ", ".join(dropped),
        )
    return live


SWEEP_EPOCH_PRECEDENCE = _resolve_epoch_precedence(_env_epoch_list("CONDUCTRESS_SWEEP_EPOCH_PRECEDENCE", ("v3",)))

# ---------------------------------------------------------------------------
# cachecannon-v3 sweep protocol
# ---------------------------------------------------------------------------
# Every value below was CONFIRMED by the Stage 2 ramp measurement on 2026-09-04
# (20 cells, ~1,780 per-second samples, G4 + AMD, 400c and 1200c, 5 fresh server
# starts each).  Two results are load-bearing and must not be "tidied":
#
#   * Warmup is 10s, NOT the 30s originally extrapolated from memtier.  Every
#     cell converges by second 10; cachecannon settles roughly 3x faster than
#     memtier.  Zero warmup fails only on G4/1200c, where an opening ~5% spike
#     leaks into the scored window (+0.643% median bias).
#   * Connections are 400, NOT 1200.  1200c was rejected on stability grounds:
#     AMD showed 2.520% between-restart CV across a 6% spread, which can never
#     satisfy the target below and would burn the rep ceiling on every cell.
#
# Changing any of these values defines a NEW workload identity, not a
# continuation of the series.  See conductress-cachecannon-v3/epoch-specification.md.
# cachecannon v0.0.34 (tag v0.0.34, 2026-10-28). Bumped from 31a2befa (0.0.19+,
# 2026-07-22) for the private-CA TLS client (tls_ca_file, cachecannon #123);
# measured neutral on the closed-loop GET/SET p10 cells before the bump.
SWEEP_V3_CACHECANNON_COMMIT = "1bea936b81026c1cc04c86dcb9cc251f58e430a1"
SWEEP_V3_WARMUP = 10
SWEEP_V3_DURATION = 30
SWEEP_V3_CONNECTIONS = 400
SWEEP_V3_CLIENT_THREADS = 8  # 50 connections per client thread at 400c
# Client budget for the unpipelined P1 GET throughput series only.  P10 sends
# one syscall per ten requests; P1 sends one per request, so cachecannon spends
# 4-5x more CPU per request and the 8-thread client saturates well below the
# server's ceiling.  Measured Sep 21 2026 from live v3 P1 backfill points:
# Graviton3 1,221,594 GET/s at client utilization 0.941, saturated=True (the
# series was measuring the client, not the server, on that commit); Graviton4
# 1,317,049 at 0.653; intel 779,797 at 0.724; AMD 468,790 at 0.728 (headroom
# 1.24-1.38x elsewhere, zero on Graviton3).  P10 series sit at 0.27-0.51 and
# stay at 8 threads.  16 client threads (25 connections per thread at 400c)
# gives Graviton3 the headroom to measure the server.  A series' client budget
# is part of its identity and never changes within the series (the same rule
# as its floor tag): changing it defines a new workload, so the P1 series
# history is cleared and restarted rather than mixing two client shapes on one
# line.
SWEEP_V3_P1_CLIENT_THREADS = 16
SWEEP_V3_IO_THREADS = 7  # server io-threads
SWEEP_V3_PIPELINING = 10
SWEEP_V3_VAL_SIZE = 16
SWEEP_V3_KEYSPACE = 3_000_000
SWEEP_V3_DISTRIBUTION = "uniform"
SWEEP_V3_SET_RATIO = 20  # the canonical mixed workload
SWEEP_V3_REPETITIONS = SWEEP_MIN_REPS  # minimum reps (references the common floor)
SWEEP_V3_MAX_REPS = 10  # adaptive ceiling
# Precision target for adaptive stopping.  NOTE: despite the historical field
# name, should_stop_adaptive() bounds the 95% CI half-width as a percent of the
# mean, which is a *wider* statistic than CV.  At n=5 the half-width is roughly
# 1.24x the CV, so the measured 0.28-0.30% CV at 400c corresponds to about
# 0.35-0.37% here -- inside this bound, but with less margin than the raw CV
# numbers suggest.
SWEEP_V3_TARGET_CV = 0.5

# v3 latency workload.  The same generator, connection count, client threads,
# key space, value size and scored window as the v3 throughput workloads, but
# driven at a fixed request rate with no pipelining, so the measurement is the
# per-request latency the server delivers under a load it can comfortably serve
# rather than its capacity.  The rate is the same on every platform so the
# series compare across hardware.  cachecannon reports latency in exact integer
# microseconds and, in rate-limited mode, whether it held the requested rate.
SWEEP_V3_LATENCY_RATE = 100_000  # requests/s, shared across all connections
SWEEP_V3_LATENCY_PIPELINING = 1
SWEEP_V3_LATENCY_REPETITIONS = SWEEP_MIN_REPS
SWEEP_V3_LATENCY_MAX_REPS = 10
# Precision target for adaptive stopping on p99 (95% CI half-width as a percent
# of the mean, see SWEEP_V3_TARGET_CV).  Tail latency varies more between
# server restarts than throughput does, so the bound is wider.
SWEEP_V3_LATENCY_TARGET_CV = 2.0

# v3 large-value GET workload.  Same identity as the canonical GET series except
# the value: 1024 bytes is a raw (non-embstr) encoding whose reply is dominated
# by buffer copies rather than dispatch, so it moves on reply-path changes that
# a 16-byte value cannot see (measured Sep 20 2026 on graviton4: PR #2078 Reply
# Copy Avoidance +54% at v1024 vs -1.7% at v16; PR #2516 embstr-only -6.4% at
# v16 vs -1.2% at v1024).  The series exists to catch reply-buffer and large-value
# encoding regressions going forward, so it does not backfill the whole
# history: its commit range starts at the merge-base of this tag with the sweep
# ref.  A series floor overrides the engine floor for that one series; a
# comparison engine measured at release-and-tip never reads it.
SWEEP_V3_LARGE_VAL_SIZE = 1024
SWEEP_V3_LARGE_VALUE_FLOOR_TAG = "9.0.0"

# v3 GET-over-TLS workload.  Same identity as the canonical GET series except
# the transport: the server opens a TLS listener with the bootstrap test certs
# and the generator connects over it, so the series measures the encrypted read
# path (handshake amortized, per-request decrypt/encrypt) rather than the
# plaintext ceiling.  The TLS listener is a SECOND port so the plaintext port
# keeps serving the server's own housekeeping; the generator's endpoint is the
# TLS port.  Like the large-value series it does not backfill the whole history:
# its commit range starts at SWEEP_V3_TLS_FLOOR_TAG (the release that carries the
# TLS behaviour worth tracking) rather than the fork point, so a seventh series
# guards the TLS path going forward without slowing the other six for two years
# of history.  A series floor overrides the engine floor for that one series; a
# comparison engine measured at release-and-tip never reads it.
SWEEP_V3_TLS_PORT = 16379  # server TLS listener; distinct from the plaintext primary and the 9000+ range
SWEEP_V3_TLS_FLOOR_TAG = "9.0.0"

# Epochs a generator-independent series belongs to.  Memory overhead is read
# from the server's own INFO after Conductress fills it through its populator;
# no load generator is involved, so one memory series is valid in every epoch
# and is published under each of these.  The first entry is the epoch the
# series schedules and pauses under.  v1 is archived, so memory now publishes
# only under v3; the v1-named memory files already on the data server stay as
# archived copies (preserved by the publish export dir, see PUBLISH_EXPORT_DIR).
SWEEP_GENERATOR_INDEPENDENT_EPOCHS: tuple[str, ...] = ("v3",)

# Epoch-1 series that a v3 series has replaced.  A retired series keeps
# publishing the history it already holds but never queues another task, so
# the replacement is the only one still measuring.  Keyed "metric:workload";
# built below once the engines and workload roster are defined.
#
# Valkey throughput is measured only in the v3 epoch, so every v1 Valkey
# throughput series is retired: the ones with a v3 counterpart because that
# counterpart replaces them, the rest (extra value sizes, platform-optimal
# thread/pipeline shapes) because their history moved with their neighbours
# and they carried no independent signal.  Rather than
# hand-list the labels (which drift the moment the roster changes), derive them
# from the same roster data that defines the v1 series: the default GET series
# plus every entry in SWEEP_THROUGHPUT_WORKLOADS.  The v1 latency series is
# retired by name because it has no entry in that roster.  The Valkey-only
# retired set is completed below with the engine-mirror derivation once the
# engines are defined.

# Epoch registry: id -> dashboard metadata.  The publisher advertises these in
# every manifest so old URLs keep working while new dashboards can discover
# additional epochs.  Unknown ids fall back to a generic label rather than
# being silently mislabelled as some other epoch.
SWEEP_EPOCHS: dict[str, dict[str, str]] = {
    "v1": {"label": "Legacy v1 (stock generator)", "generator": "stock"},
    "v2": {"label": "Scalable v2 (patched generator)", "generator": "patched"},
    "v3": {"label": "Cachecannon v3 (io_uring generator)", "generator": "cachecannon"},
}

# Additional throughput workloads (each gets its own state file + series).
# Label is auto-generated as {test}-k{key_size}-v{val_size}-t{io_threads}-p{pipelining}.
# "platforms" limits the workload to specific architectures (omit for all platforms).
SWEEP_THROUGHPUT_WORKLOADS: list[dict] = [
    {"val_size": 64},
    {"val_size": 128},
    {"val_size": 16, "test": "set"},
    {"val_size": 128, "test": "set"},
    # No-pipeline workloads: single-request latency-sensitive baseline
    {"val_size": 16, "pipelining": 1},
    {"val_size": 128, "pipelining": 1},
    {"val_size": 16, "test": "set", "pipelining": 1},
    {"val_size": 128, "test": "set", "pipelining": 1},
    # Platform-optimal workloads: realistic configs for performance-sensitive users
    {"val_size": 16, "io_threads": 24, "pipelining": 100, "platforms": ["intel"]},
    {"val_size": 16, "io_threads": 24, "pipelining": 100, "test": "set", "platforms": ["intel"]},
    {"val_size": 16, "io_threads": 9, "pipelining": 50, "platforms": ["arm64", "graviton4"]},
    {"val_size": 16, "io_threads": 9, "pipelining": 50, "test": "set", "platforms": ["arm64", "graviton4"]},
]


# =============================================================================
# Sweep engine configuration: multi-engine support (Valkey, Redis, etc.)
# =============================================================================


# An engine is described on two independent axes.
#
# Provisioning: how a server binary for a given revision comes to exist.
#   "built-from-git"   clone ``source``, check out the revision, run make with
#                      ``make_args`` (Valkey, Redis).
#   "prebuilt-release" download the official release asset for the host
#                      architecture; no source is ever checked out.  Reserved
#                      for engines whose source licence the project does not
#                      accept on its runners; not implemented yet.
#
# Scope: which revisions the sweep measures.
#   "history"          every merge commit since the floor: release landmarks,
#                      bisection of significant deltas, backfill of gaps, and
#                      each new tip (Valkey).
#   "release-and-tip"  the latest release only, plus the current tip sampled at
#                      most once per ``tip_interval_hours``.  No bisection, no
#                      backfill.  Enough to power the engine comparison, which
#                      reads one release point and the recent tip and nothing
#                      else (Redis).
ENGINE_PROVISIONING = ("built-from-git", "prebuilt-release")
ENGINE_SCOPES = ("history", "release-and-tip")


@dataclass
class SweepEngine:
    """Configuration for a benchmarking engine (server software to sweep)."""

    source: str  # REPOSITORIES entry name (e.g. "valkey", "redis")
    ref: str  # git ref to track (e.g. "origin/unstable")
    binary_name: str  # server binary produced by build (e.g. "valkey-server", "redis-server")
    floor_tag: Optional[str] = None  # earliest tag to sweep from (None = use find_fork_point)
    make_args: str = DEFAULT_MAKE_ARGS  # compiler flags
    heap_alloc_funcs: list[str] = field(default_factory=list)  # for memory profiling
    profile_internals: bool = True  # collect CPU flamegraphs + jemalloc allocation breakdown for this engine
    provisioning: str = "built-from-git"  # see ENGINE_PROVISIONING
    scope: str = "history"  # see ENGINE_SCOPES
    tip_interval_hours: float = 24.0  # release-and-tip: minimum spacing between tip samples

    def __post_init__(self) -> None:
        if self.provisioning not in ENGINE_PROVISIONING:
            raise ValueError(f"engine {self.source!r}: provisioning must be one of {ENGINE_PROVISIONING}")
        if self.provisioning == "prebuilt-release":
            raise NotImplementedError(f"engine {self.source!r}: prebuilt-release provisioning is not implemented")
        if self.scope not in ENGINE_SCOPES:
            raise ValueError(f"engine {self.source!r}: scope must be one of {ENGINE_SCOPES}")
        if self.tip_interval_hours <= 0:
            raise ValueError(f"engine {self.source!r}: tip_interval_hours must be positive")

    @property
    def tracks_history(self) -> bool:
        """True when the sweep bisects and backfills this engine's history."""
        return self.scope == "history"

    @property
    def tip_interval_seconds(self) -> float:
        return self.tip_interval_hours * 3600.0


SWEEP_ENGINES: list[SweepEngine] = [
    SweepEngine(
        source="valkey",
        ref="origin/unstable",
        binary_name="valkey-server",
        floor_tag=None,
        make_args="",
        heap_alloc_funcs=["valkey_malloc", "valkey_calloc", "valkey_realloc"],
    ),
    SweepEngine(
        source="redis",
        ref="origin/unstable",
        binary_name="redis-server",
        floor_tag="8.0.0",
        make_args="",
        heap_alloc_funcs=["zmalloc", "zcalloc", "zrealloc"],
        # CPU flamegraphs and jemalloc allocation breakdowns expose the Redis binary's
        # symbol table (function names / call graph), which we do not analyze or surface.
        # Redis keeps aggregate performance data only: throughput, latency, total memory.
        profile_internals=False,
        # Redis exists here to power the engine comparison, which needs the latest
        # release and the current tip.  Its history is not bisected.
        scope="release-and-tip",
        tip_interval_hours=24.0,
    ),
]


def get_sweep_engine(source: str) -> Optional["SweepEngine"]:
    """Look up a SweepEngine by source name."""
    for engine in SWEEP_ENGINES:
        if engine.source == source:
            return engine
    return None


def engine_tracks_history(engine: Optional["SweepEngine"]) -> bool:
    """Whether a series bisects and backfills history (True for the default Valkey sweep)."""
    return engine.tracks_history if engine else True


def engine_tip_interval_seconds(engine: Optional["SweepEngine"]) -> float:
    """Minimum spacing between tip samples; zero for history engines, which measure every new tip."""
    return 0.0 if engine is None or engine.tracks_history else engine.tip_interval_seconds


def engine_series_prefix(engine: Optional["SweepEngine"]) -> str:
    """The workload-id prefix that keeps one engine's series apart from another's."""
    return f"{engine.source}-" if engine and engine.source != "valkey" else ""


def engine_repo_slug(engine: Optional["SweepEngine"]) -> str:
    """The ``owner/repo`` an engine's commit links point at, from its REPOSITORIES entry."""
    source = engine.source if engine else "valkey"
    for url, name in REPOSITORIES:
        if name == source:
            path = url.rsplit("github.com/", 1)[-1]
            return path[:-4] if path.endswith(".git") else path
    return "valkey-io/valkey"


def sweep_throughput_label(
    test: str = SWEEP_TEST,
    val_size: int = SWEEP_VAL_SIZE,
    io_threads: int = SWEEP_IO_THREADS,
    pipelining: int = SWEEP_PIPELINING,
    engine: Optional["SweepEngine"] = None,
) -> str:
    """Workload id of an epoch-1 throughput series, e.g. ``redis-get-k16-v64-t7-p10``."""
    return f"{engine_series_prefix(engine)}{test}-k{SWEEP_KEY_SIZE}-v{val_size}-t{io_threads}-p{pipelining}"


def _v1_engine_mirror_series(engine: "SweepEngine") -> frozenset[str]:
    """Every epoch-1 throughput series the sweep mirrors for a non-Valkey engine."""
    labels = {sweep_throughput_label(engine=engine)}
    for wl in SWEEP_THROUGHPUT_WORKLOADS:
        labels.add(
            sweep_throughput_label(
                test=wl.get("test", SWEEP_TEST),
                val_size=wl["val_size"],
                io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                pipelining=wl.get("pipelining", SWEEP_PIPELINING),
                engine=engine,
            )
        )
    return frozenset(f"throughput:{label}" for label in labels)


def _v1_retired_valkey_series() -> frozenset[str]:
    """The Valkey epoch-1 series that no longer queue.

    Derived from the same roster data that defines the v1 series so the two can
    never drift: every v1 Valkey throughput series (the default GET series plus
    every entry in ``SWEEP_THROUGHPUT_WORKLOADS``) is retired, whether a v3
    series replaces it or it was dropped as redundant, and the v1 latency
    series -- which has no roster entry -- is retired by name.  Passing ``engine=None`` yields the unprefixed Valkey
    labels, exactly the reverse of ``_v1_engine_mirror_series`` for a prefixed
    comparison engine.
    """
    throughput = {sweep_throughput_label(engine=None)}
    for wl in SWEEP_THROUGHPUT_WORKLOADS:
        throughput.add(
            sweep_throughput_label(
                test=wl.get("test", SWEEP_TEST),
                val_size=wl["val_size"],
                io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                pipelining=wl.get("pipelining", SWEEP_PIPELINING),
                engine=None,
            )
        )
    return frozenset({f"throughput:{label}" for label in throughput} | {"latency:get-k16-v16"})


# The full registry: the Valkey series a v3 series replaced, plus every
# epoch-1 throughput mirror of a comparison engine (Redis).  Those mirrors are
# replaced by the engine's v3 series, which measure only what the comparison
# reads (release and tip), so no new epoch-1 Redis data is produced.
SWEEP_V1_RETIRED_SERIES: frozenset[str] = _v1_retired_valkey_series().union(
    *(_v1_engine_mirror_series(e) for e in SWEEP_ENGINES if e.source != "valkey")
)


def should_profile_internals(engine: Optional["SweepEngine"]) -> bool:
    """Whether to collect internal profiling (CPU flamegraphs, jemalloc breakdown) for an engine.

    Absent/unknown engine (e.g. a fork source, or legacy state with no engine) defaults to True
    so Valkey profiling is unaffected; an engine only opts out via profile_internals=False (Redis).
    This is the single source of truth for the policy — call it everywhere rather than inlining
    `engine.profile_internals` checks (which have to re-handle the None case and read poorly).
    """
    return engine.profile_internals if engine else True


# =============================================================================
# Retired epoch-1 latency series (memtier_benchmark GET p99 at a fixed rate).
# Only what publishing its recorded history needs; the series never queues.
# =============================================================================
LATENCY_STATE_FILE = SWEEP_STATE_DIR / "latency_state.json"
LATENCY_TARGET_RPS = 100_000  # the fixed rate every point in that history was measured at
MEMTIER_COMMIT = "d52544b1"  # pinned memtier_benchmark build used by scenario tasks
VALKEY_BENCHMARK_COMMIT = "d2eee78a151884518441572c53fc378bf6689e81"  # pinned valkey commit for benchmark client binary

# =============================================================================
# Sweep configuration: memory
# =============================================================================
MEMORY_STATE_DIR = SWEEP_STATE_DIR

# Benchmark metric collection interval (seconds). valkey-benchmark outputs ~4/sec.
BENCHMARK_UPDATE_INTERVAL = 0.1

# Status heartbeat interval during benchmark runs (seconds)
HEARTBEAT_INTERVAL = 5.0

# Maximum iterations for valkey-benchmark (-n flag). Set high so duration controls exit.
BENCHMARK_MAX_ITERATIONS = 2_000_000_000

# Server readiness: max attempts and delay between retries
SERVER_READY_MAX_RETRIES = 10
SERVER_READY_RETRY_DELAY = 1.0  # seconds

# Thread pinning: brief delay for scheduler to apply affinity changes
THREAD_PIN_SETTLE_DELAY = 0.1  # seconds

# when multiple valkey instances run on one host, they will start at this port number and count up
# (e.g. 9000, 9001, 9002, etc)
SERVER_PORT_RANGE_START = 9000

CONDUCTRESS_LOG = PROJECT_ROOT / "log.txt"

VALKEY_CLI = "valkey-cli"
VALKEY_BENCHMARK = "valkey-benchmark"

CONDUCTRESS_RESULTS = PROJECT_ROOT / "results"
CONDUCTRESS_OUTPUT = CONDUCTRESS_RESULTS / "output.jsonl"

# Directory holding the per-runner self-signed test certificates used by the
# connection-storm scenario's TLS herd (a CA plus a server cert for
# 127.0.0.1/localhost). Generated once, idempotently, by
# bootstrap.ensure_tls_test_certs; never used for anything but local testing.
TLS_CERT_DIR = PROJECT_ROOT / "tls"

CONDUCTRESS_QUEUE = PROJECT_ROOT / "benchmark_queue"
CONDUCTRESS_TMP = PROJECT_ROOT / "tmp"
# Export directory the dashboard publisher writes on every publish and rsyncs
# from. Kept on the project disk rather than under tempfile.gettempdir(): on
# many hosts that is a RAM-backed tmpfs, and a full export copy is large enough
# that staging it there competes with the benchmark for memory and can fill the
# filesystem that perf and other scratch writers also depend on. The directory
# persists across runner restarts so the per-commit CPU-stack files (the bulk
# of the export, multi-MB each) keep their mtimes and rsync's quick check skips
# them; it is safe to delete, at the cost of one full re-export and re-sync.
PUBLISH_EXPORT_DIR = CONDUCTRESS_TMP / "publish-export"
# Wall-clock bound on each publish rsync pass. The dashboard files (series,
# manifests, notable feeds; ~100 MB) and the CPU-stack files (~10 GB, mostly
# unchanged) are synced in separate passes so a slow stacks pass cannot hold
# back the files the dashboard reads. A publish runs at a task boundary with no
# measurement in flight, so the bound protects the queue, not a measurement.
PUBLISH_RSYNC_TIMEOUT_SECONDS = 600
CONDUCTRESS_FAILED_LOG = PROJECT_ROOT / "failed_tasks.jsonl"
CONDUCTRESS_FAILED_DIR = PROJECT_ROOT / "failed"

# ssh key to use when accessing the server
# Replace this with the path to your private key file
SSH_KEYFILE = PROJECT_ROOT / "server-keyfile.pem"

# Repositories to make available for testing
# format: (git_url, directory_name)
# Each will be cloned into ~/directory_name on each server
# The directory name is used to refer to the repo in the task queue and in results
#
# _BUILTIN_REPOSITORIES is the shipped default. An optional gitignored
# repositories.json in PROJECT_ROOT (same local-config pattern as servers.json /
# runner.json) can EXTEND this list, or REPLACE it entirely when the file sets
# "replace": true. This lets a fleet host trim the defaults without editing
# source; the built-in list stays authoritative when no file is present.
_BUILTIN_REPOSITORIES = [
    ("https://github.com/valkey-io/valkey.git", "valkey"),
    ("https://github.com/rainsupreme/valkey.git", "rainsupreme"),
    ("https://github.com/valkey-io/valkey.git", "zuiderkwast"),
    ("https://github.com/JimB123/valkey.git", "JimB123"),
    ("https://github.com/valkey-rainfall/valkey.git", "valkey-rainfall"),
    ("https://github.com/redis/redis.git", "redis"),
]


def load_repositories() -> list[tuple[str, str]]:
    """Resolve the repository list, applying an optional repositories.json.

    Returns the built-in defaults unchanged when no repositories.json exists.
    When the file is present it either EXTENDS the defaults (appending its
    entries, skipping directory names already present) or REPLACES them when it
    carries ``"replace": true``. Each entry is ``{"url": ..., "name": ...}``.

    A malformed file (bad JSON, missing keys, wrong types, empty replace list)
    raises ValueError with a clear message rather than silently degrading the
    fleet's repository set.
    """
    config_path = PROJECT_ROOT / "repositories.json"
    if not config_path.exists():
        return list(_BUILTIN_REPOSITORIES)

    try:
        raw = json.loads(config_path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"repositories.json is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"repositories.json must be a JSON object, got {type(raw).__name__}")

    replace = raw.get("replace", False)
    if not isinstance(replace, bool):
        raise ValueError(f"repositories.json 'replace' must be a boolean, got {replace!r}")
    entries = raw.get("repositories", [])
    if not isinstance(entries, list):
        raise ValueError(f"repositories.json 'repositories' must be a list, got {type(entries).__name__}")

    parsed: list[tuple[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"repositories.json entry must be an object, got {entry!r}")
        url = entry.get("url")
        name = entry.get("name")
        if not isinstance(url, str) or not url:
            raise ValueError(f"repositories.json entry missing string 'url': {entry!r}")
        if not isinstance(name, str) or not name:
            raise ValueError(f"repositories.json entry missing string 'name': {entry!r}")
        parsed.append((url, name))

    if replace:
        if not parsed:
            raise ValueError("repositories.json sets 'replace': true but lists no repositories")
        return parsed

    result = list(_BUILTIN_REPOSITORIES)
    existing_names = {name for _, name in result}
    for url, name in parsed:
        if name not in existing_names:
            result.append((url, name))
            existing_names.add(name)
    return result


REPOSITORIES = load_repositories()
REPO_NAMES = [repo[1] for repo in REPOSITORIES]

# unique name indicating the binary was uploaded manually
MANUALLY_UPLOADED = "manually_uploaded"
assert MANUALLY_UPLOADED not in REPO_NAMES, "MANUALLY_UPLOADED must not overlap with any repository names"


@dataclass
class ServerInfo:
    """Information about a server used in benchmarking."""

    ip: str
    """IPv4 address of the server."""
    username: str = ""
    """username to connect with"""
    name: str = ""
    """A unique descriptive name"""
    disabled: bool = False
    """Whether this server is disabled and should be skipped"""
    role: str = ""
    """``runner`` (this machine runs the Conductress runner service and may also host
    servers) or ``server`` (a benchmark server only: no runner service is ever
    installed there). Empty means the default of :meth:`effective_role`."""

    ROLES = ("runner", "server")

    def __post_init__(self) -> None:
        if self.role and self.role not in self.ROLES:
            raise ValueError(f"server {self.name or self.ip!r}: role must be one of {self.ROLES}, got {self.role!r}")

    def effective_role(self) -> str:
        """The explicit role, else ``runner`` for this machine (loopback) and ``server`` for any other host.

        A remote entry in servers.json is a benchmark server by default: a
        runner unit there would compete with the measured server for CPU (a
        crash-looping runner on a server host disturbed a whole boot of results).
        """
        if self.role:
            return self.role
        return "runner" if self.ip in ("localhost", "127.0.0.1", "::1") else "server"

    def __eq__(self, other) -> bool:
        if not isinstance(other, ServerInfo):
            return False

        def normalize_localhost(ip):
            return "127.0.0.1" if ip in ("localhost", "127.0.0.1", "::1") else ip

        return normalize_localhost(self.ip) == normalize_localhost(other.ip)


def load_server_ips() -> list[ServerInfo]:
    """Load server IPs from a JSON configuration file."""
    config_path = PROJECT_ROOT / "servers.json"
    default_path = PROJECT_ROOT / "servers.default.json"
    if config_path.exists():
        data = json.loads(config_path.read_text())["valkey_servers"]
    elif default_path.exists():
        data = json.loads(default_path.read_text())["valkey_servers"]
    else:
        raise FileNotFoundError(f"No server config found at {config_path} or {default_path}")
    all_servers = [ServerInfo(**entry) for entry in data]
    return [s for s in all_servers if not s.disabled]


_SERVERS: Optional[list[ServerInfo]] = None


def get_servers() -> list[ServerInfo]:
    """Lazy accessor for server list. Loads from servers.json on first call."""
    global _SERVERS
    if _SERVERS is None:
        _SERVERS = load_server_ips()
    return _SERVERS
