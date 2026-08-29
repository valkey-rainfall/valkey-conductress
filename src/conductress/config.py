"""Configuration for the Conductress benchmark framework"""

import json
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

# Benchmark defaults (single source of truth for CLI and TUI)
DEFAULT_IO_THREADS = 9
DEFAULT_PIPELINING = 10
DEFAULT_WARMUP = 5  # seconds
DEFAULT_DURATION = 30  # seconds
DEFAULT_REPETITIONS = 3
DEFAULT_VAL_SIZE = 512  # bytes
DEFAULT_KEY_SIZE = 0  # 0 = standard keys

# Dashboard data server (rsync target for --publish)
PUBLISH_TARGET = "ec2-user@data.conductress.rainsupreme.net:/var/www/data"

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

# TUI refresh interval in seconds
TUI_REFRESH_INTERVAL = 15

# =============================================================================
# RUNTIME CONSTANTS
# =============================================================================

# Task runner polls the local queue at this interval when idle (seconds)
QUEUE_POLL_INTERVAL = 4

# Fleet mailbox management happens only between tasks. The runner sleeps after
# the final boundary contact before starting benchmark work.
FLEET_IDLE_POLL_INTERVAL = 30
MANAGEMENT_SETTLE_SECONDS = 2.0
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
    ),
]


def get_sweep_engine(source: str) -> Optional["SweepEngine"]:
    """Look up a SweepEngine by source name."""
    for engine in SWEEP_ENGINES:
        if engine.source == source:
            return engine
    return None


def should_profile_internals(engine: Optional["SweepEngine"]) -> bool:
    """Whether to collect internal profiling (CPU flamegraphs, jemalloc breakdown) for an engine.

    Absent/unknown engine (e.g. a fork source, or legacy state with no engine) defaults to True
    so Valkey profiling is unaffected; an engine only opts out via profile_internals=False (Redis).
    This is the single source of truth for the policy — call it everywhere rather than inlining
    `engine.profile_internals` checks (which have to re-handle the None case and read poorly).
    """
    return engine.profile_internals if engine else True


# =============================================================================
# Sweep configuration: latency
# =============================================================================
LATENCY_STATE_FILE = SWEEP_STATE_DIR / "latency_state.json"
LATENCY_TARGET_RPS = 100_000  # flat rate, same across all platforms/commits
LATENCY_MAKE_ARGS = ""
LATENCY_DETECTION_THRESHOLD = 0.10  # 10% p99 change triggers bisection
LATENCY_THREADS = 4
LATENCY_CLIENTS = 16  # 64 total connections
LATENCY_PIPELINE = 1  # no pipelining — measures true per-request latency
LATENCY_DURATION = 60
LATENCY_KEYSPACE = 1_000_000
LATENCY_VAL_SIZE = 16
LATENCY_REPS = 3
MEMTIER_COMMIT = "d52544b1"  # pinned version for reproducible latency measurements
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

CONDUCTRESS_QUEUE = PROJECT_ROOT / "benchmark_queue"
CONDUCTRESS_TMP = PROJECT_ROOT / "tmp"
CONDUCTRESS_FAILED_LOG = PROJECT_ROOT / "failed_tasks.jsonl"
CONDUCTRESS_FAILED_DIR = PROJECT_ROOT / "failed"

# ssh key to use when accessing the server
# Replace this with the path to your private key file
SSH_KEYFILE = PROJECT_ROOT / "server-keyfile.pem"

# Repositories to make available for testing
# format: (git_url, directory_name)
# Each will be cloned into ~/directory_name on each server
# The directory name is used to refer to the repo in the task queue and in results
REPOSITORIES = [
    ("https://github.com/valkey-io/valkey.git", "valkey"),
    ("https://github.com/rainsupreme/valkey.git", "rainsupreme"),
    ("https://github.com/valkey-io/valkey.git", "zuiderkwast"),
    ("https://github.com/JimB123/valkey.git", "JimB123"),
    ("https://github.com/valkey-rainfall/valkey.git", "valkey-rainfall"),
    ("https://github.com/redis/redis.git", "redis"),
]
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
