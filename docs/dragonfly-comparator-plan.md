# Dragonfly as a comparator engine: integration plan

Status: plan, not started. Written 2026-09-11 from a code read of `main` at `7740769`
and an engine survey (sources at the end). Intended to be picked up in a fresh
session; everything a fresh session needs is here.

## Goal

Publish Dragonfly throughput, latency and memory series on the dashboard next to
Valkey and Redis, measured by the same Conductress cells on the same hosts, with
the parameter table published alongside. Valkey stays the subject; Dragonfly is a
reference line, as Redis is today.

## Non-goals

- Bisecting Dragonfly history. It is a pinned release re-measured on cadence, not
  a swept ref.
- Profiling Dragonfly internals. Same posture as Redis: aggregate numbers only.
- Cluster mode, persistence, replication, or any workload beyond the GET/SET/mixed
  cells the dashboard already runs.
- Making every engine-specific assumption in Conductress generic. Only the ones
  Dragonfly actually hits (listed below).

## Why Dragonfly and not the others

Dragonfly is shared-nothing thread-per-core with fibers, io_uring, mimalloc and
non-forking snapshots: the architecture the io-threads and thread-owned-clients
work is arguing about, shipped. Its only rigorous public numbers are its own
(2.4x Valkey at 16 vCPU, 4.5x at 48 vCPU, GCP, March 2025); no independent,
documented benchmark exists. Garnet (Microsoft, MIT, .NET) is the second most
informative and reuses most of this plan. KeyDB is unmaintained since 2023.
Redict is Redis 7.2.4. Kvrocks is RocksDB-on-disk and its maintainers say a
direct comparison "makes no sense".

Licence: BSL 1.1 with an additional-use grant that forbids competing in-memory
store products and hosted offerings. Nothing in the licence or the Terms of Use
restricts running or publishing benchmarks.

## What Conductress assumes today, and where Dragonfly breaks it

Every engine is a Redis-lineage git tree. Three places encode that:

1. **Build.** `SweepEngine` (`src/conductress/config.py`) is `source` (a
   `REPOSITORIES` entry), `ref`, `binary_name`, `make_args`. `binary_manager.py`
   checks out a commit and runs `make distclean && cd src && make -j`, expecting
   `src/<binary_name>`. Dragonfly is CMake, C++20, with vendored dependencies
   (helio, abseil, boost); a from-source build is 20-30 minutes and needs a
   toolchain the RHEL 9 and Amazon Linux runners may not have.
2. **Launch.** `server.py` builds a Valkey command line: `--io-threads N`,
   `--server-cpulist`, `--bio-cpulist`, `--aof-rewrite-cpulist`,
   `--bgsave-cpulist`, `--save ""`, `--protected-mode no`, `--daemonize yes`,
   `--logfile`. Dragonfly accepts none of the cpulist flags, has no `--daemonize`,
   and names its thread count `--proactor_threads`.
3. **Series.** `sweep/coordinator.py` populates `merge_commits` from the ref and
   bisects them; `sweep/exporter.py` emits `series-{platform}-{workload}-*.json`
   keyed by commit; `publisher.py` and the coordinator map `engine.source` to a
   GitHub repo for commit links (`redis/redis` else `valkey-io/valkey`). A pinned
   release has one "commit" and a version string, and re-measures over time.

Two smaller ones:

4. `memory_coordinator.py` and the exporter read jemalloc statistics for the
   memory decomposition. Dragonfly uses mimalloc. `profile_internals=False`
   already skips the flamegraph and jemalloc breakdown for Redis; confirm nothing
   else parses `MEMORY STATS` or `INFO memory` fields that mimalloc lacks.
5. `Server.get_num_cpus` allocates `io_threads + 2` CPUs (main + io + two
   background). Dragonfly's model is N proactor threads and nothing else, so the
   mapping is `threads -> proactor_threads` with `threads` CPUs pinned via
   `taskset`, not `threads + 2`.

## Design

### Engine kinds

Extend `SweepEngine` with a `kind` and a launch profile rather than adding
Dragonfly special cases inline:

```python
@dataclass
class SweepEngine:
    source: str
    binary_name: str
    kind: str = "redis-lineage"          # or "prebuilt-release"
    ref: Optional[str] = None            # redis-lineage: git ref to sweep
    release: Optional[str] = None        # prebuilt-release: pinned tag, e.g. "v1.40.2"
    release_asset: Optional[str] = None  # prebuilt-release: asset name pattern with {arch}
    launch: str = "valkey"               # launch profile name (see below)
    thread_flag: str = "--io-threads"
    cpu_overhead: int = 2                # extra CPUs beyond threads
    make_args: str = DEFAULT_MAKE_ARGS
    heap_alloc_funcs: list[str] = field(default_factory=list)
    profile_internals: bool = True
```

Existing entries keep working with defaults. Dragonfly:

```python
SweepEngine(
    source="dragonfly", binary_name="dragonfly", kind="prebuilt-release",
    release="v1.40.2",
    release_asset="dragonfly-{arch}.tar.gz",   # confirm exact names on the release page
    launch="dragonfly", thread_flag="--proactor_threads", cpu_overhead=0,
    profile_internals=False,
)
```

### Provisioning (binary_manager)

Add a `prebuilt-release` path beside the existing build path: resolve
`release_asset` for the host arch (`x86_64` / `aarch64`), download from
`https://github.com/dragonflydb/dragonfly/releases/download/{release}/...`,
verify the published checksum, unpack into the existing build cache keyed by
`(source, release, arch)` so the cache and `ensure_binary_cached` logic are
unchanged. The "commit hash" for a prebuilt release is the tag string.

Build-from-source is the fallback if arm64 assets turn out not to exist. Do not
build both paths up front; decide after the asset check in step 1 below.

### Launch profiles (server.py)

Factor the Valkey command-line construction into a `valkey` profile and add a
`dragonfly` profile. Both receive `(binary, port, threads, cpus, logfile,
extra_args)` and return a shell command. Dragonfly, to be confirmed against
`dragonfly --helpfull` on the pinned release:

```
taskset -c {cpus} {binary} --port {port} --bind 0.0.0.0
  --proactor_threads {threads} --dbfilename "" --snapshot_cron ""
  --logtostderr=false --log_dir {logdir}
  --maxmemory {mem} --conn_use_incoming_cpu=false
```

Launched with `nohup ... &` since there is no daemonize. Readiness is the
existing `PING` probe. Shutdown is the existing `SHUTDOWN NOSAVE`, which
Dragonfly supports. Check whether `--ulimit memlock` matters bare-metal; the
official Docker examples pass `--ulimit memlock=-1`.

Thread pinning: Dragonfly has `--proactor_affinity_mode`; with `taskset` on the
whole process and `proactor_threads == len(cpus)` the placement is equivalent to
the Valkey cpulist flags for benchmarking purposes.

### Series semantics

A pinned-release engine has exactly one commit. Add a coordinator mode for it:
`merge_commits = [release]`, no bisection, re-measure on the canary cadence so
the series shows drift over time (host and kernel changes) rather than over
commits. The exporter emits the same `series-{platform}-{workload}-*.json` with
`metadata.engine = "dragonfly"` and `metadata.pinned = "v1.40.2"`; the dashboard
draws a pinned series as a flat reference band across the Valkey x-axis (date),
which is also how Redis 8.0.0's floor should probably be drawn.

Commit links: extend the `engine.source -> repo` mapping to
`dragonflydb/dragonfly` and link releases, not commits, for pinned engines.

### Dashboard

- Engine selector gains Dragonfly. `compare.html` becomes three-way.
- Pinned series render as a horizontal band with the release label.
- The methodology text gains a paragraph: Dragonfly version, flags, thread
  count, the note that `MGET` across shards pays a multi-shard transaction
  Valkey does not, and that cluster-mode emulation is off.
- Publish the parameter table (threads, connections, pipelining, key/value
  sizes, key count, warmup, duration) next to any chart with a comparator.
  Dragonfly's own benchmark post does this; match it.

## Fleet rollout

1. **Asset check** (blocking). On the `v1.40.2` release page, confirm an
   aarch64 asset exists and note exact asset names and checksum file. If none:
   build from source on armbench once, time it, and decide source vs prebuilt.
2. **Kernel and ulimit.** All four runners are 5.10+ (armbench is RHEL 9 with
   the io_uring backport). Check `ulimit -l` on each; raise via the existing
   systemd drop-in if Dragonfly complains.
3. **Smoke on g4bench.** Provision, launch with `--proactor_threads 7`, run one
   cachecannon GET P10/400c cell through the normal task path, confirm INFO
   fields the memory task reads (`used_memory`) are present and sane.
4. **One-cell A/B.** Same cell against Valkey unstable head on the same host,
   same hour. Sanity-check the ratio against Dragonfly's published 16-vCPU
   figure; a wildly different number means a launch-flag mistake, not a finding.
5. **Fleet enable** as a v3-epoch coordinator at low urgency, one platform at a
   time, via the existing sweep pause and epoch precedence levers.
6. **Dashboard** PR after the first series lands, so the methodology paragraph
   describes what actually ran.

## Acceptance

- `SWEEP_ENGINES` gains Dragonfly with no change to Valkey or Redis behaviour;
  existing tests pass.
- Dragonfly GET/SET/mixed series on all four platforms, with the parameter
  table published.
- No flamegraph, jemalloc or symbol data collected for Dragonfly (same as Redis).
- The guide's brand rule holds: the comparator is presented as a reference line,
  the dashboard title and identity are unchanged.

## Open questions

- arm64 prebuilt availability (docs only link x86-64).
- Kernel floor: Dragonfly's docs say 4.14, 4.19 and 5.11 in three places.
  5.10+ is assumed sufficient.
- Whether `SHUTDOWN NOSAVE` and `INFO` field names match closely enough for the
  existing parsers; `INFO` is Redis-compatible by design but has extra sections.
- Pinning cadence: daily like the drift canary, or weekly.
- Whether to also pin a Valkey release as a band, so Dragonfly is compared to a
  release as well as to unstable head.

## Effort

Two to three days to the first published series: one day for provisioning and
launch profiles, half a day for the pinned-series mode, half a day for the
dashboard, the rest for the fleet steps and a rollout that waits on real cells.
Garnet afterwards: one more day (reuses kinds, profiles and series mode; adds a
.NET runtime install and its own launch profile; `MSET` is non-atomic by
default, irrelevant to the GET/SET cells).

## Sources

- Dragonfly licence: https://github.com/dragonflydb/dragonfly/blob/main/LICENSE.md
- Dragonfly vs Valkey benchmark (their methodology): https://www.dragonflydb.io/blog/dragonfly-vs-valkey-benchmark-on-google-cloud
- Dragonfly releases (v1.40.2, 2026-09-03; 1.40.1 fixed pipelined connection state): https://github.com/dragonflydb/dragonfly/releases
- Dragonfly shared-nothing design: https://github.com/dragonflydb/dragonfly/blob/main/docs/df-share-nothing.md
- Dragonfly build from source: https://github.com/dragonflydb/dragonfly/blob/main/docs/build-from-source.md
- Dragonfly cluster-mode emulation: https://www.dragonflydb.io/docs/managing-dragonfly/cluster-mode
- Garnet: https://github.com/microsoft/garnet (MIT; v2.1.7, 2026-09-09)
- KeyDB status: https://github.com/Snapchat/KeyDB/issues/895 (creator's departure, recommends Valkey)
- Kvrocks on direct comparison: https://github.com/apache/kvrocks/discussions/389
