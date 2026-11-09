# Configuration

Conductress reads its settings from three places, in order of specificity:

1. **`config.py`** — the built-in defaults, committed to the repository. These
   are the values a runner uses when nothing overrides them.
2. **Environment variables** — override a subset of those defaults without
   editing or redeploying source. Listed per setting below.
3. **Local JSON files** in the project root — per-host configuration that is
   never committed (each is gitignored, with a committed `*.default.json`
   example alongside it).

Nothing here changes benchmark behavior on its own: the defaults are the
effective values unless an override is set.

## Local files

These live in the project root next to `config.py`. Each is gitignored so a host
keeps its own copy; a committed `*.default.json` shows the expected shape and is
used as a fallback when the real file is absent.

| File | Purpose | Example file |
| --- | --- | --- |
| `servers.json` | Benchmark server list (`ip`, `username`, `name`, optional `disabled`). | `servers.default.json` |
| `runner.json` | This runner's stable identity (`runner_id`, `display_name`). | `runner.default.json` |
| `repositories.json` | Optional. Extends or replaces the built-in repository list (see below). | `repositories.default.json` |

## Paths

The load generators run on the remote benchmark host over SSH, so their paths
are resolved on that host, not on the machine running the CLI.

| Setting | Default | Environment override | Notes |
| --- | --- | --- | --- |
| `REMOTE_MEMTIER_BENCHMARK` | `~/conductress/memtier_benchmark` | — | Used in the benchmark command strings. `~` expands on the **remote** host, so this stays a `~/...` string. |
| `MEMTIER_BENCHMARK` | `PROJECT_ROOT/memtier_benchmark` | — | Local-path form for code that resolves the binary under this checkout. |
| `CACHECANNON_BINARY` | `/home/ec2-user/cachecannon/target/release/cachecannon` | `CONDUCTRESS_CACHECANNON_BINARY` | Absolute path the cachecannon generator builds to on the benchmark host. |

Both binaries are installed on each host by the bootstrap step; you rarely
need to override these. cachecannon drives the throughput, mixed and latency
tasks and the sweeps; memtier_benchmark drives the scenario tasks, which need
its finite request-count mode and per-second timeseries output.

## Benchmark defaults

The throughput and latency defaults (connection counts, thread counts,
pipelining, warmup and scored durations, value sizes, adaptive-repetition
targets) are grouped near the top of `config.py` under the `Benchmark defaults`
and `Sweep configuration` headings. Change them in source; they are the single
source of truth shared by the CLI and the runner. The cachecannon v3 sweep
values are locked to a measured workload identity — changing any of them defines
a new workload, not a continuation of the series.

## Sweep environment toggles

The sweep measures each new commit under a fixed combination of load generator
and benchmark parameter set; one such combination is a *sweep epoch*, and each
epoch carries an identifier (`v1`, `v3`, ...). Results are comparable only
within an epoch, so the `v1`/`v3` shorthand used here and elsewhere names which
generator-and-parameter identity produced a point. `v1` identifies the sweep
driven by `valkey-benchmark`, Valkey's stock load generator; `v3` identifies the
cachecannon-driven sweep.

The v3 Valkey roster is seven series, all at the v3 identity (400 connections, 8
client threads, 7 server io-threads, 3M uniform keys, 16-byte keys and values
unless the series says otherwise):

- **GET throughput at P10** (`get-k16-v16-t7-p10`) — the pipelined-read ceiling.
- **mixed GET/SET throughput at P10** (`mixed-s20-k16-v16-t7-p10`) — the
  canonical 80:20 read/write mix.
- **SET throughput at P10** (`set-k16-v16-t7-p10`) — the write counterpart of
  the GET series.
- **GET throughput at P1** (`get-k16-v16-t7-p1`) — the unpipelined read path.
  It runs a **16-client-thread** budget (`SWEEP_V3_P1_CLIENT_THREADS`), not the
  8 the other throughput series use: at P1 cachecannon issues one syscall per
  request instead of one per ten, so it spends 4–5x more CPU per request and 8
  client threads saturate the client below the server's ceiling — on Graviton3
  the 8-thread P1 backfill hit client utilization 0.941, meaning it was
  measuring the client rather than the server. 16 threads (25 connections per
  thread at 400c) restores the headroom. The client budget is part of the
  series identity and never changes within the series, so the P1 series history
  was cleared and restarted at 16 threads rather than mixing two client shapes
  on one line. Its point is scored on the **median** of the per-rep series
  rather than the mean, because P1 throughput can land in two distinct modes
  across server restarts; the median reports the mode most reps reached, where a
  mean would report a value no rep produced. The coefficient of variation and
  the published score bounds still show the full between-restart spread, so a
  mode split is visible rather than hidden.
- **GET throughput at P10 with 1024-byte values** (`get-k16-v1024-t7-p10`) —
  the raw-encoded, copy-dominated reply path. A 16-byte value is embstr-encoded
  and its reply is dispatch-bound; a 1024-byte value is a separate encoding
  whose reply cost is the buffer copy, so it moves on reply-path changes the
  16-byte series cannot see (and does not echo embstr-path changes that series
  does). This series starts at `SWEEP_V3_LARGE_VALUE_FLOOR_TAG` (`9.0.0`)
  rather than the fork point: it guards the reply path going forward, and a
  full backfill would slow the other five series for history nobody has asked
  for. A series floor overrides the engine floor for that one series.
- **GET latency at P1** (`get-k16-v16-t7-p1-r100k`) — p99 at a fixed request
  rate (`SWEEP_V3_LATENCY_*`), lower is better.
- **GET throughput at P10 over TLS** (`get-k16-v16-t7-p10-tls`) — the encrypted
  read path. Same identity as the canonical GET series except the transport:
  the server opens a TLS listener on `SWEEP_V3_TLS_PORT` with the bootstrap test
  certificates (a CA plus a `127.0.0.1`/`localhost` server certificate,
  generated once per runner by `bootstrap.ensure_tls_test_certs`) and cachecannon
  connects over it (`[target] tls = true`, `tls_verify = true`,
  `tls_hostname`/`tls_ca_file` pointed at the loopback address and that CA). The
  plaintext port keeps serving the server's own housekeeping. A TLS cell needs a
  `BUILD_TLS=yes` binary — Valkey compiles TLS out by default — so the task
  appends `BUILD_TLS=yes` to the build's make arguments; the build cache is keyed
  by the make arguments, so a TLS binary and a plaintext binary of the same
  commit occupy distinct cache entries and neither is ever served to the other
  transport. Like the large-value series it starts at
  `SWEEP_V3_TLS_FLOOR_TAG` (`9.0.0`) rather than the fork point: it guards the
  TLS path going forward without slowing the other six for two years of history.
  A series floor overrides the engine floor for that one series; a comparison
  engine measured at release-and-tip never reads it. The export records `tls` in
  the series `metadata` so a reader can tell the TLS GET line apart from the
  plaintext GET line.

A cachecannon task chooses how the recorded score aggregates the per-rep series
through `score_aggregate` (`mean`, the default, or `median`), and always records
the minimum and maximum of that series; the v3 export publishes those bounds
beside the score, its coefficient of variation and its repetition count. The
export also records each series' client budget — `connections` and
`client_threads` — in the series `metadata`, so a reader can tell the 16-thread
P1 line apart from the 8-thread P10 lines.

Every v3 point also carries the load generator's CPU utilization, lifted from
the result row's `client_cpu` block, and the export publishes it beside the
score: `client_utilization` (peak cores busy over the point's reps divided by
the generator's allocated cores), `client_saturated`, `client_cores_busy` (the
peak cores-busy value itself) and `client_allocated_cores`. `client_saturated`
is true when the generator ran at or above 90% of its allocated cores
(`CLIENT_CPU_SATURATION_THRESHOLD`): a saturated point measured the *client's*
capacity, not the server's, so a saturated point on an otherwise flat line is
the signal that the load generator, not the server, was the binding constraint.
The keys are omitted (never exported as null) on v1 series and on points
recorded before the fields existed.

Two series rules follow from the epoch definition:

- A series that needs no load generator belongs to every epoch. Memory
  overhead is read from the server's own `INFO` after Conductress fills it
  through its populator, so one memory series is published under each epoch in
  `SWEEP_GENERATOR_INDEPENDENT_EPOCHS` from one state file; the first entry is
  the epoch it schedules and pauses under.
- An epoch-1 series that a newer epoch has replaced is *retired*
  (`SWEEP_V1_RETIRED_SERIES`, keyed `metric:workload`): it keeps its state and
  keeps publishing the history it holds, but never queues another task, so the
  replacement is the only series still measuring that workload. Every v1 Valkey
  throughput series — the default GET series and every entry in
  `SWEEP_THROUGHPUT_WORKLOADS` (the 64- and 128-byte GET series, the SET series,
  the four P1 variants, and the four platform-optimal series) — is retired, as
  is the v1 GET latency series; the set is derived from that same roster data
  rather than hand-listed, so it cannot drift from it. Memory series (which
  belong to every epoch) and a comparison engine's own series follow the
  engine's scope and are not retired here. The `conductress sweep pause`
  selectors are the runtime lever for everything else; retirement is the
  permanent one.

### Archived epochs

Retirement stops a series from *scheduling* but still builds its coordinator
and loads its state at every runner start so the publisher can re-export it.
For v1 that load is the whole problem: its state files are 1.2–1.5 GB each
(points embed perf stacks), several per platform, and their history never
changes again, so a runner spent 6–7 GB of resident memory reloading dead data
at every restart. **Archiving** goes one step further than retirement: an
archived epoch (`SWEEP_ARCHIVED_EPOCHS`, currently `v1`) has *no coordinator
built and no state loaded* by the runner service, and is dropped from the
scheduling precedence. v1 history keeps rendering because its exported
dashboard files (`series-<plat>-*.json` under their legacy unqualified names,
`notable-<plat>.json`, and `manifest-<plat>.json`) persist in the publish
export dir (`PUBLISH_EXPORT_DIR`) across restarts and are re-synced as-is, and
every manifest, live or archived, advertises the same epoch list: live epochs
first, then archived ones, each entry carrying an `archived` boolean. The
dashboard reads the legacy `manifest-<plat>.json` to discover which epochs
exist before it picks one, so the archived manifest cannot be left with the
list it had when it was last exported: each publish rewrites that one key in
place (its workload lists, which describe the archived data, are untouched),
and skips the write when the list already matches so the file's mtime, and
rsync's quick check, are undisturbed. This is what lets the dashboard default
to a live epoch and label archived ones without inferring liveness from list
order.

Two consequences follow:

- Memory series now publish only under v3 (`SWEEP_GENERATOR_INDEPENDENT_EPOCHS
  = ("v3",)`). The v1-named memory files already on the data server stay as
  archived copies.
- A runner whose export dir was wiped (an old-code restart, a fresh host) has
  no v1 files to re-sync and, with no v1 coordinator ever loading state,
  nothing regenerates them. The one-time repair is:

  ```
  conductress sweep export-archived --epoch v1
  ```

  It loads the v1 state files once, exports the v1 series, notable and manifest
  files into the publish export dir, and exits — the runner service never pays
  that load cost. Run it once on such a host, then the ordinary boundary
  publishes re-sync the files like any other. It exports nothing (and exits
  non-zero) when no v1 state files are present.

`SWEEP_V1_RETIRED_SERIES` and the coordinator `retired` flag are unchanged and
still apply to the coordinators that `conductress sweep export` and
`export-archived` build; they are simply unreachable from the archived runtime
path, which builds no v1 coordinator at all.

### Resetting a series

A series' measurement definition is part of its identity, so a change to it —
the P1 GET series moving from 8 to 16 client threads, or the memory series
switching from a loaded to a settled sample — must clear the old points rather
than mix two definitions on one chart line. `conductress sweep reset-series`
clears a series' history so the next boundary publish restarts it fresh:

```
conductress sweep reset-series --epoch v3 --workload get-k16-v16-t7-p1
conductress sweep reset-series --epoch v3 --workload memory-sadd-m20
conductress sweep reset-series --epoch v3 --all-memory
```

A throughput series is named by its workload label; a memory series by the
memory coordinator's workload id, `memory-<shape>`, which selects that
coordinator's state file (`memory_state_[<engine>-]<shape>.json`) and note
format (`[memory-sweep:<shape>]`, with the engine read from the queued task's
`source`). `--all-memory` resets every memory series on the roster for one
engine under a single timestamp, which is the form a memory definition change
needs: it applies to every memory series at once.

It backs up the coordinator's state file to `<file>.bak-<UTC timestamp>` and
deletes it, then relocates (never deletes) any queued task files whose note
names the series into a `benchmark_queue/reset-<timestamp>/` folder, so a cell
queued at the old budget cannot complete after the deploy and land a stale point
on the fresh line. Pass `--engine redis` to reset a comparison engine's
prefixed series (e.g. `redis-get-k16-v16-t7-p1`), `--dry-run` to see what it
would do without touching anything, and `--force` to override its refusal to
run while `conductress.service` is active (a running runner may be mid-task on
the series). The coordinator's completion-time identity guard (below) is the
backstop: a queued 8-thread cell that slips past the reset is refused at
completion rather than recorded.

The deploy-time procedure is: stop the runner service, reset the affected
series for every engine, then start it again —

```
sudo systemctl stop conductress.service
conductress sweep reset-series --epoch v3 --workload get-k16-v16-t7-p1
conductress sweep reset-series --epoch v3 --workload get-k16-v16-t7-p1 --engine redis
sudo systemctl start conductress.service
```

or, for a memory definition change, `--all-memory` and `--all-memory --engine
redis` in place of the two `--workload` lines.

### Environment periods

Every point a coordinator records now carries the task id, the result row's
end time (`measured_at`) and the row's reduced environment
(`runner_identity.environment_for_period`: kernel, glibc, microcode,
Conductress revision, cachecannon commit, boot id, the same for the server
host when it is another machine, and the provenance schema version). The
exporter groups the ordered points of a series into environment PERIODS: a
new period starts wherever the recorded environment differs between two
consecutive points, judged by the same `environment_changes` rule as the
canary annotation. `series.json` publishes `periods` (id, commit index and
date range, the period's PUBLIC environment, `boot_only`, a short `label` such
as `kernel_release: 6.1.170-... -> 6.1.189-...; cpu_microcode: ...`) and each
point's `period`. Public means the allowlist of `public_fields.py`: package
versions and CPU words stay; a harness or load-generator commit, a boot id or
a host id never appear, and a change of one of them is published as its type
(`harness revision changed`, `load generator changed`, `reboot`, `host
replaced`). An annotation whose two points sit in different periods is
flagged `env_period_crossed` so a reader does not blame a commit for an OS
change. A change of boot id alone is a `boot_only` period (a reboot: a
possible change, drawn lighter than a package or kernel change). Points
recorded before the fields existed join the previous period, so the deploy
that starts recording draws no boundary.

Periods are MARKERS. Nothing splits a series automatically; that is the
manual decision described under "Series generations".

Manual events live in `sweep_data/environment_events.json`, next to
`generations.json`, for changes the provenance fields cannot see (a BIOS
update, a package release that keeps the library version string). Record
them with the CLI, which checks the text and writes the file atomically:

```
conductress sweep add-event --at 2026-11-06T16:14:23Z --until 2026-11-06T18:21:58Z \
    --label "package update and reboot" --fields glibc,microcode_ctl,reboot
conductress sweep explain-event --date 2026-09-21 --text "calibration-period harness deploys"
```

The file then holds:

```
{"schema_version": 1,
 "events": [{"at": "2026-11-06T16:14:23Z", "until": "2026-11-06T18:21:58Z",
             "label": "package update and reboot", "fields": ["glibc", "microcode_ctl", "reboot"],
             "recorded_at": "...", "recorded_by": "operator"}],
 "explanations": [{"event_date": "2026-09-21", "text": "calibration-period harness deploys",
                   "recorded_at": "...", "recorded_by": "operator"}]}
```

A point measured at or after an event's `at` starts a period labelled with
the event. The manifest `version` is 4 since periods and generations exist.
The live manifest also publishes `environment_events` and `explanations`
without `recorded_at` and `recorded_by`; the canary findings on the data host
read them (see "Findings" in `docs/canary-drift-analysis.md`). Labels and
texts are PUBLIC: the CLI refuses text that looks like a revision id, an IP
address, an absolute path, an e-mail address or an instance id. The older
form of the file, a bare list of `{at, label}`, is still read. Published
generation entries carry only `id, epoch, until, live_from, label, reason,
workloads, excluded_tasks, closed_at, archived`; who closed a generation
stays on the runner.

### Series generations

An epoch names the measurement protocol and is the same on every platform. A
GENERATION is per platform: it closes a platform's live v3 history at a point
in time when the host's environment changed so much that later points cannot
share a chart line with earlier ones (intelbench, 2026-11-06: kernel, glibc
and microcode update, canary +12.3%). Closing a generation is a MANUAL
decision taken after such a change was confirmed, never an automatic step; a
reboot or a small update is only marked, not split.

A point belongs to the generation of the task that produced it, by the task's
START time (``end_time - observed_duration_sec`` of its result row): a task
that started before the cut measured the old environment for its whole run.
Tasks that started between the cut and the resume time (the host was being
updated or rebooted) are discarded: dropped from both generations and listed
in `sweep_data/excluded_tasks.json` with the reason. Their raw result rows in
`results/output.jsonl` are not touched by this command. Every coordinator and
exporter skips a task on that list, so a replayed or re-exported result can
never bring a discarded task back. Memory series are not split (bytes per
key do not depend on kernel, glibc or microcode).

The procedure, on the runner, with the service stopped:

```
conductress sweep pause "*"                 # earlier, between tasks
sudo systemctl stop conductress.service
conductress sweep close-generation --epoch v3 \
    --cut 2026-11-06T16:14:23Z --resume 2026-11-06T18:21:58Z \
    --label "kernel 6.1.170 -> 6.1.189, glibc, microcode 0x2b000685" \
    --reason "canary alarm +12.3% on 2026-11-07" --by rain [--dry-run]
conductress sweep export-archived --generation 2026-11-06
sudo systemctl start conductress.service
conductress sweep resume
```

`close-generation` backs up every live state file (`<file>.bak-<UTC>`),
writes the archived points to `sweep_data/v3/generations/<cut date>/`, keeps
the live points in place, relocates queued cells of the split series like
`reset-series` does, and appends the generation to `sweep_data/generations.json`
(id, cut, resume, label, reason, workloads, number of excluded tasks, who and
when). `export-archived --generation` loads the archived state once and writes
`series-<platform>-<workload>-<metric>.epoch-v3.gen-<id>.json`, the notable
feed and `manifest-<platform>.epoch-v3.gen-<id>.json` into the publish export
dir, where the next publish re-syncs them. The live manifest lists the
platform's closed generations under `generations`, so the dashboard can offer
their read-only history next to the live line. The runner service never loads
archived state. The same event on the data host is handled by
`conductress-control archive-runner` (see control-service.md).

### Memory series: settled sample, deferred backfill

The memory series sample each cell **settled**: after the populate, the memory
task reads every item once (`MemTaskData.settle`) so an in-progress incremental
rehash finishes, and asserts via `DEBUG HTSTATS-KEY` / `DEBUG HTSTATS` that no
rehash is still running before it samples. A sequentially loaded collection is
otherwise caught with two hash tables live, and the extra table is recorded as
the data type's cost. Series files carry `"settled": true` in `metadata`; a
memory series file without the key was measured as loaded and is not comparable
to a settled one, which is why the switch is a series reset rather than a
continuation. The coordinator's identity guard refuses to record a completed
cell that was not measured settled.

Memory is deterministic and its per-item cost moves at a handful of commits per
year, so the memory coordinators defer their **gap-filling backfill**
(`MemorySweepCoordinator.defer_backfill`): when a memory series' next planner
task is `BACKFILL`, the runner schedules it only once every throughput and
latency series has nothing to queue. Nightly HEAD keeps its absolute priority,
and a memory series' release landmarks and bisections compete on urgency with
every other series, so a new memory series still gets its landmarks and
change-points promptly; only its flat regions wait for the performance backfill
to run out of work.

The completion-time **identity guard** in the v3 coordinator refuses to record a
completed cell whose `threads` (or any other identity field) does not match the
series, logging a WARNING and skipping the point rather than crashing — so a
stale 8-thread cell that completes after the deploy never contaminates the
16-thread line. Published per-commit perf and CPU-stack files for the old points
remain on the data server (the publish `rsync` has no `--delete`); they are
harmless orphans the dashboard no longer references once the series file is
rewritten.

### Engines

The sweep can measure more than one server (`SWEEP_ENGINES` in `config.py`).
An engine is described on two independent axes:

- **Provisioning** says how a binary for a revision comes to exist.
  `built-from-git` clones the engine's repository, checks out the revision and
  runs `make` with the engine's `make_args`. `prebuilt-release` is reserved for an engine whose source licence the
  project does not accept on its runners: only the official release asset for
  the host architecture would be downloaded and run, and no source would ever
  be checked out. Declaring an engine with it is a startup error until the
  download path exists.
- **Scope** says how much of the engine's history the sweep measures. `history`
  is the full treatment: release landmarks, bisection of significant deltas,
  backfill of gaps, and every new tip. `release-and-tip` measures the latest
  release once and the current tip at most once per `tip_interval_hours`, with
  no bisection and no backfill.

Valkey is `history`. Redis is `release-and-tip` at 24 hours: it exists to power
the engine comparison, which reads one point at each engine's latest release
and the recent tip and nothing else, so measuring its history would spend
runner time on data nobody reads. A comparison engine's series carry the
engine name as a prefix (`redis-get-k16-v16-t7-p10`), are exported with
`metadata.engine` and `metadata.scope`, and tag each point with why it was
measured (`sample`: `release`, `tip` or `history`). Every epoch-1 throughput
series of a comparison engine is retired; its v3 series are the ones that
measure. Memory series follow the engine's scope like any other series.

An engine that opts out of internal profiling (`profile_internals=False`)
records aggregate results only: throughput, latency and total memory, with no
CPU flamegraph and no allocation breakdown, so nothing from its binary's symbol
table is published.

These variables control which epochs run. Each toggle enables or disables one
epoch's coordinators, and the precedence variable sets which epoch measures a
new commit first. They fail startup on an unrecognized value rather than
silently selecting the wrong epoch.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `CONDUCTRESS_SWEEP_V3_ENABLED` | `false` | Enable the v3 (cachecannon) sweep coordinators. |
| `CONDUCTRESS_SWEEP_EPOCH_PRECEDENCE` | `v3` | Comma-separated scheduling precedence between LIVE epochs, highest priority first. The first-listed epoch measures each new commit first. Archived epochs (see below) listed here are dropped with a log line rather than scheduled. |
| `CONDUCTRESS_SWEEP_ARCHIVED_EPOCHS` | `v1` | Comma-separated epochs whose history keeps rendering on the dashboard but which the runner never builds, loads, or schedules. See "Archived epochs" below. |

Booleans accept `1/true/yes/on` and `0/false/no/off` (case-insensitive); any
other value, including an empty string, is a startup error.

## Repositories

`REPOSITORIES` in `config.py` is the built-in list of source repositories made
available for benchmarking. It is authoritative when no `repositories.json` is
present.

An optional `repositories.json` in the project root adjusts that list:

- **Extend** (default): its entries are appended to the built-in list. A
  directory name already present in the built-in list is skipped.
- **Replace**: set `"replace": true` and only the file's entries are used.

Each entry is `{"url": <git url>, "name": <directory name>}`. A malformed file
(bad JSON, missing `url`/`name`, or `replace: true` with no entries) is a clear
error, not a silent fallback.

Example — upstream-only (this is exactly `repositories.default.json`):

```json
{
    "replace": true,
    "repositories": [
        {"url": "https://github.com/valkey-io/valkey.git", "name": "valkey"}
    ]
}
```

Example — keep the defaults and add one more:

```json
{
    "repositories": [
        {"url": "https://github.com/example/valkey.git", "name": "example"}
    ]
}
```

## Publish and control endpoints

The dashboard data server and the fleet control-plane URL both have a default
that points at the project's default endpoint and an environment override.

| Setting | Default | Environment override | Notes |
| --- | --- | --- | --- |
| `PUBLISH_TARGET` | `ec2-user@data.conductress.rainsupreme.net:/var/www/data` | `CONDUCTRESS_PUBLISH_TARGET` | rsync target for `--publish`. |
| `PUBLISH_RSYNC_TIMEOUT_SECONDS` | `600` | — | Bound on each of the two publish rsync passes (see below). |
| control URL | `https://data.conductress.rainsupreme.net/api/v1` | `CONDUCTRESS_CONTROL_URL` | Base URL for the fleet control API. Plain HTTP is rejected except for `localhost`/`127.0.0.1`. |

#### How a publish reaches the data server

At every task boundary the publisher regenerates the dashboard files (series,
manifests, notable feeds, CPU-stack indexes; ~100 MB per runner) from
coordinator state into `PUBLISH_EXPORT_DIR` and rsyncs the directory. The
per-commit raw CPU-stack files (`series-*-cpu-stacks-<commit>.json`, multi-MB
each, thousands per runner, ~10 GB in total) are different: their content
never changes once written, so they are exported once and skipped thereafter,
and the export directory is kept across runner restarts so their mtimes stay
put and rsync's quick check skips them. Deleting the directory is safe; the
cost is one full re-export and re-sync.

The rsync runs in two passes, each bounded by `PUBLISH_RSYNC_TIMEOUT_SECONDS`:
first everything except the per-commit stack files, then the stack files
alone. rsync sends in name order, so in a single pass a slow stacks transfer
would sit ahead of every series file that sorts after `cpu-stacks`; the split
lets the dashboard data land first regardless of how much stack data is
pending. A pass that hits its bound is logged at ERROR and retried in full at
the next boundary; rsync has no `--delete`, so nothing already published is
ever removed.

The fleet client reads several more control-plane variables — see the
[Fleet-aware CLI](fleet-cli.md) guide for `CONDUCTRESS_CONTROL_TIMEOUT`,
`CONDUCTRESS_CONTROL_CA_BUNDLE`, and the operator-token variables.

## Operator token

The control CLI needs an operator bearer token.

| Environment variable | Meaning |
| --- | --- |
| `CONDUCTRESS_OPERATOR_TOKEN` | The token value. |
| `CONDUCTRESS_OPERATOR_TOKEN_FILE` | Path to an owner-only file holding the token (default `~/.config/conductress/operator.token`). Group/world-readable files are rejected. |

See the [Fleet-aware CLI](fleet-cli.md) guide for full details on secure client
configuration.
