<p align="center">
  <img src="brand/logo/conductress-hero.svg" width="720" alt="Conductress: a striped keyhole C leading a trail of hexagon outlines, over a sunset grid horizon">
</p>

# Conductress

A benchmarking framework for [Valkey](https://github.com/valkey-io/valkey) that queues and runs performance, memory, and replication benchmarks. It provides a CLI for queueing, monitoring, and scripted automation, and a statistical analysis module for comparing results.

Conductress runs `valkey-server` on a target machine (or machines) distinct from the machine that conducts the tests and generates load. Localhost is also supported as a server target, and is the default.

## Standalone runner or managed fleet

Conductress works in two shapes. Nothing in the second is needed for the first.

**Standalone.** One machine is the runner: it holds the task queue, builds the requested Valkey commit, drives the load generators, and writes results to its own `results/` directory. You queue work with `conductress queue add-*`, execute it with `conductress run`, and compare results with `conductress compare` on the same machine. This is what the Quick Start below sets up.

**Managed fleet.** Several runners, typically one per hardware platform, take work from a central control service (`conductress-control`, installed with the `control` extra) instead of only their local queue. Operators queue tasks from anywhere with `conductress queue add-* --runner NAME` or `--platform NAME`, and inspect the fleet with the `fleet`, `remote`, and `canary` commands, none of which need SSH access to a runner. A runner joins by starting with `conductress run --fleet-mode live` and a per-runner token (`~/.config/conductress/runner.token`, or `CONDUCTRESS_RUNNER_TOKEN`); `--fleet-mode shadow` reports status without accepting work, and `CONDUCTRESS_CONTROL_URL` points a runner at your own control service.

Runners talk to the control service only between jobs. Between one task finishing and the next starting, a runner reports its status, pulls any task assigned to it into its local queue, and publishes results. While a task is running, the runner transfers no data and does no other work, so the control plane cannot disturb a measurement, and losing the control service never interrupts a benchmark in progress. The control service itself never opens connections to runners.

Details: [Fleet-aware CLI](docs/fleet-cli.md) for the operator side, [Fleet control service](docs/control-service.md) for deploying the service, and [Runner fleet mailbox](docs/runner-mailbox.md) for how a runner claims and reports work.

A fleet's sweep results can be published as static JSON series and browsed with the [Valkey performance dashboard](https://github.com/valkey-rainfall/valkey-perf-dashboard), a separate repository. A [live instance](https://valkey-rainfall.github.io/valkey-perf-dashboard/) tracks the Valkey `unstable` branch across several hardware platforms.

## Runner requirements

A runner is a dedicated Linux machine. `conductress setup` installs everything it can; the properties below belong to the machine itself and have to be chosen before provisioning.

**Operating system.** Amazon Linux 2023, Red Hat Enterprise Linux 9, or Ubuntu (setup picks the package list from `/etc/os-release`), running systemd, with passwordless `sudo`. Setup raises the open-file limit to 1,048,576, enables io_uring where the distribution ships it disabled, installs the C build toolchain for Valkey, and installs Rust to build the cachecannon load generator.

**Hardware performance counters.** The sweep's per-request hardware counter columns and the perf-instrumented tasks count `cycles`, `instructions`, `L1-dcache-load-misses`, `branch-misses`, and `stalled-cycles-frontend` per server thread with `perf stat` (plus top-down groups on x86), and the `--cpu-profile` options sample stacks with `perf record`. Both need the CPU's performance monitoring unit exposed to the operating system. Bare metal exposes it fully. On a virtual machine the hypervisor decides: cloud instances that own a whole socket generally expose the full set, smaller sizes of recent generations expose a reduced set, and older or small instances expose none, in which case those columns read `<not supported>` and only software events such as `context-switches` count. Check before provisioning:

```bash
perf stat -e cycles,instructions,L1-dcache-load-misses,branch-misses -- sleep 1
```

Throughput, latency, memory, and replica-read scores do not depend on the counters; only the hardware counter columns and CPU profiles do.

**Cores.** Every measured thread gets a core of its own. The runner pins the server's main thread and each I/O thread to one core, confines each load generator to its own core set, and keeps one core for itself (`RUNNER_MANAGEMENT_CPUS`). Allocation fails with `Insufficient CPUs` instead of oversubscribing. Core budgets at the default task shapes, with the server on the runner itself:

| Task at its defaults | Server | Generators | Runner | Cores |
|---|---|---|---|---|
| GET, SET, or mixed throughput: `--io-threads 7`, 8 client threads | 7 | 8 | 1 | 16 |
| Same shape at pipeline depth 1 with 16 client threads | 7 | 16 | 1 | 24 |
| Replica-read: primary and replica at `--io-threads 8`, 8 reader and 4 writer threads | 16 | 12 | 1 | 29 |

Count physical cores. The allocator packs a server into as few L3 cache groups as it can, so on a chiplet CPU the server needs a whole cache group free, not the same number of cores scattered across the package. A remote server target in `servers.json` moves the server column onto that host. The memory-efficiency task adapts, running as many server instances in parallel as the cores allow, up to nine. In practice, 32 physical cores run every task at its defaults on one machine, and 16 cover the throughput sweeps.

**Nothing else running.** A measurement is only as quiet as the host. The replica-read task samples every core during the scored window and reports a `host` verdict, rather than `server`, when a claimed core saw work from outside the benchmark; other tasks have no such check, and interference there shows up as run-to-run variance. Periodic timers, monitoring agents, and unattended package updates all count as interference on a runner.

## Installation

Conductress installs as a Python package and exposes two console scripts, `conductress` and `conductress-control`.

```bash
git clone https://github.com/rainsupreme/valkey-conductress.git
cd valkey-conductress
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev,control,plots]'
```

Requires Python 3.9 or newer. The `control` extra pulls in the fleet control service dependencies, `plots` adds matplotlib for figure rendering, and `dev` adds the test and lint tooling. Install only the extras you need.

## Quick Start

1. **Install** as above, inside an activated virtualenv.
2. **Configure the runner.** Copy `runner.default.json` to the ignored local `runner.json` and set a stable `runner_id` for this installation.
3. **Configure servers (optional).** Copy `servers.default.json` to `servers.json` to add remote server targets. Without it, localhost is used. For remote servers, place an SSH key at `server-keyfile.pem` in the project root.
4. **Provision the runner.** Check [Runner requirements](#runner-requirements) first. This step turns the machine into a benchmark runner: it uses `sudo` to upgrade and install system packages, raises file-descriptor limits, enables io_uring, builds the load generators, and installs a systemd service, on this host and on every host in `servers.json`. Skip it if you only want to work on the code or run the tests.
   ```bash
   conductress setup
   ```
   Setup may stop and ask you to make a manual fix; rerun it after doing so.
5. **Start a runner worker** in one terminal. It executes queued benchmarks:
   ```bash
   conductress run
   ```
6. **Queue a benchmark** in another terminal:
   ```bash
   conductress queue add --tests get,set --sizes 512 --io-threads 1,9
   ```
7. **Watch progress:**
   ```bash
   conductress status
   ```
8. **Compare two builds** once results exist. Prefix a specifier with `source:` to pin it to one repository, which lets a fork branch be compared against upstream (see [Comparison and Analysis](#comparison-and-analysis)):
   ```bash
   conductress compare valkey:unstable valkey-rainfall:my-feature
   ```

## Command Reference

Every command is a subcommand of `conductress`. Run `conductress <command> --help` for the full argument list. Running `conductress` with no subcommand prints usage. `conductress --version` prints the installed version; on an interactive terminal it shows the Conductress mark above it (set `NO_COLOR` to get only the version line).

### Core commands

**`run`** — Start the task runner worker that pulls queued tasks and executes them. Add `--sweep` to auto-generate historical benchmark tasks when the queue is empty, `--memory-sweep` to track per-item memory overhead across history, or `--fleet-mode {off,shadow,live}` to take work from a central control service (see [Standalone runner or managed fleet](#standalone-runner-or-managed-fleet)).

```bash
conductress run --sweep
```

**`setup`** — Provision this machine and every `servers.json` host as a benchmark runner: system packages, file-descriptor limits, io_uring, load generators, and the runner systemd service. Requires `sudo`. Not needed to develop or run the tests; run it when creating a runner and again after changing server configuration.

```bash
conductress setup
```

**`compare`** — Run a statistical comparison between two specifiers, each optionally pinned to a source repository as `source:specifier`. See [Comparison and Analysis](#comparison-and-analysis).

```bash
conductress compare valkey:unstable valkey-rainfall:my-feature
```

**`status`** — Print a non-blocking snapshot of runner and task status, including any recorded crash.

```bash
conductress status
```

**`status-export`** — Export the same status to JSON for remote monitoring, optionally publishing to an rsync target with `--publish user@host:/path`.

```bash
conductress status-export --publish user@host:/var/www/status
```

**`runner-info`** — Show this installation's stable runner identity and environment (hostname, platform, kernel, revision). Add `--json` for machine-readable output.

```bash
conductress runner-info --json
```

### Queue commands

`conductress queue` manages benchmark tasks. Every `add-*` command accepts `--runner NAME` or `--platform NAME` to send the task to a fleet runner through the control service instead of the local queue, `--priority` for ordering in that remote queue, and `--json` for machine-readable submission output. Running `conductress queue` with no subcommand lists tasks.

Manually queued tasks and the historical sweep share one results store, but a manual result is only comparable with sweep history when every parameter matches: load generator, test, value size, io-threads, pipelining, connection count, warmup and duration. The sweep runs in generator epochs, each pinning one generator and parameter set, so a manual task must reproduce the epoch it is being compared against. Treat manual tasks as A/B cells against each other unless you have matched an epoch deliberately.

**`add`** — Queue standard `valkey-benchmark` throughput tasks. Only `--tests` is required. Multi-valued parameters (`--tests`, `--sizes`, `--io-threads`, `--pipelining`, `--key-sizes`) form a Cartesian product, one task per combination. So `--tests get,set --sizes 512,1KB` creates four tasks.

```bash
conductress queue add --tests get,set,mget --sizes 512,1KB --io-threads 1,9 --pipelining 1,10 --note "vstr comparison"
```

**`add-insertion`** — Queue a finite new-key-only SET task with explicit memory bounds. Each repetition starts from an empty server, issues exactly `--insertions` sequentially unique SETs, verifies the final key count, and restarts. `--maxmemory` is applied with `noeviction`; `--max-rss` aborts the task if crossed.

```bash
conductress queue add-insertion --specifier my-branch --insertions 20M --key-size 16 --size 16 --maxmemory 8GB --max-rss 12GB
```

**`add-memory`** — Queue memory-efficiency tasks that measure per-item overhead across data types. One task per type per size. `--sizes` sets the value/member size for every type; `--key-size` (set) and `--field-size` (hset) set the other user-data dimension, so any shape is queueable (defaults keep each type's standard shape). Keys, fields and members carry a unique index, so each has a small minimum size; values do not. Add `--expire` to also test with expiration, or `--settle` to sample steady-state memory after background reclamation.

```bash
conductress queue add-memory --types set,zadd,hset --sizes 16,20,64
conductress queue add-memory --types set --key-size 32 --sizes 16 --expire
conductress queue add-memory --types hset --field-size 16 --sizes 16
```

**`add-mixed`** — Queue a mixed GET/SET throughput task driven by cachecannon. `--set-ratio` sets the write percentage (e.g. `20` = 20% SET / 80% GET). Every other default is the epoch-3 mixed sweep's shape (400 connections, 8 client threads, 16 B values, io-threads 7, pipeline 10, 10 s warmup, 30 s scored, 5 repetitions), so a cell queued with only `--set-ratio 20` is comparable with that sweep's points.

```bash
conductress queue add-mixed --set-ratio 20 --sizes 512 --duration 30s
```

**`add-scenario`** — Queue a pathological-workload scenario (a background GET load plus an overlay). Choose the scenario with `--scenario`, e.g. `bgsave`, `expiry-heavy`, `large-value-reader`, or `connection-storm`. The `connection-storm` scenario has a large family of `--storm-*` flags for tuning the reconnecting-client burst.

```bash
conductress queue add-scenario --scenario connection-storm --duration 60s
```

**`add-latency`** — Queue a latency-measurement task: cachecannon drives a fixed request rate with no pipelining and the recorded score is the p99 latency in microseconds. Takes positional `source`, `specifier`, and `target_rps`; the epoch-3 latency sweep uses 100000, and the other defaults match that sweep's shape. The result also records the achieved rate, which is how you check the generator held the requested one.

```bash
conductress queue add-latency valkey my-branch 100000 --value-size 16
```

**`add-cachecannon`** — Queue a GET/SET benchmark driven by the cachecannon generator, which has a native warmup period, a fixed-rate mode, and exact integer throughput output. Supports `--distribution {uniform,zipf}` and `--set-ratio` for a mixed workload.

```bash
conductress queue add-cachecannon --test get --sizes 512 --connections 400 --distribution zipf
```

**`add-replica-read`** — Queue a replica-read task: reads are served by a replica while the primary ingests writes at a fixed rate (`--write-rate` SET/s). Measures replica read performance under a live replication stream.

```bash
conductress queue add-replica-read --replicas 1 --write-rate 50000 --sizes 512 --duration 30s
```

**`list`** — List all pending tasks. **`remove <task_id>`** — Remove one task (id from `queue list`). **`clear`** — Remove all pending tasks.

```bash
conductress queue list
conductress queue remove <task_id>
conductress queue clear
```

### Fleet, remote, and canary commands

These commands talk to the control service of a [managed fleet](#standalone-runner-or-managed-fleet); a standalone runner has no use for them.

**`fleet`** — Discover and inspect benchmark runners: `fleet list` (runners and platform aliases), `fleet status` (status and task counts), `fleet show <runner>` (one runner in detail). See [docs/fleet-cli.md](docs/fleet-cli.md).

**`remote`** — Inspect and cancel tasks in the control-service queue: `remote list`, `remote show <id>`, `remote cancel <id>` (queued tasks only).

**`canary`** — `canary status` shows canary drift monitoring status for all runners or one runner.

### Plot and sweep commands

**`plot`** — Render a figure from a task's results. Figures: `plot connection-storm` (one column per task id, max 3).

**`sweep`** — Manage the historical sweep: `sweep status` (progress summary), `sweep list` (workload IDs and scheduling config), `sweep export --platform PLATFORM` (export results to dashboard JSON), and `sweep focus` / `sweep pause` / `sweep resume` to steer which workloads queue.

```bash
conductress sweep status
conductress sweep export --platform arm64
```

## Comparison and Analysis

The analysis module compares benchmark results between two specifiers (branches, tags, or commits) using statistical methods.

### Usage

```bash
conductress compare <specifier_a> <specifier_b> [--source SOURCE] [--method METHOD]
```

Each positional is a branch, tag, or commit hash, optionally prefixed with a source repository as `source:specifier` (split on the first colon; git specifiers never contain one). A bare specifier inherits `--source` if given; an explicit per-side source overrides `--source` for that side.

### Examples

```bash
# Compare two specifiers recorded under the same source
conductress compare unstable 8.0.0 --source valkey

# Compare a fork branch against upstream unstable
conductress compare valkey:unstable valkey-rainfall:my-feature

# Compare the same branch name across two repositories
conductress compare valkey:unstable valkey-rainfall:unstable

# Filter to a specific test type
conductress compare unstable 8.0.0 --method perf-get
```

If a side has no source constraint and its results span more than one source, `compare` prints a warning listing the sources and their sample counts; pin the side with `source:specifier` to disambiguate. The table is preceded by an `A: ... B: ...` header naming what each side resolved to.

### Output

The module prints a header naming both sides, then a formatted comparison table with confidence intervals and a measurement quality summary:

```
A: valkey:unstable  B: valkey:8.0.0
Test        |   Size |   Key |  IO | Pipe |         Mean A |  ±CI% |         Mean B |  ±CI% |   Delta |  p-value |   n
------------+--------+-------+-----+------+----------------+-------+----------------+-------+---------+----------+-----
perf-get    |   512B |     0 |   1 |    1 |    131,122 rps |  1.1% |    132,209 rps |  0.7% |  +0.83% |   0.1173 | 5/5
perf-set    |   512B |     0 |   1 |    1 |    117,502 rps |  0.5% |    116,884 rps |  0.8% |  -0.53% |   0.1602 | 5/5

Comparisons: 24
Significant (p < 0.05): 3/24
Measurement precision: avg ±0.58%, max ±1.10% (95% CI as % of mean)
Good precision — sufficient to detect effects ≥1%.
Minimum detectable effect: ~±1.2% (approximate)
```

The **±CI%** columns show the 95% confidence interval as a percentage of the mean for each specifier — smaller values mean tighter measurements. The summary at the bottom tells you:
- How many comparisons reached statistical significance
- The overall measurement precision (average and worst-case CI)
- Whether more repetitions would help detect smaller effects
- The approximate minimum effect size detectable with the current data

Results are grouped by matching parameters (test type, value size, key size, IO threads, pipelining). A [Welch's t-test](https://en.wikipedia.org/wiki/Welch%27s_t-test) is performed for each group when both specifiers have at least 2 samples. Groups with insufficient data show `N/A` for the p-value.

## Available Performance Test Types

| Test | Description | Preload | Command |
|------|-------------|---------|---------|
| `set` | SET key-value pairs | SET preload | `-t set` |
| `get` | GET key-value pairs | SET preload | `-t get` |
| `sadd` | SADD set members | SADD preload | `-t sadd` |
| `hset` | HSET hash fields | HSET preload | `-t hset` |
| `zadd` | ZADD sorted set members | ZADD preload | `-t zadd` |
| `zincrby` | ZINCRBY score updates of existing members | ZADD preload | Custom command |
| `zrank` | ZRANK sorted set lookups | ZADD preload | Custom command |
| `zcount` | ZCOUNT sorted set range queries | ZADD preload | Custom command |
| `zscore` | ZSCORE sorted set score lookups | ZADD preload | Custom command |
| `zrange` | ZRANGE rank-range scans (~100 elements at a random offset) | ZADD preload | Custom command |
| `zrangebyscore` | ZRANGEBYSCORE score-range scans (~100 elements in a random window) | ZADD preload | Custom command |
| `zrandmember` | ZRANDMEMBER random sample of 100 members | ZADD preload | Custom command |
| `zrem` | ZREM scattered deletes, paired 50/50 with ZADD so the set stays populated | ZADD preload | Custom command |
| `zpop` | ZPOPMIN sliding window, paired with a ZADD append from a much larger namespace | ZADD preload | Custom command |
| `sismember` | SISMEMBER set membership checks | SADD preload | Custom command |
| `ping` | PING latency (no data) | None | `-t ping` |
| `mget` | MGET multi-key reads (4 keys) | SET preload | Custom command |

All tests use `valkey-benchmark` under the hood. Tests marked "Custom command" use the `-- COMMAND arg1 arg2` syntax to execute arbitrary Valkey commands.

## Key-Size Feature

The key-size feature lets you benchmark with keys of a specific byte length, useful for measuring how key size affects throughput.

### How It Works

When `key_size` is greater than 0, Conductress generates a **padded key** by appending deterministic padding characters to the standard `key:__rand_int__` pattern (16 bytes) to reach the target size. For example, a `key_size` of 64 produces a key like:

```
key:__rand_int__AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
```

The padded key is then used in custom command syntax (`-- COMMAND padded_key ...`) for both the preload and test phases, replacing the standard `-t <test>` invocation.

When `key_size` is 0 (the default), standard commands are used without modification.

### CLI Usage

```bash
# Test with 64-byte and 256-byte keys
conductress queue add --tests get,set --key-sizes 64,256

# Mix standard and padded keys
conductress queue add --tests get --key-sizes 0,64,256
```

Key sizes are included in the Cartesian product of parameters, so `--key-sizes 0,64` doubles the number of tasks generated.

## Repetitions Feature

The repetitions feature runs each benchmark configuration multiple times and aggregates the results with statistical summaries.

### How It Works

When `repetitions` is greater than 1, the task runner executes the benchmark N times sequentially within a single task. Between each run, the server is restarted with a fresh state and data is re-preloaded. After all runs complete, the runner computes:

- **Mean RPS**: Arithmetic mean of per-run average requests per second
- **95% Confidence Interval**: Calculated as `t_critical × (stdev / √N)` where `t_critical` comes from the Student's t-distribution

A single aggregated result is recorded in `results/output.jsonl` containing the number of runs, per-run averages, overall mean, and confidence interval.

When `repetitions` is 1, the result is recorded as a single run without aggregation. The default is 3.

### CLI Usage

```bash
# Run each configuration with the default repetitions (3)
conductress queue add --tests get,set

# Override to 10 repetitions
conductress queue add --tests get,set --repetitions 10
```

### Using Repetitions with Analysis

The analysis module uses per-run averages from aggregated results as individual samples for statistical tests. This means running with `--repetitions 5` gives you 5 samples per configuration, enabling meaningful Welch's t-tests in the comparison output.

```bash
# Queue the same workload for upstream unstable and a fork branch
conductress queue add --source valkey --specifier unstable --tests get --repetitions 5
conductress queue add --source valkey-rainfall --specifier my-feature --tests get --repetitions 5

# After both complete, compare across the two sources
conductress compare valkey:unstable valkey-rainfall:my-feature
```

## Configuration

Benchmark defaults and runtime constants live in `src/conductress/config.py`, the single source of truth for the CLI and the runner. Generator paths, publish and control endpoints, and the repository list can be overridden without editing source through environment variables and local JSON files; [docs/configuration.md](docs/configuration.md) lists every setting and its override. The benchmark defaults:

| Setting | Default | Description |
|---------|---------|-------------|
| `DEFAULT_MAKE_ARGS` | `""` (empty) | Extra compiler flags for Valkey builds. Bare `make` already gives O3+LTO+frame-pointer, so no extra flags are needed. |
| `DEFAULT_IO_THREADS` | `9` | Default server IO thread count |
| `DEFAULT_PIPELINING` | `10` | Default pipelining depth |
| `DEFAULT_WARMUP` | `5` | Default warmup in seconds |
| `DEFAULT_DURATION` | `30` | Default test duration in seconds |
| `DEFAULT_REPETITIONS` | `3` | Default independent runs per config |
| `DEFAULT_VAL_SIZE` | `512` | Default value size in bytes |
| `PERF_BENCH_KEYSPACE` | `3,000,000` | Number of keys used in benchmarks |
| `PERF_BENCH_CLIENTS` | `1,200` | Number of concurrent benchmark clients |
| `PERF_BENCH_THREADS` | `16` | Number of benchmark threads |
| `REPOSITORIES` | — | Git repositories available for testing |
| `SERVER_PORT_RANGE_START` | `9000` | Starting port when multiple instances run on one host |

### Server Configuration

Create a `servers.json` file in the project root to configure remote servers:

```json
{
    "valkey_servers": [
        {
            "ip": "192.168.1.100",
            "username": "ec2-user",
            "name": "bench-server-1"
        }
    ]
}
```

See `servers.default.json` for the default localhost configuration.

### Runtime state

The runner writes per-host runtime state into the project root: `sweep_data/`, `benchmark_queue/`, `results/`, `tmp/`, `log.txt`, and similar. All of it is git-ignored and must never be committed.

## Documentation

### User guides
- [Configuration](docs/configuration.md) — every user-editable setting, the environment overrides, and the local `servers.json` / `runner.json` / `repositories.json` files.
- [Benchmark precision guide](docs/benchmark-precision-guide.md) — measurement stability, the bimodal between-restart distribution, and how repetitions and adaptive stopping keep results trustworthy.
- [Binary layout noise](docs/layout-noise.md) — why a stock build's per-commit throughput can swing several percent with no code cost, how to tell layout from work, and how to hold the layout fixed with `--make-args`.
- [Connection-storm scenario](docs/connection-storm-scenario.md) — the reconnecting-client burst overlay, its `--storm-*` knobs, and the TLS herd.
- [Replica-read task](docs/replica-read-task.md) — measuring replica read performance under a live replication stream.
- [Real-NIC hairpin](docs/real-nic-hairpin.md) — running the benchmark client in a separate network namespace over a real NIC path.
- [Fleet-aware CLI](docs/fleet-cli.md) — fleet discovery, remote queue management, secure client configuration, and runner/platform routing.

### Design notes and implementation history
- [Dragonfly as a comparator engine](docs/dragonfly-comparator-plan.md) — plan for a prebuilt-release reference engine: official binaries only (no source), release tracking, launch profiles, stepped-series semantics, fleet rollout.
- [Fleet control plane and daily drift canary](docs/fleet-control-plane-implementation-plan.md) — per-runner inboxes that runners pull from between jobs, fleet discovery, and canary rollout.
- [Fleet control service](docs/control-service.md) — SQLite mailbox/status API, authentication, deployment examples, and safety invariants.
- [Runner fleet mailbox](docs/runner-mailbox.md) — how a runner claims, imports, accepts, and reports one remote task between jobs, with a recovery journal and a shadow mode for rollout.
- [Canary drift analysis](docs/canary-drift-analysis.md) — how daily canary runs detect and attribute performance drift.
- [Implementation note](docs/implementation-note.md) — mixed-client scaling: implementation notes and rationale.
- [Brand guide and assets](brand/README.md) — the Conductress mark, lockups, palette, terminal and animated versions, and the rules for using them.

## Running Tests

### Unit tests

```bash
PYTHONPATH=src pytest tests/unit tests/control
```

Unit and control-service tests cover core logic including test-type definitions, key-size generation, CLI argument parsing, statistical computations, and the control service. They do not require a running Valkey server.

### Integration tests

```bash
PYTHONPATH=src pytest tests/integration -m "not requires_server"
```

Integration tests verify end-to-end workflows like CLI task queuing and analysis against fixture data. Tests that require a running Valkey server are marked `@pytest.mark.requires_server` and excluded from the default CI run.

### Type checking

```bash
mypy src/ --ignore-missing-imports
```

Always set `PYTHONPATH=src` when running tests or mypy: other editable installs on the same host can otherwise shadow this tree.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup, the checks CI runs (`make ci` runs the same set locally), the DCO sign-off requirement, and the writing conventions for anything committed to the repository.

## License

See [LICENSE](LICENSE) for details.
