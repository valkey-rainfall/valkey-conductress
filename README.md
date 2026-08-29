# Conductress

A benchmarking framework for [Valkey](https://github.com/valkey-io/valkey) that queues and runs performance, memory, and replication benchmarks. It provides a TUI for interactive use, a CLI for scripted automation, and a statistical analysis module for comparing results.

Conductress assumes a separate machine (or machines) to run `valkey-server`, distinct from the machine conducting the tests and generating load (always localhost). Localhost is also supported as a server target.

## Design and implementation plans

- [Fleet control plane and daily drift canary](docs/fleet-control-plane-implementation-plan.md) — pull-based remote inboxes, boundary-only network activity, fleet discovery, and canary rollout. Multiple same-platform runners are deferred.
- [Fleet control service](docs/control-service.md) — Phase 2 SQLite mailbox/status API, authentication, deployment examples, and safety invariants.
- [Fleet-aware CLI](docs/fleet-cli.md) — fleet discovery, remote queue management, secure client configuration, and runner/platform routing.

## Quick Start

1. Install Git and Python 3.9+
2. Clone this repo to `~/conductress`
3. Review `src/config.py` and edit as needed
4. Copy `runner.default.json` to the ignored local `runner.json` and assign this installation a stable `runner_id`
5. Optionally create `servers.json` to add remote servers (see `servers.default.json` for format). Localhost is used by default.
6. Copy an SSH keyfile to `~/conductress/server-keyfile.pem` for remote server access
7. Run setup:
   ```bash
   python -m src setup
   ```
   This installs system packages, pip dependencies, and configures servers. It may prompt you to make manual fixes, and you may need to run it more than once if it installs its own dependencies.
8. Launch the TUI or start queuing tasks via CLI (see below)

## Unified Entry Point

All Conductress functionality is accessible through a single entry point:

```bash
python -m src <subcommand>
```

### Available Subcommands

| Subcommand | Description |
|------------|-------------|
| `tui`      | Launch the interactive TUI for monitoring and queuing tasks |
| `run`      | Start the task runner worker that executes queued benchmarks |
| `setup`    | Run the setup/bootstrap script to configure servers |
| `queue`    | Manage local or remotely routed benchmark tasks |
| `fleet`    | Discover runners and inspect fleet status |
| `remote`   | Inspect or cancel tasks in the control-service queue |
| `compare`  | Run statistical comparison between two specifiers |
| `runner-info` | Show stable runner identity and environment (`--json` for automation) |
| `status`   | Show runner and task status (non-blocking) |

Running `python -m src` without a subcommand prints usage information.

### Examples

```bash
# Launch the TUI
python -m src tui

# Start the task runner worker
python -m src run

# Queue perf tasks (see CLI section below)
python -m src queue add --tests get,set --sizes 512 --io-threads 1,9

# List queued tasks
python -m src queue list

# Compare two branches
python -m src compare unstable my-feature-branch --source valkey
```

## CLI Interface

The CLI provides a non-interactive way to queue and manage benchmark tasks, useful for scripting and automation.

### Queuing Performance Tasks

```bash
python -m src queue add \
  --tests get,set,mget \
  --sizes 512,1KB \
  --io-threads 1,9 \
  --pipelining 1,4 \
  --warmup 30s \
  --duration 5m \
  --repetitions 5 \
  --key-sizes 0,64 \
  --note "vstr comparison" \
  --make-args 'OPTIMIZATION=-O2'
```

Only `--tests` is required. All other arguments have sensible defaults defined in `src/config.py`.

#### Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--source` | No | `valkey` | Repository source name |
| `--specifier` | No | `unstable` | Branch name, tag, or commit hash |
| `--tests` | Yes | — | Comma-separated test names (e.g., `get,set,mget`) |
| `--sizes` | No | `512` | Comma-separated value sizes (e.g., `16,512,1KB`) |
| `--io-threads` | No | `9` | Comma-separated IO thread counts (e.g., `1,9`) |
| `--pipelining` | No | `10` | Comma-separated pipelining values (e.g., `1,4,10`) |
| `--warmup` | No | `30s` | Warmup duration (human-readable, e.g., `30s`, `1m`) |
| `--duration` | No | `5m` | Test duration (human-readable, e.g., `5m`, `15m`) |
| `--repetitions` | No | `5` | Number of independent runs per configuration |
| `--key-sizes` | No | `0` | Comma-separated key sizes in bytes (e.g., `0,64,256`) |
| `--note` | No | `""` | Optional note attached to each task |
| `--make-args` | No | `USE_FAST_FLOAT=yes` | Build arguments for compiling Valkey |
| `--perf-stat` | No | `false` | Collect hardware performance counters |
| `--no-preload` | No | `false` | Disable key preloading |

The CLI computes the **Cartesian product** of all multi-valued parameters (`tests`, `sizes`, `io-threads`, `pipelining`, `key-sizes`) and submits each combination as a separate task. For example, `--tests get,set --sizes 512,1KB` creates 4 tasks.

The `--source` value is validated against configured repository names.

### Managing the Queue

```bash
# List pending tasks
python -m src queue list

# Remove a specific task
python -m src queue remove <task_id>

# Clear all pending tasks
python -m src queue clear
```

Running `python -m src queue` without a subcommand defaults to listing tasks.

## Comparison and Analysis

The analysis module compares benchmark results between two specifiers (branches, tags, or commits) using statistical methods.

### Usage

```bash
python -m src compare <specifier_a> <specifier_b> [--source SOURCE] [--method METHOD]
```

### Examples

```bash
# Compare unstable vs a feature branch
python -m src compare unstable vstr-zerocopy

# Filter to a specific repository
python -m src compare unstable vstr-zerocopy --source valkey

# Filter to a specific test type
python -m src compare unstable vstr-zerocopy --method perf-get
```

### Output

The module prints a formatted comparison table with confidence intervals and a measurement quality summary:

```
Test       | Size  | Key  | IO | Pipe |     Mean A     | ±CI% |     Mean B     | ±CI% |  Delta | p-value |   n
-----------+-------+------+----+------+----------------+------+----------------+------+--------+---------+-----
perf-get   | 512B  |    0 |  1 |    1 |   131,122 rps  | 1.1% |   132,209 rps  | 0.7% | +0.83% |  0.1173 | 5/5
perf-set   | 512B  |    0 |  1 |    1 |   117,502 rps  | 0.5% |   116,884 rps  | 0.8% | -0.53% |  0.1602 | 5/5

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
| `zrank` | ZRANK sorted set lookups | ZADD preload | Custom command |
| `zcount` | ZCOUNT sorted set range queries | ZADD preload | Custom command |
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
python -m src queue add --tests get,set --key-sizes 64,256

# Mix standard and padded keys
python -m src queue add --tests get --key-sizes 0,64,256
```

Key sizes are included in the Cartesian product of parameters, so `--key-sizes 0,64` doubles the number of tasks generated.

## Repetitions Feature

The repetitions feature runs each benchmark configuration multiple times and aggregates the results with statistical summaries.

### How It Works

When `repetitions` is greater than 1, the task runner executes the benchmark N times sequentially within a single task. Between each run, the server is restarted with a fresh state and data is re-preloaded. After all runs complete, the runner computes:

- **Mean RPS**: Arithmetic mean of per-run average requests per second
- **95% Confidence Interval**: Calculated as `t_critical × (stdev / √N)` where `t_critical` comes from the Student's t-distribution

A single aggregated result is recorded in `output.jsonl` containing the number of runs, per-run averages, overall mean, and confidence interval.

When `repetitions` is 1 (the default), the result is recorded as a single run without aggregation.

### CLI Usage

```bash
# Run each configuration 5 times (this is the default)
python -m src queue add --tests get,set

# Override to 10 repetitions
python -m src queue add --tests get,set --repetitions 10
```

### Using Repetitions with Analysis

The analysis module uses per-run averages from aggregated results as individual samples for statistical tests. This means running with `--repetitions 5` gives you 5 samples per configuration, enabling meaningful Welch's t-tests in the comparison output.

```bash
# Queue benchmarks for both specifiers
python -m src queue add --specifier unstable --tests get

python -m src queue add --specifier my-feature --tests get

# After running, compare with statistical significance
python -m src compare unstable my-feature --source valkey
```

## Running Tests

### Unit Tests

```bash
pytest tests/unit
```

Unit tests cover core logic including test type definitions, key-size generation, CLI argument parsing, statistical computations, and analysis module behavior. They do not require a running Valkey server.

### Integration Tests

```bash
pytest tests/integration -m "not requires_server"
```

Integration tests verify end-to-end workflows like CLI task queuing and analysis against fixture data. Tests that require a running Valkey server are marked with `@pytest.mark.requires_server` and excluded from the default CI run.

### All Tests

```bash
pytest
```

### Type Checking

```bash
mypy src/ --ignore-missing-imports
```

## Configuration

Key configuration lives in `src/config.py`:

| Setting | Description |
|---------|-------------|
| `DEFAULT_MAKE_ARGS` | Default compiler flags for Valkey builds (`USE_FAST_FLOAT=yes`). Bare make gives proper O3+LTO. |
| `DEFAULT_IO_THREADS` | Default IO thread count (9) |
| `DEFAULT_PIPELINING` | Default pipelining value (10) |
| `DEFAULT_WARMUP` | Default warmup in seconds (30) |
| `DEFAULT_DURATION` | Default test duration in seconds (300) |
| `DEFAULT_REPETITIONS` | Default repetitions per config (5) |
| `PERF_BENCH_KEYSPACE` | Number of keys used in benchmarks (default: 3,000,000) |
| `PERF_BENCH_CLIENTS` | Number of concurrent clients (default: 1,200) |
| `PERF_BENCH_THREADS` | Number of benchmark threads (default: 16) |
| `REPOSITORIES` | List of Git repositories available for testing |
| `SERVER_PORT_RANGE_START` | Starting port for multiple Valkey instances on one host |

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

## Current Features

- Works on Red Hat, Amazon Linux 2023, and Ubuntu
- Performance throughput tests based on `valkey-benchmark` with data collection at 4Hz and CPU pinning
- Memory efficiency tests measuring overhead across a range of value sizes
- Optional flame graph collection for performance tests
- Hardware performance counter collection via `perf stat`
- Support for fork repositories for testing exploratory work
- Non-interactive CLI for scripted benchmark campaigns (`queue add/list/remove/clear`)
- Statistical comparison module with Welch's t-test
- Key-size benchmarking with padded keys
- Run repetitions with aggregated statistical summaries
- TUI for interactive monitoring and task management
- Crash logging and non-blocking status command

## License

See [LICENSE](LICENSE) for details.
