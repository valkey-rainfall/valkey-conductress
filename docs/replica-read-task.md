# Replica-read task

`conductress queue add-replica-read` measures read throughput served by a
**replica** while its primary ingests writes at a fixed rate over a real
replication link. Score = mean reader throughput (GET/s at the first replica).

Why a replica: on a replica the only writer is the replication stream, executed
on the main thread, so no client connection ever interleaves writes with reads.
That makes it the cleanest topology for measuring any change to how a server
serves reads while writes are arriving (I/O-thread read offload, read-path
locking, memory reclamation under replication load, and so on). Most production
read scaling is done by adding replicas, so it is also the node population such
a change would reach first.

## Topology

All instances run on one benchmark host over loopback, each with its own
port, working directory (`--dir`, so full-sync RDB files do not collide),
logfile and CPU allocation from the topology-aware allocator.

| instance | port          | io-threads             | generator                          |
|----------|---------------|------------------------|------------------------------------|
| primary  | `--base-port` | `--primary-io-threads` | writer: cachecannon SET, `rate_limit` = `--write-rate` |
| replica  | base + 1      | `--io-threads`         | reader: cachecannon GET, closed loop, `--connections`/`--threads`/`--pipelining` |
| replica  | base + 2 ...  | `--io-threads`         | none (extra `--replicas` add fan-out load only) |

`--replicas` is an instance count on the runner host, not a host count: the
task's topology names one machine, so it needs exactly one `servers.json`
entry. (A task whose topology places replicas on further host slots needs one
configured server per slot.)

Server arguments: `--server-args` applies to every instance; `--primary-args`
and `--replica-args` are appended after it for their role, so a role-specific
flag overrides a shared one (valkey applies later command-line config over
earlier). `--replica-args` is the natural A/B lever for a replica-side config
change on a single build.

## Phases per repetition

1. Kill stray servers, start the topology, `REPLICAOF`, wait for link up.
2. Preload: cachecannon prefill of `--keyspace` keys at the primary, then wait
   until every replica's offset equals the primary's and both key counts match.
3. Measure: start the fixed-rate writer at the primary, one second later the
   reader at the first replica (`--warmup` + `--duration`). `INFO replication
   stats` is sampled from every instance every `--sample-interval` seconds
   with a single remote shell per sample, so primary/replica offset skew is a
   few milliseconds. `--info-fields` adds arbitrary INFO fields to the sample
   (for example counters a build under test exposes); when any are requested
   the sample fetches `INFO everything`, so a field is found in whichever
   section its build puts it, and absent fields are omitted, not errors. The
   same shell also reads the host's per-core
   `/proc/stat` and every instance's per-thread CPU ticks; the two local
   cachecannon processes' thread ticks are read from `/proc` on the runner in
   the same tick.
4. Guards (fail loudly): reader 0% errors and >= 99% hit rate; writer 0%
   errors and achieved rate >= 90% of `--write-rate`; replica mean lag over
   the scored window at most `--max-lag-seconds` (default 1) of replication
   stream. An under-delivering writer would silently turn the run into a
   lower-write-rate run. A replica that falls behind keeps answering every
   GET at a full hit rate from a dataset the primary has moved past, so its
   reader throughput describes a different experiment; the guard measures
   lag in seconds of stream (mean lag bytes over the primary's offset rate)
   because that unit means the same thing at every write rate, while the
   sampling skew that bounds resolution is a few milliseconds at any rate.
5. Bottleneck verdict (recorded, logged at WARNING when not `server`; the run
   does not fail, its data is still diagnostic). Computed over the samples
   after `--warmup`:

   | Verdict | Rule | Score is |
   |---|---|---|
   | `host` | a core no party was allocated is > 5% busy | invalid: interference |
   | `generator` | a cachecannon reader thread >= 0.95 of a core | invalid: reader-bound |
   | `ambiguous` | reader thread >= 0.85 but < 0.95 | untrusted |
   | `server` | none of the above | the server's number |
   | `unknown` | fewer than 2 samples in the window | untrusted |

   A `host` verdict names its offender. Every sample also scans every thread
   on the host (`tid pid ticks processor comm`, ~35 ms and ~65 KB on a
   192-CPU runner, on the sampler's own cores), and the verdict charges each
   thread's CPU over an interval to the core it was on at the interval's end;
   a thread first seen in a sample is charged its lifetime ticks, which is
   what catches short-lived work such as the sampler's own sshd session.
   `bottleneck.cores.foreign_attribution` lists, per busy foreign core, the
   top threads (`comm`, `pid`, `tid`, mean fraction of the core) and an
   `unexplained` remainder: load no thread owns, i.e. kernel work charged to
   the core itself (softirq, RCU callbacks, IRQ handling). The reason string
   carries the top thread per core, e.g. `attribution: 150: sshd[13578/13578]
   0.14`, or `no thread found (kernel work 0.15)` when the load is the
   kernel's. The raw scan is not stored in the row; the attribution is.

   `server` means "nothing else is implicated", **not** "the server was proven
   saturated". Valkey's I/O threads busy-wait for work and the main thread
   polls without sleeping while I/O jobs are in flight, so server CPU time
   reads close to 100% far below the throughput ceiling. On one 16-core x86
   host with a stock build and `--io-threads 2`, the I/O thread
   read 1.00 of a core at one fifth of the throughput it later sustained, and
   the main thread read 0.81 at one half. Server CPU is therefore stored but
   never thresholded. The main thread's `loop_duty` (`eventloop_duration_sum`
   per wall second, which excludes the poll) is attached as evidence; on the
   same host it rose 0.09 / 0.23 / 0.58 / 0.81 at 5% / 20% / 50% / 100% of the
   ceiling. cachecannon's worker threads block in their poller, so their CPU
   time does track load and is safe to threshold. Confirming a ceiling needs a
   load step (the same run at a higher connection count showing no gain),
   which is a separate control run, not yet built. Foreign-core detection needs
   to know where the generators run; `--client-cpus` supplies that as well as
   the allocator does, so the check runs either way.

   The runner process is itself a CPU consumer on the benchmark host: its log
   and status writes plus the per-second sampler measured about 0.35 of a core
   on a 192-CPU runner, and unpinned it lands on an arbitrary core, where the
   check correctly reported it as foreign (`1 unallocated core(s) busy: {129:
   0.155}`). The task runner loop therefore confines itself to management
   cores for every task (`runner_affinity`, `RUNNER_MANAGEMENT_CPUS` in
   `config.py`): cores reserved through the allocator from the tail of a
   non-network NUMA node so no server or generator placement moves, every
   thread pinned there, the mask restored afterwards including on failure.
   Local generator launches start under the runner's original mask (a child
   inherits its parent's mask at fork). This task additionally runs the
   sampler shell, which executes under sshd rather than under the runner, on
   the same cores via `run_host_command(pin_cpus=...)`, and lists them as
   `runner` in the verdict's claimed CPU map.

   The generators have unpinned threads of their own. cachecannon pins only
   its ringline workers to the TOML's `cpu_list`; its main thread and the
   admin thread that emits the per-second metrics stay on the launch mask,
   and with the runner pinned that mask is the whole host. With the runner
   confined, the next smoke cell still read `host` (`{107: 0.159}`, 18 reader
   threads for 8 workers): the admin thread summarising 1.4M ops/s on a random
   node-1 core. This task therefore launches each cachecannon process under
   its own allocation (`taskset -c <reader cpus>` / `<writer cpus>`;
   `RealtimeCommand(launch_cpus=...)`), so every thread of a generator sits on
   cores the verdict counts as claimed. The admin thread then shares the
   worker cores instead of a free core elsewhere, which is the honest
   accounting for what the generator costs.

## Result row

`method = "replica-read"`, `score` = mean reader rps over reps, `cv`/`reps`
as for other tasks. `data` carries: the topology spec, per-port server CPUs,
writer target and achieved rate, replication lag (primary minus replica
offset, bytes: max/mean, per-rep p99; per rep also the scored window alone,
with the stream rate and the mean lag in seconds of stream, and at row level
the worst rep's scored mean seconds beside the `--max-lag-seconds` limit), per-rep reader/writer latency, the
per-instance INFO series (offsets, ops/sec, eventloop counters, `--info-fields`),
both generated TOMLs, and `bottleneck`: the row-level verdict (`server` only
if every rep was), the worst rep's reason, mean replica `loop_duty`, and the
hottest reader thread. Each rep's `bottleneck` holds the full evidence:
per-role thread CPU (main, io-threads, others, hottest), replica and primary
event-loop duty, reader/writer thread utilisation, and per-core softirq share
on the replica's and reader's cores. Raw per-core and per-thread counters are
not stored (they would add about 1 MB per rep on a 96-core host).

Each rep's INFO series is stored under `series` in columnar form: `count`
ticks, the sample clocks `t` and `t_local` (monotonic seconds, 3 decimals),
and `instances.<port>.<field>` as one list of `count` values per INFO field,
in tick order (`None` where an instance did not report the field on a tick).
Stored one dict per tick, the same numbers were about 30 KB per rep, mostly
repeated key names, and a five-rep row grew past 190 KB; columnar it is about
8 KB per rep (65 KB per row on the real rows measured). With
`--read-rate-search` every probe entry carries its own `series`. The series
is a bulk key for outcome delivery: a runner drops it first when an outcome
must be trimmed to fit the control service (see `control-service.md`,
"Outcome size"); the full row on the runner keeps it.

Lag resolution is bounded by sample skew times replication bytes per second;
lags below that are noise and are clamped at zero.

### CPU profile (`--cpu-profile`)

`--cpu-profile` runs `perf record -g --call-graph fp` on the measured replica
(main thread and io-threads) for the scored window of the **last rep** only:
perf sleeps through the reader's warmup, then records for `duration`. The
collapsed stacks land on the row as `cpu_stacks_main` / `cpu_stacks_io` (the
perf task's keys, so `file_protocol` also writes `cpu_stacks_main.json` /
`cpu_stacks_io.json` beside the row and the flamegraph tooling reads them
unchanged), with `cpu_profile_rep` naming the rep that was sampled. perf's
sampling interrupts cost the replica a little, so treat that rep's throughput
as slightly depressed and keep profiled cells out of A/B means. The profile is
diagnostic and best effort: a failed `perf record`/`perf script` logs a
warning and leaves the row without stacks, never failing the cell; a reader
failure inside the window cancels perf instead of waiting it out. Use it when a
verdict says `server` and you want to know what the replica's main thread is
doing with its loop duty (a profiled run showed ~0.90 loop
duty with ~0.01 command duty at io4/io8, which only a profile can explain).

## A/B recipe

Two tasks that differ only in `--specifier` (two builds) or `--replica-args`
(one build, two configurations):

```bash
conductress queue add-replica-read --source valkey --specifier <baseline-sha> \
  --io-threads 8 --write-rate 50000 --runner <runner> --note "replica-read baseline w50k io8"
conductress queue add-replica-read --source <fork> --specifier <candidate-sha> \
  --io-threads 8 --write-rate 50000 --runner <runner> --note "replica-read candidate w50k io8"
```

Both arms of a comparison must run on the same runner. Sweeping `--write-rate`
(the replication-stream rate) and `--io-threads` (the replica's parallelism)
covers the two axes a read-path change is most likely to depend on. Match
rows by note and specifier in `results/output.jsonl`.

## Not yet

- Cluster mode: `TopologySpec` can describe `cluster_enabled` per instance
  (bus port = port + 10000) but the bootstrap (ADDSLOTS / MEET / REPLICATE) is
  not implemented; `TopologyGroup.start` refuses such a spec. In cluster mode
  the reader would also need `READONLY` on each connection.
- Event phases (full sync at time t, failover, slot migration) are the natural
  next step on the same runner: a phase list on the task plus per-instance
  actions, with INFO sampled across the event.
- Sweep coordinator integration: this is a manual-queue task type only.
- Load-step control run: repeat the reader at 2x connections (and at half the
  reader threads) and compare. No gain at 2x with the reader still cool is the
  actual proof of a server ceiling, and a proportional drop at half threads
  would expose a reader-bound run that CPU sampling missed. Memory-bandwidth
  and last-level-cache contention between co-located instances are only
  visible this way.
- Loopback is not a network. Replication lag, full-sync duration and failover
  timing measured on one host are lower bounds for what a real link would
  show; read throughput under a given replication rate is the question this
  task answers.
