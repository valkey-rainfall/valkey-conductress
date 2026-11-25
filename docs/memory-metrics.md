# Memory metrics

How a memory task measures an engine, what it records, and what the published series say.
The same definitions apply to every engine (Valkey, Redis, and any server that answers INFO).

## Definitions

All values are bytes per item. N is the item count the task reached (5,000,000 in the sweep). A manual task can
fill another count with `queue add-memory --item-count N` (10,000 to 50,000,000); the sweep always uses the default.
The task result records `item_count` (reached) and `item_count_requested`.

| Name | Definition |
|---|---|
| total | (used_memory after settle - used_memory before the fill) / N. used_memory comes from INFO memory. |
| user data | The payload bytes of one item, fixed per workload: key + value for SET, member (+ 8 score bytes) for ZADD and SADD, field + value for HSET. A collection's own key is counted once and does not matter at N = 5M. The 8-byte expire time of the TTL variants is NOT user data: it is part of the overhead. |
| overhead | total - user data. This is the published `rps` value of a memory series (kept under that name for compatibility). |
| RSS | (VmRSS after settle - VmRSS before the fill) / N, read from `/proc/<server pid>/status` on the server host. This works for any engine, including one without `used_memory_rss` in INFO. `RssAnon` is recorded as well. INFO `used_memory_rss` is recorded when the engine has it, and is the fallback if /proc cannot be read. |
| settle | The step that makes the sample a steady-state size: wait until used_memory stops moving, read every item once so an incremental rehash completes, check with DEBUG HTSTATS that no rehash is left (up to 3 read passes, otherwise the task fails), then wait until used_memory and any hash-table overhead fields of MEMORY STATS are stable. |
| profiled | The allocator was profiling heap allocations (jemalloc `opt.prof`). Profiling adds metadata that makes RSS about 100 times larger. It does not change used_memory. |

A profiled run has a valid total and an invalid RSS. A run is treated as profiled when the allocator
says so (MEMORY MALLOC-STATS `opt.prof`), or, when the engine cannot say, when profiling was requested.

## Task types

- The default memory task runs unprofiled for every engine. used_memory and RSS are both valid.
- The breakdown task (`queue add-memory --breakdown`, or the sweep's own) runs a profiling build with
  the profiler on and records only the per-structure breakdown (Valkey). Its RSS is recorded as
  invalid (null). The sweep never uses its value, only its breakdown.
- The sweep schedules a breakdown task for a tip point of an engine that publishes a breakdown, at most
  once per commit date for each series (one a day at most). History points have no new breakdown.

## What a task records

The result of each task has a `memory_record` object:

| Field | Meaning |
|---|---|
| engine | `server_name`, `version` (valkey_version, else redis_version), `executable` (file name only), `git_sha1` from INFO server. None when the engine does not report it. |
| binary | `sha256` of the server binary and the first line of `<binary> --version`. |
| profiling | `requested`, `active` (from MEMORY MALLOC-STATS; None = the engine cannot say), `profiled`. |
| settle | `requested`, `settled`, `passes`, `key_check`, `stability`. |
| samples | `before_fill`, `after_fill`, `after_settle`; each has `used_memory`, `used_memory_rss`, `vm_rss`, `rss_anon` (None when not available). |
| encoding | OBJECT ENCODING of the collection key (None for SET). |
| config | CONFIG GET of the settings that change memory: listpack and set/zset/list thresholds, activerehashing, hz, lazyfree and maxmemory settings. Only what the engine knows. |
| item_count | The count the task reached. |
| total_bytes_per_item, rss_bytes_per_item, rss_source | The values defined above. `rss_source` is `proc_vmrss` or `info_used_memory_rss`. |
| conductress_revision | The harness revision of the runner. |

A field the engine cannot answer is recorded as null. A missing optional field never fails a task.

## Published series

A memory series file keeps `results.<workload>.rps` (overhead), `cv` and `reps`. Each point also has a
`memory` object:

| Field | Meaning |
|---|---|
| total_bytes_per_item | total, as defined above |
| rss_bytes_per_item, rss_source | RSS per item and where it came from; null for a profiled run |
| n | item count reached |
| settled, settle_passes | settle state; `settled` is null (unknown) for a point recorded before the record existed |
| profiled | true, false, or null (unknown, old point) |
| engine_server_name, engine_version, binary_sha256 | engine identity; null when unknown |
| conductress_revision | first 12 characters of the harness revision |

For an old point (no record) the total is rebuilt from the overhead and the series' user data, and the
identity fields are null. Old points are not rewritten: the export only reads the stored state.

The series metadata has `user_data_bytes`, `item_count` and `profiled` (whether the series' runs are
profiled by default; false for the sweep). `metadata.settled` describes the series as it is measured
now; the per-point `settled` is the authoritative value for a single point.

## Reading the numbers

- Compare engines on overhead and total, never on the profile breakdown: the breakdown is a Valkey-only
  split of the same total (it adds about 0.2 B of server structures).
- Overhead depends on N through the hash table size: for one structure it can move by several bytes
  between item counts (it is a saw-tooth in N). Compare at the same N, and prefer more than one N.
- MEMORY USAGE is a per-key estimate and differs from the used_memory delta by several bytes on some
  engines. It is not used.
