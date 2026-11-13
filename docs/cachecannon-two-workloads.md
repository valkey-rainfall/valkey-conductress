# Two workloads in one cachecannon task

`queue add-cachecannon` can run a second workload at the same time as the main
one, against the same server and the same keyspace. Each workload has its own
connection count, worker threads, command mix and (optional) rate limit. A
typical use is dedicated writer connections: a small group of connections that
send only SET, and the rest of the connections send only GET.

Without `--workload2-connections` (or with 0) the task is unchanged: same
TOML, same files, same result row.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--workload2-connections N` | 0 (off) | Connections of the second workload. |
| `--workload2-threads N` | 1 | Worker threads of the second workload, in addition to `--threads`. |
| `--workload2-set-ratio P` | 100 | SET percentage of the second workload: 100 = SET-only, 0 = GET-only. |
| `--workload2-rate R` | 0 | Fixed request rate of the second workload in req/s; 0 = closed loop. |

The main workload keeps its own flags (`--test`, `--set-ratio`,
`--connections`, `--threads`, `--rate`, `--processes`). Both workloads share
`--pipelining`, `--sizes`, `--keyspace`, `--distribution`, `--warmup`,
`--duration` and the transport (`--tls`, `--client-netns`). The client cpus
(allocated, or `--client-cpus`) are cut into two disjoint slices in proportion
to `--threads` and `--workload2-threads`.

## How a rep runs

1. The server starts as usual.
2. One shared prefill run: a short cachecannon run of the main workload's
   shape with `prefill = true`, `warmup = 0s` and `duration = 1s`, run to
   completion (`cachecannon_rep<N>_prefill.toml`). It writes every key once.
3. The main workload processes (`cachecannon_rep<N>.toml`, or `_p<i>` with
   `--processes`) and the second workload (`cachecannon_rep<N>_w2.toml`) start
   together, all with `prefill = false`. Their warmup and scored windows start
   at the same time, so the two workloads overlap for the whole scored window.

## Results

The top-level throughput, request counts, hit rate and latency blocks are the
merged totals of both workloads, as with `--processes`. The score is the
merged throughput. In addition the row carries:

- `workload2`: the second workload's settings and its TOML;
- `workloads.main` and `workloads.workload2`: per-workload `throughput_rps`,
  `requests`, `get_count`, `set_count`, `error_pct`, `hit_rate`,
  `latency_get` and `latency_set` (last rep);
- `achieved_set_pct`: SETs as a percentage of all GETs and SETs completed in
  the scored window, over both workloads (last rep);
- `per_rep_results[i].workloads` and `per_rep_results[i].achieved_set_pct`
  for every rep.

The method name gets a `w2s<P>` suffix (for example `cachecannon-get-w2s100`),
so a two-workload task is a separate series from a single-workload one.

## Getting a target mix

In closed loop, a connection sends its next request when the previous reply
returns. SET and GET connections then run at different per-connection rates
(a SET usually costs more server time than a GET), so a fixed connection split
does NOT give an exact mix: 10% writer connections does not mean 10% SETs.
Two ways to reach a target mix such as 95/5 or 80/20 GET/SET:

- Rate-limit the writers. Run the cell once in closed loop and read the total
  throughput T. Then set `--workload2-rate` to the target SET share of T (5% of
  T for 95/5, 20% of T for 80/20). The writers then send a fixed rate and the
  closed-loop readers use the rest of the server's capacity. Check
  `achieved_set_pct` in the result; if the total changed a lot, repeat once
  with the new T.
- Calibrate the connection split. Keep both workloads closed loop, run a short
  task per cell, read `achieved_set_pct`, and change
  `--workload2-connections` until it is close to the target. Record the final
  split with the cell.

In both cases, report `achieved_set_pct` with the result, not the requested
split.

## Example

10% of 1200 connections SET-only, 90% GET-only, 16 B values, 3 M keys,
pipeline 1:

```bash
conductress queue add-cachecannon --runner <runner> --source <repo> --specifier <ref> \
  --test get --sizes 16 --keyspace 3000000 --pipelining 1 \
  --connections 1080 --threads 14 \
  --workload2-connections 120 --workload2-threads 2 --workload2-set-ratio 100
```
