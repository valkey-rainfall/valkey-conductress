"""Latency benchmark task: measures per-request latency at a controlled rate using memtier_benchmark.

Supports GET-only (default) and mixed GET/SET workloads with per-command-class
percentile extraction.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Dict, Optional

from conductress.config import (
    CONDUCTRESS_RESULTS,
    LATENCY_CLIENTS,
    LATENCY_DURATION,
    LATENCY_KEYSPACE,
    LATENCY_PIPELINE,
    LATENCY_REPS,
    LATENCY_TARGET_RPS,
    LATENCY_THREADS,
    LATENCY_VAL_SIZE,
    ServerInfo,
)
from conductress.file_protocol import BenchmarkStatus
from conductress.replication_group import ReplicationGroup
from conductress.runner_identity import get_result_provenance
from conductress.server import Server
from conductress.task_queue import BaseTaskData, BaseTaskRunner
from conductress.tasks.task_mixed import set_ratio_to_memtier_ratio

logger = logging.getLogger(__name__)

# Percentile points to extract from HDR histogram
HISTOGRAM_PERCENTILES = [0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.999, 1.0]


@dataclass
class LatencyTaskData(BaseTaskData):
    """Task data for latency measurement at a flat load."""

    target_rps: int = LATENCY_TARGET_RPS
    io_threads: int = 9
    repetitions: int = LATENCY_REPS
    sweep_commit: str = ""  # non-empty marks this as a sweep task
    server_args: str = ""  # extra raw args appended to the server command line
    set_ratio: int = 0  # percentage of SET commands (0-100, 0 = GET-only)
    value_size: int = LATENCY_VAL_SIZE  # value size in bytes for populate + measure

    def __post_init__(self):
        super().__post_init__()
        self.task_type = "LatencyTaskData"
        if not (0 <= self.set_ratio <= 100):
            raise ValueError(f"set_ratio must be 0-100, got {self.set_ratio}")
        if self.value_size < 1:
            raise ValueError(f"value_size must be >= 1, got {self.value_size}")

    def short_description(self) -> str:
        parts = [f"Latency @ {self.target_rps} rps (P=1 flat"]
        if self.set_ratio > 0:
            parts.append(f", SET={self.set_ratio}%")
        parts.append(")")
        return "".join(parts)

    def prepare_task_runner(self, server_infos: list[ServerInfo]) -> "LatencyTaskRunner":
        return LatencyTaskRunner(
            task_name=self.task_id,
            server_infos=server_infos,
            source=self.source,
            specifier=self.specifier,
            make_args=self.make_args,
            io_threads=self.io_threads,
            target_rps=self.target_rps,
            repetitions=self.repetitions,
            server_args=self.server_args,
            set_ratio=self.set_ratio,
            value_size=self.value_size,
        )


class LatencyTaskRunner(BaseTaskRunner):
    """Runs memtier_benchmark with rate limiting (P=1) to measure latency at fixed load."""

    def __init__(
        self,
        task_name: str,
        server_infos: list[ServerInfo],
        source: str,
        specifier: str,
        make_args: str,
        io_threads: int,
        target_rps: int,
        repetitions: int,
        server_args: str = "",
        set_ratio: int = 0,
        value_size: int = LATENCY_VAL_SIZE,
    ):
        super().__init__(task_name)
        self.server_infos = server_infos
        self.source = source
        self.specifier = specifier
        self.make_args = make_args
        self.io_threads = io_threads
        self.target_rps = target_rps
        self.repetitions = repetitions
        self.server_args = server_args
        self.set_ratio = set_ratio
        self.value_size = value_size

    async def run(self) -> None:
        """Execute the latency benchmark: start server, populate, measure, collect results."""
        total_conns = LATENCY_THREADS * LATENCY_CLIENTS
        rate_per_conn = self.target_rps // total_conns
        if rate_per_conn <= 0:
            raise ValueError(f"target_rps {self.target_rps} too low for {total_conns} connections")

        self.status = BenchmarkStatus(steps_total=self.repetitions * 2, task_type="latency")
        self.file_protocol.write_status(self.status)

        replication_group = ReplicationGroup(
            self.server_infos,
            self.source,
            self.specifier,
            self.io_threads,
            self.make_args,
            server_args=self.server_args,
        )

        all_reps: list[dict] = []

        try:
            for rep in range(self.repetitions):
                logger.info("Latency rep %d/%d (target %d rps, P=1)", rep + 1, self.repetitions, self.target_rps)

                # Between-rep: stop server, drop caches
                if rep > 0:
                    await replication_group.stop_all_servers()
                    server = replication_group.primary or Server(self.server_infos[0].ip)
                    await server.run_host_command("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'", check=False)

                # Start fresh server
                await replication_group.kill_all_valkey_instances()
                await replication_group.start()
                if not replication_group.primary:
                    raise RuntimeError("Server failed to start")

                server = replication_group.primary

                # Populate keys using memtier (also serves as warmup)
                populate_cmd = (
                    f"~/conductress/memtier_benchmark "
                    f"--server {server.ip} --port {server.port} --protocol redis "
                    f"--threads {LATENCY_THREADS} --clients {LATENCY_CLIENTS} "
                    f"--ratio 1:0 --key-pattern P:P "
                    f"--key-minimum 1 --key-maximum {LATENCY_KEYSPACE} "
                    f"--data-size {self.value_size} "
                    f"--requests {LATENCY_KEYSPACE // total_conns} "
                    f"--hide-histogram"
                )
                await server.run_host_command(populate_cmd)
                self.status.steps_completed = rep * 2 + 1
                self.file_protocol.write_status(self.status)

                # Run latency measurement (P=1, flat rate)
                hdr_prefix = "/tmp/latency-hdr"
                ratio_str = set_ratio_to_memtier_ratio(self.set_ratio)
                measure_cmd = (
                    f"~/conductress/memtier_benchmark "
                    f"--server {server.ip} --port {server.port} --protocol redis "
                    f"--threads {LATENCY_THREADS} --clients {LATENCY_CLIENTS} "
                    f"--ratio {ratio_str} --key-pattern R:R "
                    f"--key-minimum 1 --key-maximum {LATENCY_KEYSPACE} "
                    f"--data-size {self.value_size} "
                    f"--pipeline {LATENCY_PIPELINE} "
                    f"--rate-limiting {rate_per_conn} "
                    f"--test-time {LATENCY_DURATION} "
                    f"--print-percentiles 50,99,99.9,100 "
                    f"--hdr-file-prefix {hdr_prefix} "
                    f"--hide-histogram"
                )
                stdout, _ = await server.run_host_command(measure_cmd)
                self.status.steps_completed = rep * 2 + 2
                self.file_protocol.write_status(self.status)

                # Parse results
                percentiles = _parse_memtier_output(stdout, mixed=(self.set_ratio > 0))
                if percentiles:
                    histogram = await self._parse_hdr_histogram(server, hdr_prefix)
                    all_reps.append({**percentiles, "histogram": histogram})
                else:
                    logger.warning("Failed to parse memtier output for rep %d", rep + 1)

        finally:
            await replication_group.stop_all_servers()

        if not all_reps:
            raise RuntimeError("No successful latency repetitions")

        aggregated = self._aggregate_reps(all_reps)
        self._write_result(aggregated)

    async def _parse_hdr_histogram(self, server: Server, hdr_prefix: str) -> list[list[float]]:
        """Parse HDR .txt file to extract CDF at target percentile points."""
        hdr_file = f"{hdr_prefix}_GET_command_run_1.txt"
        try:
            stdout, _ = await server.run_host_command(f"cat {hdr_file}")
        except Exception:
            return []

        entries: list[tuple[float, float]] = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Value"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    latency_ms = float(parts[0])
                    percentile = float(parts[1])
                    entries.append((percentile, latency_ms * 1000))  # ms -> µs
                except ValueError:
                    continue

        if not entries:
            return []

        # Extract at target percentile points (closest match)
        histogram: list[list[float]] = []
        for target_pct in HISTOGRAM_PERCENTILES:
            closest = min(entries, key=lambda e: abs(e[0] - target_pct))
            histogram.append([target_pct, closest[1]])
        return histogram

    def _aggregate_reps(self, reps: list[dict]) -> dict:
        """Aggregate multiple reps: median of each metric."""
        result: dict = {
            "reps": len(reps),
            "histogram": reps[len(reps) // 2]["histogram"],  # median rep's histogram
        }

        # Aggregate totals (always present)
        if "totals" in reps[0]:
            # Mixed mode: per-class aggregation
            for cls_key in ("totals", "gets", "sets"):
                if cls_key not in reps[0]:
                    continue
                cls_data: dict = {}
                for metric in ("actual_rps", "p50_us", "p99_us", "p99_9_us", "p100_us"):
                    values = [r[cls_key][metric] for r in reps if cls_key in r]
                    if values:
                        cls_data[metric] = median(values)
                result[cls_key] = cls_data
            # Top-level convenience fields from totals
            result["actual_rps"] = result["totals"]["actual_rps"]
            result["p50_us"] = result["totals"]["p50_us"]
            result["p99_us"] = result["totals"]["p99_us"]
            result["p99_9_us"] = result["totals"]["p99_9_us"]
            result["p100_us"] = result["totals"]["p100_us"]
            result["per_rep_p99"] = [r["totals"]["p99_us"] for r in reps]
        else:
            # Legacy GET-only mode (flat dict)
            result["actual_rps"] = median([r["actual_rps"] for r in reps])
            result["p50_us"] = median([r["p50_us"] for r in reps])
            result["p99_us"] = median([r["p99_us"] for r in reps])
            result["p99_9_us"] = median([r["p99_9_us"] for r in reps])
            result["p100_us"] = median([r["p100_us"] for r in reps])
            result["per_rep_p99"] = [r["p99_us"] for r in reps]

        return result

    def _write_result(self, result: dict) -> None:
        """Write result to output.jsonl."""
        from datetime import datetime

        output_file = CONDUCTRESS_RESULTS / "output.jsonl"

        ratio_note = f", SET={self.set_ratio}%" if self.set_ratio > 0 else ""
        detailed_data: dict = {
            "actual_rps": result["actual_rps"],
            "target_rps": self.target_rps,
            "p50_us": result["p50_us"],
            "p99_us": result["p99_us"],
            "p99_9_us": result["p99_9_us"],
            "p100_us": result["p100_us"],
            "histogram": result["histogram"],
            "reps": result["reps"],
            "set_ratio": self.set_ratio,
            "value_size": self.value_size,
        }

        # Include per-class data if available
        for cls_key in ("totals", "gets", "sets"):
            if cls_key in result:
                detailed_data[cls_key] = result[cls_key]

        entry = {
            "task_id": self.task_name,
            "method": "latency",
            "source": self.source,
            "specifier": self.specifier,
            "commit_hash": self.specifier[:8],
            "note": f"latency @ {self.target_rps} rps (P=1 flat{ratio_note})",
            "end_time": datetime.now().strftime("%Y.%m.%d_%H.%M.%S.%f"),
            "score": result["p99_us"],  # p99 is the primary bisection metric
            "data": detailed_data,
        }
        entry.update(get_result_provenance())
        with open(output_file, "a") as f:
            f.write(json.dumps(entry) + "\n")


def _parse_memtier_output(output: str, mixed: bool = False) -> Optional[dict]:
    """Parse memtier summary to extract ops/sec and percentiles.

    When mixed=True, parses separate Gets and Sets rows in addition to Totals,
    returning a dict with "totals", "gets", and optionally "sets" sub-dicts.

    When mixed=False (legacy GET-only), returns a flat dict with top-level
    actual_rps/p50_us/p99_us/p99_9_us/p100_us for backward compatibility.
    """
    if mixed:
        return _parse_memtier_output_mixed(output)
    return _parse_memtier_output_simple(output)


def _parse_memtier_output_simple(output: str) -> Optional[dict]:
    """Parse memtier output for GET-only workload (backward-compatible flat dict)."""
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 9 and parts[0] in ("Gets", "Totals"):
            try:
                actual_rps = float(parts[1])
                p50 = float(parts[5]) * 1000  # ms -> µs
                p99 = float(parts[6]) * 1000
                p99_9 = float(parts[7]) * 1000
                p100 = float(parts[8]) * 1000
                return {
                    "actual_rps": actual_rps,
                    "p50_us": p50,
                    "p99_us": p99,
                    "p99_9_us": p99_9,
                    "p100_us": p100,
                }
            except (IndexError, ValueError) as e:
                logger.warning("Parse error: %s", e)
    return None


def _parse_row_percentiles(parts: list[str]) -> Optional[Dict[str, float]]:
    """Parse a single memtier summary row into percentile dict.

    Expected columns (after Type name):
    Ops/sec  Hits/sec  Misses/sec  Avg.Latency  p50  p99  p99.9  p100  KB/sec

    The percentile columns are at indices 5, 6, 7, 8 (0-indexed from full parts).
    Values are in milliseconds; we convert to microseconds.
    """
    if len(parts) < 9:
        return None
    try:
        actual_rps = float(parts[1])
        p50 = float(parts[5]) * 1000
        p99 = float(parts[6]) * 1000
        p99_9 = float(parts[7]) * 1000
        p100 = float(parts[8]) * 1000
        return {
            "actual_rps": actual_rps,
            "p50_us": p50,
            "p99_us": p99,
            "p99_9_us": p99_9,
            "p100_us": p100,
        }
    except (ValueError, IndexError):
        return None


def _parse_memtier_output_mixed(output: str) -> Optional[dict]:
    """Parse memtier output for mixed GET/SET workload with per-class percentiles.

    Returns dict with "totals", "gets", and optionally "sets" sub-dicts, each
    containing actual_rps, p50_us, p99_us, p99_9_us, p100_us.
    """
    result: dict = {}

    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 9:
            continue

        row_type = parts[0]
        if row_type == "Gets":
            parsed = _parse_row_percentiles(parts)
            if parsed:
                result["gets"] = parsed
        elif row_type == "Sets":
            parsed = _parse_row_percentiles(parts)
            if parsed:
                result["sets"] = parsed
        elif row_type == "Totals":
            parsed = _parse_row_percentiles(parts)
            if parsed:
                result["totals"] = parsed

    if "totals" not in result:
        return None

    return result
