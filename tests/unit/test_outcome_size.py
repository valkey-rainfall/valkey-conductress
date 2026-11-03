"""Size contract: measuring, trimming and classifying outcome rejections."""

import json

from conductress.control.config import DEFAULT_MAX_BODY_BYTES
from conductress.outcome_size import (
    DEFAULT_MAX_OUTCOME_BYTES,
    DELIVERY_KEY,
    bulk_candidates,
    encode_json_body,
    is_permanent_rejection,
    trim_outcome,
    wire_bytes,
)


def _outcome(result):
    return {
        "schema_version": 1,
        "task_id": "t-1",
        "runner_id": "armbench",
        "state": "completed",
        "completed_at": "2026-10-31T02:00:00Z",
        "result": result,
        "error": None,
    }


def _replica_read_result():
    """A replica-read row shaped like the real one: scalars, per-rep aggregates, bulk series."""
    fields = {
        "role": ["master"] * 35,
        "master_repl_offset": [180_000_000 + 6000 * i for i in range(35)],
        "instantaneous_ops_per_sec": [34_000 + i for i in range(35)],
        "total_commands_processed": [3_800_000 + 34_000 * i for i in range(35)],
        "eventloop_cycles": [4800 + i for i in range(35)],
        "eventloop_duration_sum": [7_800_000 + 1000 * i for i in range(35)],
        "repl_lane_reads": [0] * 35,
    }
    series = {
        "count": 35,
        "t": [14228192.748 + 1.167 * i for i in range(35)],
        "t_local": [14228192.929 + 1.167 * i for i in range(35)],
        "instances": {"6379": fields, "6380": dict(fields, role=["slave"] * 35)},
    }
    rep = {
        "reader_rps": 4_230_000.0,
        "lag": {"samples": 26, "mean_bytes": 12.0},
        "bottleneck": {"verdict": "ambiguous", "samples": 26},
        "series": series,
    }
    return {
        "task_id": "t-1",
        "method": "replica-read",
        "score": 4_230_000.0,
        "data": {
            "per_run_rps": [4.2e6] * 5,
            "mean_rps": 4_230_000.0,
            "bottleneck": {"verdict": "ambiguous", "per_rep": ["ambiguous"] * 5},
            "per_rep_results": [dict(rep, series=dict(series)) for _ in range(5)],
            "lscpu": "Architecture: aarch64\n" * 60,
            "reader_toml": "[reader]\n" * 40,
            "writer_toml": "[writer]\n" * 40,
            "memory": {"schema": 1, "samples": [{"t": i, "used_memory": i} for i in range(40)]},
        },
    }


def test_client_budget_is_below_server_default():
    assert DEFAULT_MAX_OUTCOME_BYTES < DEFAULT_MAX_BODY_BYTES


def test_wire_bytes_is_the_compact_encoding_the_client_sends():
    body = {"a": [1, 2, 3], "b": {"c": "d"}}
    assert encode_json_body(body) == b'{"a":[1,2,3],"b":{"c":"d"}}'
    assert wire_bytes(body) == len(json.dumps(body, separators=(",", ":")))
    assert wire_bytes(body) < len(json.dumps(body))


def test_permanent_rejection_is_payload_statuses_only():
    assert is_permanent_rejection(413)
    assert is_permanent_rejection(400)
    assert is_permanent_rejection(422)
    for status in (None, 401, 403, 404, 409, 429, 500, 502, 503, 504):
        assert not is_permanent_rejection(status), status


def test_bulk_candidates_lists_series_biggest_first_and_keeps_scalars():
    result = _replica_read_result()
    candidates = bulk_candidates(result)
    paths = [path for _size, path in candidates]
    # Every per-rep series, the memory series, lscpu and both TOMLs.
    assert ("data", "per_rep_results", 0, "series") in paths
    assert ("data", "memory", "samples") in paths
    assert ("data", "lscpu") in paths
    assert ("data", "reader_toml") in paths and ("data", "writer_toml") in paths
    # Scalars that share a bulk name are not candidates.
    assert ("data", "per_rep_results", 0, "lag", "samples") not in paths
    assert ("data", "per_rep_results", 0, "bottleneck", "samples") not in paths
    sizes = [size for size, _path in candidates]
    assert sizes == sorted(sizes, reverse=True)


def test_trim_to_budget_drops_biggest_first_and_records_what_went():
    outcome = _outcome(_replica_read_result())
    full = wire_bytes(outcome)
    budget = full - 2 * wire_bytes(outcome["result"]["data"]["per_rep_results"][0]["series"])

    trimmed, dropped = trim_outcome(outcome, budget, reason="preflight", full_record_path="/r/output.jsonl")

    assert wire_bytes(trimmed) <= budget
    assert len(dropped) == 3  # two series were not enough once the record is counted
    assert all("series" in item for item in dropped)
    data = trimmed["result"]["data"]
    assert sum("series" not in rep for rep in data["per_rep_results"]) == 3
    # Aggregates and scalars are untouched; the smaller bulk keys survive.
    assert data["per_run_rps"] == [4.2e6] * 5
    assert data["per_rep_results"][0]["reader_rps"] == 4_230_000.0
    assert data["lscpu"] and data["memory"]["samples"]
    record = trimmed["result"][DELIVERY_KEY]
    assert record["trimmed"] is True
    assert record["reason"] == "preflight"
    assert record["original_bytes"] == full
    assert record["sent_bytes"] == wire_bytes(trimmed)
    assert record["full_result_path"] == "/r/output.jsonl"
    assert record["dropped"] == dropped
    # The original outcome is not modified.
    assert wire_bytes(outcome) == full and DELIVERY_KEY not in outcome["result"]


def test_trim_with_zero_budget_drops_every_bulk_key():
    outcome = _outcome(_replica_read_result())
    reduced, dropped = trim_outcome(outcome, 0, reason="rejected", full_record_path="/r/output.jsonl")
    data = reduced["result"]["data"]
    assert all("series" not in rep for rep in data["per_rep_results"])
    assert "lscpu" not in data and "reader_toml" not in data and "writer_toml" not in data
    assert "samples" not in data["memory"] and data["memory"]["schema"] == 1
    assert len(dropped) == 5 + 1 + 3
    assert wire_bytes(reduced) < wire_bytes(outcome) / 5
    # Still a complete record for the compare/plot code.
    assert data["mean_rps"] == 4_230_000.0 and data["bottleneck"]["per_rep"] == ["ambiguous"] * 5
    assert all(rep["lag"]["samples"] == 26 for rep in data["per_rep_results"])


def test_trim_is_a_no_op_when_it_fits_or_nothing_is_bulk():
    small = _outcome({"task_id": "t-1", "score": 1, "data": {"mean_rps": 1}})
    assert trim_outcome(small, 10**6, reason="x", full_record_path="p") == (small, [])
    assert trim_outcome(small, 10, reason="x", full_record_path="p") == (small, [])
    assert trim_outcome(small, 0, reason="x", full_record_path="p") == (small, [])
    failed = dict(_outcome(None), state="failed", error="boom")
    assert trim_outcome(failed, 0, reason="x", full_record_path="p") == (failed, [])


def test_trim_survives_one_dict_shared_by_several_entries():
    series = {"count": 3, "t": [1.0, 2.0, 3.0], "instances": {"6380": {"offset": [1, 2, 3]}}}
    result = {"task_id": "t-1", "data": {"per_rep_results": [{"reader_rps": 1.0, "series": series}] * 3}}
    reduced, dropped = trim_outcome(_outcome(result), 0, reason="rejected", full_record_path="p")
    assert len(dropped) == 3
    assert all("series" not in rep and rep["reader_rps"] == 1.0 for rep in reduced["result"]["data"]["per_rep_results"])
