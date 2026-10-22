"""Multi-process cachecannon generator: budget split and result merge."""

import pytest

from conductress.cachecannon import (
    compact_cpu_list,
    expand_cpu_list,
    merge_parsed_results,
    parse_json_results,
    split_generator,
)


def test_cpu_list_round_trip():
    assert expand_cpu_list("8-11,20") == [8, 9, 10, 11, 20]
    assert compact_cpu_list([8, 9, 10, 11, 20]) == "8-11,20"
    assert compact_cpu_list([]) == ""
    assert expand_cpu_list("") == []


def test_split_single_process_is_identity():
    plan = split_generator(1, 2400, 32, "8-39")
    assert plan == [{"connections": 2400, "threads": 32, "cpu_list": "8-39"}]


def test_split_preserves_totals_and_disjoint_cpus():
    plan = split_generator(4, 2400, 64, "8-71")
    assert sum(p["connections"] for p in plan) == 2400
    assert sum(p["threads"] for p in plan) == 64
    cpus = [c for p in plan for c in expand_cpu_list(p["cpu_list"])]
    assert len(cpus) == len(set(cpus)) == 64
    assert plan[0]["cpu_list"] == "8-23"
    assert plan[3]["cpu_list"] == "56-71"


def test_split_uneven_remainders_go_first():
    plan = split_generator(3, 10, 7, "0-7")
    assert [p["connections"] for p in plan] == [4, 3, 3]
    assert [p["threads"] for p in plan] == [3, 2, 2]
    assert [p["cpu_list"] for p in plan] == ["0-2", "3-5", "6-7"]


def test_split_without_cpu_list_leaves_os_scheduling():
    plan = split_generator(2, 100, 8, "")
    assert all(p["cpu_list"] == "" for p in plan)


def test_split_rejects_impossible_budgets():
    with pytest.raises(ValueError):
        split_generator(0, 10, 10, "")
    with pytest.raises(ValueError):
        split_generator(4, 3, 10, "")
    with pytest.raises(ValueError):
        split_generator(4, 10, 3, "")
    with pytest.raises(ValueError):
        split_generator(4, 10, 10, "0-1")


def _result(throughput, hits, misses, p50, p99, err=0.0, requests=None):
    return parse_json_results(
        '{"type":"result","throughput":%d,"err_pct":%s,"hits":%d,"misses":%d,"hit_pct":%s,'
        '"requests":%s,"responses":%s,"errors":0,"duration_secs":30,'
        '"get":{"p50_us":%d,"p90_us":%d,"p99_us":%d,"p999_us":%d,"p9999_us":%d,"max_us":%d,"count":%d}}'
        % (
            throughput,
            err,
            hits,
            misses,
            (100.0 * hits / (hits + misses)) if hits + misses else 0.0,
            requests if requests is not None else hits + misses,
            requests if requests is not None else hits + misses,
            p50,
            p50,
            p99,
            p99,
            p99,
            p99,
            hits + misses,
        )
    )


def test_merge_single_is_passthrough():
    r = _result(1000, 900, 100, 50, 90)
    assert merge_parsed_results([r]) is r


def test_merge_sums_and_recomputes():
    a = _result(24_671_086, 740_000_000, 0, 7000, 9000)
    b = _result(24_026_665, 720_000_000, 80_000, 6500, 12000, err=0.5)
    m = merge_parsed_results([a, b])
    assert m["throughput_rps"] == 48_697_751.0
    assert m["error_pct"] == 0.5
    assert m["hit_rate"]["hits"] == 1_460_000_000
    assert m["hit_rate"]["misses"] == 80_000
    assert abs(m["hit_rate"]["percent"] - 100.0 * 1_460_000_000 / 1_460_080_000) < 1e-9
    assert m["processes"] == 2
    assert m["per_process_rps"] == [24_671_086.0, 24_026_665.0]
    # pessimistic latency block: the process with the worse p99, counts summed, parts kept
    assert m["latency"]["p99_ms"] == 12.0
    assert m["latency"]["count"] == 740_000_000 + 720_080_000
    assert [x["p99_ms"] for x in m["latency"]["merged_from"]] == [9.0, 12.0]
    assert m["requests"] == 740_000_000 + 720_080_000
    assert m["duration_secs"] == 30
