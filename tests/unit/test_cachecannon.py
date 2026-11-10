"""Unit tests for the cachecannon task type (second-opinion generator instrument)."""

import os
import tempfile
from pathlib import Path

import pytest

from conductress import config
from conductress.tasks.task_cachecannon import (
    CachecannonTaskData,
    CachecannonTaskRunner,
    generate_toml_config,
    parse_json_results,
)
from conductress.topology import TopologySpec


def _valid_source():
    """Return whatever source is valid in the current test context.

    test_cli.py mutates config.REPO_NAMES at module-level, so we must read it
    at call time to stay consistent with the runtime state.
    """
    return config.REPO_NAMES[0]


# --- TOML generation tests ---


def test_toml_generation_basic():
    """TOML config includes all required sections and key fields."""
    toml = generate_toml_config(
        duration=30,
        warmup=5,
        threads=16,
        cpu_list="8,9,10,11,12,13,14,15",
        endpoint="127.0.0.1:6379",
        connections=1200,
        pipeline_depth=10,
        keyspace_count=3000000,
        val_size=512,
        test="get",
    )

    # Check sections exist
    assert "[general]" in toml
    assert "[target]" in toml
    assert "[connection]" in toml
    assert "[workload]" in toml
    assert "[workload.keyspace]" in toml
    assert "[workload.commands]" in toml
    assert "[workload.values]" in toml
    assert "[timestamps]" in toml

    # Check key values
    assert 'duration = "30s"' in toml
    assert 'warmup = "5s"' in toml
    assert "threads = 16" in toml
    assert 'cpu_list = "8,9,10,11,12,13,14,15"' in toml
    assert 'io_engine = "uring"' in toml
    assert '"127.0.0.1:6379"' in toml
    assert 'protocol = "resp"' in toml
    assert "connections = 1200" in toml
    assert "pipeline_depth = 10" in toml
    assert "prefill = true" in toml
    assert 'prefill_settle = "0s"' in toml
    assert "count = 3000000" in toml
    assert "length = 16" in toml
    assert 'distribution = "uniform"' in toml
    assert "get = 100" in toml
    assert "length = 512" in toml
    assert "userspace = true" not in toml
    assert "enabled = true" in toml
    assert 'mode = "userspace"' in toml
    # Exact results require cachecannon's JSON formatter, not the human output.
    assert "[admin]" in toml
    assert 'format = "json"' in toml


def test_toml_humantime_and_schema_shape():
    """Regression for the Aug 5 production failure: cachecannon parses
    [general] duration/warmup with humantime_serde (REQUIRES strings like
    "300s"; bare integers are rejected at startup with 'invalid type:
    integer, expected a string'), and [timestamps] has keys enabled/mode
    (there is no 'userspace' key). Parse the generated TOML and verify the
    actual types, not just substrings."""
    try:
        import tomllib  # Python 3.11+
    except ModuleNotFoundError:
        import tomli as tomllib  # Python <3.11 (pytest dependency chain)

    toml = generate_toml_config(
        duration=300,
        warmup=30,
        threads=16,
        cpu_list="8,9",
        endpoint="127.0.0.1:6379",
        connections=1200,
        pipeline_depth=10,
        keyspace_count=3000000,
        val_size=16,
        test="get",
    )
    parsed = tomllib.loads(toml)
    assert isinstance(parsed["general"]["duration"], str)
    assert parsed["general"]["duration"] == "300s"
    assert isinstance(parsed["general"]["warmup"], str)
    assert parsed["general"]["warmup"] == "30s"
    assert parsed["timestamps"] == {"enabled": True, "mode": "userspace"}


def _tomllib():
    try:
        import tomllib  # Python 3.11+

        return tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # Python <3.11 (pytest dependency chain)

        return tomllib


def test_toml_plaintext_omits_all_tls_keys():
    """The default (non-TLS) config carries no tls keys at all -- falsifies a
    change that would leak tls into every cell."""
    toml = generate_toml_config(
        duration=30,
        warmup=10,
        threads=8,
        cpu_list="",
        endpoint="127.0.0.1:6379",
        connections=400,
        pipeline_depth=10,
        keyspace_count=3_000_000,
        val_size=16,
        test="get",
    )
    parsed = _tomllib().loads(toml)
    assert "tls" not in parsed["target"]
    assert "tls_ca_file" not in parsed["target"]


def test_toml_tls_emits_target_keys():
    """With tls=True the [target] section carries the cachecannon TLS keys."""
    toml = generate_toml_config(
        duration=30,
        warmup=10,
        threads=8,
        cpu_list="",
        endpoint="127.0.0.1:16379",
        connections=400,
        pipeline_depth=10,
        keyspace_count=3_000_000,
        val_size=16,
        test="get",
        tls=True,
        tls_hostname="127.0.0.1",
        tls_ca_file="/home/user/conductress/tls/ca.crt",
    )
    parsed = _tomllib().loads(toml)
    assert parsed["target"]["tls"] is True
    assert parsed["target"]["tls_verify"] is True
    assert parsed["target"]["tls_hostname"] == "127.0.0.1"
    assert parsed["target"]["tls_ca_file"] == "/home/user/conductress/tls/ca.crt"
    # The endpoint points at the TLS port; no client certificate is sent.
    assert parsed["target"]["endpoints"] == ["127.0.0.1:16379"]
    assert "tls_cert_file" not in parsed["target"]
    assert "tls_key_file" not in parsed["target"]


def test_toml_tls_requires_hostname_and_ca():
    """tls without the verification inputs is rejected, not silently insecure."""
    base = dict(
        duration=30,
        warmup=10,
        threads=8,
        cpu_list="",
        endpoint="127.0.0.1:16379",
        connections=400,
        pipeline_depth=10,
        keyspace_count=3_000_000,
        val_size=16,
        test="get",
    )
    with pytest.raises(ValueError, match="tls_hostname"):
        generate_toml_config(**base, tls=True, tls_ca_file="/x/ca.crt")
    with pytest.raises(ValueError, match="tls_ca_file"):
        generate_toml_config(**base, tls=True, tls_hostname="127.0.0.1")


def test_toml_generation_set_test():
    """TOML config correctly sets 'set = 100' for SET test."""
    toml = generate_toml_config(
        duration=60,
        warmup=10,
        threads=8,
        cpu_list="0,1,2,3",
        endpoint="10.0.0.1:6379",
        connections=600,
        pipeline_depth=1,
        keyspace_count=1000000,
        val_size=64,
        test="set",
    )
    assert "set = 100" in toml
    assert "get = 100" not in toml
    # All three weights must be explicit: cachecannon serde-defaults omitted
    # weights (get=80, set=20), so 'set = 100' alone would run 80% GET.
    assert "get = 0" in toml
    assert "delete = 0" in toml


def test_toml_generation_mixed_ratio():
    """set_ratio > 0 produces a mixed GET/SET workload overriding 'test'."""
    toml = generate_toml_config(
        duration=60,
        warmup=10,
        threads=8,
        cpu_list="0,1,2,3",
        endpoint="10.0.0.1:6379",
        connections=600,
        pipeline_depth=10,
        keyspace_count=1000000,
        val_size=64,
        test="get",
        set_ratio=30,
    )
    assert "get = 70" in toml
    assert "set = 30" in toml
    assert "delete = 0" in toml


def test_toml_generation_zipf_distribution():
    """distribution='zipf' is templated into the keyspace section."""
    toml = generate_toml_config(
        duration=60,
        warmup=10,
        threads=8,
        cpu_list="0,1,2,3",
        endpoint="10.0.0.1:6379",
        connections=600,
        pipeline_depth=10,
        keyspace_count=1000000,
        val_size=64,
        test="get",
        distribution="zipf",
    )
    assert 'distribution = "zipf"' in toml
    assert 'distribution = "uniform"' not in toml


def test_toml_generation_invalid_set_ratio_raises():
    """set_ratio outside 0-100 raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="set_ratio"):
        generate_toml_config(
            duration=30,
            warmup=5,
            threads=4,
            cpu_list="",
            endpoint="127.0.0.1:6379",
            connections=400,
            pipeline_depth=10,
            keyspace_count=1000000,
            val_size=128,
            test="get",
            set_ratio=101,
        )


def test_toml_generation_invalid_distribution_raises():
    """Unknown distribution raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="distribution"):
        generate_toml_config(
            duration=30,
            warmup=5,
            threads=4,
            cpu_list="",
            endpoint="127.0.0.1:6379",
            connections=400,
            pipeline_depth=10,
            keyspace_count=1000000,
            val_size=128,
            test="get",
            distribution="gaussian",
        )


def test_toml_generation_empty_cpu_list():
    """TOML config handles empty cpu_list (OS scheduling)."""
    toml = generate_toml_config(
        duration=30,
        warmup=5,
        threads=4,
        cpu_list="",
        endpoint="127.0.0.1:6379",
        connections=400,
        pipeline_depth=10,
        keyspace_count=1000000,
        val_size=128,
        test="get",
    )
    assert 'cpu_list = ""' in toml


# --- Throughput parsing tests ---


# A realistic cachecannon NDJSON stream. Throughput is deliberately NOT round:
# the retired clean-output parser would have rendered 3,104,882 as "3.10M" and
# read back exactly 3,100,000.
SAMPLE_NDJSON = """[precheck]
{"type":"config","target":"127.0.0.1:6379","protocol":"Resp","tls":false,"threads":8,\
"conns":400,"pipeline":10,"keyspace":3000000,"key_size":16,"value_size":16,\
"engine":"io_uring","warmup_secs":30,"duration_secs":30}
{"type":"sample","ts":"2026-09-04T18:00:00Z","req_s":3101044,"err_s":0.0,"hit_pct":100.0,\
"p50_us":1247,"p90_us":1359,"p99_us":1895,"p999_us":2210,"p9999_us":3011,"max_us":9001}
{"type":"result","duration_secs":30.0,"requests":93146460,"responses":93146460,"errors":0,\
"err_pct":0.0,"hits":93146460,"misses":0,"hit_pct":100.0,"throughput":3104882,\
"rx_bytes":5401234567,"tx_bytes":3201234567,"rx_bps":180041152,"tx_bps":106707818,\
"get":{"count":93146460,"p50_us":1247,"p90_us":1359,"p99_us":1895,"p999_us":2210,\
"p9999_us":3011,"max_us":15600},\
"set":{"count":0,"p50_us":0,"p90_us":0,"p99_us":0,"p999_us":0,"p9999_us":0,"max_us":0},\
"conns_active":400,"conns_failed":0,"requests_dropped":0,"offered":93146460}
[ringline diag] iters=5398 dead=0
"""


def test_parse_json_results_throughput_is_exact():
    """THE regression this parser exists for.

    The retired clean-output parser scraped an abbreviated 'N.NNM req/s' string,
    quantizing to three significant figures. 3,104,882 became 3,100,000 -- a
    0.16% error, and every repetition collapsed onto the same value so CV always
    read 0.000%.
    """
    parsed = parse_json_results(SAMPLE_NDJSON)
    assert parsed["throughput_rps"] == 3_104_882.0
    assert parsed["throughput_rps"] != 3_100_000.0


def test_parse_json_results_skips_unstructured_lines():
    """[precheck] and ringline diagnostics share the stream and must be ignored."""
    parsed = parse_json_results(SAMPLE_NDJSON)
    assert parsed["error_pct"] == 0.0
    assert parsed["requests"] == 93_146_460


def test_parse_json_results_exact_hit_rate():
    parsed = parse_json_results(SAMPLE_NDJSON)
    assert parsed["hit_rate"]["percent"] == 100.0
    assert parsed["hit_rate"]["hits"] == 93_146_460
    assert parsed["hit_rate"]["misses"] == 0


def test_parse_json_results_latency_converted_from_microseconds():
    """JSON reports integer microseconds; recorded schema stays milliseconds."""
    parsed = parse_json_results(SAMPLE_NDJSON)
    assert parsed["latency"]["p50_ms"] == 1.247
    assert parsed["latency"]["p99_ms"] == 1.895
    assert parsed["latency"]["max_ms"] == 15.6


def test_parse_json_results_primary_latency_is_the_workload_command():
    """A GET-only run must not record the all-zero SET row as its latency.

    The retired parser kept the LAST latency row it matched, so a GET workload
    could report SET (or a slip row) percentiles.
    """
    parsed = parse_json_results(SAMPLE_NDJSON)
    assert parsed["latency"]["command"] == "GET"
    assert parsed["latency_get"]["p50_ms"] == 1.247
    assert parsed["latency_set"]["count"] == 0


def test_parse_json_results_primary_latency_follows_a_set_workload():
    ndjson = SAMPLE_NDJSON.replace('"get":{"count":93146460,', '"get":{"count":0,').replace(
        '"set":{"count":0,', '"set":{"count":93146460,'
    )
    parsed = parse_json_results(ndjson)
    assert parsed["latency"]["command"] == "SET"


def test_parse_json_results_uses_last_result_message():
    doubled = SAMPLE_NDJSON + SAMPLE_NDJSON.replace('"throughput":3104882', '"throughput":2999777')
    assert parse_json_results(doubled)["throughput_rps"] == 2_999_777.0


def test_parse_json_results_missing_result_raises():
    import pytest

    with pytest.raises(ValueError, match="No cachecannon JSON result message"):
        parse_json_results('[precheck]\n{"type":"config","threads":8}\n')


def test_parse_json_results_missing_field_raises():
    import pytest

    incomplete = '{"type":"result","err_pct":0.0,"hits":1,"misses":0,"hit_pct":100.0}'
    with pytest.raises(ValueError, match="missing 'throughput'"):
        parse_json_results(incomplete)


def test_parse_json_results_malformed_latency_raises():
    import pytest

    broken = (
        '{"type":"result","throughput":100,"err_pct":0.0,"hits":1,"misses":0,'
        '"hit_pct":100.0,"get":{"count":1,"p50_us":5}}'
    )
    with pytest.raises(ValueError, match="latency object missing"):
        parse_json_results(broken)


def test_task_data_short_description():
    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="unstable",
        make_args="",
        topology=TopologySpec.standalone(),
        note="test",
        requirements={},
        test="get",
        val_size=512,
        pipelining=10,
        connections=1200,
        threads=16,
        warmup=5,
        duration=30,
        repetitions=3,
    )
    desc = task.short_description()
    assert "cachecannon" in desc
    assert "get" in desc
    assert "x3" in desc


def test_task_data_short_description_mixed_zipf():
    """Mixed/zipf workloads are reflected in the description."""
    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="unstable",
        make_args="",
        topology=TopologySpec.standalone(),
        note="test",
        requirements={},
        test="get",
        val_size=16,
        pipelining=10,
        connections=1200,
        threads=16,
        warmup=5,
        duration=30,
        repetitions=3,
        set_ratio=30,
        distribution="zipf",
    )
    desc = task.short_description()
    assert "mixed s30" in desc
    assert "zipf" in desc


def test_task_data_task_type_registration():
    """CachecannonTaskData should be registered in the task registry."""
    from conductress.task_queue import BaseTaskData

    # Registration happens at import time via __init_subclass__
    assert "CachecannonTaskData" in BaseTaskData._BaseTaskData__task_registry


def test_task_data_serialization_roundtrip():
    """Task data should serialize to dict and back."""
    import json
    import tempfile
    from dataclasses import asdict
    from pathlib import Path

    from conductress.task_queue import BaseTaskData

    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="abc123",
        make_args="",
        topology=TopologySpec.standalone(),
        note="roundtrip test",
        requirements={},
        test="get",
        val_size=256,
        pipelining=5,
        connections=600,
        threads=8,
        warmup=3,
        duration=15,
        repetitions=2,
        keyspace_count=1000000,
        cachecannon_binary="/usr/local/bin/cachecannon",
        server_args="--io-threads-ownership yes",
        set_ratio=50,
        distribution="zipf",
    )

    # Save and reload
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        task.save_to_file(Path(f.name))
        loaded = BaseTaskData.from_file(Path(f.name))

    assert isinstance(loaded, CachecannonTaskData)
    assert loaded.test == "get"
    assert loaded.val_size == 256
    assert loaded.pipelining == 5
    assert loaded.connections == 600
    assert loaded.threads == 8
    assert loaded.cachecannon_binary == "/usr/local/bin/cachecannon"
    assert loaded.server_args == "--io-threads-ownership yes"
    assert loaded.set_ratio == 50
    assert loaded.distribution == "zipf"
    assert loaded.note == "roundtrip test"

    import os

    os.unlink(f.name)


# --- Runner construction test ---


def test_runner_construction():
    """CachecannonTaskRunner should initialize from task data."""
    from conductress.config import ServerInfo

    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="abc123",
        make_args="",
        topology=TopologySpec.standalone(),
        note="",
        requirements={},
    )
    server_infos = [ServerInfo(ip="127.0.0.1")]
    runner = task.prepare_task_runner(server_infos)
    assert isinstance(runner, CachecannonTaskRunner)
    assert runner.test == "get"
    assert runner.val_size == 512
    assert runner.pipelining == 10


def test_runner_plaintext_leaves_build_and_server_args_alone():
    """A non-TLS cell touches neither make_args nor server_args -- falsifies a
    change that would build TLS for every cell."""
    import pytest

    from conductress.config import ServerInfo

    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="abc123",
        make_args="",
        topology=TopologySpec.standalone(),
        note="",
        requirements={},
        server_args="--io-threads 7",
    )
    assert task.tls is False
    runner = task.prepare_task_runner([ServerInfo(ip="127.0.0.1")])
    assert runner.tls is False
    assert "BUILD_TLS" not in runner.make_args
    assert "--tls-port" not in runner.server_args


def test_runner_tls_appends_build_flag_and_listener_args():
    """A TLS cell requests a BUILD_TLS=yes binary and opens a TLS listener with
    the bootstrap test certs."""
    from conductress.config import SWEEP_V3_TLS_PORT, TLS_CERT_DIR, ServerInfo

    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="abc123",
        make_args="",
        topology=TopologySpec.standalone(),
        note="",
        requirements={},
        tls=True,
    )
    assert task.tls is True
    runner = task.prepare_task_runner([ServerInfo(ip="127.0.0.1")])
    assert runner.tls is True
    # Distinct build-cache entry: BUILD_TLS is part of make_args, which the cache
    # keys on, so a TLS binary never serves a plaintext cell.
    assert "BUILD_TLS=yes" in runner.make_args
    # The TLS listener is on the dedicated TLS port with the bootstrap certs.
    assert f"--tls-port {SWEEP_V3_TLS_PORT}" in runner.server_args
    assert f"--tls-cert-file {TLS_CERT_DIR}/server.crt" in runner.server_args
    assert f"--tls-key-file {TLS_CERT_DIR}/server.key" in runner.server_args
    assert f"--tls-ca-cert-file {TLS_CERT_DIR}/ca.crt" in runner.server_args
    assert "--tls-auth-clients no" in runner.server_args
    assert runner._tls_port == SWEEP_V3_TLS_PORT


def test_runner_tls_does_not_double_append_build_flag():
    """A make_args that already carries BUILD_TLS=yes is not duplicated."""
    from conductress.config import ServerInfo

    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="abc123",
        make_args="BUILD_TLS=yes",
        topology=TopologySpec.standalone(),
        note="",
        requirements={},
        tls=True,
    )
    runner = task.prepare_task_runner([ServerInfo(ip="127.0.0.1")])
    assert runner.make_args.count("BUILD_TLS=yes") == 1


def test_tls_is_a_real_dataclass_field_and_survives_roundtrip():
    """tls must be a dataclass field: asdict() drops a dynamic attribute, so a
    dynamic flag would deserialize False on the runner and run plaintext."""
    import json
    import os
    import tempfile
    from dataclasses import fields

    from conductress.task_queue import BaseTaskData

    assert "tls" in {f.name for f in fields(CachecannonTaskData)}

    task = CachecannonTaskData(
        source=_valid_source(),
        specifier="abc123",
        make_args="",
        topology=TopologySpec.standalone(),
        note="roundtrip",
        requirements={},
        tls=True,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        task.save_to_file(Path(f.name))
    reloaded = BaseTaskData.from_file(Path(f.name))
    os.unlink(f.name)
    assert isinstance(reloaded, CachecannonTaskData)
    assert reloaded.tls is True

    legacy = {
        "source": _valid_source(),
        "specifier": "abc123",
        "replicas": 0,
        "note": "queued before tls",
        "requirements": {},
        "make_args": "",
        "task_type": "CachecannonTaskData",
        "timestamp": "2026-08-29T00:00:00.123456",
        "test": "get",
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        f.write(json.dumps(legacy))
    legacy_task = BaseTaskData.from_file(Path(f.name))
    os.unlink(f.name)
    assert legacy_task.tls is False


def test_workload_issues_gets_pure_get():
    from conductress.tasks.task_cachecannon import workload_issues_gets

    assert workload_issues_gets("get", 0) is True


def test_workload_issues_gets_pure_set_via_test():
    # Pure SET has hit rate 0.0 by definition; the prefill hit-rate guard
    # must not apply (this failed every pure-SET task before the fix).
    from conductress.tasks.task_cachecannon import workload_issues_gets

    assert workload_issues_gets("set", 0) is False


def test_workload_issues_gets_mixed_ratios():
    from conductress.tasks.task_cachecannon import workload_issues_gets

    assert workload_issues_gets("get", 20) is True
    assert workload_issues_gets("set", 20) is True  # ratio overrides test
    assert workload_issues_gets("get", 99) is True


def test_workload_issues_gets_all_set_via_ratio():
    from conductress.tasks.task_cachecannon import workload_issues_gets

    assert workload_issues_gets("get", 100) is False
    assert workload_issues_gets("set", 100) is False
