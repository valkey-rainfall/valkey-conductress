"""Unit tests for cachecannon task profiling, INFO capture, and fixed-rate options."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conductress import cli, config, task_queue
from conductress.tasks.task_cachecannon import (
    CachecannonTaskData,
    info_window_deltas,
    is_scored_sample_line,
    parse_info_sections,
    snapshot_info,
)
from conductress.topology import TopologySpec


@pytest.fixture(autouse=True)
def _repo_names(monkeypatch):
    """The CLI validates --source against config.REPO_NAMES; pin it like test_fleet_cli does."""
    monkeypatch.setattr(cli.config, "REPO_NAMES", ["valkey", "valkey-rainfall"])
    monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey", "valkey-rainfall"])


def _valid_source():
    return config.REPO_NAMES[0]


def _task(**overrides):
    base = dict(
        source=_valid_source(),
        specifier="unstable",
        make_args="",
        topology=TopologySpec.standalone(),
        note="test",
        requirements={},
        test="get",
        val_size=512,
        pipelining=16,
        connections=400,
        threads=16,
        warmup=5,
        duration=30,
        repetitions=3,
    )
    base.update(overrides)
    return CachecannonTaskData(**base)


# --------------------------------------------------------------------------- task data


def test_defaults_are_closed_loop_and_unprofiled():
    task = _task()
    assert task.rate_limit == 0
    assert task.perf_stat_enabled is False
    assert task.info_sections == ""


def test_negative_rate_rejected():
    with pytest.raises(ValueError, match="rate_limit"):
        _task(rate_limit=-1)


def test_rate_shows_in_description_and_runner_title():
    task = _task(rate_limit=1_800_000)
    assert "1800000 req/s" in task.short_description()
    runner = task.prepare_task_runner([])
    assert "1800000 req/s" in runner.title
    assert runner.rate_limit == 1_800_000


def test_info_sections_normalized_and_passed_to_runner():
    task = _task(info_sections=" Stats, io_uring ,stats")
    assert task.info_sections == "stats,io_uring"
    runner = task.prepare_task_runner([])
    assert runner.info_sections == ["stats", "io_uring"]


def test_info_sections_reject_shell_metacharacters():
    with pytest.raises(ValueError, match="invalid INFO section"):
        _task(info_sections="stats;rm -rf /")


def test_perf_stat_flag_reaches_runner():
    runner = _task(perf_stat_enabled=True).prepare_task_runner([])
    assert runner.perf_stat_enabled is True


def test_task_round_trips_through_serialization():
    """New fields survive the queue's dict round trip (remote envelopes)."""
    from conductress.task_envelope import serialize_task
    from conductress.task_queue import BaseTaskData

    task = _task(rate_limit=250_000, perf_stat_enabled=True, info_sections="stats,io_uring")
    restored = BaseTaskData.from_dict(serialize_task(task))
    assert isinstance(restored, CachecannonTaskData)
    assert restored.rate_limit == 250_000
    assert restored.perf_stat_enabled is True
    assert restored.info_sections == "stats,io_uring"


# --------------------------------------------------------------------------- window detection


def test_scored_sample_line_detection():
    sample = '{"type":"sample","ts":"2026-09-17T23:50:52Z","req_s":2252715,"p50_us":2834}'
    assert is_scored_sample_line(sample)
    assert is_scored_sample_line("  " + sample + "\n")
    assert not is_scored_sample_line('{"type":"config","warmup_secs":5}')
    assert not is_scored_sample_line('{"type":"result","throughput":100}')
    assert not is_scored_sample_line("[precheck ok 0ms]")
    assert not is_scored_sample_line('not json but mentions "sample"')


# --------------------------------------------------------------------------- INFO helpers


def test_parse_info_sections_variants():
    assert parse_info_sections("") == []
    assert parse_info_sections("stats") == ["stats"]
    assert parse_info_sections("stats,,IO_URING, stats") == ["stats", "io_uring"]


def test_info_window_deltas_numeric_only_keeps_idle_counters():
    t0 = {"stats": {"total_commands_processed": "100", "eventloop_cycles": "10", "role": "master", "x": "1.5"}}
    t1 = {"stats": {"total_commands_processed": "1100", "eventloop_cycles": "10", "role": "master", "x": "2.0"}}
    out = info_window_deltas(t0, t1, 15.0004)
    assert out["window_secs"] == 15.0
    sec = out["sections"]["stats"]
    assert sec["total_commands_processed"] == 1000
    assert sec["eventloop_cycles"] == 0  # present and idle, not dropped
    assert "role" not in sec  # non-numeric configuration is dropped
    assert sec["x"] == 0.5


def test_info_window_deltas_ignores_fields_missing_at_t0():
    t0 = {"stats": {}}
    t1 = {"stats": {"new_counter": "5"}}
    assert info_window_deltas(t0, t1, 1.0)["sections"]["stats"] == {}


def test_snapshot_info_records_empty_section_on_failure():
    server = MagicMock()

    async def fake_info(section):
        if section == "io_uring":
            raise RuntimeError("no such section")
        return {"k": "1"}

    server.info = AsyncMock(side_effect=fake_info)
    out = asyncio.run(snapshot_info(server, ["stats", "io_uring"]))
    assert out == {"stats": {"k": "1"}, "io_uring": {}}


# --------------------------------------------------------------------------- CLI


def _fake_client():
    client = MagicMock()
    client.fleet.return_value = {
        "schema_version": 1,
        "runners": [
            {
                "runner_id": "armbench",
                "display_name": "Graviton 3",
                "platform": "arm64/c7g.metal/graviton3",
                "platform_aliases": ["graviton3", "arm64"],
                "enabled": True,
                "status_ttl_seconds": 900,
                "status": None,
                "status_updated_at": None,
                "task_counts": {"queued": 0},
            }
        ],
    }
    client.submitted = []

    def submit_task(envelope, idempotency_key):
        client.submitted.append(envelope)
        return {
            "schema_version": 1,
            "task": {"task_id": envelope["task_id"], "runner_id": envelope["runner_id"], "state": "queued"},
            "created": True,
        }

    client.submit_task.side_effect = submit_task
    return client


def test_cli_flags_land_in_remote_envelope(capsys):
    client = _fake_client()
    with patch("conductress.fleet_client.FleetClient.from_user_env", return_value=client):
        result = cli.main(
            [
                "queue",
                "add-cachecannon",
                "--runner",
                "armbench",
                "--rate",
                "1800000",
                "--perf-stat",
                "--info-sections",
                "stats,io_uring",
                "--json",
            ]
        )
    assert result == 0
    assert len(client.submitted) == 1
    payload = client.submitted[0]["task"]
    assert payload["rate_limit"] == 1800000
    assert payload["perf_stat_enabled"] is True
    assert payload["info_sections"] == "stats,io_uring"
    assert json.loads(capsys.readouterr().out)["data"]["destination"] == "remote"


def test_cli_defaults_unchanged_without_new_flags():
    client = _fake_client()
    with patch("conductress.fleet_client.FleetClient.from_user_env", return_value=client):
        assert cli.main(["queue", "add-cachecannon", "--runner", "armbench", "--json"]) == 0
    payload = client.submitted[0]["task"]
    assert payload["rate_limit"] == 0
    assert payload["perf_stat_enabled"] is False
    assert payload["info_sections"] == ""


def test_cli_rejects_negative_rate(capsys):
    client = _fake_client()
    with patch("conductress.fleet_client.FleetClient.from_user_env", return_value=client):
        assert cli.main(["queue", "add-cachecannon", "--runner", "armbench", "--rate", "-5"]) == 1
    assert "--rate" in capsys.readouterr().err
    assert not client.submitted


def test_cli_rejects_bad_info_section(capsys):
    client = _fake_client()
    with patch("conductress.fleet_client.FleetClient.from_user_env", return_value=client):
        assert cli.main(["queue", "add-cachecannon", "--runner", "armbench", "--info-sections", "a b"]) == 1
    assert "--info-sections" in capsys.readouterr().err
    assert not client.submitted


# --------------------------------------------------------------------------- result record


def _run_record_result(runner, perf_counters):
    """Drive _record_result with a fake server and file protocol; return the results row."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    server = MagicMock()
    server.run_host_command = AsyncMock(return_value=("lscpu-output", ""))
    server.server_cpus = "2-5"
    written = {}
    runner.file_protocol = MagicMock()
    runner.file_protocol.write_results = lambda results: written.setdefault("results", results)
    asyncio.run(
        runner._record_result(
            server,
            per_run_rps=[1_990_000.0, 2_000_000.0],
            all_results=[{"throughput_rps": 1_990_000.0, "error_pct": 0.0}] * 2,
            toml_content="",
            perf_counters=perf_counters,
        )
    )
    return written["results"]


def test_result_row_carries_perf_normalisation_fields():
    """The sweep exporter divides summed counters by perf_duration_seconds * perf_rep_count."""
    runner = _task(perf_stat_enabled=True).prepare_task_runner([])
    runner._perf_stat_scope = "user+kernel"
    runner._perf_rep_count = 2
    runner._perf_windows_seconds = [29.0, 29.4]
    row = _run_record_result(runner, {"all": {"cycles": 20}, "main": {"cycles": 12}, "io": {"cycles": 8}})
    assert row.data["perf_counters"] == {"all": {"cycles": 20}, "main": {"cycles": 12}, "io": {"cycles": 8}}
    assert row.data["perf_counters_scope"] == "user+kernel"
    assert row.data["perf_duration_seconds"] == pytest.approx(29.2)
    assert row.data["perf_rep_count"] == 2


def test_result_row_falls_back_to_nominal_duration_without_measured_windows():
    runner = _task(perf_stat_enabled=True).prepare_task_runner([])
    row = _run_record_result(runner, {"all": {"cycles": 20}})
    assert row.data["perf_duration_seconds"] == 30.0
    assert row.data["perf_rep_count"] == 1
    assert "perf_counters_scope" not in row.data


def test_result_row_has_no_perf_fields_when_unprofiled():
    runner = _task().prepare_task_runner([])
    row = _run_record_result(runner, None)
    assert not any(k.startswith("perf_") for k in row.data)
