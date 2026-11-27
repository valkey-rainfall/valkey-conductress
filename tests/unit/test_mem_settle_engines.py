"""The memory task's settle: Valkey and Redis stay as they were, an engine without DEBUG HTSTATS settles neutrally."""

import json
from pathlib import Path

import pytest

from conductress.memory_record import build_memory_record

from .mem_settle_harness import ScriptedServer, make_runner, run_single

GOLDEN = Path(__file__).parent / "golden"


def _doc(server, result):
    """The trace and result of one run, as the golden files store it."""
    doc = {
        "commands": server.commands,
        "infos": server.infos,
        "touches": server.touches,
        "start_kwargs": {k: str(v) for k, v in server.start_args[1].items()},
        "result": result,
    }
    return json.loads(json.dumps(doc, sort_keys=True, default=str))


@pytest.mark.asyncio
@pytest.mark.parametrize("test", ["zadd", "sadd", "hset", "set"])
async def test_valkey_settle_is_pinned_to_the_recorded_trace(test, monkeypatch):
    """Commands, their order, server args and every result field equal what main produced before this change.

    The golden files were written by the code before the neutral settle existed. If this test fails, the Valkey
    and Redis settle changed; do not regenerate the files to make it pass without a reason.
    """
    server = ScriptedServer(keys=5000 if test == "set" else 1)
    result = await run_single(server, make_runner(test=test), monkeypatch)
    expected = json.loads((GOLDEN / f"mem_settle_valkey_{test}.json").read_text())
    assert _doc(server, result) == expected


@pytest.mark.asyncio
async def test_valkey_task_without_settle_is_pinned_too(monkeypatch):
    server = ScriptedServer()
    result = await run_single(server, make_runner(settle=False), monkeypatch)
    expected = json.loads((GOLDEN / "mem_settle_valkey_zadd_nosettle.json").read_text())
    assert _doc(server, result) == expected


@pytest.mark.asyncio
async def test_valkey_settle_still_asserts_no_rehash(monkeypatch):
    """A table that stays mid-rehash after the read passes fails the task; the neutral path must not weaken this."""
    server = ScriptedServer(rehash_forever=True)
    with pytest.raises(RuntimeError, match="still rehashing"):
        await run_single(server, make_runner(), monkeypatch)


@pytest.mark.asyncio
async def test_valkey_style_settle_fails_on_an_engine_without_debug(monkeypatch):
    """Why a neutral settle exists: with the rehash check on, an engine that answers DEBUG with an error fails."""
    server = ScriptedServer(debug=False)
    with pytest.raises(RuntimeError, match="DEBUG HTSTATS failed"):
        await run_single(server, make_runner(), monkeypatch)


@pytest.mark.asyncio
async def test_neutral_settle_records_that_no_rehash_check_was_made(monkeypatch):
    server = ScriptedServer(debug=False)
    result = await run_single(server, make_runner(rehash_check=False), monkeypatch)

    assert not [c for c in server.commands if c.upper().startswith("DEBUG")]
    assert server.start_args[1]["server_args"] == ""  # no --enable-debug-command for an engine that lacks it
    assert server.touches == 1  # one full read pass, like the Valkey settle
    assert result["rehash_check"] is False
    assert result["rehash_passes"] == 1
    assert result["settle_key_check"] == "not_applicable"
    assert result["settle_stability"]["verdict"] == "stable"
    settle = result["memory_record"]["settle"]
    assert settle["rehash_check"] is False
    assert (settle["requested"], settle["settled"], settle["key_check"], settle["stability"]) == (
        True,
        True,
        "not_applicable",
        "stable",
    )
    assert result["memory_record"]["total_bytes_per_item"] == pytest.approx((5_900_000 - 1_000_000) / 5000)


@pytest.mark.asyncio
async def test_neutral_settle_without_settle_flag_is_a_plain_run(monkeypatch):
    server = ScriptedServer(debug=False)
    result = await run_single(server, make_runner(settle=False, rehash_check=False), monkeypatch)
    assert server.touches == 0
    assert result["rehash_check"] is False  # the engine's profile is recorded even when no settle was asked
    assert result["memory_record"]["settle"]["requested"] is False


def test_the_record_of_a_valkey_run_has_no_rehash_check_field():
    kwargs = dict(
        engine={},
        binary_sha256=None,
        version_text=None,
        profiling_requested=False,
        profiling_active_at_runtime=None,
        settle_requested=True,
        settle_passes=1,
        settle_key_check="hashtable",
        settle_stability="stable",
        encoding=None,
        config={},
        item_count=1,
        samples={},
        conductress_revision=None,
    )
    assert "rehash_check" not in build_memory_record(**kwargs)["settle"]
    assert build_memory_record(settle_rehash_check=False, **kwargs)["settle"]["rehash_check"] is False
