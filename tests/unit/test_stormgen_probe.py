"""Fixed-rate open-loop goodput probe (``conductress.stormgen.probe``).

The probe sends ``first_command`` at a fixed aggregate rate against a fake
server, measuring served commands/s and per-request latency at 1 s. These
tests assert its open-loop pacing (sent ~= rate x seconds), that a stalled
server produces skips rather than a growing backlog, that percentiles come
from the recorded latencies, and that ``probe_recovery_s`` reads a synthetic
timeline correctly.
"""

import asyncio
import time

import pytest

from conductress.stormgen.metrics import probe_recovery_s
from conductress.stormgen.probe import ProbeConfig, ProbeSample, _bucketize, measure_connect_capacity, run_probe
from conductress.stormgen.resp import ReplyParser
from tests.unit.asyncio_server_support import close_server


class _ProbeFakeServer:
    """A minimal RESP server for the probe: answers HELLO and GET, can stall."""

    def __init__(self):
        self._server = None
        self.host = "127.0.0.1"
        self.port = 0
        self._gate = asyncio.Event()
        self._gate.set()

    async def start(self):
        self._server = await asyncio.start_server(self._handle, self.host, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self):
        await close_server(self._server, self._gate)

    def stall(self):
        self._gate.clear()

    def resume(self):
        self._gate.set()

    async def _handle(self, reader, writer):
        parser = ReplyParser()
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                parser.feed(chunk)
                await self._gate.wait()
                if b"HELLO" in chunk:
                    writer.write(b"%1\r\n$6\r\nserver\r\n$6\r\nvalkey\r\n")
                else:
                    writer.write(b"$3\r\nval\r\n")
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()


@pytest.mark.asyncio
async def test_probe_open_loop_sends_at_the_configured_rate():
    """The probe issues rate x seconds slots, whatever the load on the machine.

    The rate is low (a slot every 100 ms per client) so that a loaded test host still answers
    in time for most slots.
    """
    server = _ProbeFakeServer()
    await server.start()
    try:
        origin = time.monotonic()

        def clock():
            return time.monotonic() - origin

        config = ProbeConfig(
            host=server.host,
            port=server.port,
            clients=2,
            rate=20,  # 20/s aggregate: 10 slots per client in 1 s
            reply_timeout=1.0,
            connect_timeout=1.0,
        )
        timeline = await run_probe(config, clock, deadline=1.0)
        total_sent = sum(b["sent"] for b in timeline)
        total_skipped = sum(b["skipped"] for b in timeline)
        # The pacing is what this test checks: the number of slots the probe issued. Each client
        # has a slot every 100 ms (the second is offset by 50 ms), so 20 slots; allow one of drift.
        # A slot is either sent or skipped (the previous reply was late), so the sum does not
        # depend on how fast the machine answers. test_probe_counts_skips_when_the_server_stalls
        # covers skips; here only most slots must be served by a server that keeps up.
        assert 19 <= total_sent + total_skipped <= 21
        assert sum(b["served"] for b in timeline) >= (total_sent + total_skipped) // 2
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_probe_counts_skips_when_the_server_stalls():
    """A stalled server keeps one request outstanding, so later slots are skipped."""
    server = _ProbeFakeServer()
    await server.start()
    try:
        origin = time.monotonic()

        def clock():
            return time.monotonic() - origin

        async def stall_window():
            await asyncio.sleep(0.3)
            server.stall()
            await asyncio.sleep(0.6)
            server.resume()

        config = ProbeConfig(
            host=server.host,
            port=server.port,
            clients=1,
            rate=100,
            reply_timeout=2.0,  # long, so the stalled request stays outstanding
            connect_timeout=1.0,
        )
        stall_task = asyncio.ensure_future(stall_window())
        timeline = await run_probe(config, clock, deadline=1.2)
        await stall_task
        # While the one client's request is outstanding (server stalled), every
        # scheduled slot is skipped rather than queued.
        assert sum(b["skipped"] for b in timeline) > 0
    finally:
        await server.stop()


def test_bucketize_percentiles_from_recorded_latencies():
    """p50/p99/max come from the served samples' latencies, per 1 s bucket."""
    # Ten served requests in bucket 0 with latencies 1..10 ms; one skip.
    samples = [
        ProbeSample(t_send=0.0 + i * 0.05, latency_ms=float(i + 1), served=True, skipped=False) for i in range(10)
    ]
    samples.append(ProbeSample(t_send=0.5, latency_ms=None, served=False, skipped=True))
    buckets = _bucketize(samples, bucket_ms=1000)
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket["sent"] == 11
    assert bucket["served"] == 10
    assert bucket["skipped"] == 1
    assert bucket["max_ms"] == 10.0
    assert 5.0 <= bucket["p50_ms"] <= 6.0
    assert bucket["p99_ms"] >= 9.0


def test_probe_recovery_s_on_a_synthetic_timeline():
    """Recovery is the first second, after the stall end, where served holds for 3s."""
    timeline = []
    for i in range(3):  # pre-storm baseline (t < stall)
        timeline.append({"t_ms": i * 1000, "served": 1000})
    # stall from t=3s to t=5s: served collapses.
    timeline.append({"t_ms": 3000, "served": 50})
    timeline.append({"t_ms": 4000, "served": 20})
    # recovery from t=5s: three consecutive buckets within 10% of 1000.
    timeline.append({"t_ms": 5000, "served": 950})
    timeline.append({"t_ms": 6000, "served": 980})
    timeline.append({"t_ms": 7000, "served": 1000})
    recovery = probe_recovery_s(timeline, stall_end=5.0, storm_origin=0.0)
    # The first bucket at/after 5s that begins a 3-in-a-row streak is t=5s.
    assert recovery == pytest.approx(0.0, abs=0.01)


def test_probe_recovery_s_none_when_never_recovers():
    timeline = [{"t_ms": i * 1000, "served": 1000} for i in range(3)]
    timeline += [{"t_ms": (3 + i) * 1000, "served": 10} for i in range(5)]  # never recovers
    assert probe_recovery_s(timeline, stall_end=3.0, storm_origin=0.0) is None


def test_probe_recovery_s_none_on_empty_timeline():
    assert probe_recovery_s([], stall_end=3.0) is None


@pytest.mark.asyncio
async def test_measure_connect_capacity_reports_a_positive_rate():
    """The startup capacity check returns a positive connects/s against a live server."""
    server = _ProbeFakeServer()
    await server.start()
    try:
        config = ProbeConfig(
            host=server.host,
            port=server.port,
            clients=1,
            rate=1,
            reply_timeout=1.0,
            connect_timeout=1.0,
        )
        rate = await measure_connect_capacity(config, connects=50)
        assert rate > 0.0
    finally:
        await server.stop()
