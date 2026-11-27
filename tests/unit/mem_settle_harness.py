"""A scripted server for the memory task's settle path, plus a runner that records what the task did.

Used by the settle tests to pin the Valkey and Redis behaviour (the commands sent, the order, the result
fields) and to check an engine without DEBUG HTSTATS. No engine is started.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

from conductress.tasks import task_mem_efficiency as tme
from conductress.tasks.task_mem_efficiency import MemTaskRunner

PROC_STATUS = "Name:\tengine\nVmRSS:\t{rss} kB\nRssAnon:\t{anon} kB\n"
HTSTATS_REHASHING = "Hash table 0 stats (main hash table):\nrehashing target 4096\n"
HTSTATS_DONE = "Hash table 0 stats (main hash table):\n table size: 4096\n"


class ScriptedServer:
    """Answers like a server whose fill leaves a rehash that one read pass completes.

    ``debug`` False makes DEBUG and OBJECT ENCODING answer an error, as an engine without them would.
    ``info_server`` is the INFO server section.
    """

    def __init__(self, debug=True, info_server=None, memory_stats=False, keys=1, rehash_forever=False):
        self.ip = "127.0.0.1"
        self.port = 6400
        self.valkey_pid = 4242
        self.debug = debug
        self.info_server = info_server or {"server_name": "valkey", "valkey_version": "9.0.0"}
        self.memory_stats = memory_stats
        self.keys = keys
        self.rehash_forever = rehash_forever
        self.touches = 0
        self.memory_reads = 0
        self.commands = []  # run_valkey_command, in order
        self.host_commands = []
        self.infos = []  # INFO sections asked, in order
        self.start_args = None

    def get_build_hash(self):
        return "0" * 40

    async def info(self, section):
        self.infos.append(section)
        if section == "server":
            return dict(self.info_server)
        self.memory_reads += 1
        used = 1_000_000 if self.memory_reads == 1 else (6_000_000 if self.touches == 0 else 5_900_000)
        return {"used_memory": str(used)}

    async def run_valkey_command(self, command):
        self.commands.append(command)
        upper = command.upper()
        if upper.startswith("DEBUG HTSTATS"):
            if not self.debug:
                return "ERR unknown command 'DEBUG'"
            return HTSTATS_DONE if self.touches and not self.rehash_forever else HTSTATS_REHASHING
        if upper.startswith("OBJECT ENCODING"):
            return "skiplist" if self.debug else "ERR unknown command 'OBJECT'"
        if upper == "MEMORY STATS":
            return "overhead.hashtable.main\n100\n" if self.memory_stats else "ERR unknown command"
        if upper.split()[0] in ("ZCARD", "SCARD", "HLEN"):
            return "5000"
        if upper.startswith("CONFIG GET"):
            return ""
        return None

    async def run_host_command(self, command, check=True):
        self.host_commands.append(command)
        if "/proc/" in command:
            return PROC_STATUS.format(rss=100 + 10 * self.touches, anon=90), ""
        return "", ""

    async def count_items_expires(self):
        return (self.keys, 0)

    async def stop(self):
        pass


def make_runner(test="zadd", settle=True, **kwargs):
    return MemTaskRunner(
        task_name="t",
        server_ip="127.0.0.1",
        source="valkey",
        specifier="unstable",
        test=test,
        val_sizes=[16],
        has_expire=False,
        make_args="",
        note="",
        key_size=0,
        user_data_bytes=24,
        settle=settle,
        item_count=5000,
        **kwargs,
    )


async def run_single(server, runner, monkeypatch):
    """Run test_single_size_overhead against ``server``; return its result."""

    async def fast_sleep(_):
        return None

    # The poll loops bind asyncio.sleep as a keyword default; replace it so the test does not wait.
    monkeypatch.setitem(tme.quiesce_until_stable.__kwdefaults__, "sleep_fn", fast_sleep)
    monkeypatch.setitem(tme.check_memory_stability.__kwdefaults__, "sleep_fn", fast_sleep)

    def fake_touch(host, port, workload):
        server.touches += 1
        return workload.item_count

    async def fake_with_path(*args, **kwargs):
        server.start_args = (args, kwargs)
        return server

    runner.cached_binary_path = Path("/mock/binary")
    with (
        patch.object(tme.Server, "with_path", AsyncMock(side_effect=fake_with_path)),
        patch.object(tme, "populate"),
        patch.object(tme, "touch", fake_touch),
        patch.object(tme, "conductress_revision", lambda: "rev0"),
    ):
        return await runner.test_single_size_overhead(16, 6400, asyncio.Semaphore(1))
