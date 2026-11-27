"""The exact command lines Server.start sends: pinned for Valkey and Redis, checked for the other engines."""

import json
from pathlib import Path

import pytest

from conductress import engines
from conductress.engines import ENGINES, EngineProfile
from conductress.server import Server

from .launch_harness import BINARY, CASES, start_and_record

GOLDEN = json.loads((Path(__file__).parent / "golden" / "server_start_valkey.json").read_text())


@pytest.mark.asyncio
@pytest.mark.parametrize("case", sorted(CASES))
async def test_valkey_launch_command_is_pinned(case):
    """The Valkey command line (and the host commands around it) equals what the code sent before engines existed.

    The golden file was written by the code before this change. If this fails, the Valkey launch changed.
    """
    assert await start_and_record(case) == GOLDEN[case]


@pytest.mark.asyncio
async def test_the_pinned_valkey_command_text_is_literal():
    """One command spelled out, so a reader can see what is pinned without opening the golden file."""
    commands = await start_and_record("default")
    cache = "~/build_cache/valkey/" + "0" * 40 + "/0123456789abcdef"
    assert commands == [
        f'LD_LIBRARY_PATH={cache} {cache}/valkey-server --port 6400 --save "" --protected-mode no --daemonize yes '
        "--logfile ~/valkey-server-6400.log --server-cpulist 0,1 --bio-cpulist 1,2,3,4,5 "
        "--aof-rewrite-cpulist 1,2,3,4,5 --bgsave-cpulist 1,2,3,4,5",
        "lsof -ti :6400",
    ]


@pytest.mark.asyncio
async def test_redis_built_from_source_uses_the_same_launch():
    redis_binary = Path(str(BINARY).replace("/valkey/", "/redis/"))
    valkey = await start_and_record("default")
    redis = await start_and_record("default", binary=redis_binary)
    assert redis[0] == valkey[0].replace("/valkey/", "/redis/")
    assert redis[1:] == valkey[1:]


@pytest.mark.asyncio
async def test_a_valkey_launch_does_not_use_the_generic_launch():
    command = (await start_and_record("default"))[0]
    for word in ("nohup", "setsid", "taskset"):
        assert word not in command


# --- other engines (command strings only) ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_dragonfly_command_line():
    commands = await start_and_record(engine="dragonfly", start_kwargs={"io_threads": 1})
    assert commands == [
        "LD_LIBRARY_PATH=~/build_cache/valkey/0000000000000000000000000000000000000000/0123456789abcdef nohup setsid "
        "taskset -c 0 ~/build_cache/valkey/0000000000000000000000000000000000000000/0123456789abcdef/valkey-server "
        '--port 6400 --proactor_threads 1 --dbfilename "" >> ~/valkey-server-6400.log 2>&1 < /dev/null &',
        "lsof -ti :6400",
    ]


@pytest.mark.asyncio
async def test_garnet_command_line_has_no_thread_flag_and_binds_the_ip():
    commands = await start_and_record(engine="garnet", start_kwargs={"io_threads": 1})
    assert commands[0].endswith(
        "taskset -c 0 ~/build_cache/valkey/0000000000000000000000000000000000000000/0123456789abcdef/valkey-server "
        "--port 6400 --bind 127.0.0.1 >> ~/valkey-server-6400.log 2>&1 < /dev/null &"
    )
    assert "--proactor_threads" not in commands[0]


@pytest.mark.asyncio
async def test_a_fake_third_engine_command_line(monkeypatch):
    monkeypatch.setitem(
        ENGINES,
        "fakeengine",
        EngineProfile(
            name="fakeengine",
            rehash_check=False,
            thread_flag="--workers",
            base_flags=("--listen {ip}", "--no-disk"),
            dir_flag="--data",
        ),
    )
    commands = await start_and_record(
        engine="fakeengine",
        server_kwargs={"instance_dir": Path("/work/inst-9")},
        start_kwargs={"io_threads": 2, "server_args": "--extra 1"},
    )
    cache = "~/build_cache/valkey/0000000000000000000000000000000000000000/0123456789abcdef"
    assert commands == [
        "mkdir -p /work/inst-9",
        f"LD_LIBRARY_PATH={cache} nohup setsid taskset -c 0,1 {cache}/valkey-server --port 6400 --workers 2 "
        "--listen 127.0.0.1 --no-disk --data /work/inst-9 --extra 1 >> ~/valkey-server-6400.log 2>&1 < /dev/null &",
        "lsof -ti :6400",
    ]


@pytest.mark.asyncio
async def test_a_generic_engine_gets_env_prefix_and_numa_like_valkey():
    commands = await start_and_record(
        engine="dragonfly",
        start_kwargs={"io_threads": 1, "env_prefix": "FOO=1"},
        features={"BIND_NUMA_MEMORY"},
    )
    assert commands[0].startswith("FOO=1 LD_LIBRARY_PATH=")
    assert " numactl --membind=1 nohup setsid taskset -c 0 " in commands[0]


def test_cpus_needed():
    assert ENGINES["valkey"].cpus_needed(1) == Server.get_num_cpus(1) == 3
    assert ENGINES["valkey"].cpus_needed(3) == Server.get_num_cpus(3)
    assert ENGINES["dragonfly"].cpus_needed(1) == 1
    assert ENGINES["dragonfly"].cpus_needed(4) == 4


def test_the_valkey_profile_refuses_the_generic_launch():
    with pytest.raises(ValueError, match="uses the Valkey launch"):
        ENGINES["valkey"].launch_command(binary=Path("/b"), port=1, ip="x", threads=1, cpus="0", logfile=Path("/l"))


def test_unknown_engine_is_refused():
    with pytest.raises(ValueError, match="Unknown engine"):
        engines.get_engine("nope")
    with pytest.raises(ValueError, match="Unknown engine"):
        Server("127.0.0.1", 6400, engine="nope")


# --- stop and kill ----------------------------------------------------------------------------------


async def _stop_with_process_name(engine, name):
    from unittest.mock import AsyncMock, MagicMock

    server = Server.__new__(Server)
    server.port = 9000
    server.ip = "127.0.0.1"
    server.engine = engine
    server.valkey_pid = 5
    server.server_cpus = None
    server.instance_dir = None
    server._profiling = MagicMock()
    server._profiling.cleanup = AsyncMock()

    async def run(command, check=True):
        return (f"  PID TTY TIME CMD\n 5 ? 00:00:01 {name}\n", "") if "ps -p" in command else ("", "")

    server.run_host_command = run
    await server.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "engine,name,ok",
    [
        ("valkey", "valkey-server", True),
        ("valkey", "dragonfly", False),
        ("dragonfly", "dragonfly", True),
        ("dragonfly", "valkey-server", True),  # a binary uploaded by hand is stored under that name
        ("dragonfly", "bash", False),
        ("garnet", "dotnet", True),  # a runtime host: the name is not checked
    ],
)
async def test_stop_checks_the_process_name_of_the_engine(engine, name, ok):
    if ok:
        await _stop_with_process_name(engine, name)
    else:
        with pytest.raises(RuntimeError, match="expected"):
            await _stop_with_process_name(engine, name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "engine,expected",
    [
        ("valkey", ["pkill -f valkey-server"]),
        ("dragonfly", ["pkill -f valkey-server", "pkill -x dragonfly"]),
        ("garnet", ["pkill -f valkey-server"]),
    ],
)
async def test_kill_all_uses_exact_names_for_other_engines(engine, expected):
    server = Server("127.0.0.1", 6400, engine=engine)
    commands = []

    async def host(command, check=True):
        commands.append(command)
        return "", ""

    server._host.run_host_command = host
    await server.kill_all_valkey_instances_on_host()
    assert commands == expected
