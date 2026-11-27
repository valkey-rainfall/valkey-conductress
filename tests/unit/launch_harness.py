"""Run Server.start against a recording host, to pin the exact command lines it sends. No engine is started."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

from conductress import config
from conductress.server import Server

BINARY = Path("~/build_cache/valkey/" + "0" * 40 + "/0123456789abcdef/valkey-server")

# name -> (Server kwargs, start kwargs, feature flags that are on)
CASES = {
    "default": ({}, {"io_threads": 1}, set()),
    "three_threads_with_server_args": ({}, {"io_threads": 3, "server_args": "--enable-debug-command yes"}, set()),
    "env_prefix": ({}, {"io_threads": 1, "env_prefix": 'JE_MALLOC_CONF="prof:true"'}, set()),
    "instance_dir": ({"instance_dir": Path("/work/inst-1")}, {"io_threads": 2}, set()),
    "numa_bound": ({}, {"io_threads": 1}, {"BIND_NUMA_MEMORY"}),
    "cpu_override": ({}, {"io_threads": 2, "server_cpu_override": "4,5,6"}, set()),
}


async def start_and_record(case=None, *, server_kwargs=None, start_kwargs=None, features=(), binary=BINARY, **extra):
    """Start a Server with a recording host; return the host commands it issued, in order."""
    if case is not None:
        server_kwargs, start_kwargs, features = CASES[case]
    server = Server("127.0.0.1", 6400, **(server_kwargs or {}), **extra)
    commands = []

    async def allocate(needed):
        # Valkey keeps the six CPUs its pinned command lines were recorded with; others get what they asked for.
        return list(range(6)) if server.engine == "valkey" else list(range(needed))

    async def host(command, check=True):
        commands.append(command)
        return ("4242\n" if command.startswith("lsof") else ""), ""

    server._host.run_host_command = host  # the SshHost the Server wraps
    with (
        patch.object(Server, "_Server__pre_start", AsyncMock()),
        patch.object(Server, "remove_persistence_files", AsyncMock()),
        patch.object(Server, "_allocate_server_cpus", AsyncMock(side_effect=allocate)),
        patch.object(Server, "wait_until_ready", AsyncMock()),
        patch.object(config, "check_feature", lambda feature: feature.name in set(features)),
        patch.object(server._cpu_allocator, "get_net_interface_numa", return_value=1, create=True),
    ):
        await server.start(binary, **(start_kwargs or {}))
    return commands
