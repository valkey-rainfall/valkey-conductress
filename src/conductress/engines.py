"""Engine registry: how a memory task starts and checks each engine.

``valkey`` is the launch Conductress has always used (Valkey, and Redis built the same way). Its command line is
written in ``Server.start`` and is pinned by tests; this module only names it. Other engines get a generic launch
built here: pinned to CPUs with taskset, started in the background, output to the server log.

The flag names of the non-Valkey profiles come from each engine's public documentation. Confirm each one against
the ``--help`` output of the pinned binary before the first run; nothing here has been run against a real engine.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import config


@dataclass(frozen=True)
class EngineProfile:
    """How to start one engine and what its memory task may assume."""

    name: str
    # True: use the Valkey command line in Server.start (unchanged). False: use launch_command below.
    valkey_launch: bool = False
    # True: the engine answers DEBUG HTSTATS, so the settle can assert that no rehash is left.
    rehash_check: bool = True
    # Process names `ps` may show for the running server (the file name it was started from); None skips the
    # check in Server.stop (a runtime host). A binary uploaded by hand is stored under the name valkey-server.
    process_names: Optional[tuple[str, ...]] = ("valkey-server",)
    # Exact process names `pkill -x` may kill between tasks (never a pattern: -f would match command lines).
    kill_names: tuple[str, ...] = ()
    thread_flag: Optional[str] = None  # flag that sets the thread count, e.g. "--proactor_threads"
    base_flags: tuple[str, ...] = ()  # fixed flags; "{ip}" is replaced by the bind address
    dir_flag: Optional[str] = None  # flag that sets the working directory, if the engine has one

    def cpus_needed(self, threads: int) -> int:
        """CPUs to reserve for one server. Valkey adds two for bio and background save; others use the threads."""
        threads = max(threads, 1)
        return threads + 2 if self.valkey_launch else threads

    def launch_command(
        self,
        *,
        binary: Path,
        port: int,
        ip: str,
        threads: int,
        cpus: str,
        logfile: Path,
        instance_dir: Optional[Path] = None,
        extra_args: str = "",
    ) -> str:
        """The shell command that starts this engine in the background (not for the Valkey launch)."""
        if self.valkey_launch:
            raise ValueError(f"{self.name} uses the Valkey launch in Server.start")
        parts = ["nohup", "setsid", "taskset", "-c", cpus, str(binary), "--port", str(port)]
        if self.thread_flag:
            parts += [self.thread_flag, str(max(threads, 1))]
        parts += [flag.format(ip=ip) for flag in self.base_flags]
        if self.dir_flag and instance_dir is not None:
            parts += [self.dir_flag, str(instance_dir)]
        if extra_args:
            parts.append(extra_args)
        return " ".join(parts) + f" >> {logfile} 2>&1 < /dev/null &"


ENGINES: dict[str, EngineProfile] = {
    "valkey": EngineProfile(name="valkey", valkey_launch=True),
    "dragonfly": EngineProfile(
        name="dragonfly",
        rehash_check=False,
        process_names=("dragonfly", "valkey-server"),
        kill_names=("dragonfly",),
        thread_flag="--proactor_threads",
        base_flags=('--dbfilename ""',),
    ),
    "garnet": EngineProfile(
        name="garnet",
        rehash_check=False,
        process_names=None,  # a .NET host process: its name is the runtime's, not the engine's
        base_flags=("--bind {ip}",),
    ),
}

DEFAULT_ENGINE = "valkey"


def non_valkey_sources() -> tuple[str, ...]:
    """Sources a non-Valkey engine may be provisioned from (a binary that is not built from a git commit)."""
    return (config.MANUALLY_UPLOADED,)


def get_engine(name: str) -> EngineProfile:
    """The profile for an engine name; ValueError names the valid ones."""
    try:
        return ENGINES[name]
    except KeyError:
        raise ValueError(f"Unknown engine: {name!r}. Valid: {', '.join(sorted(ENGINES))}") from None
