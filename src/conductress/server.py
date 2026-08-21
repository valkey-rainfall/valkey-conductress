"""Represents a server running a Valkey instance."""

import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import asyncssh

from conductress.binary_manager import BinaryManager
from conductress.cpu_allocator import AllocationTag, CpuAllocator
from conductress.profiling_manager import ProfilingManager
from conductress.ssh_host import SshHost
from conductress.stabilization_manager import StabilizationManager
from conductress.utility import async_run, parse_cpulist

from . import config

VALKEY_BINARY = "valkey-server"


class Server:
    """Represents a server running a Valkey instance."""

    # Class-level CPU allocator shared across all Server instances
    _cpu_allocator = CpuAllocator()

    # These are remote paths - they exist on the valkey server
    path_root = Path("~")
    server_logfile = path_root / "valkey-server.log"

    # =============================================================================
    # INITIALIZATION AND FACTORY METHODS
    # =============================================================================

    def __init__(self, ip: str, port: int = 6379, username="") -> None:
        self._host = SshHost(ip, username)
        self._binary = BinaryManager(self._host)
        self._profiling = ProfilingManager(self._host)
        self._stabilization = StabilizationManager(self._host)
        self.ip = ip
        self.port = port
        self.username = username

        self.logger = logging.getLogger(self.__class__.__name__ + "." + ip)

        self.args: Optional[list[str]] = None
        self.threads: int = 1

        self.valkey_pid: int = -1  # -1 indicates server is not running
        self.server_cpus: list[int] = []  # CPUs allocated to this server
        self._allocation_tag: Optional[AllocationTag] = None  # Tag for CPU allocation

    # Properties delegating to BinaryManager (backward compat for code that reads these)
    @property
    def source(self) -> Optional[str]:
        return self._binary.source

    @source.setter
    def source(self, value: Optional[str]) -> None:
        self._binary.source = value

    @property
    def specifier(self) -> Optional[str]:
        return self._binary.specifier

    @specifier.setter
    def specifier(self, value: Optional[str]) -> None:
        self._binary.specifier = value

    @property
    def hash(self) -> Optional[str]:
        return self._binary.hash

    @hash.setter
    def hash(self, value: Optional[str]) -> None:
        self._binary.hash = value

    @property
    def make_args(self) -> str:
        return self._binary.make_args

    @make_args.setter
    def make_args(self, value: str) -> None:
        self._binary.make_args = value

    @classmethod
    async def with_build(
        cls,
        ip: str,
        port: int,
        username: str,
        binary_source: str,
        specifier: str,
        io_threads: int,
        make_args: str,
        server_cpu_override: str = "",
        server_args: str = "",
    ) -> "Server":
        """Create a server instance and ensure it is running with the specified build."""
        server = cls(ip, port, username)

        cached_binary_path: Path = await server.ensure_binary_cached(binary_source, specifier, make_args)
        await server.start(
            cached_binary_path, io_threads, server_cpu_override=server_cpu_override, server_args=server_args
        )
        return server

    @classmethod
    async def with_path(cls, ip: str, port: int, binary_path: Path, io_threads: int, env_prefix: str = ""):
        """Create a server instance running the specified binary"""
        server = cls(ip, port)
        await server.start(binary_path, io_threads, env_prefix=env_prefix)
        return server

    # =============================================================================
    # CPU AND IRQ PINNING METHODS
    # =============================================================================

    async def ensure_host_cpu_allocation(self):
        """Main orchestrator method for network IRQ pinning"""
        net_interface = await self._detect_interface_for_ip()

        await self._register_host_cpus(net_interface)

        # Skip IRQ pinning for loopback, but do everything else
        if net_interface != "lo":
            await self._disable_irqbalance()
            await self._pin_network_irqs(net_interface)
        else:
            logging.info("Connected via loopback - no network IRQs to pin")

    async def _detect_interface_for_ip(self) -> str:
        """Find network interface for the server IP"""
        if self.ip in ("localhost", "127.0.0.1", "::1"):
            return "lo"
        stdout, _ = await self.run_host_command(f"ip -4 -o addr show | grep {self.ip}")
        return stdout.strip().split()[1]

    async def _register_host_cpus(self, net_interface: str):
        """Get NUMA node for the interface and register host with allocator"""
        # Skip if already registered
        if self._cpu_allocator.is_host_registered(self.ip):
            return

        stdout, _ = await self.run_host_command("lscpu -p=node,cpu | grep -v '^#'")
        lines = stdout.strip().split("\n")

        # Parse all CPUs and build NUMA topology
        all_cpus = []
        numa_topology = defaultdict(list)
        for line in lines:
            node, cpu = line.split(",")
            cpu_id = int(cpu)
            numa_node = int(node)
            all_cpus.append(cpu_id)
            numa_topology[numa_node].append(cpu_id)

        # Detect L3 cache topology
        l3_cache_topology = await self._detect_l3_cache_topology(all_cpus)

        if net_interface == "lo":
            net_interface_numa = 0  # use NUMA node 0 for cache locality
        else:
            # Get the numa node matching interface
            numa_stdout, _ = await self.run_host_command(f'cat "/sys/class/net/{net_interface}/device/numa_node"')
            net_interface_numa = int(numa_stdout.strip())

        # Register host with allocator
        self._cpu_allocator.register_host(
            self.ip,
            all_cpus=all_cpus,
            numa_topology=dict(numa_topology),
            l3_cache_topology=l3_cache_topology,
            net_interface_numa=net_interface_numa,
        )

    async def _detect_l3_cache_topology(self, all_cpus: list[int]) -> dict[int, list[int]]:
        """Detect L3 cache topology by parsing sysfs.

        Returns mapping of L3 cache ID -> list of CPUs sharing that cache.
        """
        l3_topology = defaultdict(list)

        for cpu in all_cpus:
            # Read L3 cache ID for this CPU
            cache_path = f"/sys/devices/system/cpu/cpu{cpu}/cache/index3/id"
            result, _ = await self.run_host_command(f"cat {cache_path} 2>/dev/null || echo -1", check=False)

            try:
                l3_id = int(result.strip())
                if l3_id >= 0:
                    l3_topology[l3_id].append(cpu)
            except ValueError:
                # L3 cache info not available, skip
                pass

        return dict(l3_topology) if l3_topology else {}

    async def _disable_irqbalance(self):
        """Disable irqbalance to allow manual IRQ pinning"""
        await self.run_host_command("sudo systemctl stop irqbalance")
        await self.run_host_command("sudo systemctl disable irqbalance")
        await self.run_host_command("sudo pkill irqbalance", check=False)

    async def _pin_network_irqs(self, net_interface: str):
        """Find and pin network IRQs to specific CPUs"""
        # Skip for loopback interface - no IRQs to pin
        if net_interface == "lo":
            return

        # Skip if IRQs already configured on this host
        irq_tag = AllocationTag(task_id="system", purpose="irq")
        if self._cpu_allocator.get_allocation(self.ip, irq_tag) is not None:
            logging.info(
                "Network IRQs already configured on %s, using existing CPUs %s",
                self.ip,
                self._cpu_allocator.get_allocation(self.ip, irq_tag),
            )
            return

        # Get IRQ interrupts for interface
        stdout, _ = await self.run_host_command(f"grep -i {net_interface} /proc/interrupts")
        net_irqs = [int(irq.split(":")[0].strip()) for irq in stdout.strip().split("\n")]

        # Allocate CPUs for IRQs on the network interface NUMA node
        # Try to avoid server cache if server already allocated
        net_numa = self._cpu_allocator.get_net_interface_numa(self.ip)
        server_tags = [
            tag for tag in self._cpu_allocator.get_all_allocations(self.ip).keys() if tag.purpose == "server"
        ]

        irq_cpus = self._cpu_allocator.allocate(
            self.ip,
            irq_tag,
            count=len(net_irqs),
            require_numa=net_numa,
            avoid_tags=server_tags,
            prefer_different_cache=True,
        )

        # Get total CPU count for mask calculation
        stdout, _ = await self.run_host_command("lscpu -p=node,cpu | grep -v '^#'")
        total_cpus = len(stdout.strip().split("\n"))

        # Pin IRQs to allocated CPUs
        for i, irq in enumerate(net_irqs):
            irq_cpu = irq_cpus[-(i + 1)]  # Use last CPUs from allocation

            # Create affinity mask with just this CPU enabled
            digits = (total_cpus + 3) // 4
            mask_int = 1 << irq_cpu
            mask = f"{mask_int:X}".zfill(digits)
            if digits > 8:
                # Insert commas between every 8 hex digits from the right
                mask = mask[::-1]  # Reverse for right-to-left processing
                mask = ",".join(mask[i : i + 8] for i in range(0, len(mask), 8))
                mask = mask[::-1]  # Reverse back to normal order

            # Set the IRQ affinity using the mask
            await self.run_host_command(f"sudo sh -c 'echo {mask} > /proc/irq/{irq}/smp_affinity'")

        logging.info(
            "Pinned net IRQs %s for %s to CPUs %s (NUMA node %d)",
            net_irqs,
            net_interface,
            irq_cpus,
            net_numa,
        )

    async def _allocate_server_cpus(self, cpu_count: int) -> list[int]:
        """Allocate CPUs for this server on the network interface NUMA node"""
        # Validate before attempting allocation
        self._validate_sufficient_cpus()

        # Create allocation tag for this server
        self._allocation_tag = AllocationTag(task_id=f"server_{self.ip}_{self.port}", purpose="server")

        # Allocate on network interface NUMA node
        net_numa = self._cpu_allocator.get_net_interface_numa(self.ip)
        cpus = self._cpu_allocator.allocate(
            self.ip,
            self._allocation_tag,
            count=cpu_count,
            require_numa=net_numa,
        )

        logging.info(
            "Allocated CPUs %s for server on %s:%d (NUMA node %d)",
            cpus,
            self.ip,
            self.port,
            net_numa,
        )

        return cpus

    async def _pin_valkey_threads(self):
        """Pin main thread and I/O threads to individual CPUs after server starts"""
        pid_out, _ = await self.run_host_command(f"lsof -ti :{self.port}")
        main_pid = pid_out.strip()

        threads_out, _ = await self.run_host_command(f"ps -T -p {main_pid} -o tid,comm --no-headers")

        # Pin main thread to first CPU
        main_cpu = self.server_cpus[0]
        await self.run_host_command(f"taskset -cp {main_cpu} {main_pid}")
        logging.info("Pinned main thread %s to CPU %d", main_pid, main_cpu)

        # Pin I/O threads to individual CPUs
        io_thread_cpu_index = 1
        for line in threads_out.strip().split("\n"):
            parts = line.strip().split()
            if len(parts) >= 2:
                tid, comm = parts[0], parts[1]

                if tid == main_pid:
                    continue

                if comm.startswith("io_thd_"):
                    if io_thread_cpu_index >= len(self.server_cpus):
                        logging.warning("More IO threads than allocated CPUs — skipping pin for %s", comm)
                        break
                    cpu = self.server_cpus[io_thread_cpu_index]
                    await self.run_host_command(f"taskset -cp {cpu} {tid}")
                    logging.info("Pinned I/O thread %s (%s) to CPU %d", tid, comm, cpu)
                    io_thread_cpu_index += 1

        # Brief delay to ensure scheduler applies affinity changes
        await asyncio.sleep(config.THREAD_PIN_SETTLE_DELAY)

    def _release_server_cpus(self):
        """Release CPUs allocated to this server"""
        if self._allocation_tag and self.server_cpus:
            self._cpu_allocator.release(self.ip, self._allocation_tag)
            logging.info(
                "Released CPUs %s for server on %s:%d",
                self.server_cpus,
                self.ip,
                self.port,
            )
            self.server_cpus = []
            self._allocation_tag = None

    async def get_available_cpu_count(self) -> int:
        """Get the number of CPUs available on this host."""
        # Ensure host is registered with allocator
        await self.ensure_host_cpu_allocation()

        net_numa = self._cpu_allocator.get_net_interface_numa(self.ip)
        return self._cpu_allocator.get_available_count(self.ip, prefer_numa=net_numa)

    def _validate_sufficient_cpus(self) -> None:
        """Validate sufficient CPUs available on network interface NUMA node.

        Raises:
            RuntimeError: If insufficient CPUs available for server needs
        """
        net_numa = self._cpu_allocator.get_net_interface_numa(self.ip)
        available = self._cpu_allocator.get_available_count(self.ip, prefer_numa=net_numa)
        needed = Server.get_num_cpus(self.threads)

        if available < needed:
            raise RuntimeError(
                f"Insufficient CPUs on {self.ip} NUMA node {net_numa}: " f"need {needed}, available {available}"
            )

    # =============================================================================
    # PROFILING METHODS (delegated to ProfilingManager)
    # =============================================================================

    def cpu_profile_start(self, duration: int) -> None:
        """Start CPU profile (perf record) for the given duration."""
        self._profiling.target_pid = self.valkey_pid
        self._profiling.cpu_profile_start(duration)

    async def cpu_profile_collect(self) -> tuple[list[list], list[list]]:
        """Collect CPU profile stacks. Returns (main_stacks, io_stacks)."""
        return await self._profiling.cpu_profile_collect()

    # =============================================================================
    # PERF STAT METHODS (delegated to ProfilingManager)
    # =============================================================================

    async def perf_stat_start(self) -> None:
        """Start perf stat collection."""
        self._profiling.target_pid = self.valkey_pid
        await self._profiling.perf_stat_start()

    async def perf_stat_stop(self) -> None:
        """Signal perf stat to stop."""
        await self._profiling.perf_stat_stop()

    def perf_stat_wait(self) -> None:
        """Block until perf stat completes."""
        self._profiling.perf_stat_wait()

    async def perf_stat_report(self, result_dir: Path) -> dict:
        """Copy perf stat results and return parsed counters."""
        return await self._profiling.perf_stat_report(result_dir)

    @staticmethod
    def parse_perf_stat(path: Path) -> dict:
        """Parse perf stat output file into a dict of event_name -> count."""
        return ProfilingManager.parse_perf_stat(path)

    # =============================================================================
    # CPU CONSISTENCY TUNING METHODS (delegated to StabilizationManager)
    # =============================================================================

    async def enable_cpu_consistency_mode(self) -> None:
        """Configure CPU settings for consistent benchmarks."""
        await self._stabilization.enable()
        self._platform_info = self._stabilization.platform_info

    async def disable_cpu_consistency_mode(self) -> None:
        """Restore default CPU settings."""
        await self._stabilization.disable()

    async def verify_cpu_consistency_mode(self) -> bool:
        """Verify stabilization settings are applied. Retry once on failure."""
        return await self._stabilization.verify()

    # =============================================================================
    # SERVER LIFECYCLE METHODS
    # =============================================================================

    async def __pre_start(self) -> None:
        """Configuration and preparation before starting Valkey server"""
        await self.ensure_host_cpu_allocation()

        if config.check_feature(config.Features.ENABLE_CPU_CONSISTENCY_MODE):
            await self.enable_cpu_consistency_mode()
            await self.verify_cpu_consistency_mode()
        else:
            await self.disable_cpu_consistency_mode()

        # Enable memory overcommit
        await self.run_host_command("sudo sh -c 'echo 1 > /proc/sys/vm/overcommit_memory'")

        await self.stop()
        await asyncio.sleep(1)  # short delay to it doesn't get our new server (TODO verify this)

    @staticmethod
    def get_num_cpus(io_threads: int) -> int:
        """Get number of CPUs allocated for server with specified io-threads parameter"""
        return io_threads + 2  # (io-threads + extra for bio threads, aof rewrite, and bgsave)

    async def start(
        self,
        cached_binary_path: Path,
        io_threads: int,
        env_prefix: str = "",
        server_cpu_override: str = "",
        server_args: str = "",
    ) -> None:
        """Ensure specified build is running on the server.

        Args:
            cached_binary_path: Path to the built valkey-server binary.
            io_threads: Number of I/O threads to configure.
            env_prefix: Optional environment variable prefix for the command
                        (e.g. 'JE_MALLOC_CONF="prof:true"' for jemalloc profiling).
            server_cpu_override: Expert cpulist override for server pinning, bypasses
                                 topology-aware allocation when non-empty.
            server_args: Extra raw arguments appended to the server command line
                         (e.g. '--io-threads-ownership yes'). Appended last, so they
                         override any conductress-generated defaults. Passed through
                         verbatim -- the caller is trusted to provide coherent flags.
        """
        if io_threads < 1:
            io_threads = 1
        self.threads = io_threads
        await self.__pre_start()

        # Allocate CPUs for this server (or use explicit override)
        if server_cpu_override:
            self.server_cpus = parse_cpulist(server_cpu_override)
            logging.info(
                "Using explicit CPU override %s for server on %s:%d",
                self.server_cpus,
                self.ip,
                self.port,
            )
        else:
            needed_cpus = Server.get_num_cpus(self.threads)
            self.server_cpus = await self._allocate_server_cpus(needed_cpus)

        self.args = []
        if self.threads > 1:
            self.args += ["--io-threads", str(self.threads)]

        # Configure CPU pinning using Valkey's built-in options
        # Main thread + I/O threads: CPUs 0 through N
        main_io_cpu_list = ",".join(map(str, self.server_cpus[: self.threads + 1]))
        self.args += ["--server-cpulist", main_io_cpu_list]

        # Background threads: remaining CPUs (bio, AOF rewrite, bgsave)
        bg_thread_start_cpu = self.threads
        if bg_thread_start_cpu >= len(self.server_cpus):
            raise RuntimeError("Not enough CPUs allocated for background threads")
        bg_cpu_list = ",".join(map(str, self.server_cpus[bg_thread_start_cpu:]))
        self.args += ["--bio-cpulist", bg_cpu_list]
        self.args += ["--aof-rewrite-cpulist", bg_cpu_list]
        self.args += ["--bgsave-cpulist", bg_cpu_list]

        # User-supplied extra args go last so they override generated defaults
        # (valkey applies later command-line config over earlier). Raw passthrough.
        if server_args:
            self.args.append(server_args)

        command = (
            f"{cached_binary_path} --port {self.port} "
            f'--save "" --protected-mode no --daemonize yes '
            f"--logfile {Server.server_logfile} " + " ".join(self.args)
        )

        # Optionally bind memory to NUMA node for consistent performance
        if config.check_feature(config.Features.BIND_NUMA_MEMORY):
            numa_node = self._cpu_allocator.get_net_interface_numa(self.ip)
            command = f"numactl --membind={numa_node} {command}"
            logging.info("Binding memory to NUMA node %d", numa_node)

        # Make the cache dir visible to the dynamic loader: module-era valkey
        # builds dlopen libvalkeylua.so, which is cached next to the binary.
        # (Their DT_RPATH points into the shared source tree, but the build
        # step removes the tree copy after caching, so the RPATH lookup misses
        # and this takes effect.) Placed OUTSIDE the numactl wrap -- numactl
        # does not accept env assignments as its command.
        command = f"LD_LIBRARY_PATH={cached_binary_path.parent} {command}"

        # Prepend environment variables if specified (e.g. jemalloc profiling)
        if env_prefix:
            command = f"{env_prefix} {command}"

        out, err = await self.run_host_command(command)

        pid_out, _ = await self.run_host_command(f"lsof -ti :{self.port}")
        self.valkey_pid = int(pid_out.strip())

        await self.wait_until_ready()

        # Optionally pin individual threads to specific CPUs for precise control
        if config.check_feature(config.Features.PIN_VALKEY_THREADS):
            await self._pin_valkey_threads()

    async def wait_until_ready(self) -> None:
        """Wait until the server is ready to accept commands."""
        for _ in range(config.SERVER_READY_MAX_RETRIES):
            try:
                out = await self.run_valkey_command("PING")
                if out == "PONG":
                    return
                self.logger.debug(out)
            except asyncssh.ProcessError:
                self.logger.warning("CLI error during wait_until_ready")
            time.sleep(config.SERVER_READY_RETRY_DELAY)

        raise RuntimeError("Server did not start successfully")

    async def kill_all_valkey_instances_on_host(self):
        """Stop all instances of valkey server, regardless of which port they're running on"""
        await self.run_host_command(f"pkill -f {VALKEY_BINARY}", check=False)

        # Clear server allocations for this host (keep IRQ and benchmark allocations)
        all_allocations = self._cpu_allocator.get_all_allocations(self.ip)
        for tag in list(all_allocations.keys()):
            if tag.purpose == "server":
                self._cpu_allocator.release(self.ip, tag)

    async def stop(self):
        # Release allocated CPUs
        if self.server_cpus:
            self._release_server_cpus()

        if self.valkey_pid == -1:
            return

        # Check if process is still alive (it may have already crashed)
        try:
            name, _ = await self.run_host_command(f"ps -p {self.valkey_pid}")
        except Exception:
            # Server already dead — log the crash
            crashed_pid = self.valkey_pid
            await self._log_server_crash()
            self.valkey_pid = -1
            raise RuntimeError(
                f"valkey-server (pid {crashed_pid}) crashed during benchmark. "
                "Check runner log for server crash output."
            )

        name = name.strip().split()[-1]
        if name != VALKEY_BINARY:
            raise RuntimeError(f"Process on port {self.port} is '{name}', expected '{VALKEY_BINARY}'")

        cli_path = str(config.PROJECT_ROOT / config.VALKEY_CLI)
        await self.run_host_command(
            f"{cli_path} -p {self.port} SHUTDOWN NOSAVE 2>/dev/null; "
            f"for i in 1 2 3 4 5; do kill -0 {self.valkey_pid} 2>/dev/null || break; sleep 1; done; "
            f"kill -9 {self.valkey_pid} 2>/dev/null || true",
            check=False,
        )
        self.valkey_pid = -1

        # clean up any rdb files from replication or snapshotting
        # valkey will automatically load "dump.rdb" if it is present in its working dir
        await self.run_host_command(
            f"rm -f {Server.path_root}/*.rdb {config.PROJECT_ROOT}/*.rdb",
            check=False,
        )
        # clean up any files created by profiling or other metric collection
        await self._profiling.cleanup()

    async def _log_server_crash(self) -> None:
        """Read and log the server crash dump from the logfile."""
        try:
            # Valkey crash dumps start with this signature
            crash_log, _ = await self.run_host_command(
                f"grep -n 'CRASHED BY SIGNAL\\|=== VALKEY BUG REPORT' {Server.server_logfile} "
                f"| tail -1 | cut -d: -f1",
                check=False,
            )
            crash_line = crash_log.strip()
            if crash_line:
                # Grab 10 lines before the crash marker through EOF
                start = max(1, int(crash_line) - 10)
                log_tail, _ = await self.run_host_command(f"sed -n '{start},$p' {Server.server_logfile}", check=False)
            else:
                # No crash signature found — grab last 100 lines as fallback
                log_tail, _ = await self.run_host_command(f"tail -100 {Server.server_logfile}", check=False)
            if log_tail.strip():
                self.logger.error("=== valkey-server crash log ===\n%s", log_tail.strip())
        except Exception:
            self.logger.error("Could not read server logfile after crash")

    # =============================================================================
    # VALKEY COMMAND EXECUTION METHODS
    # =============================================================================

    async def run_valkey_command(self, command: str) -> Optional[str]:
        """Run a valkey command on the server and return its output."""
        self.logger.info("Valkey cli command: %s", command)
        cli_command: str = f"{str(config.PROJECT_ROOT / config.VALKEY_CLI)} -h {self.ip} -p {self.port} " + command
        stdout, _ = await async_run(cli_command, check=False)
        return stdout.strip() if stdout else None

    async def run_valkey_command_over_keyspace(self, keyspace_size: int, command: str) -> None:
        """Run valkey-benchmark, sequentially covering the entire keyspace."""
        sequential_command: str = (
            f"{str(config.PROJECT_ROOT / config.VALKEY_BENCHMARK)} -h {self.ip} -p {self.port} -c 32 -P 20 "
            f"--threads 16 -q --sequential -r {keyspace_size} -n {keyspace_size} "
        )
        sequential_command += command
        self.logger.info("Benchmark Command: %s", sequential_command)
        await async_run(sequential_command)

    async def info(self, section: str) -> dict[str, str]:
        """Run the 'info' command on the server and return the specified section."""
        response = await self.run_valkey_command(f"info {section}")
        if not response:
            raise RuntimeError(f"Failed to run 'info {section}' on server {self.ip}")
        lines = response.strip().split("\n")
        keypairs: dict[str, str] = {}
        for item in lines:
            if ":" in item:
                (key, value) = item.split(":", 1)
                keypairs[key.strip()] = value.strip()
        return keypairs

    async def count_items_expires(self) -> tuple[int, int]:
        """Count total items and items with expiry in the keyspace."""
        info = await self.info("keyspace")
        counts: defaultdict[str, int] = defaultdict(int)
        for line in info.values():
            # 'keys=98331,expires=0,avg_ttl=0'
            for item in line.split(","):
                name, val = item.split("=")
                counts[name] += int(val)
        return (counts["keys"], counts["expires"])

    # =============================================================================
    # REPLICATION METHODS
    # =============================================================================

    async def is_primary(self) -> bool:
        """Check if the server is a replica."""
        info = await self.info("replication")
        return info.get("role") == "master"

    async def is_synced_replica(self) -> bool:
        """Check if the server is a replica and in sync with the primary."""
        info = await self.info("replication")
        return info.get("role") == "slave" and info.get("master_link_status") == "up"

    async def get_replicas(self) -> list[str]:
        """Get a list of replicas for the server."""
        info = await self.info("replication")
        count = int(info.get("connected_slaves", 0))
        replicas: list[str] = []
        for i in range(count):
            replica = info.get(f"slave{i}")
            if replica is None:
                raise RuntimeError(f"Replica {i} not found in replication info")
            ip = replica.split(",")[0].split("=")[1]
            replicas.append(ip)
        return replicas

    async def replicate(self, primary_ip: Optional[str], port="6379") -> None:
        """Set the server to replicate from the specified primary."""
        if primary_ip is None:
            primary_ip = "no"
            port = "one"
        response = await self.run_valkey_command(f"replicaof {primary_ip} {port}")
        if response != "OK":
            raise RuntimeError(f"Failed REPLICAOF {repr(primary_ip)} {repr(port)}: {repr(response)}")

    # =============================================================================
    # BINARY MANAGEMENT METHODS (delegated to BinaryManager)
    # =============================================================================

    async def ensure_binary_cached(
        self,
        source: Optional[str] = None,
        specifier: Optional[str] = None,
        make_args: Optional[str] = None,
    ) -> Path:
        """Ensure a binary is built and cached. Returns path to cached binary."""
        if self.valkey_pid != -1:
            raise RuntimeError("Cannot change binary while server is running")
        return await self._binary.ensure_binary_cached(source, specifier, make_args)

    def get_build_hash(self):
        """Get the commit hash of the current build."""
        return self._binary.get_build_hash()

    @staticmethod
    async def delete_entire_build_cache(server_ips) -> None:
        """Delete the entire build cache on all servers."""
        await BinaryManager.delete_entire_build_cache(server_ips)

    # =============================================================================
    # SSH AND FILE OPERATIONS (delegated to SshHost)
    # =============================================================================

    @property
    def ssh(self):
        """Access the underlying SSH connection (for asyncssh.scp compatibility)."""
        return self._host.ssh

    async def run_host_command(self, command: str, check: bool = True) -> tuple[str, str]:
        """Run a terminal command on the server and return (stdout, stderr)."""
        return await self._host.run_host_command(command, check)

    async def check_file_exists(self, path: Path) -> bool:
        """Check if a file exists on the server."""
        return await self._host.check_file_exists(path)

    async def get_remote_file(self, server_src: Path, local_dest: Path) -> None:
        """Copy a file from the server to the local machine."""
        await self._host.get_remote_file(server_src, local_dest)

    async def put_remote_file(self, local_src: Path, server_dest: Path) -> None:
        """Copy a file from the local machine to the server."""
        await self._host.put_remote_file(local_src, server_dest)
