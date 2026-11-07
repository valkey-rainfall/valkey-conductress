"""Unit tests for Server CPU allocation integration with CpuAllocator."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from conductress.cpu_allocator import AllocationTag, CpuAllocator
from conductress.server import Server


class TestServerCpuAllocation:
    """Test Server's integration with CpuAllocator."""

    def setup_method(self):
        """Reset CPU allocator before each test."""
        Server._cpu_allocator = CpuAllocator()

    @pytest.mark.asyncio
    async def test_allocate_uses_network_interface_numa_node(self):
        """Test that Server allocates CPUs on the network interface NUMA node."""
        server = Server("192.168.1.1", 9000)

        # Register with network interface on NUMA 1
        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: [0, 1, 2, 3], 1: [4, 5, 6, 7]},
            net_interface_numa=1,
        )

        cpus = await server._allocate_server_cpus(2)

        # Should allocate from NUMA node 1 (network interface node)
        assert all(cpu in [4, 5, 6, 7] for cpu in cpus)

    @pytest.mark.asyncio
    async def test_allocate_creates_proper_tag(self):
        """Test that Server creates allocation tag with correct format."""
        server = Server("192.168.1.1", 9000)

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(4)),
            numa_topology={0: list(range(4))},
            net_interface_numa=0,
        )

        await server._allocate_server_cpus(2)

        # Check tag was created correctly
        assert server._allocation_tag is not None
        assert server._allocation_tag.task_id == "server_192.168.1.1_9000"
        assert server._allocation_tag.purpose == "server"

    @pytest.mark.asyncio
    async def test_release_clears_allocation_tag(self):
        """Test that releasing CPUs clears the allocation tag."""
        server = Server("192.168.1.1", 9000)

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(4)),
            numa_topology={0: list(range(4))},
            net_interface_numa=0,
        )

        cpus = await server._allocate_server_cpus(2)
        server.server_cpus = cpus

        assert server._allocation_tag is not None

        server._release_server_cpus()

        assert server._allocation_tag is None
        assert server.server_cpus == []

    @pytest.mark.asyncio
    async def test_start_appends_server_args_last(self):
        """server_args are appended verbatim after all generated args (so they override defaults)."""
        server = Server("192.168.1.1", 9000)
        server._Server__pre_start = AsyncMock()
        server._allocate_server_cpus = AsyncMock(return_value=list(range(6)))
        server.wait_until_ready = AsyncMock()
        server._pin_valkey_threads = AsyncMock()

        commands = []

        async def mock_run(cmd, check=True):
            commands.append(cmd)
            if cmd.startswith("lsof"):
                return ("12345", "")
            return ("", "")

        server.run_host_command = mock_run

        await server.start(Path("/tmp/valkey-server"), 4, server_args="--io-threads-ownership yes")

        launch = next(c for c in commands if "/tmp/valkey-server" in c)
        assert launch.rstrip().endswith("--io-threads-ownership yes")
        # Generated args still present, and positioned before the user args
        assert launch.index("--bgsave-cpulist") < launch.index("--io-threads-ownership")

    @pytest.mark.asyncio
    async def test_start_without_server_args_unchanged(self):
        """Default (empty) server_args leaves the launch command untouched."""
        server = Server("192.168.1.1", 9000)
        server._Server__pre_start = AsyncMock()
        server._allocate_server_cpus = AsyncMock(return_value=list(range(6)))
        server.wait_until_ready = AsyncMock()
        server._pin_valkey_threads = AsyncMock()

        commands = []

        async def mock_run(cmd, check=True):
            commands.append(cmd)
            if cmd.startswith("lsof"):
                return ("12345", "")
            return ("", "")

        server.run_host_command = mock_run

        await server.start(Path("/tmp/valkey-server"), 4)

        launch = next(c for c in commands if "/tmp/valkey-server" in c)
        assert "--bgsave-cpulist" in launch
        assert launch.rstrip().endswith("5")  # ends with the bgsave cpulist, nothing appended

    @pytest.mark.asyncio
    async def test_stop_releases_allocation(self):
        """Test that stop() releases CPU allocation."""
        server = Server("192.168.1.1", 9000)
        server.valkey_pid = 12345

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: list(range(8))},
            net_interface_numa=0,
        )

        cpus = await server._allocate_server_cpus(3)
        server.server_cpus = cpus
        tag = server._allocation_tag

        server.run_host_command = AsyncMock(
            side_effect=[
                ("valkey-server", ""),
                ("", ""),
                ("", ""),
                ("", ""),
            ]
        )
        # Also mock the underlying host (profiling cleanup calls it directly)
        server._host.run_host_command = AsyncMock(return_value=("", ""))

        await server.stop()

        # Verify allocation was released
        assert Server._cpu_allocator.get_allocation("192.168.1.1", tag) is None

    @pytest.mark.asyncio
    async def test_kill_all_preserves_irq_allocations(self):
        """Test that kill_all clears server allocations but preserves IRQ allocations."""
        server = Server("192.168.1.1", 9000)
        server.run_host_command = AsyncMock(return_value=("", ""))

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: list(range(8))},
            net_interface_numa=0,
        )

        # Allocate IRQ
        irq_tag = AllocationTag("system", "irq")
        Server._cpu_allocator.allocate("192.168.1.1", irq_tag, count=2, require_numa=0)

        # Allocate server
        cpus = await server._allocate_server_cpus(3)
        server.server_cpus = cpus
        server_tag = server._allocation_tag

        await server.kill_all_valkey_instances_on_host()

        # IRQ should remain, server should be cleared
        assert Server._cpu_allocator.get_allocation("192.168.1.1", irq_tag) is not None
        assert Server._cpu_allocator.get_allocation("192.168.1.1", server_tag) is None

    @pytest.mark.asyncio
    async def test_get_available_cpu_count_uses_network_numa(self):
        """Test that get_available_cpu_count queries the network interface NUMA node."""
        server = Server("192.168.1.1", 9000)

        async def mock_setup():
            Server._cpu_allocator.register_host(
                "192.168.1.1",
                all_cpus=list(range(8)),
                numa_topology={0: [0, 1, 2, 3], 1: [4, 5, 6, 7]},
                net_interface_numa=1,
            )
            # Allocate some CPUs on NUMA 1
            tag = AllocationTag("other", "server")
            Server._cpu_allocator.allocate("192.168.1.1", tag, count=2, require_numa=1)

        server.ensure_host_cpu_allocation = mock_setup

        available = await server.get_available_cpu_count()

        # Should count only NUMA 1 CPUs: 4 total - 2 allocated = 2 available
        assert available == 2

    @pytest.mark.asyncio
    async def test_validate_cpu_allocation_checks_network_numa(self):
        """Test that validation fails when insufficient CPUs on network interface NUMA node."""
        server = Server("192.168.1.1", 9000)
        server.threads = 3

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: [0, 1, 2, 3], 1: [4, 5, 6, 7]},
            net_interface_numa=1,
        )

        # Allocate IRQs on NUMA 1
        irq_tag = AllocationTag("system", "irq")
        Server._cpu_allocator.allocate("192.168.1.1", irq_tag, count=2, require_numa=1)

        # threads=3 needs 5 CPUs, but only 2 available on NUMA 1
        with pytest.raises(RuntimeError, match="Insufficient CPUs.*NUMA node 1"):
            server._validate_sufficient_cpus()

    def test_get_num_cpus_calculation(self):
        """Test CPU count calculation for io-threads."""
        # io-threads + 2 for background threads
        assert Server.get_num_cpus(1) == 3
        assert Server.get_num_cpus(4) == 6
        assert Server.get_num_cpus(8) == 10

    @pytest.mark.asyncio
    async def test_multiple_servers_different_ports_same_host(self):
        """Test that multiple servers on same host get non-overlapping CPUs."""
        server1 = Server("192.168.1.1", 9000)
        server2 = Server("192.168.1.1", 9001)

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: list(range(8))},
            net_interface_numa=0,
        )

        cpus1 = await server1._allocate_server_cpus(3)
        cpus2 = await server2._allocate_server_cpus(2)

        # Should not overlap
        assert set(cpus1).isdisjoint(set(cpus2))

        # Both should have proper tags
        assert server1._allocation_tag.task_id == "server_192.168.1.1_9000"
        assert server2._allocation_tag.task_id == "server_192.168.1.1_9001"

    @pytest.mark.asyncio
    async def test_allocation_fails_when_numa_node_full(self):
        """Test that allocation fails when network interface NUMA node is full."""
        server = Server("192.168.1.1", 9000)

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: [0, 1, 2, 3], 1: [4, 5, 6, 7]},
            net_interface_numa=0,
        )

        # Fill up NUMA node 0
        tag1 = AllocationTag("other1", "server")
        tag2 = AllocationTag("other2", "server")
        Server._cpu_allocator.allocate("192.168.1.1", tag1, count=2, require_numa=0)
        Server._cpu_allocator.allocate("192.168.1.1", tag2, count=2, require_numa=0)

        # Only 0 CPUs left in NUMA 0, requesting 2 should fail
        with pytest.raises(RuntimeError, match="Insufficient CPUs"):
            await server._allocate_server_cpus(2)

    @pytest.mark.asyncio
    async def test_validate_sufficient_cpus_passes_when_enough_available(self):
        """Test validation passes when sufficient CPUs are available."""
        server = Server("192.168.1.1", 9000)
        server.threads = 2  # Needs 4 CPUs (2 + 2)

        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: list(range(8))},
            net_interface_numa=0,
        )

        # Should not raise
        server._validate_sufficient_cpus()

    @pytest.mark.asyncio
    async def test_detect_l3_cache_topology_parses_sysfs(self):
        """Test L3 cache detection with mocked sysfs responses."""
        server = Server("192.168.1.1", 9000)

        # Mock responses: CPUs 0-3 share L3 cache 0, CPUs 4-7 share L3 cache 1
        async def mock_run_host_command(cmd, check=True):
            if "cpu0/cache/index3/id" in cmd:
                return ("0", "")
            elif "cpu1/cache/index3/id" in cmd:
                return ("0", "")
            elif "cpu2/cache/index3/id" in cmd:
                return ("0", "")
            elif "cpu3/cache/index3/id" in cmd:
                return ("0", "")
            elif "cpu4/cache/index3/id" in cmd:
                return ("1", "")
            elif "cpu5/cache/index3/id" in cmd:
                return ("1", "")
            elif "cpu6/cache/index3/id" in cmd:
                return ("1", "")
            elif "cpu7/cache/index3/id" in cmd:
                return ("1", "")
            return ("-1", "")

        server.run_host_command = mock_run_host_command

        result = await server._detect_l3_cache_topology(list(range(8)))

        assert result == {0: [0, 1, 2, 3], 1: [4, 5, 6, 7]}

    @pytest.mark.asyncio
    async def test_detect_l3_cache_topology_handles_missing_sysfs(self):
        """Test graceful fallback when L3 cache sysfs doesn't exist."""
        server = Server("192.168.1.1", 9000)

        async def mock_run_host_command(cmd, check=True):
            return ("-1", "")  # Simulates missing sysfs

        server.run_host_command = mock_run_host_command

        result = await server._detect_l3_cache_topology(list(range(4)))

        assert result == {}

    @pytest.mark.asyncio
    async def test_pin_threads_handles_more_io_threads_than_cpus(self):
        """Guard against IndexError when binary spawns more IO threads than allocated CPUs."""
        server = Server("127.0.0.1", 9000)
        server.server_cpus = [0, 1]  # Only 2 CPUs: main + 1 IO thread max

        # Simulate ps output with 3 IO threads (exceeds allocation)
        ps_output = "  1000 valkey-server\n" "  1001 io_thd_1\n" "  1002 io_thd_2\n" "  1003 io_thd_3\n"
        server.run_host_command = AsyncMock(
            side_effect=[
                ("1000\n", ""),  # lsof -ti :port
                (ps_output, ""),  # ps -T
                ("", ""),  # taskset main
                ("", ""),  # taskset io_thd_1
                # Should NOT attempt io_thd_2 or io_thd_3
            ]
        )

        # Must not raise IndexError
        await server._pin_valkey_threads()

        # Verify only 3 calls: lsof + ps + pin main + pin 1 IO thread
        assert server.run_host_command.call_count == 4


class TestClientCpuApi:
    """Server.allocate_client_cpus / allocated_cpu_list / release_cpus against the real allocator."""

    def setup_method(self):
        Server._cpu_allocator = CpuAllocator()
        Server._cpu_allocator.register_host(
            "192.168.1.1",
            all_cpus=list(range(8)),
            numa_topology={0: [0, 1, 2, 3], 1: [4, 5, 6, 7]},
            net_interface_numa=1,
        )

    def test_client_cpus_land_on_net_numa_away_from_the_server(self):
        client = Server("192.168.1.1", 9000)
        server_tag = AllocationTag(task_id="server_192.168.1.1_9000", purpose="server")
        Server._cpu_allocator.allocate("192.168.1.1", server_tag, count=2, require_numa=1)
        server_cpus = Server._cpu_allocator.get_allocation("192.168.1.1", server_tag)

        tag = AllocationTag(task_id="bench", purpose="benchmark")
        cpus = client.allocate_client_cpus(tag, 2, avoid_tags=[server_tag])

        assert client.net_numa_node() == 1
        assert set(cpus) <= {4, 5, 6, 7}
        assert not set(cpus) & set(server_cpus)
        assert client.allocated_cpu_list(tag) == ",".join(map(str, cpus))
        assert client.cpu_allocator is Server._cpu_allocator

    def test_release_and_unknown_tag_are_safe(self):
        client = Server("192.168.1.1", 9000)
        tag = AllocationTag(task_id="bench", purpose="benchmark")
        assert client.allocated_cpu_list(tag) == ""
        client.release_cpus(tag)  # nothing held: no error
        client.allocate_client_cpus(tag, 1, avoid_tags=[])
        assert client.allocated_cpu_list(tag) != ""
        client.release_cpus(tag)
        assert client.allocated_cpu_list(tag) == ""
