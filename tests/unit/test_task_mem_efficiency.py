"""Tests for memory efficiency task module."""

import asyncio
import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conductress.config import MANUALLY_UPLOADED, ServerInfo
from conductress.file_protocol import BenchmarkResults
from conductress.server import Server
from conductress.tasks.task_mem_efficiency import MemTaskData, MemTaskRunner
from conductress.topology import TopologySpec


def memory_info_sequence(before, after):
    """An ``info`` side effect: INFO memory answers ``before`` first and ``after`` from then on.

    Other sections (server) answer an empty dict, as an engine that reports nothing would.
    """
    calls = {"memory": 0}

    async def _info(section):
        if section != "memory":
            return {}
        calls["memory"] += 1
        return before if calls["memory"] == 1 else after

    return _info


class TestMemTaskData:
    """Test MemTaskData class."""

    @patch("conductress.config.REPO_NAMES", ["valkey", "test_repo"])
    @patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "test_repo"])
    def test_init_valid_data(self):
        """Test initialization with valid data."""
        task = MemTaskData(
            source="valkey",
            specifier="unstable",
            topology=TopologySpec.standalone(),
            note="test",
            requirements={},
            make_args="-O2 -g",
            type="set",
            val_sizes=[64, 128, 256],
            has_expire=False,
        )
        assert task.type == "set"
        assert task.val_sizes == [64, 128, 256]
        assert task.has_expire is False

    @patch("conductress.config.REPO_NAMES", ["valkey", "test_repo"])
    @patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "test_repo"])
    def test_short_description_single_size(self):
        """Test short description with single value size."""
        task = MemTaskData(
            source="valkey",
            specifier="unstable",
            topology=TopologySpec.standalone(),
            note="test",
            requirements={},
            make_args="-O2 -g",
            type="set",
            val_sizes=[1024],
            has_expire=False,
        )
        desc = task.short_description()
        assert "set" in desc
        assert "1KB" in desc
        assert "expiration" not in desc

    @patch("conductress.config.REPO_NAMES", ["valkey", "test_repo"])
    @patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "test_repo"])
    def test_short_description_multiple_sizes(self):
        """Test short description with multiple value sizes."""
        task = MemTaskData(
            source="valkey",
            specifier="unstable",
            topology=TopologySpec.standalone(),
            note="test",
            requirements={},
            make_args="-O2 -g",
            type="zadd",
            val_sizes=[64, 128, 256],
            has_expire=True,
        )
        desc = task.short_description()
        assert "zadd" in desc
        assert "3 sizes" in desc
        assert "with expiration" in desc

    @patch("conductress.config.REPO_NAMES", ["valkey", "test_repo"])
    @patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "test_repo"])
    def test_prepare_task_runner(self):
        """Test task runner preparation."""
        task = MemTaskData(
            source="valkey",
            specifier="unstable",
            topology=TopologySpec.standalone(),
            note="test",
            requirements={},
            make_args="-O2 -g",
            type="set",
            val_sizes=[64],
            has_expire=False,
        )
        server_info = ServerInfo(ip="127.0.0.1", username="test", name="test_server")
        runner = task.prepare_task_runner([server_info])
        assert isinstance(runner, MemTaskRunner)
        assert runner.server_ip == "127.0.0.1"
        assert runner.test == "set"


class TestMemTaskRunner:
    """Test MemTaskRunner class."""

    @pytest.fixture
    def mock_server(self):
        """Mock server for testing."""
        server = MagicMock(spec=Server)
        server.ip = "127.0.0.1"
        server.port = 6379
        server.get_available_cpu_count = AsyncMock(return_value=4)
        server.kill_all_valkey_instances_on_host = AsyncMock()
        server.ensure_binary_cached = AsyncMock(return_value=Path("/mock/binary"))
        server.get_build_hash = MagicMock(return_value="abc123")
        server.info = AsyncMock(return_value={"used_memory": "1000000"})
        server.run_valkey_command_over_keyspace = AsyncMock()
        server.count_items_expires = AsyncMock(return_value=(5000000, 0))
        return server

    @pytest.fixture
    def runner(self):
        """Create a test runner."""
        return MemTaskRunner(
            task_name="test_task",
            server_ip="127.0.0.1",
            source="valkey",
            specifier="unstable",
            test="set",
            val_sizes=[64, 128],
            has_expire=False,
            make_args="-O2 -g",
            note="test note",
            key_size=16,
            user_data_bytes=80,  # 16B key + 64B value
        )

    def test_init_valid_test(self, runner):
        """Test initialization with valid test type."""
        assert runner.test == "set"
        assert runner.val_sizes == [64, 128]
        assert runner.has_expire is False

    def test_init_invalid_test(self):
        """Test initialization with invalid test type."""
        with pytest.raises(ValueError, match="Test invalid is not supported"):
            MemTaskRunner(
                task_name="test_task",
                server_ip="127.0.0.1",
                source="valkey",
                specifier="unstable",
                test="invalid",
                val_sizes=[64],
                has_expire=False,
                make_args="-O2 -g",
                note="test note",
            )

    @patch("conductress.tasks.task_mem_efficiency.Server")
    @patch("conductress.tasks.task_mem_efficiency.print_pretty_header")
    @pytest.mark.asyncio
    async def test_run_basic_flow(self, mock_print, mock_server_class, runner, mock_server):
        """Test basic run flow."""
        # Ensure Server() instantiation returns mock_server
        mock_server_class.return_value = mock_server
        mock_server_class.with_path = AsyncMock(return_value=mock_server)
        mock_server_class.get_num_cpus = MagicMock(return_value=1)

        # Mock file protocol
        runner.file_protocol = MagicMock()

        # Mock test_single_size_overhead to return simple results
        async def mock_test_overhead(val_size, port, semaphore):
            return {
                "val_size": val_size,
                "per_item_overhead": 10.0 + val_size * 0.1,
                "before_memory": {"used_memory": "1000000"},
                "after_memory": {"used_memory": "2000000"},
                "has_expire": False,
                "user_data_per_item": 80,
                "per_key_size": 20.0,
            }

        runner.test_single_size_overhead = mock_test_overhead
        runner.plot = MagicMock()

        await runner.run()

        # Verify server interactions
        mock_server.get_available_cpu_count.assert_called_once()
        assert mock_server.kill_all_valkey_instances_on_host.call_count == 2
        mock_server.ensure_binary_cached.assert_called_once()

        # Verify file protocol calls
        assert runner.file_protocol.write_status.call_count >= 2
        runner.file_protocol.write_results.assert_called_once()

    @patch("conductress.tasks.task_mem_efficiency.populate")
    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_test_single_size_overhead(self, mock_server_class, mock_populate, runner, mock_server):
        """Test single size overhead calculation."""
        mock_server_class.with_path = AsyncMock(return_value=mock_server)

        # Setup memory info responses
        mock_server.info.side_effect = memory_info_sequence({"used_memory": "1000000"}, {"used_memory": "6000000"})

        # Set cached_binary_path as it would be set in run()
        runner.cached_binary_path = Path("/mock/binary")

        semaphore = asyncio.Semaphore(1)
        result = await runner.test_single_size_overhead(64, 6379, semaphore)

        assert result["val_size"] == 64
        assert result["user_data_per_item"] == 16 + 64  # key_size + val_size for "set"
        assert "per_item_overhead" in result
        assert "per_key_size" in result

        # Verify server calls
        mock_server.info.assert_called()
        mock_populate.assert_called_once()
        mock_server.count_items_expires.assert_called_once()

    @patch("conductress.tasks.task_mem_efficiency.populate")
    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_expiration_with_non_set_command(self, mock_server_class, mock_populate, runner, mock_server):
        """Test that expiration with non-set command logs error."""
        runner.test = "zadd"
        runner.has_expire = True
        runner.user_data_bytes = 28

        mock_server_class.with_path = AsyncMock(return_value=mock_server)
        mock_server.info.side_effect = memory_info_sequence({"used_memory": "1000000"}, {"used_memory": "2000000"})
        mock_server.count_items_expires = AsyncMock(return_value=(5000000, 0))  # No expiration for non-set

        # Set cached_binary_path as it would be set in run()
        runner.cached_binary_path = Path("/mock/binary")

        semaphore = asyncio.Semaphore(1)
        with pytest.raises(RuntimeError):  # Should fail since expire_count != count
            await runner.test_single_size_overhead(64, 6379, semaphore)

    @patch("conductress.tasks.task_mem_efficiency.populate")
    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_expiration_with_set_command(self, mock_server_class, mock_populate, runner, mock_server):
        """Test expiration works with set command."""
        runner.has_expire = True

        mock_server_class.with_path = AsyncMock(return_value=mock_server)
        mock_server.info.side_effect = memory_info_sequence({"used_memory": "1000000"}, {"used_memory": "2000000"})
        mock_server.count_items_expires.return_value = (
            5000000,
            5000000,
        )  # All items have expiry

        # Set cached_binary_path as it would be set in run()
        runner.cached_binary_path = Path("/mock/binary")

        semaphore = asyncio.Semaphore(1)
        _ = await runner.test_single_size_overhead(64, 6379, semaphore)

        # Should call populate with has_expire=True
        mock_populate.assert_called_once()
        call_workload = mock_populate.call_args[0][2]
        assert call_workload.has_expire is True

    def test_plot_basic(self, runner):
        """Test basic plotting functionality."""
        with patch("conductress.tasks.task_mem_efficiency.plt") as mock_plt:
            efficiency_map = {64: 10.5, 128: 12.3}
            runner.plot(efficiency_map)

            mock_plt.clear_terminal.assert_called_once()
            mock_plt.clear_figure.assert_called_once()
            mock_plt.plot.assert_called_once()
            mock_plt.show.assert_called_once()

    def test_plot_with_missing_data(self, runner):
        """Test plotting with missing data points."""
        with patch("conductress.tasks.task_mem_efficiency.plt") as mock_plt:
            # Only one size has data
            efficiency_map = {64: 10.5}
            runner.plot(efficiency_map)

            # Should still plot (with None values)
            mock_plt.plot.assert_called_once()
            plot_args = mock_plt.plot.call_args[0]
            overheads = plot_args[1]
            assert None in overheads  # Missing data should be None

    @patch("conductress.tasks.task_mem_efficiency.populate")
    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_memory_calculation_accuracy(self, mock_server_class, mock_populate, runner, mock_server):
        """Test memory calculation accuracy."""
        mock_server_class.with_path = AsyncMock(return_value=mock_server)

        # Precise memory values for calculation testing
        before_mem = 1000000  # 1MB
        after_mem = 6000000  # 6MB (5MB increase)
        count = 5000000  # 5M items
        val_size = 64
        key_size = 16

        mock_server.info.side_effect = memory_info_sequence(
            {"used_memory": str(before_mem)}, {"used_memory": str(after_mem)}
        )

        # Set cached_binary_path as it would be set in run()
        runner.cached_binary_path = Path("/mock/binary")

        semaphore = asyncio.Semaphore(1)
        result = await runner.test_single_size_overhead(val_size, 6379, semaphore)

        expected_per_key = (after_mem - before_mem) / count  # 1.0 bytes per key
        expected_overhead = expected_per_key - val_size - key_size  # user_data = key_size + val_size for "set"

        assert result["per_key_size"] == expected_per_key
        assert result["per_item_overhead"] == expected_overhead
        assert result["user_data_per_item"] == key_size + val_size

    @patch("conductress.tasks.task_mem_efficiency.populate")
    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_assertion_failures(self, mock_server_class, mock_populate, runner, mock_server):
        """Test assertion failures in count verification."""
        mock_server_class.with_path = AsyncMock(return_value=mock_server)
        mock_server.info.side_effect = memory_info_sequence({"used_memory": "1000000"}, {"used_memory": "2000000"})

        # Wrong item count
        mock_server.count_items_expires.return_value = (4000000, 0)  # Should be 5M

        # Set cached_binary_path as it would be set in run()
        runner.cached_binary_path = Path("/mock/binary")

        semaphore = asyncio.Semaphore(1)
        with pytest.raises(RuntimeError):
            await runner.test_single_size_overhead(64, 6379, semaphore)

    @patch("conductress.tasks.task_mem_efficiency.populate")
    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_expire_count_assertion(self, mock_server_class, mock_populate, runner, mock_server):
        """Test expire count assertion."""
        runner.has_expire = True

        mock_server_class.with_path = AsyncMock(return_value=mock_server)
        mock_server.info.side_effect = memory_info_sequence({"used_memory": "1000000"}, {"used_memory": "2000000"})

        # Wrong expire count when expiration is enabled
        mock_server.count_items_expires.return_value = (
            5000000,
            0,
        )  # Should have expires

        # Set cached_binary_path as it would be set in run()
        runner.cached_binary_path = Path("/mock/binary")

        semaphore = asyncio.Semaphore(1)
        with pytest.raises(RuntimeError):
            await runner.test_single_size_overhead(64, 6379, semaphore)

    def test_status_initialization(self, runner):
        """Test status initialization."""
        assert runner.status.steps_total == len(runner.val_sizes) + 2
        assert runner.status.state == "starting"
        assert runner.status.steps_completed == 0

    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_cpu_limit_handling(self, mock_server_class, runner, mock_server):
        """Test CPU limit handling."""
        # Test with high CPU count
        mock_server.get_available_cpu_count.return_value = 20
        mock_server_class.return_value = mock_server
        mock_server_class.get_num_cpus = MagicMock(return_value=1)

        runner.file_protocol = MagicMock()
        runner.test_single_size_overhead = AsyncMock(return_value={"val_size": 64, "per_item_overhead": 10.0})
        runner.plot = MagicMock()

        await runner.run()

        # Should be limited to 9 CPUs
        # Verify semaphore was created (indirectly through successful execution)
        assert runner.test_single_size_overhead.call_count == len(runner.val_sizes)

    def test_results_data_structure(self, runner):
        """Test results data structure format."""
        # Mock the completion flow
        runner.commit_hash = "test_hash"
        results = [
            {"val_size": 64, "per_item_overhead": 10.0},
            {"val_size": 128, "per_item_overhead": 12.0},
        ]

        # Simulate results creation
        completion_time = datetime.datetime.now()
        results_data = BenchmarkResults(
            method=f"mem-{runner.test}",
            source=runner.source,
            specifier=runner.specifier,
            commit_hash=runner.commit_hash,
            score=results[0]["per_item_overhead"],  # Single result score
            end_time=completion_time,
            data=results,
            make_args="-O2 -g",
        )

        assert results_data.method == "mem-set"
        assert results_data.source == "valkey"
        assert results_data.score == 10.0
        assert len(results_data.data) == 2

    @patch("conductress.tasks.task_mem_efficiency.Server")
    @pytest.mark.asyncio
    async def test_concurrent_execution(self, mock_server_class, runner, mock_server):
        """Test concurrent execution of multiple size tests."""
        mock_server_class.with_path = AsyncMock(return_value=mock_server)

        # Mock Server constructor to return mock_server
        mock_server_instance = MagicMock()
        mock_server_instance.get_available_cpu_count = AsyncMock(return_value=2)
        mock_server_instance.kill_all_valkey_instances_on_host = AsyncMock()
        mock_server_instance.ensure_binary_cached = AsyncMock(return_value=Path("/mock/binary"))
        mock_server_class.return_value = mock_server_instance
        mock_server_class.get_num_cpus = MagicMock(return_value=1)

        call_order = []

        async def track_calls(val_size, _, semaphore):
            async with semaphore:
                call_order.append(f"start_{val_size}")
                await asyncio.sleep(0.01)  # Simulate work
                call_order.append(f"end_{val_size}")
                return {"val_size": val_size, "per_item_overhead": 10.0}

        runner.test_single_size_overhead = track_calls
        runner.file_protocol = MagicMock()
        runner.plot = MagicMock()

        # Use small semaphore to test concurrency
        with patch("asyncio.Semaphore") as mock_semaphore:
            mock_semaphore.return_value = asyncio.Semaphore(1)
            await runner.run()

        # Verify both sizes were processed
        assert len([x for x in call_order if x.startswith("start_")]) == 2
        assert len([x for x in call_order if x.startswith("end_")]) == 2


class TestQuiesceUntilStable:
    """Test the opt-in quiesce-until-stable helper (background-reclamation settling)."""

    @staticmethod
    def _seq_sampler(values):
        """Return an async sample_fn yielding `values` in order, repeating the last."""
        state = {"i": 0}

        async def _fn():
            i = state["i"]
            state["i"] = i + 1
            return values[i] if i < len(values) else values[-1]

        return _fn

    @staticmethod
    async def _noop_sleep(_seconds):
        return None

    def test_window_stable(self):
        from conductress.tasks.task_mem_efficiency import _window_stable

        assert _window_stable([100_000, 100_050, 99_970], 0.001)  # spread 80 <= ~100
        assert not _window_stable([100_000, 100_300, 100_000], 0.001)  # spread 300 > ~100
        # slow monotonic drift: each consecutive delta (40) is < eps, but the
        # spread across the window (120) exceeds eps -> correctly NOT stable.
        assert not _window_stable([100_000, 100_040, 100_080, 100_120], 0.001)
        assert _window_stable([500, 500, 500], 0.001)  # flat

    @pytest.mark.asyncio
    async def test_slow_drift_not_stable(self):
        from conductress.tasks.task_mem_efficiency import quiesce_until_stable

        # Declines 40/poll: each pairwise delta (~0.04%) is under rel_eps, but the
        # windowed spread accumulates past it, so this must NOT be called stable
        # (a pairwise criterion would have stopped early here).
        state = {"v": 100_000}

        async def slow_drift():
            state["v"] -= 40
            return state["v"]

        value, elapsed = await quiesce_until_stable(
            slow_drift, interval=1.0, stable_samples=3, rel_eps=0.001, max_seconds=8, sleep_fn=self._noop_sleep
        )
        assert elapsed >= 8.0  # ran to timeout -- never falsely declared stable
        assert value < 100_000

    @pytest.mark.asyncio
    async def test_noisy_flat_converges(self):
        from conductress.tasks.task_mem_efficiency import quiesce_until_stable

        # Jitter within the epsilon band (spread ~60 on ~100k = 0.06% < 0.1%) must
        # still be treated as settled rather than resetting forever.
        vals = [100_000, 100_040, 99_990, 100_010, 100_020, 100_000, 100_030, 99_995]
        value, elapsed = await quiesce_until_stable(
            self._seq_sampler(vals),
            interval=1.0,
            stable_samples=3,
            rel_eps=0.001,
            max_seconds=100,
            sleep_fn=self._noop_sleep,
        )
        assert 99_900 <= value <= 100_100  # converged to the noisy plateau
        assert elapsed < 100.0  # did not time out

    @pytest.mark.asyncio
    async def test_stops_at_plateau(self):
        from conductress.tasks.task_mem_efficiency import quiesce_until_stable

        # declines then flattens; last 3 deltas are ~0 -> stable
        sampler = self._seq_sampler([100_000, 90_000, 85_000, 84_000, 84_000, 84_000, 84_000])
        value, elapsed = await quiesce_until_stable(
            sampler, interval=1.0, stable_samples=3, rel_eps=0.001, max_seconds=100, sleep_fn=self._noop_sleep
        )
        assert value == 84_000
        assert elapsed == 6.0  # 6 polls to reach 3 consecutive stable deltas

    @pytest.mark.asyncio
    async def test_immediate_stable(self):
        from conductress.tasks.task_mem_efficiency import quiesce_until_stable

        sampler = self._seq_sampler([500, 500, 500, 500])  # already flat
        value, elapsed = await quiesce_until_stable(
            sampler, interval=1.0, stable_samples=3, rel_eps=0.001, max_seconds=100, sleep_fn=self._noop_sleep
        )
        assert value == 500
        assert elapsed == 3.0

    @pytest.mark.asyncio
    async def test_times_out_when_never_stable(self):
        from conductress.tasks.task_mem_efficiency import quiesce_until_stable

        state = {"v": 1000}

        async def ever_changing():
            state["v"] -= 7  # always changes by > rel_eps
            return state["v"]

        value, elapsed = await quiesce_until_stable(
            ever_changing, interval=1.0, stable_samples=3, rel_eps=0.001, max_seconds=5, sleep_fn=self._noop_sleep
        )
        assert elapsed >= 5.0  # bounded by max_seconds, did not hang
        assert value < 1000


class TestSettleThreading:
    """settle must default off and thread through to the runner + serialization."""

    def test_settle_defaults_off(self):
        task = MemTaskData(
            source=MANUALLY_UPLOADED,
            specifier="unstable",
            topology=TopologySpec.standalone(),
            note="t",
            requirements={},
            make_args="",
            type="zadd",
            val_sizes=[20],
            has_expire=False,
        )
        assert task.settle is False
        runner = task.prepare_task_runner([ServerInfo(ip="127.0.0.1", username="u", name="s")])
        assert runner.settle is False

    def test_settle_threads_through(self):
        task = MemTaskData(
            source=MANUALLY_UPLOADED,
            specifier="unstable",
            topology=TopologySpec.standalone(),
            note="t",
            requirements={},
            make_args="",
            type="zadd",
            val_sizes=[20],
            has_expire=False,
            settle=True,
        )
        runner = task.prepare_task_runner([ServerInfo(ip="127.0.0.1", username="u", name="s")])
        assert runner.settle is True

    def test_settle_survives_serialization(self):
        import dataclasses

        task = MemTaskData(
            source=MANUALLY_UPLOADED,
            specifier="unstable",
            topology=TopologySpec.standalone(),
            note="t",
            requirements={},
            make_args="",
            type="zadd",
            val_sizes=[20],
            has_expire=False,
            settle=True,
        )
        d = dataclasses.asdict(task)
        assert d["settle"] is True
        # Mirror TaskQueue deserialization: pop init=False fields, then reconstruct.
        d.pop("task_type", None)
        d.pop("timestamp", None)
        restored = MemTaskData(**d)
        assert restored.settle is True


class TestSettleRehash:
    """Rehash settle-and-assert: read passes until DEBUG HTSTATS shows no rehash."""

    DICT_MID = (
        "Hash table 0 stats (main hash table):\n table size: 4194304\n number of elements: 2920322\n"
        "Hash table 1 stats (rehashing target):\n table size: 8388608\n number of elements: 2079678\n"
    )
    DICT_DONE = "Hash table 0 stats (main hash table):\n table size: 8388608\n number of elements: 5000000\n"
    HASHTABLE_MID = (
        "Hash table 0 stats (main hash table):\n table size: 524288\n number of entries: 3000000\n"
        "Hash table 1 stats (rehashing target):\n table size: 1048576\n number of entries: 2000000\n"
    )

    @staticmethod
    def _seq_stats(values):
        state = {"i": 0}

        async def _fn():
            i = state["i"]
            state["i"] = i + 1
            return values[i] if i < len(values) else values[-1]

        return _fn

    @staticmethod
    def _counting_touch(counter):
        async def _fn():
            counter["n"] += 1
            return 5_000_000

        return _fn

    def test_htstats_command_targets_the_filled_table(self):
        from conductress.tasks.task_mem_efficiency import htstats_command

        assert htstats_command("sadd") == "DEBUG HTSTATS-KEY myset"
        assert htstats_command("hset") == "DEBUG HTSTATS-KEY myhash"
        assert htstats_command("zadd") == "DEBUG HTSTATS-KEY myzset"
        assert htstats_command("set") == "DEBUG HTSTATS 0"

    def test_is_rehashing_reads_both_table_implementations(self):
        from conductress.tasks.task_mem_efficiency import is_rehashing

        assert is_rehashing(self.DICT_MID)
        assert is_rehashing(self.HASHTABLE_MID)
        assert not is_rehashing(self.DICT_DONE)

    def test_is_rehashing_surfaces_debug_failures(self):
        from conductress.tasks.task_mem_efficiency import is_rehashing

        with pytest.raises(RuntimeError, match="enable-debug-command"):
            is_rehashing(None)
        with pytest.raises(RuntimeError, match="DEBUG HTSTATS failed"):
            is_rehashing("ERR DEBUG command not allowed")

    @pytest.mark.asyncio
    async def test_already_settled_costs_no_read_pass(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash

        touches = {"n": 0}
        passes = await settle_rehash(self._seq_stats([self.DICT_DONE]), self._counting_touch(touches))
        assert passes == 0
        assert touches["n"] == 0

    @pytest.mark.asyncio
    async def test_reads_until_rehash_completes(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash

        touches = {"n": 0}
        stats = self._seq_stats([self.DICT_MID, self.DICT_MID, self.DICT_DONE])
        passes = await settle_rehash(stats, self._counting_touch(touches))
        assert passes == 2
        assert touches["n"] == 2

    @pytest.mark.asyncio
    async def test_refuses_to_record_a_table_that_never_settles(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash

        touches = {"n": 0}
        with pytest.raises(RuntimeError, match="still rehashing after 2"):
            await settle_rehash(self._seq_stats([self.DICT_MID]), self._counting_touch(touches), max_passes=2)
        assert touches["n"] == 2


NOT_HASHTABLE_REPLY = "ERR The value stored at the specified key is not represented using hash table"


class TestSettleRehashForKey:
    """A key stored in another encoding has no key-level rehash; the keyspace is still checked."""

    DONE = TestSettleRehash.DICT_DONE
    MID = TestSettleRehash.DICT_MID

    @staticmethod
    def _calls(values):
        """An async stats function that records how often it was asked."""
        state = {"i": 0}

        async def _fn():
            i = state["i"]
            state["i"] = i + 1
            return values[i] if i < len(values) else values[-1]

        _fn.state = state
        return _fn

    def test_not_hashtable_reply_is_recognized(self):
        from conductress.tasks.task_mem_efficiency import is_not_hashtable

        assert is_not_hashtable(NOT_HASHTABLE_REPLY)
        assert is_not_hashtable(NOT_HASHTABLE_REPLY.upper())
        # the exact reply of a release build says "an hash table"; older text had no article
        assert is_not_hashtable("ERR The value stored at the specified key is not represented using an hash table")
        assert is_not_hashtable("ERR The value stored at the specified key is not represented using hash table")
        assert is_not_hashtable("ERR The value is not represented using a hash table")
        assert not is_not_hashtable("The value stored at the specified key is not represented using an hash table")
        assert not is_not_hashtable("ERR value is represented using a hash table")
        assert not is_not_hashtable("ERR DEBUG command not allowed")
        assert not is_not_hashtable("ERR no such key")
        assert not is_not_hashtable(self.DONE)  # a real stats text is never an error
        assert not is_not_hashtable(None)

    @pytest.mark.asyncio
    async def test_not_hashtable_key_checks_the_keyspace_and_records_it(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        key_stats = self._calls([NOT_HASHTABLE_REPLY])
        keyspace_stats = self._calls([self.DONE])
        touches = {"n": 0}
        passes, key_check, encoding = await settle_rehash_for_key(
            key_stats, keyspace_stats, TestSettleRehash._counting_touch(touches)
        )
        assert (passes, key_check, encoding) == (0, "not_hashtable", None)
        assert keyspace_stats.state["i"] == 1  # DEBUG HTSTATS 0 was read
        assert touches["n"] == 0

    @pytest.mark.asyncio
    async def test_not_hashtable_key_still_waits_for_a_rehashing_keyspace(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        keyspace_stats = self._calls([self.MID, self.DONE])
        touches = {"n": 0}
        passes, key_check, encoding = await settle_rehash_for_key(
            self._calls([NOT_HASHTABLE_REPLY]), keyspace_stats, TestSettleRehash._counting_touch(touches)
        )
        assert (passes, key_check, encoding) == (1, "not_hashtable", None)
        assert touches["n"] == 1

    @pytest.mark.asyncio
    async def test_still_rehashing_key_is_read_until_done(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        touches = {"n": 0}
        keyspace_stats = self._calls([self.DONE])
        key_stats = self._calls([self.MID, self.MID, self.DONE])
        passes, key_check, encoding = await settle_rehash_for_key(
            key_stats,
            keyspace_stats,
            TestSettleRehash._counting_touch(touches),
        )
        assert (passes, key_check, encoding) == (2, "hashtable", None)
        assert touches["n"] == 2
        assert keyspace_stats.state["i"] == 0  # unchanged behavior: only the key table is checked
        assert key_stats.state["i"] == 3  # three readings, none repeated

    @staticmethod
    def _encoding(reply):
        async def _fn():
            return reply

        return _fn

    @pytest.mark.parametrize("reply", ["btree", "listpack", "intset", '"btree"', "BTREE\n", "listpackex"])
    @pytest.mark.asyncio
    async def test_non_hashtable_encoding_skips_the_key_check(self, reply):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        key_stats = self._calls(["ERR the key-level check must not run"])
        keyspace_stats = self._calls([self.DONE])
        passes, key_check, encoding = await settle_rehash_for_key(
            key_stats,
            keyspace_stats,
            TestSettleRehash._counting_touch({"n": 0}),
            encoding_fn=self._encoding(reply),
        )
        assert (passes, key_check) == (0, "not_hashtable")
        assert encoding == reply.strip().strip('"').lower()
        assert key_stats.state["i"] == 0  # DEBUG HTSTATS-KEY was never asked
        assert keyspace_stats.state["i"] == 1  # the keyspace is still checked

    @pytest.mark.asyncio
    async def test_non_hashtable_encoding_still_waits_for_a_rehashing_keyspace(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        touches = {"n": 0}
        passes, key_check, encoding = await settle_rehash_for_key(
            self._calls(["unused"]),
            self._calls([self.MID, self.DONE]),
            TestSettleRehash._counting_touch(touches),
            encoding_fn=self._encoding("btree"),
        )
        assert (passes, key_check, encoding) == (1, "not_hashtable", "btree")
        assert touches["n"] == 1

    @pytest.mark.parametrize("reply", ["skiplist", "hashtable"])
    @pytest.mark.asyncio
    async def test_hashtable_encoding_runs_the_key_check_and_records_the_encoding(self, reply):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        key_stats = self._calls([self.MID, self.DONE])
        passes, key_check, encoding = await settle_rehash_for_key(
            key_stats,
            self._calls([self.DONE]),
            TestSettleRehash._counting_touch({"n": 0}),
            encoding_fn=self._encoding(reply),
        )
        assert (passes, key_check, encoding) == (1, "hashtable", reply)

    @pytest.mark.parametrize("reply", [None, "", "ERR no such key", "(nil)", "some new encoding"])
    @pytest.mark.asyncio
    async def test_unreadable_encoding_falls_back_to_the_key_check(self, reply):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        key_stats = self._calls([NOT_HASHTABLE_REPLY])
        passes, key_check, encoding = await settle_rehash_for_key(
            key_stats,
            self._calls([self.DONE]),
            TestSettleRehash._counting_touch({"n": 0}),
            encoding_fn=self._encoding(reply),
        )
        assert (passes, key_check, encoding) == (0, "not_hashtable", None)
        assert key_stats.state["i"] == 1

    @pytest.mark.asyncio
    async def test_unknown_encoding_name_is_recorded_and_the_key_check_decides(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        key_stats = self._calls([self.DONE])
        passes, key_check, encoding = await settle_rehash_for_key(
            key_stats,
            self._calls([self.DONE]),
            TestSettleRehash._counting_touch({"n": 0}),
            encoding_fn=self._encoding("newtree"),
        )
        assert (passes, key_check, encoding) == (0, "hashtable", "newtree")
        assert key_stats.state["i"] == 1

    def test_normalize_encoding(self):
        from conductress.tasks.task_mem_efficiency import normalize_encoding

        assert normalize_encoding("skiplist") == "skiplist"
        assert normalize_encoding(' "BTree" \n') == "btree"
        for bad in (None, "", "   ", "ERR no such key", "(nil)", "(error) ERR x", "two words"):
            assert normalize_encoding(bad) is None

    @pytest.mark.asyncio
    async def test_key_that_never_settles_still_fails(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        with pytest.raises(RuntimeError, match="still rehashing after 2"):
            await settle_rehash_for_key(
                self._calls([self.MID]),
                self._calls([self.DONE]),
                TestSettleRehash._counting_touch({"n": 0}),
                max_passes=2,
            )

    @pytest.mark.asyncio
    async def test_other_errors_still_fail_the_task(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        for reply in ("ERR DEBUG command not allowed", "ERR no such key", None):
            with pytest.raises(RuntimeError):
                await settle_rehash_for_key(
                    self._calls([reply]), self._calls([self.DONE]), TestSettleRehash._counting_touch({"n": 0})
                )

    @pytest.mark.asyncio
    async def test_keyspace_error_in_the_not_hashtable_case_fails(self):
        from conductress.tasks.task_mem_efficiency import settle_rehash_for_key

        with pytest.raises(RuntimeError, match="DEBUG HTSTATS failed"):
            await settle_rehash_for_key(
                self._calls([NOT_HASHTABLE_REPLY]),
                self._calls(["ERR DEBUG command not allowed"]),
                TestSettleRehash._counting_touch({"n": 0}),
            )


async def _no_sleep(_seconds: float) -> None:
    return None


def _sampler(rows):
    """An async sample function that replays `rows` (dicts), repeating the last one."""
    state = {"i": 0}

    async def _fn():
        i = state["i"]
        state["i"] = i + 1
        return dict(rows[i] if i < len(rows) else rows[-1])

    _fn.state = state
    return _fn


class TestMemoryStability:
    """Engine-neutral check: used_memory (and any hash table overhead fields) must stop moving."""

    @pytest.mark.asyncio
    async def test_stable_sequence_ends_early_with_a_record(self):
        from conductress.tasks.task_mem_efficiency import check_memory_stability

        rows = [{"used_memory": v} for v in (1_000_000, 900_000, 800_500, 800_400, 800_450, 800_420)]
        sample = _sampler(rows)
        record = await check_memory_stability(sample, sleep_fn=_no_sleep)
        assert record["verdict"] == "stable"
        assert record["elapsed_s"] == 5.0  # 6 samples, one interval between each
        assert record["samples"] == rows
        assert record["series"] == ["used_memory"]
        assert sample.state["i"] == 6  # stopped as soon as the window was stable

    @pytest.mark.asyncio
    async def test_unstable_sequence_times_out_and_is_recorded_not_raised(self):
        from conductress.tasks.task_mem_efficiency import check_memory_stability

        sample = _sampler([{"used_memory": 1_000_000 + 50_000 * i} for i in range(100)])
        record = await check_memory_stability(sample, max_seconds=10.0, sleep_fn=_no_sleep)
        assert record["verdict"] == "timeout"
        assert record["elapsed_s"] == 10.0
        assert len(record["samples"]) == 11

    @pytest.mark.asyncio
    async def test_slow_drift_is_not_called_stable(self):
        """A creep below the per-sample threshold still adds up across the window."""
        from conductress.tasks.task_mem_efficiency import check_memory_stability

        sample = _sampler([{"used_memory": 1_000_000 + 700 * i} for i in range(100)])
        record = await check_memory_stability(sample, max_seconds=8.0, sleep_fn=_no_sleep)
        assert record["verdict"] == "timeout"

    @pytest.mark.asyncio
    async def test_an_overhead_field_that_still_moves_blocks_the_verdict(self):
        from conductress.tasks.task_mem_efficiency import check_memory_stability

        rows = [{"used_memory": 800_000, "overhead.hashtable.main": 1_000_000 - 100_000 * i} for i in range(5)]
        rows += [{"used_memory": 800_000, "overhead.hashtable.main": 600_000}] * 10
        record = await check_memory_stability(_sampler(rows), sleep_fn=_no_sleep)
        assert record["verdict"] == "stable"
        assert record["series"] == ["overhead.hashtable.main", "used_memory"]
        assert record["elapsed_s"] == 7.0  # used_memory alone was stable from sample 3; the field only at sample 7

    @pytest.mark.asyncio
    async def test_engine_without_memory_stats_fields_uses_used_memory_only(self):
        from conductress.tasks.task_mem_efficiency import check_memory_stability

        record = await check_memory_stability(_sampler([{"used_memory": 5_000_000}]), sleep_fn=_no_sleep)
        assert record["verdict"] == "stable"
        assert record["series"] == ["used_memory"]
        assert record["elapsed_s"] == 3.0

    @pytest.mark.asyncio
    async def test_a_field_that_disappears_does_not_break_the_check(self):
        from conductress.tasks.task_mem_efficiency import check_memory_stability

        rows = [{"used_memory": 7_000_000, "overhead.hashtable.main": 10}] + [{"used_memory": 7_000_000}] * 5
        record = await check_memory_stability(_sampler(rows), sleep_fn=_no_sleep)
        assert record["verdict"] == "stable"

    def test_memory_stats_parser_keeps_only_hash_table_fields(self):
        from conductress.tasks.task_mem_efficiency import parse_memory_stats_rehash_fields

        text = "\n".join(
            [
                "peak.allocated",
                "1000000",
                "keys.count",
                "3",
                "overhead.hashtable.main",
                "4096",
                "db.0",
                "overhead.hashtable.main",
                "2048",
                "overhead.hashtable.expires",
                "0",
                "rehashing.bytes",
                "512.5",
                "fragmentation",
                "1.25",
            ]
        )
        assert parse_memory_stats_rehash_fields(text) == {
            "overhead.hashtable.main": 2048.0,  # the nested per-db value, same name, read last
            "overhead.hashtable.expires": 0.0,
            "rehashing.bytes": 512.5,
        }

    def test_memory_stats_parser_reads_the_rehash_fields_of_a_tree_engine(self):
        """Field names seen in MEMORY STATS of a release with a tree sorted set; values made up."""
        from conductress.tasks.task_mem_efficiency import parse_memory_stats_rehash_fields

        text = "\n".join(
            [
                "keys.count",
                "1",
                "overhead.db.hashtable.rehashing",
                "1024",
                "db.dict.rehashing.count",
                "0",
                "db.0",
                "overhead.hashtable.main",
                "56",
                "overhead.hashtable.expires",
                "0",
                "allocator.allocated",
                "999999",
            ]
        )
        assert parse_memory_stats_rehash_fields(text) == {
            "overhead.db.hashtable.rehashing": 1024.0,
            "db.dict.rehashing.count": 0.0,
            "overhead.hashtable.main": 56.0,
            "overhead.hashtable.expires": 0.0,
        }

    def test_memory_stats_parser_returns_nothing_for_other_engines(self):
        from conductress.tasks.task_mem_efficiency import parse_memory_stats_rehash_fields

        assert parse_memory_stats_rehash_fields("peak.allocated\n1000\nkeys.count\n3\nfragmentation\n1.1") == {}
        assert parse_memory_stats_rehash_fields("") == {}
        assert parse_memory_stats_rehash_fields(None) == {}
        assert parse_memory_stats_rehash_fields("ERR unknown subcommand") == {}

    def test_defaults_keep_a_task_short(self):
        from conductress.tasks import task_mem_efficiency as m

        assert m.SETTLE_STABILITY_INTERVAL_S * m.SETTLE_STABILITY_SAMPLES <= 5
        assert m.SETTLE_STABILITY_MAX_SECONDS <= 60


class TestTouchOps:
    """The settle read pass must look up every item by the fill's own naming."""

    def test_one_read_per_item_matching_fill_names(self):
        from conductress.sweep.populator import _operations, _touch_ops

        fills = list(_operations("sadd", 5, 0, 20, 0, "sequential"))
        reads = list(_touch_ops("sadd", 5, 0, 20, 0))
        assert [r[:1] + r[1:2] for r in reads] == [("SISMEMBER", "myset")] * 5
        assert [r[2] for r in reads] == [f[1] for f in fills]

    def test_command_per_type(self):
        from conductress.sweep.populator import _touch_ops

        assert next(_touch_ops("set", 1, 16, 16, 0))[0] == "EXISTS"
        assert next(_touch_ops("hset", 1, 0, 16, 16))[:2] == ("HEXISTS", "myhash")
        assert next(_touch_ops("zadd", 1, 0, 20, 0))[:2] == ("ZSCORE", "myzset")
        with pytest.raises(ValueError):
            next(_touch_ops("lpush", 1, 0, 16, 0))

    def test_touch_pipelines_every_read(self):
        from conductress.sweep import populator

        pipe = MagicMock()
        client = MagicMock()
        client.pipeline.return_value = pipe
        workload = MagicMock(command="hset", item_count=12, key_size=0, value_size=16, field_size=16, label="t")
        with patch.object(populator.valkey, "Valkey", return_value=client), patch.object(populator, "BATCH_SIZE", 5):
            issued = populator.touch("127.0.0.1", 6379, workload)
        assert issued == 12
        assert pipe.execute_command.call_count == 12
        assert pipe.execute.call_count == 3  # two full batches of 5 + the remainder flush
        client.close.assert_called_once()
