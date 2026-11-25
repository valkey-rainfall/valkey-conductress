"""The memory task's item count: CLI option, task field, populate and check paths, sweep isolation."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conductress.cli import main
from conductress.config import MEM_TEST_ITEM_COUNT, ServerInfo
from conductress.server import Server
from conductress.task_queue import BaseTaskData
from conductress.tasks.task_mem_efficiency import MemTaskData, MemTaskRunner
from conductress.topology import TopologySpec


@pytest.fixture(autouse=True)
def known_sources(monkeypatch):
    """Other test modules rewrite the global source list at import; pin the one these tests use."""
    from conductress import config

    monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "redis"])


def queued_tasks(argv):
    """Run `queue add-memory` with a mocked queue; return (exit code, submitted tasks)."""
    with patch("conductress.cli.TaskQueue") as queue_cls:
        queue = MagicMock()
        queue_cls.return_value = queue
        code = main(["queue", "add-memory", "--source", "valkey", "--specifier", "x", *argv])
    return code, [call.args[0] for call in queue.submit_task.call_args_list]


class TestCliItemCount:
    def test_default_is_the_standard_count(self):
        code, tasks = queued_tasks(["--types", "set"])
        assert code == 0
        assert tasks[0].item_count == MEM_TEST_ITEM_COUNT == 5_000_000

    @pytest.mark.parametrize("raw", ["100000", "100_000", "100,000"])
    def test_item_count_is_stored_in_every_task(self, raw):
        code, tasks = queued_tasks(["--types", "set,sadd,hset,zadd", "--item-count", raw])
        assert code == 0
        assert [t.item_count for t in tasks] == [100_000] * 4

    @pytest.mark.parametrize("raw", ["10000", "50000000"])
    def test_range_ends_are_accepted(self, raw):
        code, tasks = queued_tasks(["--types", "set", "--item-count", raw])
        assert code == 0
        assert tasks[0].item_count == int(raw)

    @pytest.mark.parametrize("raw", ["9999", "50000001", "0", "-5", "abc", "1.5e6"])
    def test_out_of_range_or_malformed_is_refused(self, raw, capsys):
        code, tasks = queued_tasks(["--types", "set", "--item-count", raw])
        assert code == 1
        assert tasks == []
        assert "--item-count" in capsys.readouterr().err

    def test_size_floor_follows_the_requested_count(self, capsys):
        # "m:0000000:" is 10 bytes: enough for 5M items (7-digit index) but not for 50M (8 digits).
        code, _ = queued_tasks(["--types", "sadd", "--sizes", "10"])
        assert code == 0
        code, tasks = queued_tasks(["--types", "sadd", "--sizes", "10", "--item-count", "50000000"])
        assert code == 1
        assert tasks == []
        assert "minimum" in capsys.readouterr().err

    def test_non_default_count_is_shown_in_the_summary(self, capsys):
        queued_tasks(["--types", "set", "--item-count", "20000"])
        assert "item count: 20,000" in capsys.readouterr().out


class TestTaskField:
    def _task(self, **fields):
        return MemTaskData(
            source="valkey",
            specifier="abc123",
            topology=TopologySpec.standalone(),
            note="t",
            requirements={},
            make_args="",
            type="set",
            val_sizes=[64],
            has_expire=False,
            **fields,
        )

    def test_default(self):
        assert self._task().item_count == MEM_TEST_ITEM_COUNT

    def test_round_trip_through_a_file(self, tmp_path, monkeypatch):
        from conductress import task_queue

        monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
        path = tmp_path / "task.json"
        self._task(item_count=123_456).save_to_file(path)
        assert json.loads(path.read_text())["item_count"] == 123_456
        assert BaseTaskData.from_file(path).item_count == 123_456

    def test_task_written_before_the_field_loads_with_the_default(self, tmp_path, monkeypatch):
        from conductress import task_queue

        monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
        path = tmp_path / "task.json"
        self._task().save_to_file(path)
        document = json.loads(path.read_text())
        del document["item_count"]
        path.write_text(json.dumps(document))
        assert BaseTaskData.from_file(path).item_count == MEM_TEST_ITEM_COUNT

    def test_runner_gets_the_count(self, monkeypatch):
        from conductress import task_queue

        monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
        runner = self._task(item_count=20_000).prepare_task_runner([ServerInfo("127.0.0.1", 6379)])
        assert runner.item_count == 20_000

    def test_sweep_task_refuses_another_count(self, monkeypatch):
        from conductress import task_queue

        monkeypatch.setattr(task_queue.config, "REPO_NAMES", ["valkey"])
        sweep = self._task(sweep_commit="aaa", item_count=20_000)
        with pytest.raises(ValueError, match="sweep task keeps the default"):
            sweep.prepare_task_runner([ServerInfo("127.0.0.1", 6379)])
        assert self._task(sweep_commit="aaa").prepare_task_runner([ServerInfo("127.0.0.1", 6379)]).item_count == (
            MEM_TEST_ITEM_COUNT
        )


class TestSweepKeepsTheDefault:
    @pytest.fixture
    def coordinator(self, tmp_path, monkeypatch):
        import conductress.config as config
        import conductress.sweep.memory_coordinator as mc
        from conductress.sweep.memory_coordinator import MemorySweepCoordinator, MemoryWorkload

        monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "rainsupreme"])
        monkeypatch.setattr(mc, "MEMORY_STATE_DIR", tmp_path / "sweep_data")
        workload = MemoryWorkload(command="set", key_size=16, value_size=64, label="set-v64", user_data_bytes=80)
        return MemorySweepCoordinator(tmp_path / "repo", workload)

    def test_sweep_task_uses_the_default_count(self, coordinator):
        task = coordinator._make_task("aaa", "test", breakdown=False)
        assert task.item_count == MEM_TEST_ITEM_COUNT

    def _manual_like(self, item_count):
        task = MagicMock(spec=MemTaskData)
        for name, value in dict(
            sweep_commit="aaa",
            source="valkey",
            type="set",
            has_expire=False,
            val_sizes=[64],
            key_size=16,
            field_size=0,
            settle=True,
            item_count=item_count,
        ).items():
            setattr(task, name, value)
        return task

    def test_series_accepts_only_its_own_count(self, coordinator):
        assert coordinator._is_my_task(self._manual_like(MEM_TEST_ITEM_COUNT)) is True
        assert coordinator._is_my_task(self._manual_like(20_000)) is False


@pytest.fixture
def mock_server():
    server = MagicMock(spec=Server)
    server.ip = "127.0.0.1"
    server.port = 6379
    server.get_build_hash = MagicMock(return_value="abc123")
    server.stop = AsyncMock()
    server.valkey_pid = 4242
    server.info = AsyncMock(return_value={"used_memory": "1000000"})
    server.run_valkey_command = AsyncMock(return_value=None)
    server.run_host_command = AsyncMock(return_value=("", ""))
    return server


def make_runner(test="set", **kwargs):
    return MemTaskRunner(
        task_name="t",
        server_ip="127.0.0.1",
        source="valkey",
        specifier="unstable",
        test=test,
        val_sizes=[64],
        has_expire=kwargs.pop("has_expire", False),
        make_args="",
        note="n",
        key_size=16,
        user_data_bytes=80,
        **kwargs,
    )


async def run_single(runner, mock_server, keys, expires=0, cardinality=None):
    mock_server.count_items_expires = AsyncMock(return_value=(keys, expires))
    if cardinality is not None:
        mock_server.run_valkey_command = AsyncMock(return_value=str(cardinality))
    runner.cached_binary_path = Path("/mock/binary")
    with (
        patch("conductress.tasks.task_mem_efficiency.populate") as populate,
        patch("conductress.tasks.task_mem_efficiency.Server") as server_cls,
    ):
        server_cls.with_path = AsyncMock(return_value=mock_server)
        result = await runner.test_single_size_overhead(64, 6379, asyncio.Semaphore(1))
    return result, populate


class TestPopulateAndCheckUseTheCount:
    @pytest.mark.asyncio
    async def test_populator_is_asked_for_the_count(self, mock_server):
        result, populate = await run_single(make_runner(item_count=20_000), mock_server, keys=20_000)
        workload = populate.call_args.args[2]
        assert workload.item_count == 20_000
        assert result["item_count"] == 20_000
        assert result["item_count_requested"] == 20_000
        assert result["memory_record"]["item_count"] == 20_000

    @pytest.mark.asyncio
    async def test_default_runner_uses_the_standard_count(self, mock_server):
        result, populate = await run_single(make_runner(), mock_server, keys=MEM_TEST_ITEM_COUNT)
        assert populate.call_args.args[2].item_count == MEM_TEST_ITEM_COUNT
        assert result["item_count_requested"] == MEM_TEST_ITEM_COUNT

    @pytest.mark.asyncio
    async def test_check_rejects_the_standard_count_when_another_was_asked(self, mock_server):
        with pytest.raises(RuntimeError, match="Expected 20000 items but found 5000000"):
            await run_single(make_runner(item_count=20_000), mock_server, keys=MEM_TEST_ITEM_COUNT)

    @pytest.mark.asyncio
    async def test_per_item_math_divides_by_the_count(self, mock_server):
        infos = iter([{"used_memory": "1000000"}] + [{"used_memory": "3000000"}] * 20)
        mock_server.info = AsyncMock(side_effect=lambda section: next(infos) if section == "memory" else {})
        result, _ = await run_single(make_runner(item_count=20_000), mock_server, keys=20_000)
        assert result["per_key_size"] == pytest.approx(2_000_000 / 20_000)

    @pytest.mark.asyncio
    async def test_expire_check_uses_the_count(self, mock_server):
        result, _ = await run_single(make_runner(item_count=20_000, has_expire=True), mock_server, 20_000, 20_000)
        assert result["item_count"] == 20_000
        with pytest.raises(RuntimeError, match="Expected 20000 expires"):
            await run_single(
                make_runner(item_count=20_000, has_expire=True), mock_server, keys=20_000, expires=MEM_TEST_ITEM_COUNT
            )

    @pytest.mark.asyncio
    async def test_churn_tolerance_follows_the_count(self, mock_server):
        runner = make_runner(test="zadd", item_count=20_000, populate_mode="churn")
        result, _ = await run_single(runner, mock_server, keys=1, cardinality=39_999)
        assert result["item_count"] == 39_999
        with pytest.raises(RuntimeError, match="churn produced 40000 items, expected ~20000"):
            await run_single(runner, mock_server, keys=1, cardinality=40_000)
        # The standard count's range does not apply.
        with pytest.raises(RuntimeError, match="churn produced"):
            await run_single(runner, mock_server, keys=1, cardinality=1_000_000)
