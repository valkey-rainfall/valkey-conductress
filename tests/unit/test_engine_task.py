"""The engine field of a memory task: serialization, validation, the runner it builds, the CLI flag."""

import json
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from conductress.cli import main
from conductress.config import MANUALLY_UPLOADED, ServerInfo
from conductress.task_envelope import serialize_task
from conductress.task_queue import BaseTaskData
from conductress.tasks.task_mem_efficiency import MemTaskData
from conductress.topology import TopologySpec

from .mem_settle_harness import ScriptedServer, make_runner, run_single


def _task(**kwargs):
    base = dict(
        source="valkey",
        specifier="unstable",
        topology=TopologySpec.standalone(),
        note="n",
        requirements={},
        make_args="",
        type="zadd",
        val_sizes=[16],
        has_expire=False,
    )
    base.update(kwargs)
    return MemTaskData(**base)


@pytest.fixture(autouse=True)
def _sources():
    with (
        patch("conductress.config.REPO_NAMES", ["valkey", "redis"]),
        patch("conductress.task_queue.config.REPO_NAMES", ["valkey", "redis"]),
    ):
        yield


def test_a_default_task_is_serialized_exactly_as_before(tmp_path):
    """The saved document and the envelope body equal asdict() without the new field: nothing new appears."""
    task = _task()
    old_shape = asdict(task)
    del old_shape["engine"]
    assert task.to_document() == old_shape
    path = tmp_path / "t.json"
    task.save_to_file(path)
    saved = json.loads(path.read_text())
    assert "engine" not in saved
    assert saved == {**old_shape, "timestamp": task.timestamp.isoformat()}
    assert "engine" not in serialize_task(task)


def test_other_tasks_have_no_omitted_fields():
    assert BaseTaskData._OMIT_WHEN_DEFAULT == ()


def test_a_non_default_engine_is_saved_and_loaded(tmp_path):
    task = _task(source=MANUALLY_UPLOADED, specifier="/bin/engine", engine="dragonfly")
    assert task.to_document()["engine"] == "dragonfly"
    assert serialize_task(task)["engine"] == "dragonfly"
    path = tmp_path / "t.json"
    task.save_to_file(path)
    loaded = BaseTaskData.from_file(path)
    assert isinstance(loaded, MemTaskData) and loaded.engine == "dragonfly"


def test_an_old_document_without_engine_loads_as_valkey(tmp_path):
    document = _task().to_document()
    document["timestamp"] = _task().timestamp.isoformat()
    assert BaseTaskData.from_dict(document).engine == "valkey"


def test_unknown_engine_and_wrong_source_are_refused():
    with pytest.raises(ValueError, match="Unknown engine"):
        _task(engine="nope")
    with pytest.raises(ValueError, match="needs a source"):
        _task(engine="dragonfly", source="valkey")
    with pytest.raises(ValueError, match="sweep task"):
        _task(engine="dragonfly", source=MANUALLY_UPLOADED, specifier="/b", sweep_commit="a" * 40)


def test_the_runner_takes_its_settle_and_engine_from_the_task():
    info = ServerInfo(ip="127.0.0.1", username="u", name="s")
    valkey = _task().prepare_task_runner([info])
    assert (valkey.engine, valkey.rehash_check) == ("valkey", True)
    other = _task(source=MANUALLY_UPLOADED, specifier="/b", engine="dragonfly").prepare_task_runner([info])
    assert (other.engine, other.rehash_check) == ("dragonfly", False)


@pytest.mark.asyncio
async def test_a_dragonfly_task_starts_its_server_with_the_engine_and_records_it(monkeypatch):
    server = ScriptedServer(debug=False)
    result = await run_single(server, make_runner(engine="dragonfly"), monkeypatch)
    assert server.start_args[1]["engine"] == "dragonfly"
    assert server.start_args[1]["server_args"] == ""
    assert result["engine"] == "dragonfly" and result["rehash_check"] is False


@pytest.mark.asyncio
async def test_a_valkey_task_passes_no_engine_to_the_server(monkeypatch):
    server = ScriptedServer()
    result = await run_single(server, make_runner(), monkeypatch)
    assert "engine" not in server.start_args[1]
    assert "engine" not in result and "rehash_check" not in result


# --- CLI ------------------------------------------------------------------------------------------------


class TestAddMemoryEngineFlag:
    @patch("conductress.cli.TaskQueue")
    def test_default_engine_queues_tasks_without_the_field(self, queue_cls):
        queue = MagicMock()
        queue_cls.return_value = queue
        assert main(["queue", "add-memory", "--source", "valkey", "--specifier", "x", "--types", "set"]) == 0
        task = queue.submit_task.call_args[0][0]
        assert task.engine == "valkey" and "engine" not in task.to_document()

    @patch("conductress.cli.TaskQueue")
    def test_engine_flag_with_a_manual_upload(self, queue_cls, capsys):
        queue = MagicMock()
        queue_cls.return_value = queue
        argv = ["queue", "add-memory", "--source", MANUALLY_UPLOADED, "--specifier", "/x/engine", "--engine"]
        assert main(argv + ["dragonfly", "--types", "zadd", "--settle"]) == 0
        task = queue.submit_task.call_args[0][0]
        assert (task.engine, task.settle) == ("dragonfly", True)
        assert "engine: dragonfly" in capsys.readouterr().out

    @patch("conductress.cli.TaskQueue")
    def test_engine_flag_with_a_git_source_is_refused(self, queue_cls, capsys):
        queue_cls.return_value = MagicMock()
        argv = ["queue", "add-memory", "--source", "valkey", "--specifier", "x", "--engine", "garnet"]
        assert main(argv) == 1
        assert f"--source {MANUALLY_UPLOADED}" in capsys.readouterr().err
        queue_cls.return_value.submit_task.assert_not_called()

    def test_unknown_engine_is_refused_by_the_parser(self):
        with pytest.raises(SystemExit):
            main(["queue", "add-memory", "--source", "valkey", "--specifier", "x", "--engine", "nope"])
