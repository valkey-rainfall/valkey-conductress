"""Tests for sweep_config: focus, pause, resume, and filtering logic."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress.sweep_config import (
    SWEEP_CONFIG_FILE,
    SweepConfig,
    focus,
    load_sweep_config,
    pause,
    resume,
    save_sweep_config,
)


class TestSweepConfig:
    def test_default_allows_everything(self):
        c = SweepConfig()
        assert c.is_allowed("throughput")
        assert c.is_allowed("memory-set-64b")
        assert c.is_allowed("anything")

    def test_focus_allows_only_target(self):
        c = SweepConfig(mode="focus", target="memory-set-64b")
        assert c.is_allowed("memory-set-64b") is True
        assert c.is_allowed("throughput") is False
        assert c.is_allowed("memory-zadd-64b") is False

    def test_paused_blocks_listed(self):
        c = SweepConfig(mode="paused", paused=["throughput", "memory-zadd-64b"])
        assert c.is_allowed("throughput") is False
        assert c.is_allowed("memory-zadd-64b") is False
        assert c.is_allowed("memory-set-64b") is True

    def test_normal_mode_allows_all(self):
        c = SweepConfig(mode="normal")
        assert c.is_allowed("throughput") is True


class TestLoadSaveConfig:
    def test_load_missing_file_returns_default(self, tmp_path, monkeypatch):
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", tmp_path / "nonexistent.json")
        config = load_sweep_config()
        assert config.mode == "normal"

    def test_save_and_load_focus(self, tmp_path, monkeypatch):
        config_file = tmp_path / "sweep_config.json"
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        save_sweep_config(SweepConfig(mode="focus", target="memory-set-64b"))
        loaded = load_sweep_config()
        assert loaded.mode == "focus"
        assert loaded.target == "memory-set-64b"

    def test_save_and_load_paused(self, tmp_path, monkeypatch):
        config_file = tmp_path / "sweep_config.json"
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        save_sweep_config(SweepConfig(mode="paused", paused=["throughput"]))
        loaded = load_sweep_config()
        assert loaded.mode == "paused"
        assert loaded.paused == ["throughput"]

    def test_load_invalid_json_returns_default(self, tmp_path, monkeypatch):
        config_file = tmp_path / "sweep_config.json"
        config_file.write_text("not json{{{")
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        config = load_sweep_config()
        assert config.mode == "normal"


class TestSweepShare:
    @pytest.fixture
    def config_file(self, tmp_path, monkeypatch):
        path = tmp_path / "sweep_config.json"
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", path)
        return path

    def test_off_by_default(self, config_file):
        assert SweepConfig().sweep_share == 0
        assert load_sweep_config().sweep_share == 0  # no file
        config_file.write_text('{"mode": "paused", "paused": ["x"]}')
        assert load_sweep_config().sweep_share == 0  # a file from before the option

    def test_loads_a_share(self, config_file):
        config_file.write_text('{"sweep_share": 25}')
        config = load_sweep_config()
        assert (config.sweep_share, config.mode) == (25.0, "normal")

    @pytest.mark.parametrize("value", ["25", -1, 101, None, True, [25]])
    def test_a_bad_share_means_off(self, config_file, value):
        config_file.write_text(json.dumps({"sweep_share": value}))
        assert load_sweep_config().sweep_share == 0.0

    def test_save_keeps_the_share_and_omits_it_when_off(self, config_file):
        save_sweep_config(SweepConfig(sweep_share=25))
        assert json.loads(config_file.read_text())["sweep_share"] == 25
        save_sweep_config(SweepConfig())
        assert "sweep_share" not in json.loads(config_file.read_text())

    def test_pause_focus_and_resume_do_not_turn_the_share_off(self, config_file):
        save_sweep_config(SweepConfig(sweep_share=25))
        pause(["throughput"])
        assert load_sweep_config().sweep_share == 25 and load_sweep_config().mode == "paused"
        focus("memory-set-64b")
        assert load_sweep_config().sweep_share == 25 and load_sweep_config().mode == "focus"
        resume()
        assert load_sweep_config().sweep_share == 25 and load_sweep_config().mode == "normal"

    def test_resume_without_a_share_still_removes_the_file(self, config_file):
        pause(["throughput"])
        resume()
        assert not config_file.exists()


class TestConvenienceFunctions:
    def test_focus_writes_config(self, tmp_path, monkeypatch):
        config_file = tmp_path / "sweep_config.json"
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        focus("memory-set-64b")
        data = json.loads(config_file.read_text())
        assert data["mode"] == "focus"
        assert data["target"] == "memory-set-64b"

    def test_pause_writes_config(self, tmp_path, monkeypatch):
        config_file = tmp_path / "sweep_config.json"
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        pause(["throughput", "memory-zadd-64b"])
        data = json.loads(config_file.read_text())
        assert data["mode"] == "paused"
        assert data["paused"] == ["throughput", "memory-zadd-64b"]

    def test_resume_removes_file(self, tmp_path, monkeypatch):
        config_file = tmp_path / "sweep_config.json"
        config_file.write_text('{"mode": "focus", "target": "x"}')
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        resume()
        assert not config_file.exists()

    def test_resume_no_file_is_noop(self, tmp_path, monkeypatch):
        config_file = tmp_path / "nonexistent.json"
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)
        resume()  # should not raise


class TestTaskRunnerFiltering:
    """Verify the filtering logic that _notify_queue_empty uses."""

    def test_focus_filters_non_target(self, tmp_path, monkeypatch):
        """Simulate the filtering logic from _notify_queue_empty."""
        config_file = tmp_path / "sweep_config.json"
        config_file.write_text('{"mode": "focus", "target": "memory-set-64b"}')
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        config = load_sweep_config()

        # Simulate subscriber list with workload_ids
        subscribers = [
            MagicMock(workload_id="memory-set-64b"),
            MagicMock(workload_id="throughput"),
            MagicMock(workload_id="memory-zadd-64b"),
        ]

        # Apply the same filter as _notify_queue_empty
        allowed = [s for s in subscribers if config.is_allowed(s.workload_id)]
        assert len(allowed) == 1
        assert allowed[0].workload_id == "memory-set-64b"

    def test_paused_filters_listed(self, tmp_path, monkeypatch):
        config_file = tmp_path / "sweep_config.json"
        config_file.write_text('{"mode": "paused", "paused": ["throughput"]}')
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        config = load_sweep_config()
        subscribers = [
            MagicMock(workload_id="memory-set-64b"),
            MagicMock(workload_id="throughput"),
            MagicMock(workload_id="memory-zadd-64b"),
        ]

        allowed = [s for s in subscribers if config.is_allowed(s.workload_id)]
        assert len(allowed) == 2
        assert all(s.workload_id != "throughput" for s in allowed)

    def test_normal_allows_all(self, tmp_path, monkeypatch):
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", tmp_path / "nope.json")
        config = load_sweep_config()

        subscribers = [MagicMock(workload_id="throughput"), MagicMock(workload_id="memory-set-64b")]
        allowed = [s for s in subscribers if config.is_allowed(s.workload_id)]
        assert len(allowed) == 2

    def test_subscriber_without_workload_id_passes_through(self, tmp_path, monkeypatch):
        """Subscribers without workload_id (e.g. DashboardPublisher) are never filtered."""
        config_file = tmp_path / "sweep_config.json"
        config_file.write_text('{"mode": "focus", "target": "memory-set-64b"}')
        monkeypatch.setattr("conductress.sweep_config.SWEEP_CONFIG_FILE", config_file)

        config = load_sweep_config()
        # Simulate: subscriber has no workload_id (like DashboardPublisher)
        publisher = MagicMock(spec=[])  # no workload_id attribute
        subscribers = [publisher, MagicMock(workload_id="throughput")]

        # Apply same filter as _notify_queue_empty: "if wid and not config.is_allowed(wid): continue"
        allowed = []
        for s in subscribers:
            wid = getattr(s, "workload_id", None)
            if wid and not config.is_allowed(wid):
                continue
            allowed.append(s)

        assert publisher in allowed  # publisher passes through
        assert len(allowed) == 1  # throughput is blocked
