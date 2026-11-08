"""Unit tests for the one-time archived-epoch exporter.

The runner no longer builds or loads v1 coordinators (v1 is archived), so a
runner whose publish export dir was wiped has no v1 files and nothing to
regenerate them. ``conductress sweep export-archived --epoch v1`` is the
one-time repair: it loads the v1 state files once, writes the v1 dashboard
files into the export dir, and exits. These tests pin that behaviour and its
guards.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from conductress.sweep.archived_export import export_archived_epoch
from conductress.sweep.planner import BenchmarkPoint, PointStatus, SweepState


@pytest.fixture
def tmp_path():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def _write_v1_primary_state(state_dir: Path) -> None:
    """Write a minimal completed-point state file for the v1 primary GET series."""
    state_dir.mkdir(parents=True, exist_ok=True)
    state = SweepState(merge_commits=["a"], commit_dates={"a": "2024-01-01"})
    state.points["a"] = BenchmarkPoint(
        commit="a", date="2024-01-01", value=2_000_000, cv=0.2, reps=5, status=PointStatus.COMPLETED
    )
    # The primary GET coordinator's label is get-k16-v16-t7-p10.
    state.save(state_dir / "state_get-k16-v16-t7-p10.json")


class TestExportArchivedEpoch:
    def test_rejects_non_archived_epoch(self, tmp_path):
        with patch("conductress.config.SWEEP_ARCHIVED_EPOCHS", ("v1",)):
            with pytest.raises(ValueError, match="not archived"):
                export_archived_epoch("v3", repo_path=tmp_path, output_dir=tmp_path)

    def test_rejects_unsupported_archived_epoch(self, tmp_path):
        with patch("conductress.config.SWEEP_ARCHIVED_EPOCHS", ("v2",)):
            with pytest.raises(ValueError, match="only 'v1'"):
                export_archived_epoch("v2", repo_path=tmp_path, output_dir=tmp_path)

    def test_returns_zero_and_writes_nothing_when_no_state(self, tmp_path):
        """Tolerates a wiped/empty state dir: logs and exports nothing."""
        state_dir = tmp_path / "sweep_data"
        out_dir = tmp_path / "export"
        with (
            patch("conductress.config.SWEEP_ARCHIVED_EPOCHS", ("v1",)),
            patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", state_dir),
            patch("conductress.sweep.memory_coordinator.MEMORY_STATE_DIR", state_dir),
            patch("conductress.sweep.latency_coordinator.LATENCY_STATE_FILE", state_dir / "latency_state.json"),
            patch("conductress.config.SWEEP_ENGINES", []),
            patch("conductress.publisher.detect_platform", return_value=("graviton4", "Graviton 4")),
        ):
            written = export_archived_epoch("v1", repo_path=tmp_path / "valkey", output_dir=out_dir)
        assert written == 0
        assert not list(out_dir.glob("*.json"))

    def test_exports_present_v1_series_and_manifest(self, tmp_path):
        state_dir = tmp_path / "sweep_data"
        out_dir = tmp_path / "export"
        _write_v1_primary_state(state_dir)
        with (
            patch("conductress.config.SWEEP_ARCHIVED_EPOCHS", ("v1",)),
            patch("conductress.sweep.coordinator.SWEEP_STATE_DIR", state_dir),
            patch("conductress.sweep.memory_coordinator.MEMORY_STATE_DIR", state_dir),
            patch("conductress.sweep.latency_coordinator.LATENCY_STATE_FILE", state_dir / "latency_state.json"),
            patch("conductress.config.SWEEP_ENGINES", []),
            patch("conductress.publisher.detect_platform", return_value=("graviton4", "Graviton 4")),
        ):
            written = export_archived_epoch("v1", repo_path=tmp_path / "valkey", output_dir=out_dir)

        assert written >= 1
        # The primary GET series is written under its legacy unqualified name.
        series = out_dir / "series-graviton4-get-k16-v16-t7-p10-throughput.json"
        assert series.exists()
        # The v1 manifest keeps its legacy name and advertises the same epoch
        # list a live publish writes: live epochs first, then archived, each
        # flagged, so the dashboard defaults to a live epoch from this file too.
        manifest = out_dir / "manifest-graviton4.json"
        assert manifest.exists()
        data = json.loads(manifest.read_text())
        assert data["epoch"] == "v1"
        assert [e["id"] for e in data["epochs"]] == ["v3", "v1"]
        assert {e["id"]: e["archived"] for e in data["epochs"]} == {"v3": False, "v1": True}


class TestExportArchivedGeneration:
    def _rig(self, tmp_path):
        from conductress.sweep import generations as gen

        state_dir = tmp_path / "v3"
        gen_dir = state_dir / "generations" / "2026-11-06"
        gen_dir.mkdir(parents=True)
        state = SweepState(merge_commits=["a", "b"], commit_dates={"a": "2026-10-01", "b": "2026-10-02"})
        state.points["a"] = BenchmarkPoint(
            commit="a", date="2026-10-01", value=700_000, cv=1.0, reps=3, status=PointStatus.COMPLETED
        )
        # The canonical v3 GET series label.
        state.save(gen_dir / "state_cachecannon-v3_get-k16-v16-t7-p10.json")
        generations_file = tmp_path / "generations.json"
        gen.save_generations(
            [
                {
                    "id": "2026-11-06",
                    "epoch": "v3",
                    "until": "2026-11-06T16:14:23Z",
                    "label": "OS update",
                    "excluded_tasks": 6,
                }
            ],
            generations_file,
        )
        return state_dir, gen_dir, generations_file

    def test_unknown_generation_or_missing_dir_rejected(self, tmp_path):
        from conductress.sweep import generations as gen
        from conductress.sweep.archived_export import export_archived_generation

        generations_file = tmp_path / "generations.json"
        with pytest.raises(ValueError, match="not recorded"):
            export_archived_generation("2026-01-01", output_dir=tmp_path, generations_file=generations_file)
        gen.save_generations([{"id": "2026-01-01", "epoch": "v3"}], generations_file)
        with pytest.raises(ValueError, match="does not exist"):
            export_archived_generation(
                "2026-01-01", output_dir=tmp_path, state_dir=tmp_path / "v3", generations_file=generations_file
            )

    def test_exports_generation_files_beside_the_live_names(self, tmp_path):
        from conductress.sweep.archived_export import export_archived_generation

        state_dir, _gen_dir, generations_file = self._rig(tmp_path)
        out_dir = tmp_path / "export"
        with (
            patch("conductress.sweep.coordinator_v3.V3_STATE_DIR", state_dir),
            patch("conductress.config.SWEEP_ENGINES", []),
            patch("conductress.publisher.detect_platform", return_value=("intel", "Intel")),
        ):
            written = export_archived_generation(
                "2026-11-06",
                repo_path=tmp_path / "valkey",
                output_dir=out_dir,
                state_dir=state_dir,
                generations_file=generations_file,
            )
        assert written == 1
        series = out_dir / "series-intel-get-k16-v16-t7-p10-throughput.epoch-v3.gen-2026-11-06.json"
        assert series.exists()
        payload = json.loads(series.read_text())
        assert payload["metadata"]["epoch"] == "v3" and payload["metadata"]["generation"] == "2026-11-06"
        assert [p["commit"] for p in payload["points"]] == ["a"]
        # No live file was written or touched.
        assert not (out_dir / "series-intel-get-k16-v16-t7-p10-throughput.epoch-v3.json").exists()
        notable = out_dir / "notable-intel.epoch-v3.gen-2026-11-06.json"
        assert notable.exists()
        manifest = json.loads((out_dir / "manifest-intel.epoch-v3.gen-2026-11-06.json").read_text())
        assert manifest["generation"]["id"] == "2026-11-06" and manifest["epoch"] == "v3"
        assert manifest["generations"][0]["label"] == "OS update"
        assert ("get-k16-v16-t7-p10") in manifest["throughput_workloads"]
