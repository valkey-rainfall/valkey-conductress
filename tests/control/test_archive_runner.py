"""Archiving a runner's canary history under a new id (control-plane half of a series split)."""

import json
import sqlite3
from pathlib import Path

import pytest

from conductress.control.archive import archive_runner, backup_database, count_series_rows, manifest_entry
from conductress.control.canary_profiles import CanaryProfileRegistry
from conductress.control.db import ControlDatabase
from conductress.control.drift_analyzer import PHASE_OBSERVATION, DriftAnalyzer
from conductress.control.errors import ConflictError
from conductress.control.fleet_registry import FleetRegistry
from conductress.control.service import ControlService

from .helpers import fleet_manifest
from .test_drift_analyzer import _ingest_n, _valid_profile

RUNNER = "armbench"
ARCHIVE = "armbench-pre-2026-09-21"
UNTIL = "2026-09-20"


def _archive_entry(until=UNTIL, archive_of=RUNNER, enabled=False):
    return {
        "runner_id": ARCHIVE,
        "display_name": "Graviton 3 (until 2026-09-20)",
        "platform": "arm64/c7g.metal/graviton3",
        "platform_aliases": [],
        "enabled": enabled,
        "archived": True,
        "archive_of": archive_of,
        "archived_until": until,
        "archive_note": "kernel update",
        "canary_profile": "throughput-get-v1",
        "status_ttl_seconds": 900,
    }


@pytest.fixture
def rig(tmp_path):
    """A DB with 30 armbench observations (ready phase) plus a report, a registry and an analyzer."""
    manifest = fleet_manifest()
    manifest_path = tmp_path / "fleet.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    db = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
    db.initialize()
    registry = FleetRegistry.from_file(manifest_path)
    canary_dir = tmp_path / "canary_profiles"
    canary_dir.mkdir()
    (canary_dir / "throughput-get-v1.json").write_text(json.dumps(_valid_profile()), encoding="utf-8")
    profiles = CanaryProfileRegistry.from_directory(canary_dir)
    analyzer = DriftAnalyzer(db, canary_profiles=profiles, fleet_registry=registry)
    # 20 days before the cut, then a big step: days 21-23 are "after" rows.
    _ingest_n(analyzer, 20, runner_id=RUNNER)
    _ingest_n(analyzer, 3, runner_id=RUNNER, base_score=240000.0, start_day=21)
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO canary_schedule(runner_id, profile_id, utc_date, state, task_id, created_at, updated_at) "
            "VALUES (?, 'throughput-get-v1', '2026-09-10', 'created', 'x', 't', 't')",
            (RUNNER,),
        )
        conn.execute(
            "INSERT INTO canary_schedule(runner_id, profile_id, utc_date, state, task_id, created_at, updated_at) "
            "VALUES (?, 'throughput-get-v1', '2026-09-22', 'created', 'y', 't', 't')",
            (RUNNER,),
        )
        conn.execute(
            "INSERT INTO canary_calibration_reports(runner_id, profile_id, profile_version, sample_count, "
            "date_range_start, date_range_end, median_score, mad, robust_sigma, variability_floor_pct, "
            "recommended_warning_pct, recommended_alarm_pct, status, report_json, created_at) "
            "VALUES (?, 'throughput-get-v1', 1, 20, '2026-09-01', '2026-09-20', 1, 1, 1, 1, 1, 2, 'accepted', '{}', 't')",
            (RUNNER,),
        )
    return {
        "db": db,
        "registry": registry,
        "analyzer": analyzer,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "tmp": tmp_path,
    }


def _rows(db, runner_id):
    conn = db.connect()
    try:
        return count_series_rows(conn, runner_id)
    finally:
        conn.close()


class TestArchiveRunner:
    def test_dry_run_counts_and_changes_nothing(self, rig):
        before = _rows(rig["db"], RUNNER)
        result = archive_runner(
            rig["db"], runner_id=RUNNER, archive_id=ARCHIVE, until=UNTIL, note="n", actor="t", dry_run=True
        )
        assert result.counts == {"canary_observations": 20, "canary_schedule": 1, "canary_calibration_reports": 1}
        assert result.reset_rows == 3
        assert _rows(rig["db"], RUNNER) == before and _rows(rig["db"], ARCHIVE) == {t: 0 for t in before}
        assert any("Would rename 20" in line for line in result.summary_lines())

    def test_renames_history_resets_live_rows_and_audits(self, rig):
        backup = rig["tmp"] / "backup.db"
        live = rig["registry"].get_runner(RUNNER)
        result = archive_runner(
            rig["db"],
            runner_id=RUNNER,
            archive_id=ARCHIVE,
            until=UNTIL,
            note="kernel update",
            actor="operator:test",
            live_runner=live,
            backup=backup,
        )
        assert result.after == {"canary_observations": 20, "canary_schedule": 1, "canary_calibration_reports": 1}
        assert _rows(rig["db"], RUNNER) == {
            "canary_observations": 3,
            "canary_schedule": 1,
            "canary_calibration_reports": 0,
        }
        conn = rig["db"].connect()
        try:
            live_rows = conn.execute(
                "SELECT utc_date, phase, series_ordinal, ref_sample_count, delta_pct, ref_median, candidate_signal, environment_json "
                "FROM canary_observations WHERE runner_id = ? ORDER BY utc_date",
                (RUNNER,),
            ).fetchall()
            assert [r["utc_date"] for r in live_rows] == ["2026-09-21", "2026-09-22", "2026-09-23"]
            assert [r["series_ordinal"] for r in live_rows] == [1, 2, 3]
            assert all(
                r["phase"] == PHASE_OBSERVATION and r["delta_pct"] is None and r["ref_median"] is None
                for r in live_rows
            )
            assert [r["ref_sample_count"] for r in live_rows] == [0, 1, 2]
            assert all(r["candidate_signal"] == "insufficient-data" for r in live_rows)
            archived = conn.execute(
                "SELECT MIN(utc_date), MAX(utc_date), MAX(series_ordinal) FROM canary_observations WHERE runner_id = ?",
                (ARCHIVE,),
            ).fetchone()
            assert tuple(archived) == ("2026-09-01", "2026-09-20", 20)
            audit = conn.execute("SELECT * FROM audit_log WHERE action = 'runner.archive'").fetchall()
            assert len(audit) == 1 and audit[0]["runner_id"] == RUNNER
            detail = json.loads(audit[0]["detail_json"])
            assert detail["archive_id"] == ARCHIVE and detail["renamed"]["canary_observations"] == 20
            assert detail["backup"] == str(backup)
        finally:
            conn.close()
        # The backup holds the pre-archive state.
        copy = sqlite3.connect(backup)
        assert (
            copy.execute("SELECT COUNT(*) FROM canary_observations WHERE runner_id = ?", (RUNNER,)).fetchone()[0] == 23
        )
        copy.close()
        assert result.manifest_entry["runner_id"] == ARCHIVE and result.manifest_entry["archive_of"] == RUNNER
        assert result.manifest_entry["enabled"] is False and result.manifest_entry["archived"] is True

    def test_next_live_ingest_starts_a_fresh_series(self, rig):
        archive_runner(rig["db"], runner_id=RUNNER, archive_id=ARCHIVE, until=UNTIL, note="n", actor="t")
        obs = _ingest_n(rig["analyzer"], 1, runner_id=RUNNER, base_score=241000.0, start_day=24)[0]
        assert obs["series_ordinal"] == 4 and obs["phase"] == PHASE_OBSERVATION
        # The window now holds only the three post-cut rows: the reference is their median, not the old 200k series.
        assert obs["ref_sample_count"] == 3 and 239_000 < obs["ref_median"] < 241_000

    def test_refusals(self, rig):
        with pytest.raises(ValueError, match="differ"):
            archive_runner(rig["db"], runner_id=RUNNER, archive_id=RUNNER, until=UNTIL, note="n", actor="t")
        with pytest.raises(ValueError, match="lowercase"):
            archive_runner(rig["db"], runner_id=RUNNER, archive_id="Bad Id", until=UNTIL, note="n", actor="t")
        with pytest.raises(ValueError):
            archive_runner(rig["db"], runner_id=RUNNER, archive_id=ARCHIVE, until="20-09-2026", note="n", actor="t")
        archive_runner(rig["db"], runner_id=RUNNER, archive_id=ARCHIVE, until=UNTIL, note="n", actor="t")
        with pytest.raises(ValueError, match="already has rows"):
            archive_runner(rig["db"], runner_id=RUNNER, archive_id=ARCHIVE, until=UNTIL, note="n", actor="t")

    def test_backup_refuses_to_overwrite(self, rig):
        backup = rig["tmp"] / "b.db"
        backup_database(rig["db"], backup)
        with pytest.raises(FileExistsError):
            backup_database(rig["db"], backup)


class TestRegistryAndStatus:
    def test_archived_entry_is_listed_but_accepts_no_work(self, rig):
        manifest = rig["manifest"]
        manifest["runners"].append(_archive_entry())
        registry = FleetRegistry(manifest)
        assert registry.is_archived(ARCHIVE) and not registry.is_archived(RUNNER)
        with pytest.raises(ConflictError) as exc:
            registry.get_runner(ARCHIVE)
        assert exc.value.code == "RUNNER_ARCHIVED"
        assert registry.get_runner(ARCHIVE, require_enabled=False)["archive_of"] == RUNNER
        assert ARCHIVE not in [r["runner_id"] for r in registry.list_runners(enabled_only=True)]
        assert ARCHIVE not in [r["runner_id"] for r in registry.list_runners(include_archived=False)]
        assert ARCHIVE in [r["runner_id"] for r in registry.list_runners()]
        assert [a["runner_id"] for a in registry.archives_of(RUNNER)] == [ARCHIVE]
        assert registry.resolve_platform("graviton3") == RUNNER  # the archive has no aliases and is disabled

    def test_registry_rejects_bad_archive_entries(self, rig):
        for broken, message in (
            (_archive_entry(enabled=True), "must not be enabled"),
            (_archive_entry(archive_of="nobody"), "archive_of"),
            (dict(_archive_entry(), archive_of=ARCHIVE), "archive_of"),
        ):
            manifest = fleet_manifest()
            manifest["runners"].append(broken)
            with pytest.raises(ValueError, match=message):
                FleetRegistry(manifest)

    def test_canary_status_lists_archives_after_live_runners(self, rig):
        archive_runner(rig["db"], runner_id=RUNNER, archive_id=ARCHIVE, until=UNTIL, note="kernel update", actor="t")
        manifest = rig["manifest"]
        manifest["runners"].append(_archive_entry())
        registry = FleetRegistry(manifest)
        service = ControlService(rig["db"], registry, 300, canary_profiles=rig["analyzer"]._canary_profiles)
        status = service.canary_status()
        ids = [r["runner_id"] for r in status["runners"]]
        assert ids[-1] == ARCHIVE and RUNNER in ids and "disabled" not in ids
        archive = status["runners"][-1]
        assert archive["archived"] is True and archive["archive_of"] == RUNNER and archive["archived_until"] == UNTIL
        assert archive["series"]["phase"] is not None
        assert archive["latest_observation"]["utc_date"] == "2026-09-20"
        live = next(r for r in status["runners"] if r["runner_id"] == RUNNER)
        assert live["archived"] is False
        assert live["archives"] == [{"runner_id": ARCHIVE, "archived_until": UNTIL, "archive_note": "kernel update"}]
        assert (
            live["latest_observation"]["utc_date"] == "2026-09-23"
            and live["latest_observation"]["phase"] == PHASE_OBSERVATION
        )


def test_manifest_entry_shape():
    entry = manifest_entry(
        {"runner_id": "intelbench", "display_name": "Intel", "platform": "intel/x", "canary_profile": "p"},
        "intelbench-pre-2026-11-07",
        "2026-11-06",
        "kernel + glibc + microcode",
    )
    FleetRegistry(
        {
            "schema_version": 1,
            "runners": [
                {
                    "runner_id": "intelbench",
                    "display_name": "Intel",
                    "platform": "intel/x",
                    "platform_aliases": ["intel"],
                    "enabled": True,
                },
                entry,
            ],
        }
    )
    assert entry["display_name"] == "Intel (until 2026-11-06)" and entry["platform_aliases"] == []
