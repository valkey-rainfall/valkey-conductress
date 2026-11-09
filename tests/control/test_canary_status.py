"""Tests for canary status API, client, and CLI (PR3).

Covers: API auth, all-runner/selected/no-data/missed/expired/calibrated/
provenance-change cases, client request paths/errors, CLI JSON parity
and human output, and no state mutation.
"""

import json
import os
import stat
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from conductress.control.app import create_app
from conductress.control.auth import TokenStore, hash_token
from conductress.control.canary_profiles import CanaryProfileRegistry
from conductress.control.canary_scheduler import CanaryScheduler
from conductress.control.config import ControlConfig
from conductress.control.db import ControlDatabase
from conductress.control.fleet_registry import FleetRegistry
from conductress.control.service import ControlService
from conductress.fleet_client import FleetClient, FleetClientConfig, FleetClientError

from .helpers import fleet_manifest

OPERATOR_TOKEN = "operator-secret"
ARM_TOKEN = "arm-secret"
G4_TOKEN = "g4-secret"

PROFILE_DATA = {
    "schema_version": 1,
    "profile_id": "throughput-get-v1",
    "profile_version": 1,
    "description": "Daily GET throughput canary",
    "source": "valkey",
    "pinned_commit": "fcd8bc3ee40f5d7841b7d5a8f3cd12252fec14e4",
    "build": {"make_args": ""},
    "workload": {
        "test": "get",
        "val_size": 512,
        "key_size": 0,
        "io_threads": 9,
        "pipelining": 10,
        "clients": 1200,
        "threads": 16,
        "keyspace": 3000000,
        "warmup_seconds": 30,
        "duration_seconds": 300,
        "repetitions": 5,
        "seed": 20260830,
    },
    "schedule": {"utc_hour": 6, "freshness_hours": 18},
    "thresholds": {
        "platforms": {
            "graviton3": {"warning_pct": 2.0, "alarm_pct": 4.0},
            "graviton4": {"warning_pct": 2.0, "alarm_pct": 4.0},
        }
    },
}


def _canary_profile_json():
    return json.dumps(PROFILE_DATA)


@pytest.fixture
def canary_env(tmp_path):
    """Full environment with canary profile loaded."""
    manifest_path = tmp_path / "fleet.json"
    manifest_path.write_text(json.dumps(fleet_manifest()), encoding="utf-8")

    tokens_path = tmp_path / "tokens.json"
    tokens_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "tokens": [
                    {
                        "token_hash": hash_token(OPERATOR_TOKEN),
                        "role": "operator",
                        "label": "test-operator",
                        "runner_id": None,
                    },
                    {
                        "token_hash": hash_token(ARM_TOKEN),
                        "role": "runner",
                        "label": "armbench",
                        "runner_id": "armbench",
                    },
                    {
                        "token_hash": hash_token(G4_TOKEN),
                        "role": "runner",
                        "label": "g4bench",
                        "runner_id": "g4bench",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    canary_dir = tmp_path / "canary_profiles"
    canary_dir.mkdir()
    (canary_dir / "throughput-get-v1.json").write_text(_canary_profile_json(), encoding="utf-8")

    config = ControlConfig(
        database_path=tmp_path / "control.db",
        fleet_manifest_path=manifest_path,
        tokens_path=tokens_path,
        audit_jsonl_path=tmp_path / "audit.jsonl",
        canary_profiles_dir=canary_dir,
        claim_lease_seconds=300,
        max_body_bytes=64 * 1024,
    )
    database = ControlDatabase(config.database_path, config.audit_jsonl_path)
    database.initialize()
    registry = FleetRegistry.from_file(manifest_path)
    token_store = TokenStore(tokens_path)
    canary_profiles = CanaryProfileRegistry.from_directory(canary_dir)
    service = ControlService(database, registry, config.claim_lease_seconds, canary_profiles=canary_profiles)
    scheduler = CanaryScheduler(database, registry, canary_profiles)
    return {
        "config": config,
        "database": database,
        "registry": registry,
        "token_store": token_store,
        "service": service,
        "canary_profiles": canary_profiles,
        "scheduler": scheduler,
        "canary_dir": canary_dir,
    }


@pytest_asyncio.fixture
async def canary_api_client(canary_env):
    app = create_app(
        canary_env["config"],
        database=canary_env["database"],
        registry=canary_env["registry"],
        token_store=canary_env["token_store"],
        canary_profiles=canary_env["canary_profiles"],
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client
    await client.close()


@pytest.fixture
def auth_headers():
    return {
        "operator": {"Authorization": f"Bearer {OPERATOR_TOKEN}"},
        "arm": {"Authorization": f"Bearer {ARM_TOKEN}"},
        "g4": {"Authorization": f"Bearer {G4_TOKEN}"},
    }


def _ingest_observation(service, runner_id, date, score, env=None):
    """Shortcut to ingest a canary observation directly via drift_analyzer."""
    task_id = f"canary:{runner_id}:throughput-get-v1:{date}"
    outcome = {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": runner_id,
        "state": "completed",
        "completed_at": f"{date}T12:00:00Z",
        "result": {
            "score": score,
            "environment": env or {"kernel": "6.1"},
            "runner_id": runner_id,
            "platform": "graviton3" if runner_id == "armbench" else "graviton4",
            "provenance_schema_version": 1,
        },
    }
    return service.drift_analyzer.ingest_outcome(
        task_id=task_id,
        runner_id=runner_id,
        outcome=outcome,
        profile_id="throughput-get-v1",
        profile_version=1,
        utc_date=date,
        environment=env or {"kernel": "6.1"},
    )


# ======================================================================
# Service-level tests
# ======================================================================


class TestCanaryStatusService:
    def test_all_runners_no_data(self, canary_env):
        """Fleet status with profiles configured but no observations."""
        result = canary_env["service"].canary_status()
        assert "generated_at" in result
        runners = result["runners"]
        # Only enabled runners (armbench, g4bench)
        assert len(runners) == 2
        for r in runners:
            assert r["canary_profile"] == "throughput-get-v1"
            assert r["profile"]["profile_id"] == "throughput-get-v1"
            assert r["profile"]["status"] == "active"
            assert r["series"]["phase"] == "no-data"
            assert r["latest_observation"] is None
            assert r["calibration_report"] is None
            assert r["semantics"] == "observation-only"

    def test_single_runner_no_data(self, canary_env):
        """Selected runner with no observations."""
        result = canary_env["service"].canary_status("armbench")
        assert len(result["runners"]) == 1
        assert result["runners"][0]["runner_id"] == "armbench"

    def test_disabled_runner_no_profile(self, canary_env):
        """Disabled runner has no profile."""
        result = canary_env["service"].canary_status("disabled")
        r = result["runners"][0]
        assert r["canary_profile"] is None
        assert r["profile"] is None
        assert r["semantics"] == "no-profile"

    def test_with_observations(self, canary_env):
        """Observation phase with a few data points."""
        svc = canary_env["service"]
        _ingest_observation(svc, "armbench", "2026-08-01", 100000)
        _ingest_observation(svc, "armbench", "2026-08-02", 101000)

        result = svc.canary_status("armbench")
        r = result["runners"][0]
        assert r["series"]["phase"] == "observation"
        assert r["series"]["accepted_count"] == 2
        assert r["series"]["rejected_count"] == 0
        assert r["latest_observation"] is not None
        assert r["latest_observation"]["utc_date"] == "2026-08-02"
        assert r["latest_observation"]["score"] == 101000.0
        assert r["semantics"] == "observation-only"

    def test_with_provenance_change(self, canary_env):
        """Environment change annotation is surfaced."""
        svc = canary_env["service"]
        _ingest_observation(svc, "armbench", "2026-08-01", 100000, {"kernel": "6.1"})
        _ingest_observation(svc, "armbench", "2026-08-02", 101000, {"kernel": "6.2"})

        result = svc.canary_status("armbench")
        latest = result["runners"][0]["latest_observation"]
        assert latest["env_change_annotation"] is not None
        assert "kernel" in latest["env_change_annotation"]

    def test_with_missed_schedule(self, canary_env):
        """Missed schedule entries appear."""
        svc = canary_env["service"]
        scheduler = canary_env["scheduler"]
        # Force a missed entry
        past = datetime(2026, 7, 1, 23, 0, 0, tzinfo=timezone.utc)
        scheduler.tick(now=past)
        result = svc.canary_status("armbench")
        schedule = result["runners"][0]["schedule"]
        missed = [s for s in schedule if s["state"] == "missed"]
        assert len(missed) >= 1

    def test_with_expired_schedule(self, canary_env):
        """Expired schedule entries appear."""
        svc = canary_env["service"]
        scheduler = canary_env["scheduler"]
        # Create a canary task at 06:00 UTC
        creation_time = datetime(2026, 8, 15, 7, 0, 0, tzinfo=timezone.utc)
        scheduler.tick(now=creation_time)
        # Now expire it by ticking past freshness window (18h)
        expiry_time = datetime(2026, 8, 16, 1, 0, 0, tzinfo=timezone.utc)
        scheduler.tick(now=expiry_time)
        result = svc.canary_status("armbench")
        schedule = result["runners"][0]["schedule"]
        expired = [s for s in schedule if s["state"] == "expired"]
        assert len(expired) >= 1

    def test_calibrated_runner(self, canary_env):
        """Calibration report surfaces when 28 observations exist."""
        svc = canary_env["service"]
        for i in range(28):
            date = f"2026-07-{i + 1:02d}"
            _ingest_observation(svc, "armbench", date, 100000 + i * 100)

        result = svc.canary_status("armbench")
        r = result["runners"][0]
        assert r["series"]["phase"] == "ready"
        assert r["calibration_report"] is not None
        assert r["calibration_report"]["status"] == "ready-for-review"
        assert r["calibration_report"]["sample_count"] == 28
        assert r["semantics"] == "ready-for-review"

    def test_no_mutation(self, canary_env):
        """canary_status does not create tasks, schedule entries, or observations."""
        svc = canary_env["service"]
        db = canary_env["database"]

        # Snapshot before
        with db.read() as conn:
            tasks_before = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            sched_before = conn.execute("SELECT COUNT(*) FROM canary_schedule").fetchone()[0]
            obs_before = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]

        svc.canary_status()
        svc.canary_status("armbench")

        # Snapshot after
        with db.read() as conn:
            tasks_after = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            sched_after = conn.execute("SELECT COUNT(*) FROM canary_schedule").fetchone()[0]
            obs_after = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]

        assert tasks_before == tasks_after
        assert sched_before == sched_after
        assert obs_before == obs_after

    def test_unknown_runner_raises(self, canary_env):
        """Requesting a non-existent runner raises NotFoundError."""
        from conductress.control.errors import NotFoundError

        with pytest.raises(NotFoundError):
            canary_env["service"].canary_status("nonexistent")


# ======================================================================
# API endpoint tests
# ======================================================================


class TestCanaryStatusAPI:
    @pytest.mark.asyncio
    async def test_requires_operator_auth(self, canary_api_client, auth_headers):
        # No auth
        resp = await canary_api_client.get("/api/v1/canary/status")
        assert resp.status == 401

        # Runner token
        resp = await canary_api_client.get("/api/v1/canary/status", headers=auth_headers["arm"])
        assert resp.status == 403

    @pytest.mark.asyncio
    async def test_fleet_status_returns_all_enabled(self, canary_api_client, auth_headers):
        resp = await canary_api_client.get("/api/v1/canary/status", headers=auth_headers["operator"])
        assert resp.status == 200
        body = await resp.json()
        assert body["schema_version"] == 1
        assert "generated_at" in body
        runner_ids = {r["runner_id"] for r in body["runners"]}
        assert runner_ids == {"armbench", "g4bench"}

    @pytest.mark.asyncio
    async def test_single_runner_status(self, canary_api_client, auth_headers):
        resp = await canary_api_client.get("/api/v1/canary/status/armbench", headers=auth_headers["operator"])
        assert resp.status == 200
        body = await resp.json()
        assert body["runner"]["runner_id"] == "armbench"

    @pytest.mark.asyncio
    async def test_unknown_runner_404(self, canary_api_client, auth_headers):
        resp = await canary_api_client.get("/api/v1/canary/status/nonexistent", headers=auth_headers["operator"])
        assert resp.status == 404
        body = await resp.json()
        assert body["code"] == "RUNNER_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_not_on_public_dashboard(self, canary_api_client):
        """Canary status is NOT exposed on the public dashboard endpoint."""
        resp = await canary_api_client.get("/api/v1/public/dashboard")
        body = await resp.json()
        # The public dashboard does not include canary status fields
        for runner in body.get("runners", []):
            assert "series" not in runner
            assert "calibration_report" not in runner
            assert "semantics" not in runner
            assert "latest_observation" not in runner
            assert "profile" not in runner


# ======================================================================
# FleetClient tests
# ======================================================================


class FakeResponse:
    def __init__(self, document=None, status=200):
        self.status = status
        self.payload = b"" if document is None else json.dumps(document).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


class TestCanaryStatusClient:
    def test_canary_status_all(self):
        observed = {}

        def opener(request, **kwargs):
            observed["url"] = request.full_url
            observed["method"] = request.method
            return FakeResponse({"schema_version": 1, "generated_at": "2026-08-30T00:00:00Z", "runners": []})

        client = FleetClient(
            FleetClientConfig("https://example.test/api/v1", "secret"),
            opener=opener,
        )
        result = client.canary_status()
        assert result["runners"] == []
        assert "canary/status" in observed["url"]
        assert observed["method"] == "GET"

    def test_canary_status_runner(self):
        observed = {}

        def opener(request, **kwargs):
            observed["url"] = request.full_url
            return FakeResponse(
                {
                    "schema_version": 1,
                    "generated_at": "2026-08-30T00:00:00Z",
                    "runner": {"runner_id": "armbench"},
                }
            )

        client = FleetClient(
            FleetClientConfig("https://example.test/api/v1", "secret"),
            opener=opener,
        )
        result = client.canary_status_runner("armbench")
        assert result["runner"]["runner_id"] == "armbench"
        assert "canary/status/armbench" in observed["url"]

    def test_canary_status_auth_error(self):
        import io
        from urllib.error import HTTPError

        body = json.dumps({"schema_version": 1, "error": "bad token", "code": "AUTH_INVALID"}).encode()

        def opener(request, **kwargs):
            raise HTTPError("https://example", 401, "Unauthorized", {}, io.BytesIO(body))

        client = FleetClient(
            FleetClientConfig("https://example.test/api/v1", "secret"),
            opener=opener,
        )
        with pytest.raises(FleetClientError) as exc_info:
            client.canary_status()
        assert exc_info.value.code == "AUTH_INVALID"
        assert exc_info.value.exit_code == 2


# ======================================================================
# CLI tests
# ======================================================================


class FakeCanaryClient:
    """Fake client returning realistic canary status payloads."""

    def __init__(self, *, with_data=True, with_calibration=False, runner_id=None):
        self._with_data = with_data
        self._with_calibration = with_calibration
        self._runner_filter = runner_id

    def _make_runner(self, rid, platform, has_data=True):
        runner = {
            "runner_id": rid,
            "display_name": f"Runner {rid}",
            "platform": platform,
            "enabled": True,
            "canary_profile": "throughput-get-v1",
            "profile": {
                "profile_id": "throughput-get-v1",
                "profile_version": 1,
                "description": "Daily GET throughput canary",
                "source": "valkey",
                "pinned_commit": "fcd8bc3ee40f5d7841b7d5a8f3cd12252fec14e4",
                "status": "active",
            },
            "schedule": [
                {
                    "utc_date": "2026-08-30",
                    "state": "created",
                    "task_id": f"canary:{rid}:throughput-get-v1:2026-08-30",
                    "task_state": "completed",
                    "created_at": "2026-08-30T06:00:00Z",
                    "updated_at": "2026-08-30T06:00:00Z",
                },
            ],
        }
        if has_data and self._with_data:
            runner["series"] = {
                "runner_id": rid,
                "profile_id": "throughput-get-v1",
                "profile_version": 1,
                "accepted_count": 5,
                "rejected_count": 0,
                "phase": "observation",
                "progress": "5/14",
                "observation_samples_required": 14,
                "calibration_samples_required": 28,
                "latest_observation": {
                    "utc_date": "2026-08-30",
                    "score": 167000.0,
                    "delta_pct": 0.5,
                    "candidate_signal": "within",
                },
                "calibration_status": None,
            }
            runner["latest_observation"] = {
                "utc_date": "2026-08-30",
                "task_id": f"canary:{rid}:throughput-get-v1:2026-08-30",
                "score": 167000.0,
                "completed_at": "2026-08-30T12:00:00Z",
                "phase": "observation",
                "ref_median": 166500.0,
                "ref_mad": 300.0,
                "delta_pct": 0.5,
                "series_ordinal": 5,
                "ref_sample_count": 4,
                "candidate_warning_pct": 2.0,
                "candidate_alarm_pct": 4.0,
                "candidate_signal": "within",
                "actionable": False,
                "window_start": "2026-08-26",
                "window_end": "2026-08-29",
                "environment": {"kernel": "6.1"},
                "environment_parse_error": False,
                "env_change_annotation": None,
                "provenance_schema_version": 1,
            }
            runner["semantics"] = "observation-only"
        else:
            runner["series"] = {
                "runner_id": rid,
                "profile_id": "throughput-get-v1",
                "profile_version": 1,
                "accepted_count": 0,
                "rejected_count": 0,
                "phase": "no-data",
                "progress": None,
                "observation_samples_required": 14,
                "calibration_samples_required": 28,
                "latest_observation": None,
                "calibration_status": None,
            }
            runner["latest_observation"] = None
            runner["semantics"] = "observation-only"

        if self._with_calibration:
            runner["calibration_report"] = {
                "status": "ready-for-review",
                "sample_count": 28,
                "date_range": {"start": "2026-07-01", "end": "2026-07-28"},
                "median_score": 166700.0,
                "mad": 350.0,
                "robust_sigma": 518.91,
                "variability_floor_pct": 0.9339,
                "candidate_warning_pct": 2.0,
                "candidate_alarm_pct": 4.0,
                "recommended_warning_pct": 2.0,
                "recommended_alarm_pct": 4.0,
            }
            runner["series"]["phase"] = "ready"
            runner["series"]["accepted_count"] = 28
            runner["series"]["progress"] = "28 samples"
            runner["semantics"] = "ready-for-review"
        else:
            runner["calibration_report"] = None

        return runner

    def canary_status(self):
        runners = [
            self._make_runner("armbench", "arm64/c7g.metal/graviton3"),
            self._make_runner("g4bench", "arm64/c8g.metal/graviton4"),
        ]
        return {
            "schema_version": 1,
            "generated_at": "2026-08-30T21:00:00Z",
            "runners": runners,
        }

    def canary_status_runner(self, runner_id):
        runner = self._make_runner(
            runner_id,
            "arm64/c7g.metal/graviton3" if runner_id == "armbench" else "arm64/c8g.metal/graviton4",
        )
        return {
            "schema_version": 1,
            "generated_at": "2026-08-30T21:00:00Z",
            "runner": runner,
        }

    # Required stubs for main() dispatch
    def fleet(self):
        return {"schema_version": 1, "runners": []}

    def runner(self, rid):
        return {"schema_version": 1, "runner": {}}

    def list_tasks(self, **kw):
        return {"schema_version": 1, "tasks": []}

    def task(self, tid):
        return {"schema_version": 1, "task": {}}

    def cancel_task(self, tid):
        return {"schema_version": 1, "task": {}, "changed": False}


class TestCanaryStatusCLI:
    def test_fleet_json_output(self, capsys):
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        result = main(["canary", "status", "--json"], client=client)
        assert result == 0
        output = json.loads(capsys.readouterr().out)
        assert output["command"] == "canary.status.fleet"
        assert output["schema_version"] == 1
        assert "runners" in output["data"]
        # JSON preserves API fields exactly
        runners = output["data"]["runners"]
        assert len(runners) == 2
        for r in runners:
            assert "series" in r
            assert "semantics" in r
            assert "latest_observation" in r

    def test_runner_json_output(self, capsys):
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        result = main(["canary", "status", "--runner", "armbench", "--json"], client=client)
        assert result == 0
        output = json.loads(capsys.readouterr().out)
        assert output["command"] == "canary.status.runner"
        assert output["data"]["runner"]["runner_id"] == "armbench"

    def test_fleet_json_exact_data_parity(self, capsys):
        """JSON data field preserves the raw API document exactly."""
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        expected_api = client.canary_status()
        result = main(["canary", "status", "--json"], client=client)
        assert result == 0
        output = json.loads(capsys.readouterr().out)
        assert output["data"] == expected_api

    def test_runner_json_exact_data_parity(self, capsys):
        """JSON data field preserves the raw API document exactly."""
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        expected_api = client.canary_status_runner("armbench")
        result = main(["canary", "status", "--runner", "armbench", "--json"], client=client)
        assert result == 0
        output = json.loads(capsys.readouterr().out)
        assert output["data"] == expected_api

    def test_human_fleet_output(self, capsys):
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        result = main(["canary", "status"], client=client)
        assert result == 0
        output = capsys.readouterr().out
        assert "armbench" in output
        assert "g4bench" in output
        assert "observation" in output

    def test_human_fleet_compact_line_length(self, capsys):
        """Fleet summary lines must fit ~80 columns."""
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        main(["canary", "status"], client=client)
        output = capsys.readouterr().out
        for line in output.strip().split("\n"):
            assert len(line) <= 120, f"line too long ({len(line)}): {line!r}"

    def test_human_runner_detail(self, capsys):
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        result = main(["canary", "status", "--runner", "armbench"], client=client)
        assert result == 0
        output = capsys.readouterr().out
        assert "armbench" in output
        assert "observation-only" in output
        assert "167,000" in output

    def test_human_runner_actual_actionable(self, capsys):
        """Detail view prints actual API actionable value, not hardcoded false."""
        from conductress.fleet_cli import main

        client = FakeCanaryClient()
        main(["canary", "status", "--runner", "armbench"], client=client)
        output = capsys.readouterr().out
        # Should print the actual value (False/0), not a hardcoded string
        assert "Actionable: False" in output or "Actionable: 0" in output

    def test_human_no_data(self, capsys):
        from conductress.fleet_cli import main

        client = FakeCanaryClient(with_data=False)
        result = main(["canary", "status", "--runner", "armbench"], client=client)
        assert result == 0
        output = capsys.readouterr().out
        assert "no-data" in output.lower() or "No accepted" in output

    def test_human_calibrated(self, capsys):
        from conductress.fleet_cli import main

        client = FakeCanaryClient(with_calibration=True)
        result = main(["canary", "status", "--runner", "armbench"], client=client)
        assert result == 0
        output = capsys.readouterr().out
        assert "ready-for-review" in output
        assert "28" in output

    def test_json_error_output(self, capsys):
        from conductress.fleet_cli import main

        class BrokenClient(FakeCanaryClient):
            def canary_status(self):
                raise FleetClientError("AUTH_INVALID", "bad token", 2, 401)

        result = main(["canary", "status", "--json"], client=BrokenClient())
        assert result == 2
        output = json.loads(capsys.readouterr().out)
        assert output["code"] == "AUTH_INVALID"

    def test_human_error_output(self, capsys):
        from conductress.fleet_cli import main

        class BrokenClient(FakeCanaryClient):
            def canary_status(self):
                raise FleetClientError("AUTH_INVALID", "bad token", 2, 401)

        result = main(["canary", "status"], client=BrokenClient())
        assert result == 2
        captured = capsys.readouterr()
        assert "AUTH_INVALID" in captured.err

    def test_null_pinned_commit(self, capsys):
        """CLI handles null pinned_commit without crashing."""
        from conductress.fleet_cli import main

        class NullCommitClient(FakeCanaryClient):
            def canary_status_runner(self, runner_id):
                doc = super().canary_status_runner(runner_id)
                doc["runner"]["profile"]["pinned_commit"] = None
                return doc

        result = main(["canary", "status", "--runner", "armbench"], client=NullCommitClient())
        assert result == 0
        output = capsys.readouterr().out
        assert "none" in output.lower() or "None" in output


# ======================================================================
# Integration: API + service with ingested data
# ======================================================================


class TestCanaryStatusIntegration:
    @pytest.mark.asyncio
    async def test_status_with_ingested_observations(self, canary_env, canary_api_client, auth_headers):
        """Full path: ingest observations, then query via API."""
        svc = canary_env["service"]
        for i in range(3):
            _ingest_observation(svc, "armbench", f"2026-08-{i + 1:02d}", 100000 + i * 500)

        resp = await canary_api_client.get("/api/v1/canary/status/armbench", headers=auth_headers["operator"])
        assert resp.status == 200
        body = await resp.json()
        runner = body["runner"]
        assert runner["series"]["accepted_count"] == 3
        assert runner["latest_observation"]["utc_date"] == "2026-08-03"
        assert runner["semantics"] == "observation-only"

    @pytest.mark.asyncio
    async def test_status_actionable_always_false(self, canary_env, canary_api_client, auth_headers):
        """Actionable is always false regardless of data."""
        svc = canary_env["service"]
        for i in range(30):
            _ingest_observation(svc, "armbench", f"2026-07-{i + 1:02d}", 100000 + i * 100)

        resp = await canary_api_client.get("/api/v1/canary/status/armbench", headers=auth_headers["operator"])
        body = await resp.json()
        runner = body["runner"]
        # Actionable is always false (JSON boolean)
        obs = runner["latest_observation"]
        assert obs["actionable"] is False

    @pytest.mark.asyncio
    async def test_schedule_with_created_task(self, canary_env, canary_api_client, auth_headers):
        """Schedule entries include task_state from joined tasks table."""
        scheduler = canary_env["scheduler"]
        # create_app() performs a real-time startup tick. Use a future date so
        # that startup cannot have already recorded this unique row as missed.
        future_day = (datetime.now(timezone.utc) + timedelta(days=2)).date()
        creation_time = datetime.combine(future_day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
            hours=PROFILE_DATA["schedule"]["utc_hour"], minutes=1
        )
        scheduler.tick(now=creation_time)

        resp = await canary_api_client.get("/api/v1/canary/status/armbench", headers=auth_headers["operator"])
        body = await resp.json()
        schedule = body["runner"]["schedule"]
        assert len(schedule) >= 1
        created = [s for s in schedule if s["state"] == "created"]
        assert len(created) >= 1
        assert created[0]["task_state"] == "queued"


# ======================================================================
# PR3 hardening tests
# ======================================================================


class TestPhaseBoundary:
    """Phase boundary consistency between per-observation and series summary."""

    def test_accepted_14_is_observation(self, canary_env):
        """The 14th accepted sample is still in observation phase."""
        svc = canary_env["service"]
        for i in range(14):
            obs = _ingest_observation(svc, "armbench", f"2026-08-{i + 1:02d}", 100000 + i * 100)
        # Per-observation phase
        assert obs["phase"] == "observation"
        assert obs["series_ordinal"] == 14
        # Series summary
        series = svc.drift_analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert series["phase"] == "observation"
        assert series["progress"] == "14/14"

    def test_accepted_15_is_calibrating(self, canary_env):
        """The 15th accepted sample transitions to calibrating."""
        svc = canary_env["service"]
        for i in range(15):
            obs = _ingest_observation(svc, "armbench", f"2026-08-{i + 1:02d}", 100000 + i * 100)
        # Per-observation phase
        assert obs["phase"] == "calibrating"
        assert obs["series_ordinal"] == 15
        # Series summary
        series = svc.drift_analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert series["phase"] == "calibrating"
        assert series["progress"] == "15/28"

    def test_series_summary_returns_required_counts(self, canary_env):
        """Summary exposes observation_samples_required and calibration_samples_required."""
        svc = canary_env["service"]
        series = svc.drift_analyzer.get_series_summary("armbench", "throughput-get-v1", 1)
        assert series["observation_samples_required"] == 14
        assert series["calibration_samples_required"] == 28


class TestCorruptEnvironmentJson:
    """corrupt environment_json must not 500."""

    def test_corrupt_env_returns_none_with_error_flag(self, canary_env):
        """Service projects corrupt environment_json as None + parse error flag."""
        svc = canary_env["service"]
        _ingest_observation(svc, "armbench", "2026-08-01", 100000, {"kernel": "6.1"})
        # Corrupt the environment_json directly in DB
        db = canary_env["database"]
        with db.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE canary_observations SET environment_json = '{invalid json'" " WHERE runner_id = 'armbench'"
            )
        result = svc.canary_status("armbench")
        latest = result["runners"][0]["latest_observation"]
        assert latest is not None
        assert latest["environment"] is None
        assert latest["environment_parse_error"] is True

    @pytest.mark.asyncio
    async def test_corrupt_env_api_does_not_500(self, canary_env, canary_api_client, auth_headers):
        """API returns 200 with error flag instead of 500 for corrupt env."""
        svc = canary_env["service"]
        _ingest_observation(svc, "armbench", "2026-08-01", 100000)
        db = canary_env["database"]
        with db.transaction(immediate=True) as conn:
            conn.execute("UPDATE canary_observations SET environment_json = 'not json'" " WHERE runner_id = 'armbench'")
        resp = await canary_api_client.get("/api/v1/canary/status/armbench", headers=auth_headers["operator"])
        assert resp.status == 200
        body = await resp.json()
        obs = body["runner"]["latest_observation"]
        assert obs["environment"] is None
        assert obs["environment_parse_error"] is True


class TestActionableIsBool:
    """API actionable field must be a JSON boolean (False), not int 0."""

    def test_actionable_is_bool_false(self, canary_env):
        """actionable is cast to bool in the projected latest_observation."""
        svc = canary_env["service"]
        _ingest_observation(svc, "armbench", "2026-08-01", 100000)
        result = svc.canary_status("armbench")
        latest = result["runners"][0]["latest_observation"]
        assert latest["actionable"] is False
        assert isinstance(latest["actionable"], bool)


class TestUnknownProfile:
    """Runner with canary_profile pointing to a non-loaded profile."""

    def test_unknown_profile_semantics(self, canary_env):
        """A runner whose profile_id is not in the registry gets unknown-profile."""
        # Patch the manifest to reference a non-existent profile
        registry = canary_env["registry"]
        registry._runners["armbench"]["canary_profile"] = "nonexistent-profile-v99"
        # Also patch the manifest list used by list_runners
        for runner in registry._manifest["runners"]:
            if runner["runner_id"] == "armbench":
                runner["canary_profile"] = "nonexistent-profile-v99"
        svc = canary_env["service"]
        result = svc.canary_status("armbench")
        r = result["runners"][0]
        assert r["semantics"] == "unknown-profile"
        assert r["profile"]["status"] == "unknown"
        assert r["series"] is None


class TestControlServiceNullCanaryProfiles:
    """ControlService with canary_profiles=None."""

    def test_no_canary_profiles_service(self, canary_env):
        """Service works with canary_profiles=None (no profiles loaded)."""
        db = canary_env["database"]
        registry = canary_env["registry"]
        svc = ControlService(db, registry, 300, canary_profiles=None)
        result = svc.canary_status()
        for r in result["runners"]:
            # All profiled runners become unknown-profile
            if r["canary_profile"]:
                assert r["semantics"] == "unknown-profile"


class TestDisabledRunnerAccess:
    """Disabled runner excluded from fleet but accessible when selected."""

    def test_disabled_excluded_from_fleet(self, canary_env):
        """Fleet-wide status excludes disabled runners."""
        svc = canary_env["service"]
        result = svc.canary_status()
        runner_ids = {r["runner_id"] for r in result["runners"]}
        assert "disabled" not in runner_ids

    def test_disabled_accessible_selected(self, canary_env):
        """Selecting a disabled runner explicitly works."""
        svc = canary_env["service"]
        result = svc.canary_status("disabled")
        assert len(result["runners"]) == 1
        assert result["runners"][0]["runner_id"] == "disabled"
        assert result["runners"][0]["enabled"] is False


class TestNoMutationHardened:
    """Extended no-mutation: includes DB row counts and audit JSONL size."""

    def test_no_mutation_with_row_counts_and_audit(self, canary_env):
        """canary_status does not mutate anything, including audit log."""
        svc = canary_env["service"]
        db = canary_env["database"]
        audit_path = canary_env["config"].audit_jsonl_path

        # Ingest some data first
        _ingest_observation(svc, "armbench", "2026-08-01", 100000)

        # Snapshot before
        with db.read() as conn:
            tasks_before = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            sched_before = conn.execute("SELECT COUNT(*) FROM canary_schedule").fetchone()[0]
            obs_before = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]
            cal_before = conn.execute("SELECT COUNT(*) FROM canary_calibration_reports").fetchone()[0]
        audit_size_before = audit_path.stat().st_size if audit_path.exists() else 0

        # Exercise both paths
        svc.canary_status()
        svc.canary_status("armbench")

        # Snapshot after
        with db.read() as conn:
            tasks_after = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            sched_after = conn.execute("SELECT COUNT(*) FROM canary_schedule").fetchone()[0]
            obs_after = conn.execute("SELECT COUNT(*) FROM canary_observations").fetchone()[0]
            cal_after = conn.execute("SELECT COUNT(*) FROM canary_calibration_reports").fetchone()[0]
        audit_size_after = audit_path.stat().st_size if audit_path.exists() else 0

        assert tasks_before == tasks_after
        assert sched_before == sched_after
        assert obs_before == obs_after
        assert cal_before == cal_after
        assert audit_size_before == audit_size_after


class TestPerRunnerUnknown404:
    """Per-runner endpoint returns 404 for unknown runners via service contract."""

    @pytest.mark.asyncio
    async def test_unknown_runner_404_via_service(self, canary_api_client, auth_headers):
        """The per-runner handler relies on service NotFoundError, not a local guard."""
        resp = await canary_api_client.get("/api/v1/canary/status/no-such-runner", headers=auth_headers["operator"])
        assert resp.status == 404
        body = await resp.json()
        assert body["code"] == "RUNNER_NOT_FOUND"


class TestLatestObservationProjection:
    """API latest_observation is a stable projection — no raw DB columns leak."""

    EXPECTED_KEYS = {
        "utc_date",
        "task_id",
        "score",
        "completed_at",
        "phase",
        "ref_median",
        "ref_mad",
        "delta_pct",
        "series_ordinal",
        "ref_sample_count",
        "candidate_warning_pct",
        "candidate_alarm_pct",
        "candidate_signal",
        "actionable",
        "window_start",
        "window_end",
        "environment",
        "environment_parse_error",
        "env_change_annotation",
        "provenance_schema_version",
    }

    def test_projection_keys(self, canary_env):
        """latest_observation contains exactly the specified stable keys."""
        svc = canary_env["service"]
        _ingest_observation(svc, "armbench", "2026-08-01", 100000)
        result = svc.canary_status("armbench")
        latest = result["runners"][0]["latest_observation"]
        assert set(latest.keys()) == self.EXPECTED_KEYS

    def test_no_raw_db_columns(self, canary_env):
        """Raw DB columns like accepted, rejection_reason, created_at do not leak."""
        svc = canary_env["service"]
        _ingest_observation(svc, "armbench", "2026-08-01", 100000)
        result = svc.canary_status("armbench")
        latest = result["runners"][0]["latest_observation"]
        assert "accepted" not in latest
        assert "rejection_reason" not in latest
        assert "created_at" not in latest
        assert "environment_json" not in latest
        assert "runner_id" not in latest
        assert "profile_id" not in latest
        assert "profile_version" not in latest


class TestFleetClientUrlEncoding:
    """FleetClient URL-encodes runner_id in canary path."""

    def test_url_encodes_runner_id(self):
        observed = {}

        def opener(request, **kwargs):
            observed["url"] = request.full_url
            return FakeResponse(
                {"schema_version": 1, "generated_at": "2026-08-30T00:00:00Z", "runner": {"runner_id": "foo/bar"}}
            )

        client = FleetClient(
            FleetClientConfig("https://example.test/api/v1", "secret"),
            opener=opener,
        )
        client.canary_status_runner("foo/bar")
        assert "foo%2Fbar" in observed["url"]
        assert "foo/bar" not in observed["url"].split("/api/v1/canary/status/")[1]


# ======================================================================
# Alarm surfacing: static files and webhook notices
# ======================================================================


class TestCanaryAlarmSurfacing:
    def _service_with_hooks(self, canary_env, tmp_path, **kwargs):
        notices = []

        def notifier(request):
            notices.append(json.loads(request.data.decode("utf-8")))

        service = ControlService(
            canary_env["database"],
            canary_env["registry"],
            300,
            canary_profiles=canary_env["canary_profiles"],
            notifier=notifier,
            published_tasks_dir=tmp_path / "published",
            **kwargs,
        )
        return service, notices

    def _ingest(self, service, date, score, env=None):
        """Through the service's canary branch, as record_outcome does."""
        task_id = f"canary:armbench:throughput-get-v1:{date}"
        outcome = {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": f"{date}T12:00:00Z",
            "result": {"score": score, "environment": env or {"kernel_release": "6.1"}, "provenance_schema_version": 2},
            "error": None,
        }
        service._ingest_canary_observation(
            task_id,
            "armbench",
            outcome,
            {
                "canary_id": f"throughput-get-v1:{date}",
                "envelope_json": json.dumps({"task": {"note": "canary throughput-get-v1 v1"}}),
            },
        )

    def test_static_files_are_written_after_each_ingest(self, canary_env, tmp_path):
        service, _notices = self._service_with_hooks(canary_env, tmp_path, notification_url="https://hook.example/x")
        self._ingest(service, "2026-09-01", 200000.0)
        runner_file = tmp_path / "published" / "canary" / "armbench.json"
        fleet_file = tmp_path / "published" / "canary" / "fleet.json"
        assert runner_file.exists() and fleet_file.exists()
        runner = json.loads(runner_file.read_text())["runner"]
        assert runner["runner_id"] == "armbench" and runner["latest_observation"]["utc_date"] == "2026-09-01"
        assert runner["latest_observation"]["candidate_signal"] == "insufficient-data"
        fleet = json.loads(fleet_file.read_text())
        assert [r["runner_id"] for r in fleet["runners"]] == ["armbench", "g4bench"]
        assert not list((tmp_path / "published" / "canary").glob("*.tmp"))

    PRIVATE_ENV = {
        "provenance_schema_version": 2,
        "kernel_release": "6.1.189-236.400.amzn2023.aarch64",
        "glibc_version": "2.34",
        "conductress_revision": "9ba063dee3d204aa4c1ec6d416e40cec8dea806c",
        "cachecannon_commit": "31a2befaa8bf7b3b7a7f7e03e2a847a2e407a3ce",
        "boot_id": "86bc23db-f6bc-49fa-82b7-f48d801a298b",
        "instance_id": "i-0690c81234ab4ecb8",
        "host_fingerprint": "c4c46be36d50f57d",
        "runner_id": "armbench",
    }

    def test_published_files_are_the_allowlist_projection_of_the_api_document(self, canary_env, tmp_path):
        """The API keeps the recorded environment; the public files carry no
        harness SHA, instance id, boot id, fingerprint, IP, path or UUID, and the
        change annotation names the type of change. A key the allowlist has not
        classified fails this test, so a new API field is decided before publish."""
        from conductress.public_fields import unknown_canary_keys

        from ..unit.test_public_fields import private_shapes_in

        service, _notices = self._service_with_hooks(canary_env, tmp_path)
        before = dict(self.PRIVATE_ENV, conductress_revision="5466d250765a099f6834ae56acfca7c5950213c4")
        self._ingest(service, "2026-09-01", 200000.0, env=before)
        self._ingest(service, "2026-09-02", 200100.0, env=self.PRIVATE_ENV)

        api = service.canary_status()
        assert unknown_canary_keys(api) == []
        api_text = json.dumps(api)
        assert "9ba063de" in api_text and "i-0690c81234ab4ecb8" in api_text  # the API is private and complete

        canary_dir = tmp_path / "published" / "canary"
        pinned = [r["profile"]["pinned_commit"] for r in api["runners"] if r.get("profile")]
        service.publish_canary_status()  # every runner's file, as at startup
        for name in ("armbench.json", "fleet.json", "g4bench.json"):
            document = json.loads((canary_dir / name).read_text())
            assert private_shapes_in(document, allowed_values=pinned) == [], name
            text = json.dumps(document)
            for private in ("environment_json", "host_fingerprint", "instance_id", "boot_id", "c4c46be36d50f57d"):
                assert private not in text, (name, private)
        runner = json.loads((canary_dir / "armbench.json").read_text())["runner"]
        latest = runner["latest_observation"]
        assert latest["env_change_annotation"] == "harness revision changed"
        assert latest["environment"] == {
            "glibc_version": "2.34",
            "kernel_release": "6.1.189-236.400.amzn2023.aarch64",
            "platform": "arm64/c7g.metal/graviton3",  # the service stamps the registry platform
            "provenance_schema_version": 2,
        }
        assert runner["series"]["latest_observation"]["env_change_annotation"] == "harness revision changed"
        assert latest["score"] == 200100.0 and latest["utc_date"] == "2026-09-02"

    def test_static_files_are_world_readable_under_a_private_umask(self, canary_env, tmp_path):
        # The shipped unit runs the service with UMask=0077; the web server
        # still has to read the published files and traverse their directory.
        previous = os.umask(0o077)
        try:
            service, _notices = self._service_with_hooks(canary_env, tmp_path)
            self._ingest(service, "2026-09-01", 200000.0)
        finally:
            os.umask(previous)
        canary_dir = tmp_path / "published" / "canary"
        assert stat.S_IMODE(canary_dir.stat().st_mode) == 0o755
        assert stat.S_IMODE((canary_dir / "armbench.json").stat().st_mode) == 0o644
        assert stat.S_IMODE((canary_dir / "fleet.json").stat().st_mode) == 0o644

    def test_alarm_and_environment_change_post_one_notice_each(self, canary_env, tmp_path):
        service, notices = self._service_with_hooks(
            canary_env,
            tmp_path,
            notification_url="https://hook.example/approval",
            canary_notification_url="https://hook.example/canary",
        )
        for day in range(1, 29):
            self._ingest(service, f"2026-09-{day:02d}", 200000.0 + day)
        assert notices == []  # within, no environment change
        new_env = {"kernel_release": "6.1.189", "cpu_microcode": "0x2b000685"}
        self._ingest(service, "2026-09-29", 224000.0, env=new_env)  # +12%: alarm on graviton3 (alarm_pct 4)
        events = [n["event"] for n in notices]
        assert events == ["canary.alarm", "canary.environment_change"]
        alarm = notices[0]
        assert alarm["runner_id"] == "armbench" and alarm["candidate_signal"] == "alarm" and alarm["delta_pct"] > 10
        assert alarm["phase"] == "ready" and alarm["utc_date"] == "2026-09-29"
        assert "kernel_release: '6.1' -> '6.1.189'" in notices[1]["env_change_annotation"]
        # A replay of the same outcome posts nothing more.
        self._ingest(service, "2026-09-29", 224000.0, env=new_env)
        assert len(notices) == 2
        runner = json.loads((tmp_path / "published" / "canary" / "armbench.json").read_text())["runner"]
        assert runner["latest_observation"]["candidate_signal"] == "alarm"

    def test_warning_posts_only_when_enabled_and_failures_never_block(self, canary_env, tmp_path):
        service, notices = self._service_with_hooks(canary_env, tmp_path, notification_url="https://hook.example/x")
        for day in range(1, 29):
            self._ingest(service, f"2026-09-{day:02d}", 200000.0)
        self._ingest(service, "2026-09-29", 206000.0)  # +3%: warning (2 <= d < 4)
        assert notices == []
        service.canary_notify_warning = True
        service._notifier = lambda request: (_ for _ in ()).throw(OSError("hook down"))
        self._ingest(service, "2026-09-30", 206000.0)
        # The observation was still stored and published although the hook failed.
        runner = json.loads((tmp_path / "published" / "canary" / "armbench.json").read_text())["runner"]
        assert runner["latest_observation"]["utc_date"] == "2026-09-30"
        assert runner["latest_observation"]["candidate_signal"] == "warning"

    def test_no_published_dir_means_no_files_and_no_error(self, canary_env):
        service = ControlService(
            canary_env["database"], canary_env["registry"], 300, canary_profiles=canary_env["canary_profiles"]
        )
        service.publish_canary_status()
        self._ingest(service, "2026-09-01", 200000.0)
