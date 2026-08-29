import json

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from conductress.control.app import create_app
from conductress.control.auth import TokenStore, hash_token
from conductress.control.config import ControlConfig
from conductress.control.db import ControlDatabase
from conductress.control.fleet_registry import FleetRegistry
from conductress.control.service import ControlService

from .helpers import fleet_manifest

OPERATOR_TOKEN = "operator-secret"
ARM_TOKEN = "arm-secret"
G4_TOKEN = "g4-secret"


@pytest.fixture
def control_env(tmp_path):
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
    config = ControlConfig(
        database_path=tmp_path / "control.db",
        fleet_manifest_path=manifest_path,
        tokens_path=tokens_path,
        audit_jsonl_path=tmp_path / "audit.jsonl",
        claim_lease_seconds=300,
        max_body_bytes=4096,
    )
    database = ControlDatabase(config.database_path, config.audit_jsonl_path)
    database.initialize()
    registry = FleetRegistry.from_file(manifest_path)
    token_store = TokenStore(tokens_path)
    service = ControlService(database, registry, config.claim_lease_seconds)
    return {
        "config": config,
        "database": database,
        "registry": registry,
        "token_store": token_store,
        "service": service,
    }


@pytest_asyncio.fixture
async def api_client(control_env):
    app = create_app(
        control_env["config"],
        database=control_env["database"],
        registry=control_env["registry"],
        token_store=control_env["token_store"],
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


@pytest.fixture
def operator_token():
    return OPERATOR_TOKEN


@pytest.fixture
def arm_runner_token():
    return ARM_TOKEN
