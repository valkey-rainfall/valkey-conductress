import json

import pytest
from jsonschema.exceptions import ValidationError

from conductress.control.auth import TokenStore, hash_token
from conductress.control.errors import AuthorizationError, ConflictError, NotFoundError
from conductress.control.fleet_registry import FleetRegistry

from .helpers import fleet_manifest


def test_registry_resolves_runner_and_unique_platform():
    registry = FleetRegistry(fleet_manifest())
    assert registry.get_runner("armbench")["display_name"] == "Graviton 3"
    assert registry.resolve_platform("graviton3") == "armbench"
    assert registry.resolve_platform("arm64/c8g.metal/graviton4") == "g4bench"


def test_registry_rejects_unknown_disabled_and_ambiguous():
    manifest = fleet_manifest()
    manifest["runners"].append(
        {
            **manifest["runners"][0],
            "runner_id": "armbench-2",
            "display_name": "Graviton 3 second",
        }
    )
    registry = FleetRegistry(manifest)
    with pytest.raises(NotFoundError):
        registry.get_runner("missing")
    with pytest.raises(ConflictError, match="disabled"):
        registry.get_runner("disabled")
    with pytest.raises(ConflictError, match="multiple runners"):
        registry.resolve_platform("graviton3")


def test_registry_rejects_duplicate_ids_and_bad_schema():
    manifest = fleet_manifest()
    manifest["runners"].append(dict(manifest["runners"][0]))
    with pytest.raises(ValueError, match="duplicate"):
        FleetRegistry(manifest)
    with pytest.raises(ValidationError):
        FleetRegistry({"schema_version": 1, "runners": []})


def test_token_store_authenticates_roles_and_rejects_invalid(tmp_path):
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "tokens": [
                    {
                        "token_hash": hash_token("secret"),
                        "role": "runner",
                        "label": "arm",
                        "runner_id": "armbench",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    store = TokenStore(path)
    identity = store.authenticate("Bearer secret")
    assert identity.role == "runner"
    assert identity.runner_id == "armbench"
    with pytest.raises(AuthorizationError) as missing:
        store.authenticate(None)
    assert missing.value.status == 401
    with pytest.raises(AuthorizationError):
        store.authenticate("Bearer wrong")


def test_token_store_rejects_unknown_fields(tmp_path):
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "tokens": [
                    {
                        "token_hash": hash_token("secret"),
                        "role": "operator",
                        "label": "operator",
                        "runner_id": None,
                        "secret": "must-not-be-here",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unknown token fields"):
        TokenStore(path)
