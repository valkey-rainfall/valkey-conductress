"""Tests for stable runner identity and additive provenance."""

import json
from unittest.mock import patch

import pytest

from conductress.runner_identity import (
    clear_identity_caches,
    get_result_provenance,
    get_runner_info,
    load_runner_config,
)


@pytest.fixture(autouse=True)
def clear_caches():
    clear_identity_caches()
    yield
    clear_identity_caches()


def test_load_explicit_runner_config(tmp_path):
    path = tmp_path / "runner.json"
    path.write_text(
        json.dumps({"schema_version": 1, "runner_id": "armbench", "display_name": "Graviton 3"}),
        encoding="utf-8",
    )

    config = load_runner_config(path)

    assert config.runner_id == "armbench"
    assert config.display_name == "Graviton 3"


def test_environment_runner_id_overrides_file(monkeypatch, tmp_path):
    monkeypatch.setenv("CONDUCTRESS_RUNNER_ID", "g4bench")
    monkeypatch.setenv("CONDUCTRESS_RUNNER_NAME", "Graviton 4")

    config = load_runner_config(tmp_path / "missing.json")

    assert config.runner_id == "g4bench"
    assert config.display_name == "Graviton 4"


def test_hostname_fallback_preserves_existing_installations(tmp_path):
    with patch("conductress.runner_identity.socket.gethostname", return_value="bench.example.test"):
        config = load_runner_config(tmp_path / "missing.json")

    assert config.runner_id == "bench"
    assert config.display_name == "bench"


@pytest.mark.parametrize("runner_id", ["Uppercase", "bad_id", "-leading", "", "a" * 64])
def test_invalid_runner_id_fails_closed(tmp_path, runner_id):
    path = tmp_path / "runner.json"
    path.write_text(json.dumps({"schema_version": 1, "runner_id": runner_id}), encoding="utf-8")

    with pytest.raises(ValueError, match="runner_id"):
        load_runner_config(path)


def test_unknown_runner_config_field_fails_closed(tmp_path):
    path = tmp_path / "runner.json"
    path.write_text(json.dumps({"schema_version": 1, "runner_id": "bench", "mystery": True}), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown runner config fields"):
        load_runner_config(path)


def test_runner_info_and_result_provenance_share_identity(monkeypatch):
    monkeypatch.setenv("CONDUCTRESS_RUNNER_ID", "armbench")
    with (
        patch(
            "conductress.runner_identity.get_local_platform_info",
            return_value=("arm64", "arm64/c7g.metal/graviton3", ["graviton3", "arm64"]),
        ),
        patch("conductress.runner_identity._short_hostname", return_value="host-a"),
        patch("conductress.runner_identity._instance_id", return_value="i-test"),
        patch("conductress.runner_identity._cpu_model", return_value="Neoverse-V1"),
        patch("conductress.runner_identity._conductress_revision", return_value="deadbeef"),
        patch("conductress.runner_identity.platform.release", return_value="6.1-test"),
        patch("conductress.runner_identity.platform.machine", return_value="aarch64"),
        patch("conductress.runner_identity._read_first", return_value="machine-id"),
    ):
        info = get_runner_info()
        provenance = get_result_provenance()

    assert info["schema_version"] == 1
    assert info["runner_id"] == "armbench"
    assert info["platform"]["aliases"] == ["graviton3", "arm64"]
    assert info["environment"]["instance_id"] == "i-test"
    assert provenance == {
        "provenance_schema_version": 1,
        "runner_id": "armbench",
        "platform": "arm64/c7g.metal/graviton3",
        "environment": info["environment"],
    }
