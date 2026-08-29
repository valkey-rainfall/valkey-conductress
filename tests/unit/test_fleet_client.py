import io
import json
from urllib.error import HTTPError, URLError

import pytest

from conductress.fleet_client import FleetClient, FleetClientConfig, FleetClientError


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


def test_config_requires_token_and_https(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("CONDUCTRESS_OPERATOR_TOKEN", raising=False)
    monkeypatch.delenv("CONDUCTRESS_OPERATOR_TOKEN_FILE", raising=False)
    with pytest.raises(FleetClientError) as missing:
        FleetClientConfig.from_env()
    assert missing.value.code == "OPERATOR_TOKEN_MISSING"

    monkeypatch.setenv("CONDUCTRESS_OPERATOR_TOKEN", "secret")
    monkeypatch.setenv("CONDUCTRESS_CONTROL_URL", "http://remote.example/api/v1")
    with pytest.raises(FleetClientError) as insecure:
        FleetClientConfig.from_env()
    assert insecure.value.code == "CONTROL_URL_INSECURE"


def test_config_reads_owner_only_token_file(monkeypatch, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("secret\n", encoding="utf-8")
    token_file.chmod(0o600)
    monkeypatch.delenv("CONDUCTRESS_OPERATOR_TOKEN", raising=False)
    monkeypatch.setenv("CONDUCTRESS_OPERATOR_TOKEN_FILE", str(token_file))
    config = FleetClientConfig.from_env()
    assert config.token == "secret"

    token_file.chmod(0o644)
    with pytest.raises(FleetClientError) as permissions:
        FleetClientConfig.from_env()
    assert permissions.value.code == "TOKEN_FILE_PERMISSIONS"


def test_config_rejects_missing_ca_bundle(monkeypatch, tmp_path):
    monkeypatch.setenv("CONDUCTRESS_OPERATOR_TOKEN", "secret")
    monkeypatch.setenv("CONDUCTRESS_CONTROL_CA_BUNDLE", str(tmp_path / "missing.pem"))
    with pytest.raises(FleetClientError) as error:
        FleetClientConfig.from_env()
    assert error.value.code == "CA_BUNDLE_NOT_FOUND"


def test_client_builds_authenticated_request_and_parses_versioned_json():
    observed = {}

    def opener(request, **kwargs):
        observed["request"] = request
        observed.update(kwargs)
        return FakeResponse({"schema_version": 1, "runners": []})

    client = FleetClient(
        FleetClientConfig("https://example.test/api/v1", "secret", timeout_seconds=7),
        opener=opener,
    )
    document = client.fleet()
    assert document["runners"] == []
    assert observed["request"].get_header("Authorization") == "Bearer secret"
    assert observed["timeout"] == 7
    assert observed["context"].check_hostname is True


def test_submit_sends_json_and_idempotency_key():
    observed = {}

    def opener(request, **_kwargs):
        observed["request"] = request
        return FakeResponse({"schema_version": 1, "task": {"task_id": "t1"}, "created": True})

    client = FleetClient(FleetClientConfig("https://example.test/api/v1", "secret"), opener=opener)
    client.submit_task({"task_id": "t1"}, "runner:t1")
    request = observed["request"]
    assert request.method == "POST"
    assert request.get_header("Idempotency-key") == "runner:t1"
    assert json.loads(request.data) == {"task_id": "t1"}


def test_api_errors_map_to_stable_exit_codes():
    body = json.dumps({"schema_version": 1, "error": "ambiguous", "code": "PLATFORM_AMBIGUOUS"}).encode()

    def opener(_request, **_kwargs):
        raise HTTPError("https://example", 409, "Conflict", {}, io.BytesIO(body))

    client = FleetClient(FleetClientConfig("https://example.test/api/v1", "secret"), opener=opener)
    with pytest.raises(FleetClientError) as error:
        client.fleet()
    assert error.value.code == "PLATFORM_AMBIGUOUS"
    assert error.value.exit_code == 4


def test_network_errors_retry_then_fail():
    attempts = []
    sleeps = []

    def opener(_request, **_kwargs):
        attempts.append(1)
        raise URLError("offline")

    client = FleetClient(
        FleetClientConfig("https://example.test/api/v1", "secret"),
        opener=opener,
        sleeper=sleeps.append,
    )
    with pytest.raises(FleetClientError) as error:
        client.fleet()
    assert error.value.code == "CONTROL_UNREACHABLE"
    assert error.value.exit_code == 3
    assert len(attempts) == 3
    assert sleeps == [0.25, 0.5]


def test_unsupported_or_invalid_response_is_rejected():
    for document, expected in (({"schema_version": 2}, "API_VERSION_UNSUPPORTED"), (b"bad", "RESPONSE_INVALID")):

        def opener(_request, **_kwargs):
            if isinstance(document, bytes):
                response = FakeResponse()
                response.payload = document
                return response
            return FakeResponse(document)

        client = FleetClient(FleetClientConfig("https://example.test/api/v1", "secret"), opener=opener)
        with pytest.raises(FleetClientError) as error:
            client.fleet()
        assert error.value.code == expected


def test_delete_network_failure_is_not_retried():
    attempts = []

    def opener(_request, **_kwargs):
        attempts.append(1)
        raise URLError("uncertain cancellation")

    client = FleetClient(
        FleetClientConfig("https://example.test/api/v1", "secret"),
        opener=opener,
        sleeper=lambda _delay: None,
    )
    with pytest.raises(FleetClientError) as error:
        client.cancel_task("task-1")
    assert error.value.code == "CONTROL_UNREACHABLE"
    assert len(attempts) == 1
