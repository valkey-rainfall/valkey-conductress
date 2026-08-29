import json

from conductress.fleet_client import FleetClient, FleetClientConfig


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


def test_runner_config_reads_owner_only_token_file(monkeypatch, tmp_path):
    token_file = tmp_path / "runner.token"
    token_file.write_text("runner-secret\n", encoding="utf-8")
    token_file.chmod(0o600)
    monkeypatch.delenv("CONDUCTRESS_RUNNER_TOKEN", raising=False)
    monkeypatch.setenv("CONDUCTRESS_RUNNER_TOKEN_FILE", str(token_file))

    config = FleetClientConfig.from_runner_env()
    assert config.token == "runner-secret"


def test_runner_scoped_methods_use_expected_routes_and_idempotency_headers():
    observed = []
    responses = [
        {"schema_version": 1, "claim": {"task": {"task_id": "t1"}}},
        {"schema_version": 1, "task": {"task_id": "t1"}, "changed": True},
        {"schema_version": 1, "task": {"task_id": "t1"}, "changed": True},
        {"schema_version": 1, "updated": True},
    ]

    def opener(request, **_kwargs):
        observed.append(request)
        return FakeResponse(responses.pop(0))

    client = FleetClient(FleetClientConfig("https://example.test/api/v1", "secret"), opener=opener)
    assert client.claim_task("armbench")["claim"]["task"]["task_id"] == "t1"
    client.accept_task("t1", "claim-token")
    client.report_outcome(
        "t1",
        {
            "schema_version": 1,
            "task_id": "t1",
            "runner_id": "armbench",
            "state": "completed",
            "completed_at": "2026-08-29T00:00:00Z",
            "result": {},
            "error": None,
        },
    )
    client.push_status("armbench", {"timestamp": "2026-08-29T00:00:00Z"})

    assert [request.full_url for request in observed] == [
        "https://example.test/api/v1/runners/armbench/claim",
        "https://example.test/api/v1/tasks/t1/accept",
        "https://example.test/api/v1/tasks/t1/complete",
        "https://example.test/api/v1/runners/armbench/status",
    ]
    assert all(request.get_header("Idempotency-key") for request in observed)
    assert json.loads(observed[1].data) == {"claim_token": "claim-token"}


def test_claim_204_returns_none():
    client = FleetClient(
        FleetClientConfig("https://example.test/api/v1", "secret"),
        opener=lambda *_args, **_kwargs: FakeResponse(status=204),
    )
    assert client.claim_task("armbench") is None
