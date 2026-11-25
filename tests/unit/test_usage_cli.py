"""The 'usage' command and its client call."""

import json
from unittest.mock import patch

from conductress import cli
from conductress.fleet_client import FleetClient, FleetClientConfig

REPORT = {
    "schema_version": 1,
    "by": "agent",
    "since": "2026-08-22T00:00:00Z",
    "total_runner_seconds": 900.0,
    "rows": [
        {
            "key": "example-agent",
            "task_count": 2,
            "runner_seconds": 600.0,
            "share": 0.6667,
            "mean_wait_seconds": 12.0,
            "failed_count": 1,
            "cancelled_count": 0,
        },
        {
            "key": "others",
            "task_count": 1,
            "runner_seconds": 300.0,
            "share": 0.3333,
            "mean_wait_seconds": None,
            "failed_count": 0,
            "cancelled_count": 1,
        },
    ],
}


class FakeClient:
    def __init__(self):
        self.calls = []

    def usage(self, **kwargs):
        self.calls.append(kwargs)
        return REPORT


def test_usage_command_prints_a_table(capsys):
    client = FakeClient()
    with patch("conductress.fleet_client.FleetClient.from_user_env", return_value=client):
        assert cli.main(["usage", "--by", "agent", "--since", "7d", "--runner", "armbench"]) == 0
    assert client.calls == [{"by": "agent", "since": "7d", "runner_id": "armbench"}]
    out = capsys.readouterr().out
    assert "example-agent" in out and "others" in out
    assert "66.7%" in out


def test_usage_command_json_output(capsys):
    with patch("conductress.fleet_client.FleetClient.from_user_env", return_value=FakeClient()):
        assert cli.main(["usage", "--json"]) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["command"] == "usage"
    assert document["data"]["rows"][0]["key"] == "example-agent"


def test_client_sends_the_report_query():
    seen = {}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps(REPORT).encode()

    def opener(request, **_kwargs):
        seen["url"] = request.full_url
        return Response()

    client = FleetClient(FleetClientConfig("https://example.test/api/v1", "secret"), opener=opener)
    assert client.usage(by="batch", since="24h")["by"] == "agent"
    assert seen["url"] == "https://example.test/api/v1/usage?by=batch&since=24h"
