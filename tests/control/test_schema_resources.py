import json
from importlib.resources import files

import pytest

SCHEMA_NAMES = [
    "fleet-manifest.schema.json",
    "runner-config.schema.json",
    "runner-status.schema.json",
    "runner-usage.schema.json",
    "task-envelope.schema.json",
    "task-outcome.schema.json",
]


@pytest.mark.parametrize("schema_name", SCHEMA_NAMES)
def test_packaged_schema_is_a_versioned_contract(schema_name):
    packaged = files("conductress.schemas").joinpath(schema_name).read_text(encoding="utf-8")
    document = json.loads(packaged)
    assert document["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert document["$id"].endswith("-v1.json")


@pytest.mark.parametrize(
    "task_id",
    [
        "2026.08.30_22.35.54.757739",
        "canary:armbench:throughput-get-v1:2026-08-30",
        "manual-task_1.2",
    ],
)
def test_task_envelope_schema_accepts_safe_task_ids(task_id):
    from jsonschema import Draft202012Validator

    from conductress.control.schema import load_schema

    Draft202012Validator(load_schema("task-envelope.schema.json")).validate(
        {
            "schema_version": 1,
            "task_id": task_id,
            "runner_id": "armbench",
            "task_class": "manual",
            "priority": 100,
            "submitted_at": "2026-08-30T00:00:00Z",
            "submitted_by": "test",
            "task": {
                "task_type": "PerfTaskData",
                "source": "valkey",
                "specifier": "abc123",
                "timestamp": "2026-08-30T00:00:00.000000",
            },
        }
    )


@pytest.mark.parametrize("task_id", ["../escape", "contains/slash", "contains space", "x" * 201])
def test_task_envelope_schema_rejects_unsafe_task_ids(task_id):
    from jsonschema import Draft202012Validator, ValidationError

    from conductress.control.schema import load_schema

    document = {
        "schema_version": 1,
        "task_id": task_id,
        "runner_id": "armbench",
        "task_class": "manual",
        "priority": 100,
        "submitted_at": "2026-08-30T00:00:00Z",
        "submitted_by": "test",
        "task": {
            "task_type": "PerfTaskData",
            "source": "valkey",
            "specifier": "abc123",
            "timestamp": "2026-08-30T00:00:00.000000",
        },
    }
    with pytest.raises(ValidationError):
        Draft202012Validator(load_schema("task-envelope.schema.json")).validate(document)
