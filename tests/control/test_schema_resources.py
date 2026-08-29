from importlib.resources import files
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_NAMES = [
    "fleet-manifest.schema.json",
    "runner-config.schema.json",
    "runner-status.schema.json",
    "task-envelope.schema.json",
    "task-outcome.schema.json",
]


@pytest.mark.parametrize("schema_name", SCHEMA_NAMES)
def test_packaged_schema_matches_repository_contract(schema_name):
    packaged = files("conductress.schemas").joinpath(schema_name).read_text(encoding="utf-8")
    repository = (ROOT / "schemas" / schema_name).read_text(encoding="utf-8")
    assert packaged == repository
