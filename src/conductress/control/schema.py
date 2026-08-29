"""Access versioned JSON Schemas packaged with Conductress."""

import json
from importlib.resources import files
from typing import Any


def load_schema(name: str) -> dict[str, Any]:
    resource = files("conductress.schemas").joinpath(name)
    return json.loads(resource.read_text(encoding="utf-8"))
