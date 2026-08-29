"""Fleet manifest loading, validation, and single-runner resolution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .errors import ConflictError, NotFoundError
from .schema import load_schema


class FleetRegistry:
    def __init__(self, manifest: dict[str, Any]):
        schema = load_schema("fleet-manifest.schema.json")
        Draft202012Validator(schema).validate(manifest)
        runners = manifest["runners"]
        runner_ids = [runner["runner_id"] for runner in runners]
        if len(runner_ids) != len(set(runner_ids)):
            raise ValueError("fleet manifest contains duplicate runner_id values")
        self._manifest = manifest
        self._runners = {runner["runner_id"]: runner for runner in runners}

    @classmethod
    def from_file(cls, path: Path) -> "FleetRegistry":
        return cls(json.loads(path.read_text(encoding="utf-8")))

    def get_runner(self, runner_id: str, *, require_enabled: bool = True) -> dict[str, Any]:
        runner = self._runners.get(runner_id)
        if runner is None:
            raise NotFoundError("RUNNER_NOT_FOUND", f"unknown runner: {runner_id}")
        if require_enabled and not runner["enabled"]:
            raise ConflictError("RUNNER_DISABLED", f"runner is disabled: {runner_id}")
        return dict(runner)

    def list_runners(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        runners = self._manifest["runners"]
        if enabled_only:
            runners = [runner for runner in runners if runner["enabled"]]
        return [dict(runner) for runner in runners]

    def resolve_platform(self, selector: str) -> str:
        matches = []
        for runner in self.list_runners(enabled_only=True):
            if selector == runner["platform"] or selector in runner["platform_aliases"]:
                matches.append(runner["runner_id"])
        if not matches:
            raise NotFoundError("PLATFORM_NOT_FOUND", f"no enabled runner matches platform: {selector}")
        if len(matches) > 1:
            raise ConflictError(
                "PLATFORM_AMBIGUOUS",
                f"platform {selector} matches multiple runners; select --runner explicitly",
            )
        return matches[0]
