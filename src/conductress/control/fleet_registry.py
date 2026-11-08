"""Fleet manifest loading, validation, and single-runner resolution.

A runner entry can be an ARCHIVE: the closed history of a live runner under its
own id (``archived: true``, ``archive_of: <live runner id>``,
``archived_until: <date>``, ``archive_note``). An archive is never enabled, so
the scheduler, claims and status pushes ignore it like any disabled runner; the
canary status reports list it so its series stays readable.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .errors import ConflictError, NotFoundError
from .schema import load_schema

ARCHIVE_KEYS = ("archived", "archive_of", "archived_until", "archive_note")


class FleetRegistry:
    def __init__(self, manifest: dict[str, Any]):
        schema = load_schema("fleet-manifest.schema.json")
        Draft202012Validator(schema).validate(manifest)
        runners = manifest["runners"]
        runner_ids = [runner["runner_id"] for runner in runners]
        if len(runner_ids) != len(set(runner_ids)):
            raise ValueError("fleet manifest contains duplicate runner_id values")
        for runner in runners:
            if not runner.get("archived"):
                continue
            if runner["enabled"]:
                raise ValueError(f"archived runner must not be enabled: {runner['runner_id']}")
            if runner.get("archive_of") not in runner_ids or runner["archive_of"] == runner["runner_id"]:
                raise ValueError(f"archived runner {runner['runner_id']} must name another runner in archive_of")
        self._manifest = manifest
        self._runners = {runner["runner_id"]: runner for runner in runners}

    @classmethod
    def from_file(cls, path: Path) -> "FleetRegistry":
        return cls(json.loads(path.read_text(encoding="utf-8")))

    def is_archived(self, runner_id: str) -> bool:
        runner = self._runners.get(runner_id)
        return bool(runner and runner.get("archived"))

    def get_runner(self, runner_id: str, *, require_enabled: bool = True) -> dict[str, Any]:
        runner = self._runners.get(runner_id)
        if runner is None:
            raise NotFoundError("RUNNER_NOT_FOUND", f"unknown runner: {runner_id}")
        if require_enabled and runner.get("archived"):
            raise ConflictError("RUNNER_ARCHIVED", f"runner is an archive and accepts no work: {runner_id}")
        if require_enabled and not runner["enabled"]:
            raise ConflictError("RUNNER_DISABLED", f"runner is disabled: {runner_id}")
        return dict(runner)

    def list_runners(self, *, enabled_only: bool = False, include_archived: bool = True) -> list[dict[str, Any]]:
        runners = self._manifest["runners"]
        if enabled_only:
            runners = [runner for runner in runners if runner["enabled"]]
        if not include_archived:
            runners = [runner for runner in runners if not runner.get("archived")]
        return [dict(runner) for runner in runners]

    def archives_of(self, runner_id: str) -> list[dict[str, Any]]:
        """The archived entries whose history belongs to ``runner_id``, in manifest order."""
        return [dict(r) for r in self._manifest["runners"] if r.get("archived") and r.get("archive_of") == runner_id]

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
