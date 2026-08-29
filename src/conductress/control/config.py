"""Configuration for the data-host fleet control service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ControlConfig:
    database_path: Path
    fleet_manifest_path: Path
    tokens_path: Path
    audit_jsonl_path: Path
    claim_lease_seconds: int = 300
    max_body_bytes: int = 64 * 1024

    @classmethod
    def from_env(cls) -> "ControlConfig":
        return cls(
            database_path=Path(os.environ.get("CONTROL_DB_PATH", "/var/lib/conductress-control/control.db")),
            fleet_manifest_path=Path(os.environ.get("FLEET_MANIFEST_PATH", "/etc/conductress-control/fleet.json")),
            tokens_path=Path(os.environ.get("TOKENS_PATH", "/etc/conductress-control/tokens.json")),
            audit_jsonl_path=Path(os.environ.get("AUDIT_JSONL_PATH", "/var/log/conductress-control/audit.jsonl")),
            claim_lease_seconds=int(os.environ.get("CLAIM_LEASE_SECONDS", "300")),
            max_body_bytes=int(os.environ.get("MAX_BODY_BYTES", str(64 * 1024))),
        )

    def validate(self) -> None:
        if self.claim_lease_seconds < 1:
            raise ValueError("claim_lease_seconds must be positive")
        if self.max_body_bytes < 1024:
            raise ValueError("max_body_bytes must be at least 1024")
