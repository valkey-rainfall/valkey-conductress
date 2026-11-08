"""Configuration for the data-host fleet control service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Largest request body the app accepts (``MAX_BODY_BYTES``). The reverse proxy
# in front of the app must allow at least this much (nginx
# ``client_max_body_size``, see deploy/control-service/nginx-control.conf.example),
# or it answers 413 itself with no JSON body and the runner cannot tell the
# limit from a proxy fault. Runners trim outcomes to stay below this value
# (outcome_size.DEFAULT_MAX_OUTCOME_BYTES, 3 MiB).
DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class ControlConfig:
    database_path: Path
    fleet_manifest_path: Path
    tokens_path: Path
    audit_jsonl_path: Path
    users_path: Optional[Path] = None
    notification_url: Optional[str] = None
    canary_notification_url: Optional[str] = None
    canary_notify_warning: bool = False
    published_tasks_dir: Optional[Path] = None
    # Where canary/<runner>.json and canary/fleet.json go. Separate from the
    # task files: a task record can carry private build and host details, the
    # canary status cannot, so a deployment may publish only the canary files.
    published_canary_dir: Optional[Path] = None
    results_jsonl_path: Optional[Path] = None
    github_token: Optional[str] = None
    github_verification_enabled: bool = False
    canary_profiles_dir: Path = Path(__file__).resolve().parent.parent / "canary_profiles"
    claim_lease_seconds: int = 300
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES

    @classmethod
    def from_env(cls) -> "ControlConfig":
        default_profiles = Path(__file__).resolve().parent.parent / "canary_profiles"
        users_value = os.environ.get("USERS_PATH", "/etc/conductress-control/users.toml")
        published_value = os.environ.get("PUBLISHED_TASKS_DIR")
        canary_value = os.environ.get("PUBLISHED_CANARY_DIR") or published_value
        results_value = os.environ.get("RESULTS_JSONL_PATH", "/var/log/conductress-control/results.jsonl")
        github_token = os.environ.get("GITHUB_TOKEN") or None
        verification = os.environ.get("PROVENANCE_VERIFICATION", "").lower() in {"1", "true", "yes"}
        return cls(
            database_path=Path(os.environ.get("CONTROL_DB_PATH", "/var/lib/conductress-control/control.db")),
            fleet_manifest_path=Path(os.environ.get("FLEET_MANIFEST_PATH", "/etc/conductress-control/fleet.json")),
            tokens_path=Path(os.environ.get("TOKENS_PATH", "/etc/conductress-control/tokens.json")),
            audit_jsonl_path=Path(os.environ.get("AUDIT_JSONL_PATH", "/var/log/conductress-control/audit.jsonl")),
            users_path=Path(users_value) if users_value else None,
            notification_url=os.environ.get("NOTIFICATION_URL") or None,
            canary_notification_url=os.environ.get("CANARY_NOTIFICATION_URL") or None,
            canary_notify_warning=os.environ.get("CANARY_NOTIFY_WARNING", "").lower() in {"1", "true", "yes"},
            published_tasks_dir=Path(published_value) if published_value else None,
            published_canary_dir=Path(canary_value) if canary_value else None,
            results_jsonl_path=Path(results_value) if results_value else None,
            github_token=github_token,
            github_verification_enabled=verification,
            canary_profiles_dir=Path(os.environ.get("CANARY_PROFILES_DIR", str(default_profiles))),
            claim_lease_seconds=int(os.environ.get("CLAIM_LEASE_SECONDS", "300")),
            max_body_bytes=int(os.environ.get("MAX_BODY_BYTES", str(DEFAULT_MAX_BODY_BYTES))),
        )

    def validate(self) -> None:
        if self.claim_lease_seconds < 1:
            raise ValueError("claim_lease_seconds must be positive")
        if self.max_body_bytes < 1024:
            raise ValueError("max_body_bytes must be at least 1024")
