"""Bearer-token authentication for operators and runners."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .errors import AuthorizationError


@dataclass(frozen=True)
class AuthIdentity:
    role: str
    label: str
    runner_id: Optional[str] = None


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class TokenStore:
    def __init__(self, path: Path):
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("schema_version") != 1 or not isinstance(data.get("tokens"), list):
            raise ValueError("tokens file must use schema_version 1 with a tokens array")
        self._tokens: dict[str, AuthIdentity] = {}
        for record in data["tokens"]:
            unknown = set(record) - {"token_hash", "role", "label", "runner_id"}
            if unknown:
                raise ValueError(f"unknown token fields: {', '.join(sorted(unknown))}")
            token_hash = record.get("token_hash")
            role = record.get("role")
            label = record.get("label")
            runner_id = record.get("runner_id")
            if not isinstance(token_hash, str) or len(token_hash) != 64:
                raise ValueError("token_hash must be a SHA-256 hexadecimal digest")
            if role not in {"operator", "runner"}:
                raise ValueError("token role must be operator or runner")
            if not isinstance(label, str) or not label:
                raise ValueError("token label must be a non-empty string")
            if role == "runner" and not runner_id:
                raise ValueError("runner token requires runner_id")
            if role == "operator" and runner_id is not None:
                raise ValueError("operator token must not set runner_id")
            if token_hash in self._tokens:
                raise ValueError("duplicate token_hash")
            self._tokens[token_hash] = AuthIdentity(role=role, label=label, runner_id=runner_id)

    def authenticate(self, authorization: Optional[str]) -> AuthIdentity:
        if not authorization or not authorization.startswith("Bearer "):
            raise AuthorizationError("AUTH_REQUIRED", "Bearer authentication required", 401)
        token = authorization[len("Bearer ") :]
        presented = hash_token(token)
        identity = self._tokens.get(presented)
        if identity is not None:
            return identity
        raise AuthorizationError("AUTH_INVALID", "invalid bearer token", 401)
