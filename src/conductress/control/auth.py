"""Bearer-token authentication for operators, runners, and directory users."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Collection, Optional

from .errors import AuthorizationError


@dataclass(frozen=True)
class AuthIdentity:
    role: str
    label: str
    runner_id: Optional[str] = None
    # A user/agent token references a login in the user directory. Operator and
    # runner tokens leave this None. The submit path resolves the login to the
    # caller's full identity (github account, quota, allowed sources, approver
    # role) for provenance and quota decisions.
    login: Optional[str] = None
    # What a user token may do: "submit" (submit and manage own tasks) or "read"
    # (read-only). Operator and runner tokens have no scope.
    scope: Optional[str] = None


USER_SCOPES = {"submit", "read"}
DEFAULT_USER_SCOPE = "submit"


def _parse_scope(role: str, scope: object) -> Optional[str]:
    """Return the scope of a token record. A user token without one gets ``submit``."""
    if role != "user":
        if scope is not None:
            raise ValueError("only a user token may set scope")
        return None
    if scope is None:
        return DEFAULT_USER_SCOPE
    if scope not in USER_SCOPES:
        raise ValueError("token scope must be submit or read")
    return str(scope)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class TokenStore:
    def __init__(self, path: Path):
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("schema_version") != 1 or not isinstance(data.get("tokens"), list):
            raise ValueError("tokens file must use schema_version 1 with a tokens array")
        self._tokens: dict[str, AuthIdentity] = {}
        for record in data["tokens"]:
            unknown = set(record) - {"token_hash", "role", "label", "runner_id", "login", "scope"}
            if unknown:
                raise ValueError(f"unknown token fields: {', '.join(sorted(unknown))}")
            token_hash = record.get("token_hash")
            role = record.get("role")
            label = record.get("label")
            runner_id = record.get("runner_id")
            login = record.get("login")
            if not isinstance(token_hash, str) or len(token_hash) != 64:
                raise ValueError("token_hash must be a SHA-256 hexadecimal digest")
            if role not in {"operator", "runner", "user"}:
                raise ValueError("token role must be operator, runner, or user")
            if not isinstance(label, str) or not label:
                raise ValueError("token label must be a non-empty string")
            if role == "runner" and not runner_id:
                raise ValueError("runner token requires runner_id")
            if role != "runner" and runner_id is not None:
                raise ValueError("only a runner token may set runner_id")
            if role == "user" and not login:
                raise ValueError("user token requires login")
            if role != "user" and login is not None:
                raise ValueError("only a user token may set login")
            if token_hash in self._tokens:
                raise ValueError("duplicate token_hash")
            scope = _parse_scope(role, record.get("scope"))
            self._tokens[token_hash] = AuthIdentity(
                role=role, label=label, runner_id=runner_id, login=login, scope=scope
            )

    def check_logins(self, known_logins: Collection[str]) -> None:
        """Raise ValueError if a user token names a login that is not in the directory."""
        unknown = {t.login for t in self._tokens.values() if t.login is not None} - set(known_logins)
        if unknown:
            raise ValueError(f"tokens name logins missing from the user directory: {', '.join(sorted(unknown))}")

    def authenticate(self, authorization: Optional[str]) -> AuthIdentity:
        if not authorization or not authorization.startswith("Bearer "):
            raise AuthorizationError("AUTH_REQUIRED", "Bearer authentication required", 401)
        token = authorization[len("Bearer ") :]
        presented = hash_token(token)
        identity = self._tokens.get(presented)
        if identity is not None:
            return identity
        raise AuthorizationError("AUTH_INVALID", "invalid bearer token", 401)
