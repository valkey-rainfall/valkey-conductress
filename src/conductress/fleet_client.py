"""Synchronous stdlib client for the Conductress control API."""

from __future__ import annotations

import json
import os
import socket
import ssl
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_CONTROL_URL = "https://data.conductress.rainsupreme.net/api/v1"
CLIENT_SCHEMA_VERSION = 1


class FleetClientError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        exit_code: int,
        status: Optional[int] = None,
        details: Optional[dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.exit_code = exit_code
        self.status = status
        self.details = details


def _api_exit_code(status: int, code: str) -> int:
    if status in {401, 403} or code.startswith("AUTH_") or code.endswith("_REQUIRED"):
        return 2
    if status == 409 or code in {"RUNNER_DISABLED", "PLATFORM_AMBIGUOUS"}:
        return 4
    if status >= 500:
        return 3
    return 1


def _load_token(
    env_name: str,
    file_env_name: str,
    default_file: Path,
    label: str,
    missing_code: str,
) -> str:
    token = os.environ.get(env_name)
    token_file_value = os.environ.get(file_env_name)
    token_file = Path(token_file_value).expanduser() if token_file_value else default_file
    if not token and token_file.exists():
        mode = stat.S_IMODE(token_file.stat().st_mode)
        if mode & 0o077:
            raise FleetClientError(
                "TOKEN_FILE_PERMISSIONS",
                f"{label} token file must not be group/world accessible: {token_file}",
                2,
            )
        token = token_file.read_text(encoding="utf-8").strip()
    if not token:
        raise FleetClientError(missing_code, f"set {env_name} or {file_env_name}", 2)
    return token


@dataclass(frozen=True)
class FleetClientConfig:
    base_url: str
    token: str
    timeout_seconds: float = 10.0
    ca_bundle: Optional[Path] = None

    @classmethod
    def from_env(cls) -> "FleetClientConfig":
        token = _load_token(
            "CONDUCTRESS_OPERATOR_TOKEN",
            "CONDUCTRESS_OPERATOR_TOKEN_FILE",
            Path.home() / ".config" / "conductress" / "operator.token",
            "operator",
            "OPERATOR_TOKEN_MISSING",
        )
        return cls._from_token(token)

    @classmethod
    def from_runner_env(cls) -> "FleetClientConfig":
        token = _load_token(
            "CONDUCTRESS_RUNNER_TOKEN",
            "CONDUCTRESS_RUNNER_TOKEN_FILE",
            Path.home() / ".config" / "conductress" / "runner.token",
            "runner",
            "RUNNER_TOKEN_MISSING",
        )
        return cls._from_token(token)

    @classmethod
    def _from_token(cls, token: str) -> "FleetClientConfig":
        base_url = os.environ.get("CONDUCTRESS_CONTROL_URL", DEFAULT_CONTROL_URL).rstrip("/")
        if not base_url.startswith("https://") and not (
            base_url.startswith("http://127.0.0.1") or base_url.startswith("http://localhost")
        ):
            raise FleetClientError(
                "CONTROL_URL_INSECURE",
                "control URL must use HTTPS (HTTP is allowed only for localhost)",
                1,
            )
        try:
            timeout = float(os.environ.get("CONDUCTRESS_CONTROL_TIMEOUT", "10"))
        except ValueError as exc:
            raise FleetClientError("TIMEOUT_INVALID", "CONDUCTRESS_CONTROL_TIMEOUT must be numeric", 1) from exc
        if timeout <= 0:
            raise FleetClientError("TIMEOUT_INVALID", "control timeout must be positive", 1)
        ca_value = os.environ.get("CONDUCTRESS_CONTROL_CA_BUNDLE")
        ca_bundle = Path(ca_value).expanduser() if ca_value else None
        if ca_bundle is not None and not ca_bundle.is_file():
            raise FleetClientError("CA_BUNDLE_NOT_FOUND", f"control CA bundle does not exist: {ca_bundle}", 1)
        return cls(base_url=base_url, token=token, timeout_seconds=timeout, ca_bundle=ca_bundle)


class FleetClient:
    def __init__(
        self,
        config: FleetClientConfig,
        *,
        opener: Callable[..., Any] = urlopen,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.config = config
        self._opener = opener
        self._sleeper = sleeper
        self._ssl_context = ssl.create_default_context(cafile=str(config.ca_bundle) if config.ca_bundle else None)

    @classmethod
    def from_env(cls) -> "FleetClient":
        return cls(FleetClientConfig.from_env())

    @classmethod
    def from_runner_env(cls) -> "FleetClient":
        return cls(FleetClientConfig.from_runner_env())

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[dict[str, Any]] = None,
        query: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Optional[dict[str, Any]]:
        url = f"{self.config.base_url}/{path.lstrip('/')}"
        if query:
            encoded = urlencode({key: value for key, value in query.items() if value is not None})
            if encoded:
                url = f"{url}?{encoded}"
        request_headers = {
            "Authorization": f"Bearer {self.config.token}",
            "Accept": "application/json",
            **(headers or {}),
        }
        payload = None
        if body is not None:
            payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request = Request(url, data=payload, headers=request_headers, method=method)
        retryable = method in {"GET", "HEAD"} or (method in {"POST", "PUT"} and "Idempotency-Key" in request_headers)
        attempts = 3 if retryable else 1

        for attempt in range(attempts):
            try:
                with self._opener(
                    request,
                    timeout=self.config.timeout_seconds,
                    context=self._ssl_context,
                ) as response:
                    raw = response.read()
                    if response.status == 204 or not raw:
                        return None
                    document = json.loads(raw.decode("utf-8"))
                    if document.get("schema_version") != CLIENT_SCHEMA_VERSION:
                        raise FleetClientError(
                            "API_VERSION_UNSUPPORTED",
                            "control service returned an unsupported schema version",
                            3,
                            response.status,
                        )
                    return document
            except HTTPError as exc:
                raw = exc.read()
                try:
                    document = json.loads(raw.decode("utf-8")) if raw else {}
                except (UnicodeDecodeError, json.JSONDecodeError):
                    document = {}
                code = document.get("code", f"HTTP_{exc.code}")
                message = document.get("error", exc.reason or "control service request failed")
                if exc.code in {502, 503, 504} and attempt < attempts - 1:
                    self._sleeper(0.25 * (2**attempt))
                    continue
                raise FleetClientError(code, message, _api_exit_code(exc.code, code), exc.code) from exc
            except (URLError, TimeoutError, socket.timeout, ssl.SSLError) as exc:
                if attempt < attempts - 1:
                    self._sleeper(0.25 * (2**attempt))
                    continue
                raise FleetClientError("CONTROL_UNREACHABLE", f"control service unreachable: {exc}", 3) from exc
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise FleetClientError("RESPONSE_INVALID", "control service returned invalid JSON", 3) from exc
        raise AssertionError("retry loop exhausted")

    def fleet(self) -> dict[str, Any]:
        return self._request("GET", "fleet") or {}

    def runner(self, runner_id: str) -> dict[str, Any]:
        return self._request("GET", f"fleet/{runner_id}") or {}

    def list_tasks(
        self,
        *,
        runner_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        return (
            self._request(
                "GET",
                "tasks",
                query={"runner_id": runner_id, "state": state, "limit": limit, "offset": offset},
            )
            or {}
        )

    def task(self, task_id: str) -> dict[str, Any]:
        return self._request("GET", f"tasks/{task_id}") or {}

    def cancel_task(self, task_id: str) -> dict[str, Any]:
        return self._request("DELETE", f"tasks/{task_id}") or {}

    def submit_task(self, envelope: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        return (
            self._request(
                "POST",
                "tasks",
                body=envelope,
                headers={"Idempotency-Key": idempotency_key},
            )
            or {}
        )

    def claim_task(self, runner_id: str) -> Optional[dict[str, Any]]:
        return self._request(
            "POST",
            f"runners/{runner_id}/claim",
            headers={"Idempotency-Key": f"claim:{runner_id}"},
        )

    def accept_task(self, task_id: str, claim_token: str) -> dict[str, Any]:
        return (
            self._request(
                "POST",
                f"tasks/{task_id}/accept",
                body={"claim_token": claim_token},
                headers={"Idempotency-Key": f"accept:{task_id}"},
            )
            or {}
        )

    def report_outcome(self, task_id: str, outcome: dict[str, Any]) -> dict[str, Any]:
        state = outcome.get("state")
        if state not in {"completed", "failed"}:
            raise FleetClientError("OUTCOME_STATE_INVALID", "outcome state must be completed or failed", 1)
        endpoint = "complete" if state == "completed" else "fail"
        return (
            self._request(
                "POST",
                f"tasks/{task_id}/{endpoint}",
                body=outcome,
                headers={"Idempotency-Key": f"outcome:{task_id}:{state}"},
            )
            or {}
        )

    def push_status(self, runner_id: str, status: dict[str, Any]) -> dict[str, Any]:
        return (
            self._request(
                "PUT",
                f"runners/{runner_id}/status",
                body=status,
                headers={"Idempotency-Key": f"status:{runner_id}:{status.get('timestamp', '')}"},
            )
            or {}
        )
