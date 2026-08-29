"""Authenticated HTTP API for the Conductress fleet control service."""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from aiohttp import web

from . import CONTROL_API_SCHEMA_VERSION
from .auth import AuthIdentity, TokenStore
from .config import ControlConfig
from .db import ControlDatabase
from .errors import AuthorizationError, ControlError, NotFoundError
from .fleet_registry import FleetRegistry
from .service import ControlService

logger = logging.getLogger(__name__)
SERVICE_KEY = web.AppKey("service", ControlService)
TOKEN_STORE_KEY = web.AppKey("token_store", TokenStore)


def _actor(identity: AuthIdentity) -> str:
    return f"{identity.role}:{identity.label}"


def _response(*, http_status: int = 200, **payload: Any) -> web.Response:
    return web.json_response({"schema_version": CONTROL_API_SCHEMA_VERSION, **payload}, status=http_status)


def _require_operator(request: web.Request) -> AuthIdentity:
    identity: AuthIdentity = request["auth"]
    if identity.role != "operator":
        raise AuthorizationError("OPERATOR_REQUIRED", "operator token required")
    return identity


def _runner_id(identity: AuthIdentity) -> str:
    if identity.runner_id is None:
        raise AuthorizationError("RUNNER_ID_REQUIRED", "runner token has no runner identity")
    return identity.runner_id


def _require_runner(request: web.Request, runner_id: Optional[str] = None) -> AuthIdentity:
    identity: AuthIdentity = request["auth"]
    if identity.role != "runner":
        raise AuthorizationError("RUNNER_REQUIRED", "runner token required")
    if runner_id is not None and identity.runner_id != runner_id:
        raise AuthorizationError("RUNNER_SCOPE_MISMATCH", "runner token is scoped to another runner")
    return identity


async def _json_body(request: web.Request) -> dict[str, Any]:
    if request.content_type != "application/json":
        raise ControlError("CONTENT_TYPE_INVALID", "Content-Type must be application/json", 415)
    try:
        body = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ControlError("JSON_INVALID", "request body is not valid JSON") from exc
    if not isinstance(body, dict):
        raise ControlError("JSON_OBJECT_REQUIRED", "request body must be a JSON object")
    return body


@web.middleware
async def error_middleware(request: web.Request, handler):
    try:
        return await handler(request)
    except ControlError as exc:
        return web.json_response(
            {"schema_version": CONTROL_API_SCHEMA_VERSION, "error": exc.message, "code": exc.code},
            status=exc.status,
        )
    except web.HTTPRequestEntityTooLarge:
        return web.json_response(
            {
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "error": "request body exceeds configured limit",
                "code": "BODY_TOO_LARGE",
            },
            status=413,
        )
    except web.HTTPException as exc:
        return web.json_response(
            {
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "error": exc.reason,
                "code": f"HTTP_{exc.status}",
            },
            status=exc.status,
        )
    except Exception:
        logger.exception("unhandled control API error")
        return web.json_response(
            {
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "error": "internal server error",
                "code": "INTERNAL_ERROR",
            },
            status=500,
        )


@web.middleware
async def auth_middleware(request: web.Request, handler):
    token_store: TokenStore = request.app[TOKEN_STORE_KEY]
    request["auth"] = token_store.authenticate(request.headers.get("Authorization"))
    return await handler(request)


def create_app(
    config: ControlConfig,
    *,
    database: Optional[ControlDatabase] = None,
    registry: Optional[FleetRegistry] = None,
    token_store: Optional[TokenStore] = None,
) -> web.Application:
    config.validate()
    database = database or ControlDatabase(config.database_path, config.audit_jsonl_path)
    database.initialize()
    registry = registry or FleetRegistry.from_file(config.fleet_manifest_path)
    token_store = token_store or TokenStore(config.tokens_path)
    service = ControlService(database, registry, config.claim_lease_seconds)
    service.expire_stale_claims(actor="system:startup")

    app = web.Application(
        middlewares=[error_middleware, auth_middleware],
        client_max_size=config.max_body_bytes,
    )
    app[SERVICE_KEY] = service
    app[TOKEN_STORE_KEY] = token_store

    app.router.add_get("/api/v1/health", _health)
    app.router.add_post("/api/v1/tasks", _submit_task)
    app.router.add_get("/api/v1/tasks", _list_tasks)
    app.router.add_get("/api/v1/tasks/{task_id}", _get_task)
    app.router.add_delete("/api/v1/tasks/{task_id}", _cancel_task)
    app.router.add_get("/api/v1/fleet", _fleet)
    app.router.add_get("/api/v1/fleet/{runner_id}", _fleet_runner)
    app.router.add_put("/api/v1/runners/{runner_id}/status", _push_status)
    app.router.add_post("/api/v1/runners/{runner_id}/claim", _claim_task)
    app.router.add_post("/api/v1/tasks/{task_id}/accept", _accept_task)
    app.router.add_post("/api/v1/tasks/{task_id}/complete", _complete_task)
    app.router.add_post("/api/v1/tasks/{task_id}/fail", _fail_task)
    return app


async def _health(request: web.Request) -> web.Response:
    _require_operator(request)
    return _response(status="ok")


async def _submit_task(request: web.Request) -> web.Response:
    identity = _require_operator(request)
    task, created = request.app[SERVICE_KEY].submit_task(
        await _json_body(request),
        actor=_actor(identity),
        idempotency_key=request.headers.get("Idempotency-Key"),
    )
    return _response(task=task, created=created, http_status=201 if created else 200)


async def _list_tasks(request: web.Request) -> web.Response:
    _require_operator(request)
    try:
        limit = int(request.query.get("limit", "50"))
        offset = int(request.query.get("offset", "0"))
    except ValueError as exc:
        raise ControlError("PAGINATION_INVALID", "limit and offset must be integers") from exc
    tasks = request.app[SERVICE_KEY].list_tasks(
        runner_id=request.query.get("runner_id"),
        state=request.query.get("state"),
        limit=limit,
        offset=offset,
    )
    return _response(tasks=tasks)


async def _get_task(request: web.Request) -> web.Response:
    _require_operator(request)
    return _response(task=request.app[SERVICE_KEY].get_task(request.match_info["task_id"]))


async def _cancel_task(request: web.Request) -> web.Response:
    identity = _require_operator(request)
    task, changed = request.app[SERVICE_KEY].cancel_task(request.match_info["task_id"], actor=_actor(identity))
    return _response(task=task, changed=changed)


async def _fleet(request: web.Request) -> web.Response:
    _require_operator(request)
    return _response(runners=request.app[SERVICE_KEY].fleet_status())


async def _fleet_runner(request: web.Request) -> web.Response:
    _require_operator(request)
    runners = request.app[SERVICE_KEY].fleet_status(request.match_info["runner_id"])
    if not runners:
        raise NotFoundError("RUNNER_NOT_FOUND", "unknown runner")
    return _response(runner=runners[0])


async def _push_status(request: web.Request) -> web.Response:
    runner_id = request.match_info["runner_id"]
    identity = _require_runner(request, runner_id)
    request.app[SERVICE_KEY].push_status(runner_id, await _json_body(request), actor=_actor(identity))
    return _response(updated=True)


async def _claim_task(request: web.Request) -> web.Response:
    runner_id = request.match_info["runner_id"]
    identity = _require_runner(request, runner_id)
    claim = request.app[SERVICE_KEY].claim_task(runner_id, actor=_actor(identity))
    if claim is None:
        return web.Response(status=204)
    return _response(claim=claim)


async def _accept_task(request: web.Request) -> web.Response:
    identity = _require_runner(request)
    body = await _json_body(request)
    claim_token = body.get("claim_token")
    if not isinstance(claim_token, str) or not claim_token:
        raise ControlError("CLAIM_TOKEN_REQUIRED", "claim_token is required")
    task, changed = request.app[SERVICE_KEY].accept_task(
        _runner_id(identity),
        request.match_info["task_id"],
        claim_token,
        actor=_actor(identity),
    )
    return _response(task=task, changed=changed)


async def _complete_task(request: web.Request) -> web.Response:
    return await _record_outcome(request, "completed")


async def _fail_task(request: web.Request) -> web.Response:
    return await _record_outcome(request, "failed")


async def _record_outcome(request: web.Request, expected_state: str) -> web.Response:
    identity = _require_runner(request)
    outcome = await _json_body(request)
    if outcome.get("state") != expected_state:
        raise ControlError("OUTCOME_STATE_INVALID", f"outcome state must be {expected_state}")
    task, changed = request.app[SERVICE_KEY].record_outcome(
        _runner_id(identity),
        request.match_info["task_id"],
        outcome,
        actor=_actor(identity),
    )
    return _response(task=task, changed=changed)
