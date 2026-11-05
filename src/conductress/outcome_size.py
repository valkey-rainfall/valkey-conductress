"""Size contract between a runner's outcome report and the control service.

The control service (and the nginx in front of it) accepts a request body up
to a fixed number of bytes. A runner's outcome report must fit, or the server
answers HTTP 413 and the same bytes can never succeed on a retry. This module
holds the client side of that contract:

- :func:`wire_bytes` measures an outcome exactly as :mod:`fleet_client` will
  send it (compact JSON, no extra whitespace, UTF-8);
- :func:`trim_outcome` removes the bulk, optional parts of a result until the
  outcome fits a byte budget, biggest first, and records what it dropped;
- :func:`is_permanent_rejection` tells a payload rejection (retrying the same
  bytes cannot help) from a transient failure (retry later).

Server side the limit is ``MAX_BODY_BYTES`` (``control/config.py``, default
4 MiB); nginx must allow at least as much. ``DEFAULT_MAX_OUTCOME_BYTES`` stays
below the server default so a trimmed outcome passes both.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Optional

# Server default is 4 MiB (control/config.py MAX_BODY_BYTES). One MiB of
# headroom covers a proxy that counts differently or a slightly lower
# deployed limit without turning every large outcome into a round trip.
DEFAULT_MAX_OUTCOME_BYTES = 3 * 1024 * 1024

# Result keys whose value is bulk, optional data: a time series, a raw text
# dump, or a generated config. Everything the compare/plot code reads (scores,
# per-rep aggregates, verdicts, lag statistics, the memory scalars and the
# jemalloc breakdown) lives under other keys and is never dropped. Matched at
# any depth under ``result``; only list, dict and str values count, so a
# scalar that shares a name (``bottleneck.samples`` is a count) is kept.
BULK_RESULT_KEYS = (
    "series",  # replica-read per-rep INFO series (columnar)
    "samples",  # memory series and any list-of-samples left over
    "lscpu",
    "reader_toml",
    "writer_toml",
    "toml_config",
    "sample_errors",
)

# Stored on the result when it was trimmed, so a reader of the published
# record can see what is missing and where the full row is.
DELIVERY_KEY = "outcome_delivery"

# HTTP statuses that mean "this payload was refused": 413 is the body limit
# (nginx answers it with no JSON body, the app with code BODY_TOO_LARGE),
# 400 and 422 mean the body did not parse or did not validate. 401/403 (token),
# 404/409 (control-plane state) and 5xx/unreachable are not payload problems
# and keep the retry-forever behaviour; an operator has to look at those.
PERMANENT_REJECTION_STATUSES = frozenset({400, 413, 422})


def encode_json_body(body: Any) -> bytes:
    """Compact UTF-8 JSON, the exact encoding ``FleetClient`` puts on the wire."""
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def wire_bytes(body: Any) -> int:
    return len(encode_json_body(body))


def is_permanent_rejection(status: Optional[int]) -> bool:
    return status in PERMANENT_REJECTION_STATUSES


def bulk_candidates(result: Any) -> list[tuple[int, tuple]]:
    """Every bulk value in ``result`` as ``(bytes, path)``, biggest first.

    ``path`` is the sequence of keys/indices from ``result`` to the value.
    A bulk value's own children are not listed separately: dropping the parent
    already removes them.
    """
    found: list[tuple[int, tuple]] = []

    def walk(node: Any, path: tuple) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in BULK_RESULT_KEYS and isinstance(value, (list, dict, str)):
                    found.append((wire_bytes(value), path + (key,)))
                else:
                    walk(value, path + (key,))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, path + (index,))

    walk(result, ())
    found.sort(key=lambda item: (-item[0], _path_text(item[1])))
    return found


def _path_text(path: tuple) -> str:
    text = "result"
    for part in path:
        text += f"[{part}]" if isinstance(part, int) else f".{part}"
    return text


def _delete_path(root: Any, path: tuple) -> None:
    node = root
    for part in path[:-1]:
        node = node[part]
    # A result built in memory can hold the same dict under several entries
    # (``[rep] * n``); the first deletion then already removed this key.
    node.pop(path[-1], None)


def trim_outcome(
    outcome: dict[str, Any],
    budget: int,
    *,
    reason: str,
    full_record_path: str,
) -> tuple[dict[str, Any], list[str]]:
    """Return a copy of ``outcome`` that fits ``budget`` bytes, and what was dropped.

    Bulk values (:data:`BULK_RESULT_KEYS`) are removed biggest first until the
    encoded outcome is within ``budget``; ``budget <= 0`` removes all of them
    (the reduced form sent after a rejection whose limit is unknown). The
    dropped list names each value with its size, for the log and for the
    ``outcome_delivery`` record written on the result. When nothing can be
    dropped the outcome is returned unchanged with an empty list.

    The caller keeps the full row on disk; ``full_record_path`` tells a reader
    of the trimmed record where it is.
    """
    result = outcome.get("result")
    if not isinstance(result, dict):
        return outcome, []
    original_bytes = wire_bytes(outcome)
    if budget > 0 and original_bytes <= budget:
        return outcome, []
    trimmed = copy.deepcopy(outcome)
    dropped: list[str] = []

    def attach_record() -> int:
        """Write the delivery record for the current ``dropped`` list; return the size with it."""
        record: dict[str, Any] = {
            "trimmed": True,
            "reason": reason,
            "dropped": list(dropped),
            "original_bytes": original_bytes,
            "sent_bytes": 0,
            "full_result_path": full_record_path,
        }
        trimmed["result"][DELIVERY_KEY] = record
        record["sent_bytes"] = wire_bytes(trimmed)
        # The size field itself can change the length by a digit; settle it.
        sent = wire_bytes(trimmed)
        record["sent_bytes"] = sent
        return sent

    for size, path in bulk_candidates(result):
        # The budget applies to the bytes that go on the wire, record included.
        if dropped and budget > 0 and attach_record() <= budget:
            break
        _delete_path(trimmed["result"], path)
        dropped.append(f"{_path_text(path)} ({size} bytes)")
    if not dropped:
        return outcome, []
    attach_record()
    return trimmed, dropped
