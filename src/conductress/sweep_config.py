"""Runtime sweep configuration: focus, pause, and resume sweeps without restart.

Reads from sweep_config.json in PROJECT_ROOT. The file is checked on every
queue-empty cycle, so changes take effect within seconds.

Config format:
    {"mode": "normal"}                     -- all sweeps active (default)
    {"mode": "focus", "target": "memory-set-64b"}  -- only this workload runs
    {"mode": "paused", "paused": ["throughput"]}   -- these sweeps skip their turn
    {"sweep_share": 25}                    -- reserve 25 percent of this runner's time for the sweep
                                              while it has need (0 or absent: off, the default)

Selectors may be epoch-qualified. A bare ``workload_id`` matches that workload in
every epoch (the pre-epoch behaviour, preserved for existing config files), while
``epoch:workload_id`` matches a single epoch and ``epoch:*`` matches a whole
epoch:

    {"mode": "paused", "paused": ["v1:get-k16-v16-t7-p10"]}  -- pause only v1's GET
    {"mode": "paused", "paused": ["v1:*"]}                   -- pause the whole v1 epoch
    {"mode": "focus", "target": "v3:get-k16-v16-t7-p10"}     -- focus one epoch's GET

Qualification exists because workload ids are shared across epochs on purpose:
v1 and v3 both name the canonical GET sweep ``get-k16-v16-t7-p10``. Pausing that
bare id therefore stops BOTH epochs, which is rarely what the operator means.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from conductress.config import PROJECT_ROOT

logger = logging.getLogger(__name__)

SWEEP_CONFIG_FILE = PROJECT_ROOT / "sweep_config.json"

SELECTOR_SEPARATOR = ":"
SELECTOR_WILDCARD = "*"


def parse_selector(selector: str) -> tuple[Optional[str], str]:
    """Split a selector into ``(epoch_id, workload_id)``.

    A bare ``"get-k16-v16-t7-p10"`` yields ``(None, "get-k16-v16-t7-p10")`` and
    matches that workload in EVERY epoch, which preserves the behaviour of every
    selector written before epochs existed.

    An epoch-qualified ``"v3:get-k16-v16-t7-p10"`` yields
    ``("v3", "get-k16-v16-t7-p10")`` and matches only that epoch. This matters
    because a workload_id is deliberately shared across epochs -- v1 and v3 both
    call the canonical GET sweep ``get-k16-v16-t7-p10`` -- so before epoch
    qualification existed, pausing that id silently paused BOTH epochs.

    ``"v3:*"`` selects every workload in epoch v3.
    """
    epoch, sep, workload = selector.partition(SELECTOR_SEPARATOR)
    if not sep:
        return None, selector.strip()
    return epoch.strip(), workload.strip()


def selector_matches(selector: str, workload_id: str, epoch_id: str) -> bool:
    """Return True if ``selector`` selects this workload in this epoch."""
    want_epoch, want_workload = parse_selector(selector)
    if want_epoch is not None and want_epoch != epoch_id:
        return False
    if want_workload in ("", SELECTOR_WILDCARD):
        # "v3:" and "v3:*" both mean "every workload in this epoch". A bare "*"
        # (no epoch) would match everything, which is what it says.
        return True
    return want_workload == workload_id


@dataclass
class SweepConfig:
    """Current sweep scheduling configuration."""

    mode: str = "normal"  # "normal", "focus", "paused"
    target: Optional[str] = None  # selector to focus on (mode=focus)
    paused: Optional[list[str]] = None  # list of selectors to skip (mode=paused)
    sweep_share: float = 0.0  # percent of runner time reserved for the sweep while it has need; 0 = off

    def __post_init__(self):
        if self.paused is None:
            self.paused = []

    def is_allowed(self, workload_id: str, epoch_id: str = "v1") -> bool:
        """Check if a sweep with this workload_id/epoch is allowed to queue."""
        if self.mode == "focus":
            return self.target is not None and selector_matches(self.target, workload_id, epoch_id)
        if self.mode == "paused":
            return not any(selector_matches(sel, workload_id, epoch_id) for sel in (self.paused or []))
        return True  # normal mode


def _parse_share(value: Any) -> float:
    """A share from 0 to 100 percent. Anything else is off (0), with a warning."""
    if isinstance(value, (int, float)) and not isinstance(value, bool) and 0 <= value <= 100:
        return float(value)
    logger.warning("Invalid sweep_share %r in sweep_config.json (use 0 to 100) - the share is off", value)
    return 0.0


def load_sweep_config() -> SweepConfig:
    """Load sweep config from disk. Returns default (normal) if file missing or invalid."""
    if not SWEEP_CONFIG_FILE.exists():
        return SweepConfig()
    try:
        data = json.loads(SWEEP_CONFIG_FILE.read_text())
        return SweepConfig(
            mode=data.get("mode", "normal"),
            target=data.get("target"),
            paused=data.get("paused", []),
            sweep_share=_parse_share(data.get("sweep_share", 0)),
        )
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logger.warning("Invalid sweep_config.json: %s — using defaults", e)
        return SweepConfig()


def save_sweep_config(config: SweepConfig) -> None:
    """Write sweep config to disk."""
    data: dict[str, Any] = {"mode": config.mode}
    if config.target:
        data["target"] = config.target
    if config.paused:
        data["paused"] = config.paused
    if config.sweep_share:
        data["sweep_share"] = config.sweep_share
    SWEEP_CONFIG_FILE.write_text(json.dumps(data, indent=2) + "\n")


def focus(workload_id: str) -> None:
    """Focus on a single workload — only it will run."""
    save_sweep_config(SweepConfig(mode="focus", target=workload_id, sweep_share=load_sweep_config().sweep_share))
    logger.info("Sweep focused on: %s", workload_id)


def pause(workload_ids: list[str]) -> None:
    """Pause specific workloads — they won't queue new tasks."""
    save_sweep_config(SweepConfig(mode="paused", paused=workload_ids, sweep_share=load_sweep_config().sweep_share))
    logger.info("Paused sweeps: %s", workload_ids)


def resume() -> None:
    """Resume normal operation (all sweeps active). A sweep share that is set stays set."""
    share = load_sweep_config().sweep_share
    if share:
        save_sweep_config(SweepConfig(sweep_share=share))
    elif SWEEP_CONFIG_FILE.exists():
        SWEEP_CONFIG_FILE.unlink()
    logger.info("Sweep config reset to normal")
