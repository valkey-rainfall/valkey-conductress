"""One-time export of an archived epoch's dashboard files.

An archived epoch (``config.SWEEP_ARCHIVED_EPOCHS``, currently ``v1``) is never
built or loaded by the runner service: its state files are gigabytes each and
their history never changes again, so paying that load cost at every restart is
pure waste.  Its already-exported dashboard files normally persist in the
publish export dir (``config.PUBLISH_EXPORT_DIR``) across restarts and are
re-synced as-is, so archiving loses no history.

The one case that does lose history is a runner whose export dir was wiped (an
old-code restart, a fresh host): with no v1 coordinator ever loading state,
nothing regenerates those files.  This module is the one-time repair.  It loads
the archived epoch's state files ONCE, exports its series, per-request metric,
CPU-stack and notable files plus its manifest into the export dir, and returns.
It is invoked only by ``conductress sweep export-archived`` -- never from the
service start path -- so the load cost is paid deliberately, once, by hand.

Only ``v1`` is supported as an archived EPOCH: it is the only archived epoch,
and its coordinators (the unqualified throughput series, the retired latency
series, the memory series, and each comparison engine's v1 mirror) are exactly
the ones the runner used to build before archival.

A closed GENERATION of the live v3 epoch (``sweep/generations.py``) is exported
the same way by :func:`export_archived_generation`: its state files are read
once from ``v3/generations/<id>/`` and published under ``.gen-<id>`` names.
"""

import logging
from pathlib import Path
from typing import Optional

from conductress import config

logger = logging.getLogger(__name__)


def _build_v1_coordinators(repo_path: Path) -> list:
    """Every v1 coordinator whose state file exists, mirroring the pre-archival roster.

    Built here rather than reused from the runner so importing this one-time
    tool never drags in the runner's construction path, and so a coordinator is
    added only when its state file is actually on disk (an absent series simply
    has nothing to export).
    """
    from conductress.config import SWEEP_ENGINES, SWEEP_IO_THREADS, SWEEP_PIPELINING, SWEEP_THROUGHPUT_WORKLOADS
    from conductress.sweep.coordinator import SweepCoordinator
    from conductress.sweep.latency_coordinator import LatencySweepCoordinator
    from conductress.sweep.memory_coordinator import create_memory_coordinators

    coordinators: list = []

    def _add(coord) -> None:
        if coord.state_file.exists():
            coordinators.append(coord)
        else:
            logger.info("Skipping %s:%s -- no state file at %s", coord.metric_id, coord.workload_id, coord.state_file)

    _add(SweepCoordinator(repo_path))
    for wl in SWEEP_THROUGHPUT_WORKLOADS:
        _add(
            SweepCoordinator(
                repo_path,
                val_size=wl["val_size"],
                test=wl.get("test", "get"),
                io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                pipelining=wl.get("pipelining", SWEEP_PIPELINING),
            )
        )
    _add(LatencySweepCoordinator(repo_path))
    # Memory series are generator-independent; their v1-named files are part of
    # the v1 dashboard even though the live series now publishes only under v3.
    for mem_coord in create_memory_coordinators(repo_path):
        _add(mem_coord)

    for engine in SWEEP_ENGINES:
        if engine.source == "valkey":
            continue
        engine_repo = Path.home() / engine.source
        _add(SweepCoordinator(engine_repo, engine=engine))
        for wl in SWEEP_THROUGHPUT_WORKLOADS:
            _add(
                SweepCoordinator(
                    engine_repo,
                    val_size=wl["val_size"],
                    test=wl.get("test", "get"),
                    io_threads=wl.get("io_threads", SWEEP_IO_THREADS),
                    pipelining=wl.get("pipelining", SWEEP_PIPELINING),
                    engine=engine,
                )
            )
        for mem_coord in create_memory_coordinators(engine_repo, engine=engine):
            _add(mem_coord)

    return coordinators


def export_archived_epoch(epoch: str, repo_path: Optional[Path] = None, output_dir: Optional[Path] = None) -> int:
    """Export an archived epoch's dashboard files into the publish export dir.

    Loads the epoch's state files once, writes its series / notable / manifest
    files under the archived epoch's legacy (unqualified) names, and returns the
    number of series files written.  Raises ``ValueError`` for an epoch that is
    not archived or not supported.
    """
    if not config.epoch_is_archived(epoch):
        raise ValueError(f"epoch {epoch!r} is not archived (archived: {', '.join(config.SWEEP_ARCHIVED_EPOCHS)})")
    if epoch != "v1":
        raise ValueError(f"only 'v1' archived export is supported, got {epoch!r}")

    from conductress.publisher import detect_platform
    from conductress.sweep.exporter import NotableSource, export_manifest, export_notable

    repo_path = repo_path or (Path.home() / "valkey")
    export_dir = output_dir or Path(config.PUBLISH_EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)
    platform_id, platform_label = detect_platform()

    coordinators = _build_v1_coordinators(repo_path)
    if not coordinators:
        logger.warning("No v1 state files found under %s -- nothing to export", repo_path)
        return 0

    written = 0
    for coord in coordinators:
        output = export_dir / f"series-{platform_id}-{coord.workload_id}-{coord.metric_id}.json"
        coord.export(output, platform=platform_label)
        written += 1

    # Notable feed for the archived epoch.
    notable_sources = [
        NotableSource(
            state=coord.state,
            workload=coord.workload_id,
            metric=coord.metric_id,
            lower_is_better=coord.lower_is_better,
        )
        for coord in coordinators
        if coord.metric_id in ("throughput", "memory") and (not coord.engine or coord.engine.source == "valkey")
    ]
    export_notable(notable_sources, export_dir / f"notable-{platform_id}.json", platform_label)

    # The archived epoch's manifest keeps its legacy unqualified name so the
    # dashboard's epoch selector keeps offering it, and advertises the same
    # epoch list (live epochs first, then archived, each with its ``archived``
    # flag) that a live publish writes, so the dashboard picks the same default
    # whichever manifest it reads first.
    from conductress.publisher import DashboardPublisher

    workloads = list(dict.fromkeys((c.workload_id, c.metric_id) for c in coordinators))
    epochs = DashboardPublisher.advertised_epoch_defs(config.SWEEP_EPOCH_PRECEDENCE)
    export_manifest(
        export_dir,
        platforms=["amd64", "arm64", "graviton4", "intel"],
        workloads=workloads,
        epoch_id=epoch,
        epochs=epochs,
    )
    logger.info("Exported %d %s series to %s", written, epoch, export_dir)
    return written


def _v3_roster(repo_path: Path) -> list:
    """Every v3 coordinator of every engine, as the runner would build them."""
    from conductress.sweep.coordinator_v3 import create_v3_coordinators

    coordinators = list(create_v3_coordinators(repo_path))
    for engine in config.SWEEP_ENGINES:
        if engine.source == "valkey":
            continue
        coordinators.extend(create_v3_coordinators(Path.home() / engine.source, engine=engine))
    return coordinators


def export_archived_generation(
    generation_id: str,
    repo_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    state_dir: Optional[Path] = None,
    generations_file: Optional[Path] = None,
) -> int:
    """Export one closed v3 generation's dashboard files, once.

    The generation must be recorded in ``generations.json`` (written by
    ``conductress sweep close-generation``). Its state files under
    ``v3/generations/<id>/`` are loaded once, each series is exported under its
    live name plus a ``.gen-<id>`` suffix (``series-<plat>-<wl>-<metric>
    .epoch-v3.gen-<id>.json``), with the notable feed and a generation manifest
    beside them, into the publish export dir, where the next publish re-syncs
    them. The runner service never loads these files. Returns the number of
    series written.
    """
    from conductress.publisher import DashboardPublisher, detect_platform
    from conductress.sweep.exporter import NotableSource, export_manifest, export_notable
    from conductress.sweep.generations import (
        GENERATIONS_FILE,
        generation_dir,
        generation_path,
        load_generations,
        rehome,
    )

    closed = load_generations(generations_file or GENERATIONS_FILE)
    entry = next((g for g in closed if g.get("id") == generation_id), None)
    if entry is None:
        known = ", ".join(g.get("id", "?") for g in closed) or "none"
        raise ValueError(f"generation {generation_id!r} is not recorded in generations.json (closed: {known})")
    gen_dir = generation_dir(generation_id, state_dir) if state_dir else generation_dir(generation_id)
    if not gen_dir.is_dir():
        raise ValueError(f"generation directory does not exist: {gen_dir}")

    repo_path = repo_path or (Path.home() / "valkey")
    export_dir = output_dir or Path(config.PUBLISH_EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)
    platform_id, platform_label = detect_platform()
    epoch_id = str(entry.get("epoch", "v3"))

    coordinators = [c for c in _v3_roster(repo_path) if rehome(c, gen_dir)]
    if not coordinators:
        logger.warning("No archived state files under %s -- nothing to export", gen_dir)
        return 0

    def _path(name: str) -> Path:
        return generation_path(DashboardPublisher._epoch_path(export_dir / name, epoch_id), generation_id)

    written = 0
    for coord in coordinators:
        output = _path(f"series-{platform_id}-{coord.workload_id}-{coord.metric_id}.json")
        coord.export(output, platform=platform_label)
        DashboardPublisher._stamp_epoch(output, epoch_id, generation=generation_id)
        written += 1

    notable_sources = [
        NotableSource(
            state=coord.state, workload=coord.workload_id, metric=coord.metric_id, lower_is_better=coord.lower_is_better
        )
        for coord in coordinators
        if coord.metric_id == "throughput" and (not coord.engine or coord.engine.source == "valkey")
    ]
    notable = _path(f"notable-{platform_id}.json")
    export_notable(notable_sources, notable, platform_label)
    DashboardPublisher._stamp_epoch(notable, epoch_id, generation=generation_id)

    workloads = list(dict.fromkeys((c.workload_id, c.metric_id) for c in coordinators))
    export_manifest(
        export_dir,
        platforms=["amd64", "arm64", "graviton4", "intel"],
        workloads=workloads,
        epoch_id=epoch_id,
        epochs=DashboardPublisher.advertised_epoch_defs(config.SWEEP_EPOCH_PRECEDENCE),
        generations=[g for g in closed if g.get("epoch", "v3") == epoch_id],
        generation=entry,
    )
    logger.info("Exported %d series of generation %s to %s", written, generation_id, export_dir)
    return written
