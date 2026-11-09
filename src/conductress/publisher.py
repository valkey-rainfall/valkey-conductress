"""Dashboard publisher: exports and rsyncs data to the dashboard server after task completions."""

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterable, Optional

from conductress import config
from conductress.config import engine_repo_slug, should_profile_internals
from conductress.utility import run_rsync

if TYPE_CHECKING:
    from conductress.sweep.coordinator import BaseSweepCoordinator
    from conductress.task_queue import BaseTaskData

logger = logging.getLogger(__name__)

# Series metrics whose cells drive a request stream under perf stat, and so
# have per-request counter series to publish beside the score series.
PERF_EXPORT_METRICS = ("throughput", "latency")

# rsync filter pattern for the per-commit raw CPU-stack files
# (series-<platform>-<workload>-cpu-stacks-<40-hex-commit>.json). The hex
# class keeps the per-workload cpu-stacks-index.json, which the dashboard
# reads, out of the match.
STACKS_FILE_GLOB = "series-*-cpu-stacks-[0-9a-f]*.json"


def detect_platform() -> tuple[str, str]:
    """Detect platform ID and label. Kept as a compatibility wrapper."""
    from conductress.platform import get_local_platform_info

    platform_id, label, _aliases = get_local_platform_info()
    return platform_id, label


class DashboardPublisher:
    """Subscriber that exports sweep data and rsyncs to a remote server after task completions."""

    def __init__(self, target: str, coordinators: "list[BaseSweepCoordinator]") -> None:
        """
        Args:
            target: rsync destination, e.g. "ec2-user@host:/var/www/data"
            coordinators: list of sweep coordinators whose data to export
        """
        self.target = target
        self.coordinators = coordinators
        # Key may be at different paths depending on host
        candidates = [Path.home() / "conductress" / "server-keyfile.pem", Path.home() / ".ssh" / "openssh-ec2-pair.pem"]
        self._ssh_key = next((k for k in candidates if k.exists()), candidates[0])
        self._platform_id, self._platform_label = detect_platform()
        self._export_dir = self._prepare_export_dir(Path(config.PUBLISH_EXPORT_DIR))
        self._sweep_legacy_export_dirs()
        logger.info(
            "Publisher initialized: target=%s, platform=%s, export_dir=%s",
            target,
            self._platform_id,
            self._export_dir,
        )

    @staticmethod
    def _prepare_export_dir(path: Path) -> Path:
        """Return the export directory at a fixed path, kept across restarts.

        The small dashboard files (series, manifests, notable feeds) are
        regenerated from coordinator state on every publish. The per-commit
        CPU-stack files are not: each is multi-MB, there are thousands, and
        their content never changes once written, so they are exported once
        and thereafter skipped by mtime. Wiping the directory on start-up would
        regenerate all of them with fresh mtimes and make the next rsync move
        the whole tree again. Only staging directories left by an interrupted
        publish are removed here; a fixed path replaces a per-process
        ``mkdtemp`` under the temp directory, which left one full export copy
        behind for every runner start, on hosts where the temp directory is a
        RAM-backed tmpfs.
        """
        path.mkdir(parents=True, exist_ok=True)
        for stale_stage in path.glob(".stage-*"):
            shutil.rmtree(stale_stage, ignore_errors=True)
        return path

    @staticmethod
    def _legacy_temp_dirs() -> list[Path]:
        """Temp directories an earlier ``mkdtemp``-based publisher may have used.

        ``tempfile.gettempdir()`` returns the first writable candidate, so a
        process started while ``/tmp`` was full resolved to ``/var/tmp``; both
        are checked alongside whatever it resolves to now.
        """
        seen: list[Path] = []
        for candidate in (tempfile.gettempdir(), "/tmp", "/var/tmp"):
            path = Path(candidate)
            if path not in seen:
                seen.append(path)
        return seen

    @classmethod
    def _sweep_legacy_export_dirs(cls) -> int:
        """Remove ``conductress-publish-*`` directories left by earlier publishers.

        Only the retired ``mkdtemp`` path ever created that prefix, so anything
        matching it in a temp directory is a leak from a previous runner
        process. Returns the number of directories removed.
        """
        removed = 0
        for temp_dir in cls._legacy_temp_dirs():
            for stale in temp_dir.glob("conductress-publish-*"):
                if stale.is_dir():
                    shutil.rmtree(stale, ignore_errors=True)
                    removed += 1
                    logger.info("Removed legacy export dir %s", stale)
        return removed

    def on_task_completed(self, task: "BaseTaskData") -> None:
        """Export and publish after each completed task."""
        self._publish()

    def on_task_failed(self, task: "BaseTaskData") -> None:
        """No-op on failure."""

    def on_queue_empty(self) -> None:
        """No-op."""

    @staticmethod
    def _coord_epoch(coord: object) -> str:
        epoch_id = getattr(coord, "epoch_id", "v1")
        return epoch_id if isinstance(epoch_id, str) and epoch_id else "v1"

    @classmethod
    def _coord_epochs(cls, coord: object) -> tuple[str, ...]:
        """Every epoch a series is published under (one for generator-bound series)."""
        epoch_ids = getattr(coord, "epoch_ids", None)
        if isinstance(epoch_ids, (tuple, list)) and epoch_ids:
            return tuple(e for e in epoch_ids if isinstance(e, str) and e) or (cls._coord_epoch(coord),)
        return (cls._coord_epoch(coord),)

    @staticmethod
    def _epoch_def(epoch_id: str) -> dict:
        """Resolve dashboard metadata for an epoch from the shared registry.

        An unregistered epoch gets a generic label rather than inheriting some
        other epoch's description. The previous binary ``v1``-or-else expression
        would have labelled a v3 series "Scalable v2 (patched generator)", which
        is precisely the provenance confusion the epoch split exists to prevent.

        ``archived`` states whether the epoch is still measured. The dashboard
        uses it to default to a live epoch and to mark archived ones in its
        selector, instead of inferring liveness from list position, which
        varies between manifests during a rolling deploy.
        """
        from conductress.config import SWEEP_EPOCHS, epoch_is_archived

        archived = epoch_is_archived(epoch_id)
        entry = SWEEP_EPOCHS.get(epoch_id)
        if entry is None:
            logger.warning("Unregistered sweep epoch %r — publishing a generic label", epoch_id)
            return {"id": epoch_id, "label": f"Epoch {epoch_id}", "generator": "unknown", "archived": archived}
        return {"id": epoch_id, **entry, "archived": archived}

    @staticmethod
    def _epoch_path(path: Path, epoch_id: str) -> Path:
        """Return the legacy path for v1 or add `.epoch-<id>` for v2+."""
        if not epoch_id or epoch_id == "v1":
            return path
        return path.with_name(f"{path.stem}.epoch-{epoch_id}{path.suffix}")

    @staticmethod
    def _stamp_epoch(path: Path, epoch_id: str, generation: Optional[str] = None) -> None:
        """Stamp exported JSON with its epoch (and closed generation, if any) before publication."""
        if not path.exists():
            return
        payload = json.loads(path.read_text())
        metadata = payload.setdefault("metadata", {})
        metadata["epoch"] = epoch_id
        if generation is not None:
            metadata["generation"] = generation
        path.write_text(json.dumps(payload, indent=2))

    def _promote_epoch_stage(self, stage: Path, epoch_id: str) -> None:
        """Move staged exporter files into the epoch-qualified namespace."""
        for path in stage.glob("*.json"):
            target = self._epoch_path(self._export_dir / path.name, epoch_id)
            self._stamp_epoch(path, epoch_id)
            path.replace(target)
        shutil.rmtree(stage, ignore_errors=True)

    def _publish(self) -> None:
        """Export each measurement epoch independently, then rsync."""
        from conductress.sweep.environment_events import manifest_notes
        from conductress.sweep.exporter import (
            NotableSource,
            export_cpu_profile,
            export_cpu_stacks_raw,
            export_manifest,
            export_notable,
            export_perf_metrics,
        )
        from conductress.sweep.generations import load_generations

        try:
            epoch_ids = list(dict.fromkeys(e for c in self.coordinators for e in self._coord_epochs(c)))
            # Every manifest, live or archived, advertises the same epoch list
            # so the dashboard's epoch selector keeps offering archived history
            # even though no coordinator for it is built or exported here.
            epoch_defs = self.advertised_epoch_defs(epoch_ids)

            for coord in self.coordinators:
                base = self._export_dir / f"series-{self._platform_id}-{coord.workload_id}-{coord.metric_id}.json"
                # A generator-independent series is written once per epoch it
                # belongs to, from the same state, so every epoch's dashboard
                # shows its full history.
                for epoch_id in self._coord_epochs(coord):
                    output = self._epoch_path(base, epoch_id)
                    coord.export(output, platform=self._platform_label)
                    self._stamp_epoch(output, epoch_id)

                # Perf counters are collected by the cells that drive a request
                # stream: throughput and rate-limited latency. Memory cells run
                # no request stream and record none, so there is nothing to export.
                if coord.metric_id not in PERF_EXPORT_METRICS:
                    continue
                epoch_id = self._coord_epoch(coord)

                repo = engine_repo_slug(coord.engine)
                branch = coord._sweep_ref.replace("origin/", "") if coord.engine else "unstable"
                export_dir = self._export_dir
                stage = None
                if epoch_id != "v1":
                    stage = self._export_dir / f".stage-{epoch_id}-{coord.workload_id}"
                    shutil.rmtree(stage, ignore_errors=True)
                    stage.mkdir(parents=True)
                    export_dir = stage

                export_perf_metrics(
                    coord.state, export_dir, self._platform_id, coord.workload_id, repo=repo, branch=branch
                )
                if should_profile_internals(coord.engine):
                    export_cpu_profile(
                        coord.state, export_dir, self._platform_id, coord.workload_id, repo=repo, branch=branch
                    )
                    # The stage starts empty every publish, so the exporter's own
                    # exists() check cannot see what was already promoted; ask it
                    # to skip files the final epoch-qualified path already holds.
                    export_cpu_stacks_raw(
                        coord.state,
                        export_dir,
                        self._platform_id,
                        coord.workload_id,
                        repo=repo,
                        branch=branch,
                        already_published=self._already_published(epoch_id),
                    )
                if stage is not None:
                    self._promote_epoch_stage(stage, epoch_id)

            # Notable feeds and manifests are isolated by epoch as well.  The
            # legacy manifest advertises every available epoch so old URLs stay
            # valid while new dashboards can discover v2.
            for epoch_id in epoch_ids:
                epoch_coords = [c for c in self.coordinators if epoch_id in self._coord_epochs(c)]
                notable_sources = [
                    NotableSource(
                        state=coord.state,
                        workload=coord.workload_id,
                        metric=coord.metric_id,
                        lower_is_better=coord.lower_is_better,
                    )
                    for coord in epoch_coords
                    if coord.metric_id in ("throughput", "memory")
                    and (not coord.engine or coord.engine.source == "valkey")
                ]
                notable = self._epoch_path(self._export_dir / f"notable-{self._platform_id}.json", epoch_id)
                export_notable(notable_sources, notable, self._platform_label)
                self._stamp_epoch(notable, epoch_id)

                workloads = list(dict.fromkeys((c.workload_id, c.metric_id) for c in epoch_coords))
                # This platform's closed generations: their archived files
                # already sit in the export dir (written once by
                # ``sweep export-archived --generation``) and are re-synced as
                # is; the live manifest only has to advertise them.
                generations = [g for g in load_generations() if g.get("epoch", "v3") == epoch_id]
                environment_events, explanations = manifest_notes()
                export_manifest(
                    self._export_dir,
                    platforms=["amd64", "arm64", "graviton4", "intel"],
                    workloads=workloads,
                    epoch_id=epoch_id,
                    epochs=epoch_defs,
                    generations=generations,
                    environment_events=environment_events,
                    explanations=explanations,
                )

            self._refresh_archived_manifests(epoch_defs)

            self._rsync()
        except Exception:
            logger.error("Publish failed (non-fatal) — dashboard data may be stale", exc_info=True)

    @classmethod
    def advertised_epoch_defs(cls, live_epoch_ids: "Iterable[str]") -> list[dict]:
        """Dashboard metadata for every epoch a manifest should advertise.

        Live epochs first, in the order given, then the archived ones. Every
        manifest written for a platform carries this same list, so whichever
        one the dashboard reads first, it sees every epoch and its
        ``archived`` flag.
        """
        advertised_ids = list(dict.fromkeys([*live_epoch_ids, *config.SWEEP_ARCHIVED_EPOCHS]))
        return [cls._epoch_def(epoch_id) for epoch_id in advertised_ids]

    def _refresh_archived_manifests(self, epoch_defs: list[dict]) -> None:
        """Rewrite the epoch list inside each archived epoch's manifest.

        An archived epoch has no coordinator, so its manifest is never
        regenerated by a publish: it keeps the workload lists from its last
        export. Its ``epochs`` key, though, describes the fleet's epochs rather
        than the archived epoch's own data, and the dashboard reads the legacy
        (v1) manifest to discover which epochs exist before it picks one. Left
        alone, that manifest would keep advertising a stale list with no
        ``archived`` flags, and the dashboard would keep opening on the
        archived epoch. A manifest that is absent (wiped export dir) is left
        absent; ``conductress sweep export-archived`` recreates it.
        """
        for epoch_id in config.SWEEP_ARCHIVED_EPOCHS:
            manifest = self._epoch_path(self._export_dir / f"manifest-{self._platform_id}.json", epoch_id)
            if not manifest.exists():
                continue
            payload = json.loads(manifest.read_text())
            if payload.get("epochs") == epoch_defs:
                continue
            payload["epochs"] = epoch_defs
            manifest.write_text(json.dumps(payload, indent=2))

    def _already_published(self, epoch_id: str) -> "Callable[[str], bool]":
        """Predicate on an exporter file name: does the export dir already hold it?

        Resolves the name the way promotion will, so the answer is about the
        final epoch-qualified path rather than the empty stage.
        """

        def exists(filename: str) -> bool:
            return self._epoch_path(self._export_dir / filename, epoch_id).exists()

        return exists

    def _rsync(self) -> None:
        """Rsync the export directory to the remote target in two passes.

        Pass one carries everything the dashboard reads (series, manifests,
        notable feeds, cpu-stacks indexes): small, and regenerated on every
        publish. Pass two carries the per-commit cpu-stacks files: the bulk of
        the tree by bytes, almost all unchanged, and only fetched lazily by a
        flamegraph page. rsync sends files in name order, so in a single pass a
        slow stacks transfer would sit ahead of every series file that sorts
        after it; splitting the passes lets the dashboard data land first and
        bounds each pass on its own.
        """
        ssh_cmd = f"ssh -i {self._ssh_key} -F /dev/null -o StrictHostKeyChecking=no -o ConnectTimeout=10"
        common = ["rsync", "-az", "--chmod=D755,F644", "-e", ssh_cmd]
        source = f"{self._export_dir}/"
        run_rsync(
            common + [f"--exclude={STACKS_FILE_GLOB}", source, self.target],
            self.target,
            timeout=config.PUBLISH_RSYNC_TIMEOUT_SECONDS,
        )
        run_rsync(
            common + [f"--include={STACKS_FILE_GLOB}", "--exclude=*", source, self.target],
            self.target,
            timeout=config.PUBLISH_RSYNC_TIMEOUT_SECONDS,
        )
