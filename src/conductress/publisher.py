"""Dashboard publisher: exports and rsyncs data to the dashboard server after task completions."""

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from conductress.config import should_profile_internals
from conductress.utility import run_rsync

if TYPE_CHECKING:
    from conductress.sweep.coordinator import BaseSweepCoordinator
    from conductress.task_queue import BaseTaskData

logger = logging.getLogger(__name__)


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
        self._export_dir = Path(tempfile.mkdtemp(prefix="conductress-publish-"))
        logger.info("Publisher initialized: target=%s, platform=%s", target, self._platform_id)

    def on_task_completed(self, task: "BaseTaskData") -> None:
        """Export and publish after each completed task."""
        self._publish()

    def on_task_failed(self, task: "BaseTaskData") -> None:
        """No-op on failure."""

    def on_queue_empty(self) -> None:
        """No-op."""

    def _publish(self) -> None:
        """Export all coordinator data + perf metrics + manifest, then rsync."""
        from conductress.sweep.exporter import (
            NotableSource,
            export_cpu_profile,
            export_cpu_stacks_raw,
            export_manifest,
            export_notable,
            export_perf_metrics,
        )

        try:
            # Export each coordinator's series
            for coord in self.coordinators:
                output = self._export_dir / f"series-{self._platform_id}-{coord.workload_id}-{coord.metric_id}.json"
                coord.export(output, platform=self._platform_label)

            # Export perf metrics from all throughput coordinators
            for coord in self.coordinators:
                if coord.metric_id == "throughput":
                    repo = "redis/redis" if coord.engine and coord.engine.source == "redis" else "valkey-io/valkey"
                    branch = coord._sweep_ref.replace("origin/", "") if coord.engine else "unstable"
                    export_perf_metrics(
                        coord.state, self._export_dir, self._platform_id, coord.workload_id, repo=repo, branch=branch
                    )
                    # CPU flamegraph data exposes the binary's symbols — skip for engines that
                    # opt out (Redis). Also stops any pre-existing stacks in state from being
                    # re-published. Aggregate perf metrics above are unaffected.
                    if should_profile_internals(coord.engine):
                        export_cpu_profile(
                            coord.state,
                            self._export_dir,
                            self._platform_id,
                            coord.workload_id,
                            repo=repo,
                            branch=branch,
                        )
                        export_cpu_stacks_raw(
                            coord.state,
                            self._export_dir,
                            self._platform_id,
                            coord.workload_id,
                            repo=repo,
                            branch=branch,
                        )

            # Export combined notable-changes feed (Valkey only — the feed celebrates or
            # flags Valkey commits; other engines' series are tracked but not surfaced here).
            notable_sources = [
                NotableSource(
                    state=coord.state,
                    workload=coord.workload_id,
                    metric=coord.metric_id,
                    lower_is_better=coord.lower_is_better,
                )
                for coord in self.coordinators
                if coord.metric_id in ("throughput", "memory") and (not coord.engine or coord.engine.source == "valkey")
            ]
            export_notable(
                notable_sources, self._export_dir / f"notable-{self._platform_id}.json", self._platform_label
            )

            # Export manifest with all workload IDs, categorized by metric
            all_workloads = list(dict.fromkeys((c.workload_id, c.metric_id) for c in self.coordinators))
            export_manifest(self._export_dir, platforms=["amd64", "arm64", "intel"], workloads=all_workloads)

            # Rsync to target
            self._rsync()
        except Exception:
            logger.error("Publish failed (non-fatal) — dashboard data may be stale", exc_info=True)

    def _rsync(self) -> None:
        """Rsync export directory to remote target."""
        ssh_cmd = f"ssh -i {self._ssh_key} -F /dev/null -o StrictHostKeyChecking=no -o ConnectTimeout=10"
        run_rsync(
            ["rsync", "-az", "--chmod=D755,F644", "-e", ssh_cmd, f"{self._export_dir}/", self.target],
            self.target,
        )
