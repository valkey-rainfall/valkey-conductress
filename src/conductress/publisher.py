"""Dashboard publisher: exports and rsyncs data to the dashboard server after task completions."""

import logging
import platform
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from conductress.sweep.coordinator import BaseSweepCoordinator
    from conductress.task_queue import BaseTaskData

logger = logging.getLogger(__name__)

# Platform detection: map uname machine + optional CPU model to dashboard platform ID
_PLATFORM_MAP = {
    "aarch64": ("arm64", "arm64/c7g.metal/graviton3"),
    "x86_64": ("amd64", "amd64/epyc-9r14/zen4"),
}

# Intel override: if CPU model contains these strings, use intel platform
_INTEL_KEYWORDS = ("8488", "sapphire", "Xeon Platinum 8")


def detect_platform() -> tuple[str, str]:
    """Detect platform ID and label from hardware. Returns (id, label)."""
    arch = platform.machine()
    if arch == "x86_64":
        try:
            cpuinfo = Path("/proc/cpuinfo").read_text()
            if any(kw in cpuinfo for kw in _INTEL_KEYWORDS):
                return "intel", "intel/xeon-8488c/sapphire-rapids"
        except OSError:
            pass
    platform_id, label = _PLATFORM_MAP.get(arch, (arch, arch))
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
        from conductress.sweep.exporter import export_manifest, export_perf_metrics

        try:
            # Export each coordinator's series
            for coord in self.coordinators:
                output = self._export_dir / f"series-{self._platform_id}-{coord.workload_id}-{coord.metric_id}.json"
                coord.export(output, platform=self._platform_label)

            # Export perf metrics from throughput coordinator (it holds perf counters)
            for coord in self.coordinators:
                if coord.metric_id == "throughput":
                    export_perf_metrics(coord.state, self._export_dir, self._platform_id, coord.workload_id)
                    break

            # Export manifest
            export_manifest(
                self._export_dir,
                platforms=["amd64", "arm64", "intel"],
                workloads=[self.coordinators[0].workload_id] if self.coordinators else [],
            )

            # Rsync to target
            self._rsync()
        except Exception:
            logger.warning("Publish failed (non-fatal)", exc_info=True)

    def _rsync(self) -> None:
        """Rsync export directory to remote target."""
        ssh_cmd = f"ssh -i {self._ssh_key} -F /dev/null -o StrictHostKeyChecking=no -o ConnectTimeout=10"
        result = subprocess.run(
            ["rsync", "-az", "--chmod=D755,F644", "-e", ssh_cmd, f"{self._export_dir}/", self.target],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            logger.info("Published to %s", self.target)
        else:
            logger.warning("rsync failed (rc=%d): %s", result.returncode, result.stderr.strip())
