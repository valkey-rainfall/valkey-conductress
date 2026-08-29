"""Platform detection for benchmark stabilization.

Detects CPU vendor, topology, and determines which stabilization
strategies are appropriate for the current hardware.
"""

import logging
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

logger = logging.getLogger(__name__)


class CpuVendor(Enum):
    ARM = auto()
    AMD = auto()
    INTEL = auto()
    UNKNOWN = auto()


class CacheTopology(Enum):
    MONOLITHIC = auto()  # Single shared L3 (ARM Graviton, Intel monolithic)
    CHIPLET = auto()  # Multiple L3 caches per socket (AMD EPYC CCDs)


@dataclass
class PlatformInfo:
    vendor: CpuVendor
    cache_topology: CacheTopology
    has_frequency_scaling: bool
    has_idle_states: bool
    l3_count: int  # Number of L3 cache groups
    cores_per_l3: int  # Cores sharing each L3

    @property
    def needs_single_cache_pinning(self) -> bool:
        """Chiplet architectures need client pinned to one CCD."""
        return self.cache_topology == CacheTopology.CHIPLET

    @property
    def needs_drop_caches(self) -> bool:
        """AMD/ARM benefit from drop_caches; Intel with large monolithic L3 does not."""
        return self.vendor != CpuVendor.INTEL

    @property
    def max_io_threads_per_cache(self) -> int:
        """Max io-threads that fit in one L3 cache group (minus 1 for main thread)."""
        if self.cache_topology == CacheTopology.CHIPLET:
            return self.cores_per_l3 - 1  # e.g., 7 for 8-core CCD
        return 0  # No constraint for monolithic


async def detect_platform(run_command) -> PlatformInfo:
    """Detect platform characteristics via SSH commands.

    Args:
        run_command: async callable that runs a shell command and returns (stdout, stderr)
    """
    # Detect CPU vendor
    lscpu_out, _ = await run_command("lscpu")
    lscpu = lscpu_out.lower()

    if "arm" in lscpu or "aarch64" in lscpu or "neoverse" in lscpu:
        vendor = CpuVendor.ARM
    elif "amd" in lscpu or "epyc" in lscpu:
        vendor = CpuVendor.AMD
    elif "intel" in lscpu or "xeon" in lscpu:
        vendor = CpuVendor.INTEL
    else:
        vendor = CpuVendor.UNKNOWN
        logger.warning("Unknown CPU vendor from lscpu output")

    # Detect L3 cache topology
    l3_groups_out, _ = await run_command(
        "find /sys/devices/system/cpu/cpu*/cache/index3 -name shared_cpu_list "
        "-exec cat {} \\; 2>/dev/null | sort -u | wc -l"
    )
    l3_count = int(l3_groups_out.strip()) if l3_groups_out.strip().isdigit() else 1

    # Get cores per L3
    cores_per_l3 = 0
    if l3_count > 0:
        first_l3_out, _ = await run_command(
            "cat /sys/devices/system/cpu/cpu0/cache/index3/shared_cpu_list 2>/dev/null || echo ''"
        )
        if first_l3_out.strip():
            # Parse "0-7" or "0,1,2,3" format
            cpus = _parse_cpu_list(first_l3_out.strip())
            cores_per_l3 = len(cpus)

    cache_topology = CacheTopology.CHIPLET if l3_count > 2 else CacheTopology.MONOLITHIC

    # Detect frequency scaling
    has_freq, _ = await run_command(
        "test -f /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor && echo yes || echo no"
    )
    has_frequency_scaling = "yes" in has_freq

    # Detect idle states
    has_idle, _ = await run_command("test -d /sys/devices/system/cpu/cpu0/cpuidle/state0 && echo yes || echo no")
    has_idle_states = "yes" in has_idle

    info = PlatformInfo(
        vendor=vendor,
        cache_topology=cache_topology,
        has_frequency_scaling=has_frequency_scaling,
        has_idle_states=has_idle_states,
        l3_count=l3_count,
        cores_per_l3=cores_per_l3,
    )
    logger.info(
        "Detected platform: %s, %s, %d L3 caches × %d cores",
        vendor.name,
        cache_topology.name,
        l3_count,
        cores_per_l3,
    )
    return info


def _parse_cpu_list(cpu_list_str: str) -> list[int]:
    """Parse '0-7,16-23' format into list of CPU IDs."""
    cpus: list[int] = []
    for part in cpu_list_str.split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-", 1)
            cpus.extend(range(int(start), int(end) + 1))
        elif part.isdigit():
            cpus.append(int(part))
    return cpus


def aarch64_platform_id() -> str:
    """Map the local aarch64 CPU to a dashboard platform id.

    Neoverse-V2 (CPU part 0xd4f, AWS Graviton4) -> 'graviton4'. Everything
    else (including Neoverse-V1/Graviton3) -> 'arm64', which preserves the
    existing armbench (Graviton3) series history on the dashboard.
    """
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("CPU part"):
                    return "graviton4" if line.split(":", 1)[1].strip().lower() == "0xd4f" else "arm64"
    except OSError:
        pass
    return "arm64"


def get_local_platform_tag() -> str:
    """Detect local platform tag for workload filtering.

    Returns one of: 'arm64', 'graviton4', 'amd64', 'intel', 'unknown'.
    Uses /proc/cpuinfo for CPU-part (arm) and vendor (x86) detection.
    """
    import platform as _platform

    machine = _platform.machine()
    if machine in ("aarch64", "arm64"):
        return aarch64_platform_id()
    if machine in ("x86_64", "amd64"):
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("vendor_id"):
                        if "AuthenticAMD" in line:
                            return "amd64"
                        if "GenuineIntel" in line:
                            return "intel"
                        break
        except OSError:
            pass
        return "amd64"  # fallback for x86
    return "unknown"


_PLATFORM_LABELS = {
    "arm64": "arm64/c7g.metal/graviton3",
    "graviton4": "arm64/c8g.metal/graviton4",
    "amd64": "amd64/epyc-9r14/zen4",
    "intel": "intel/xeon-8488c/sapphire-rapids",
    "unknown": "unknown",
}

_PLATFORM_ALIASES = {
    "arm64": ["graviton3", "arm64"],
    "graviton4": ["graviton4"],
    "amd64": ["amd64", "amd"],
    "intel": ["intel"],
    "unknown": [],
}


def get_local_platform_info() -> tuple[str, str, list[str]]:
    """Return canonical local platform ID, label, and routing aliases."""

    platform_id = get_local_platform_tag()
    label = _PLATFORM_LABELS.get(platform_id, platform_id)
    aliases = list(_PLATFORM_ALIASES.get(platform_id, [platform_id]))
    return platform_id, label, aliases
