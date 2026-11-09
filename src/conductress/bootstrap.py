"""Provision benchmark runner hosts.

Driven by ``conductress setup``. For every host in ``servers.json`` plus this
machine, over SSH: upgrade and install system packages, install this package
editable from ``~/conductress``, raise file-descriptor limits, enable io_uring,
clone and build the load generators at pinned commits, and install the runner
systemd service. It needs sudo on each host and changes host-wide settings.

This is not developer setup. To work on the code, install the package into a
virtualenv (see CONTRIBUTING.md); nothing here is required to run the tests.
"""

import asyncio
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from . import config

ROOT = config.PROJECT_ROOT
DEV = True

logger = logging.getLogger(__name__)

# Get config values
SERVERS = config.get_servers()
SSH_KEYFILE = config.SSH_KEYFILE
REPOSITORIES = config.REPOSITORIES


def load_requirements(name: str) -> list[str]:
    """Load requirements from a requirements file, stripping comments and blank lines."""
    with open(f"requirements/{name}.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()

    # strip comments
    lines = [line.split("#")[0].strip() for line in lines]

    return lines


# asyncssh is a declared dependency of the package. Importing this module must
# never install software or escalate privileges on the importing machine; a
# missing dependency is an install problem for the caller to fix.
try:
    import asyncssh
except ImportError as exc:
    raise ImportError(
        "conductress.bootstrap requires asyncssh, which is a declared dependency of the "
        "conductress package. Reinstall the package (pip install -e .) in the active environment."
    ) from exc
from conductress.config import PUBLISH_TARGET
from conductress.utility import async_run


@dataclass
class Host:
    ip: str
    username: str
    name: str
    conn: asyncssh.SSHClientConnection
    distro: str = ""

    async def run(self, command: str, check=True) -> str:
        result = await self.conn.run(command)
        if result.exit_status != 0:
            logger.error(f"Command failed: {command} on {self.ip} with exit status {result.exit_status} (stderr below)")
            logger.error(result.stderr)
            if check:
                raise Exception(result.stderr)
        out = result.stdout
        if not out:
            return ""
        if isinstance(out, memoryview):
            return bytes(out).decode()
        if isinstance(out, bytes) or isinstance(out, bytearray):
            return out.decode()
        return out

    async def get_linux_distro(self) -> str:
        """Get the name of the Linux distribution."""
        if not self.distro:
            raw = await self.run("cat /etc/os-release")
            lines = raw.splitlines()
            lines = [line for line in lines if line.startswith("NAME")]
            if len(lines) != 1:
                raise RuntimeError(f"Expected exactly 1 NAME line in /etc/os-release, got {len(lines)}")
            self.distro = lines[0].split('"')[1]
        return self.distro

    def get_home_path(self) -> Path:
        if not self.username:
            return Path.home()
        else:
            return Path(f"/home/{self.username}")

    @classmethod
    async def from_server_info(cls, info: config.ServerInfo) -> "Host":
        """Create a Host instance from a host name."""
        conn: asyncssh.SSHClientConnection
        if info.ip == "localhost":
            conn = await asyncssh.connect(
                info.ip,
                client_keys=[str(SSH_KEYFILE)],
                known_hosts=None,  # Disable known hosts check for localhost
            )
        elif info.username:
            conn = await asyncssh.connect(
                info.ip,
                username=info.username,
                client_keys=[str(SSH_KEYFILE)],
            )
        else:
            conn = await asyncssh.connect(
                info.ip,
                client_keys=[str(SSH_KEYFILE)],
            )
        return cls(ip=info.ip, username=info.username, name=info.name, conn=conn)

    def log_info_msg(self, message: str):
        logger.info("%16s: %s", self.name or self.ip, message)

    def log_warn_msg(self, message: str):
        logger.warning("%16s: %s", self.name or self.ip, message)


def ensure_ssh_key() -> None:
    """Ensure ssh keyfile is present"""
    logger.info("Checking for ssh keyfile")
    if not SSH_KEYFILE.is_file():
        logger.error(
            "Missing SSH keyfile: '%s' (this must be manually copied to the server)",
            SSH_KEYFILE,
        )
        sys.exit(1)
    try:
        Path(SSH_KEYFILE).chmod(0o600)
    except PermissionError:
        logger.error("Failed to set permissions on %s", SSH_KEYFILE)
        sys.exit(1)


async def ensure_server_known(server: config.ServerInfo):
    logger.info("%16s: ensuring known", server.name)
    stdout, _ = await async_run(f"ssh-keygen -F {server.ip}", check=False)
    if not stdout:
        logger.warning("%s: Adding new fingerprint to known_hosts...", server.name)
        Path.home().joinpath(".ssh").mkdir(parents=True, exist_ok=True)
        key, _ = await async_run(f"ssh-keyscan -H {server.ip} -T 10")
        known_hosts_path = Path.home() / ".ssh" / "known_hosts"
        with open(known_hosts_path, encoding="utf-8", mode="a") as f:
            f.write("\n")
            f.write(key.strip())


async def ensure_server_ssh_fingerprints() -> None:
    """Ensure all servers are in known_hosts"""
    for server in SERVERS:
        await ensure_server_known(server)


async def path_exists(host: Host, path: Union[str, Path], expected_type: Optional[str] = None) -> bool:
    """Check if a path exists and get its type"""

    commands = [f'test -{arg} "{path}"; echo $?' for arg in "efdL"]
    raw_result = await host.run(" && ".join(commands))
    result = [int(x) == 0 for x in raw_result.strip().split("\n")]  # return code 0 means test evaluated to true
    if not result[0]:
        return False
    if expected_type:
        if expected_type == "file":
            if not result[1]:
                raise RuntimeError(f"Expected {path} to be a file. ({result})")
        elif expected_type == "directory":
            if not result[2]:
                raise RuntimeError(f"Expected {path} to be a directory. ({result})")
        elif expected_type == "symlink":
            if not result[3]:
                raise RuntimeError(f"Expected {path} to be a symlink. ({result})")
        else:
            raise ValueError(f"Unknown expected_type: {expected_type}")
    return True


async def remove_motd(host: Host) -> None:
    """Remove the insights-client motd if it exists."""
    motd_path = "/etc/motd.d/insights-client"
    if await path_exists(host, motd_path, expected_type="file"):
        host.log_info_msg("Removing insights-client motd")
        await host.run(f"sudo rm {motd_path}")


async def update_pip_packages(host: Host):
    host.log_info_msg("Updating pip packages")
    # Install editable (-e) from the repo's absolute path. asyncssh runs
    # commands from $HOME, so a bare "." target fails ("no pyproject.toml"),
    # and a non-editable install breaks config.PROJECT_ROOT resolution
    # (PROJECT_ROOT must point at ~/conductress, not site-packages).
    conductress_path = host.get_home_path() / "conductress"
    target = f"'{conductress_path}[dev]'" if DEV else f"'{conductress_path}'"
    distro = await host.get_linux_distro()
    if distro == "Ubuntu":
        venv_path = conductress_path / "python-venv"
        if not await path_exists(host, venv_path, expected_type="directory"):
            await host.run(f"python3 -m venv {venv_path}")
        pip = venv_path / "bin/pip"
        await host.run(f"{pip} install --upgrade pip")
        await host.run(f"{pip} install -e {target}")
    else:
        await host.run("python3 -m pip install --upgrade pip")
        # Same interpreter module as the upgrade above: on a fresh AL2023 host a
        # bare "pip" in a non-login SSH shell resolves to /usr/bin/pip (3.9's
        # pip 21), which rejects the quoted "<path>[dev]" editable form.
        await host.run(f"python3 -m pip install -e {target}")


async def update_rhel_packages(host: Host):
    packages = load_requirements("rhel-requirements")
    host.log_info_msg("Updating RHEL packages")
    await host.run("sudo dnf update -y", check=False)
    # dnf must not run concurrently: parallel installs race on the shared
    # /var/cache/dnf package cache and fail on a cold cache with a spurious
    # "No such file or directory: <pkg>.rpm" error. Run them sequentially.
    await host.run('sudo dnf groupinstall -y "Development Tools"')
    await host.run(f"sudo dnf install -y {' '.join(packages)}")


async def update_amazon_packages(host: Host) -> None:
    # same as rhel, just uses a different package list
    packages = load_requirements("amz_requirements")
    host.log_info_msg("Updating Amazon Linux packages")
    await host.run("sudo dnf update -y")
    # See update_rhel_packages: dnf installs must be serialized, not run
    # concurrently, to avoid a cold-cache race in /var/cache/dnf.
    await host.run("sudo dnf install -y gcc gcc-c++ make automake autoconf libtool")
    await host.run(f"sudo dnf install -y {' '.join(packages)}")


async def update_ubuntu_packages(host: Host) -> None:
    packages = load_requirements("ubuntu-requirements")
    host.log_info_msg("Updating Ubuntu packages")
    await host.run("sudo apt update")
    await host.run("sudo apt upgrade -y")
    await host.run(f"sudo apt install -y {' '.join(packages)}")


async def ensure_file_descriptor_limits(host: Host) -> None:
    """Ensure file descriptor limit is sufficient for benchmarking"""
    desired_limit = 65536

    current_limit = await host.run("ulimit -n")
    if int(current_limit.strip()) >= desired_limit:
        return

    limits_conf = await host.run("cat /etc/security/limits.conf")
    lines = [line.split() for line in limits_conf.split("\n") if "nofile" in line and not line.strip().startswith("#")]
    lines = [line for line in lines if len(line) >= 4 and line[0] == "*"]

    if lines:
        configured_limit = min([int(line[3]) for line in lines])
        if configured_limit >= desired_limit:
            host.log_info_msg(f"File descriptor limit already configured to {configured_limit}")
            return
        raise RuntimeError(f"Insufficient file limit in limits.conf: {configured_limit} < {desired_limit}")

    host.log_info_msg(f"Configuring file descriptor limit to {desired_limit}")
    await host.run(f"sudo sh -c \"echo '* soft nofile {desired_limit}' >> /etc/security/limits.conf\"")
    await host.run(f"sudo sh -c \"echo '* hard nofile {desired_limit}' >> /etc/security/limits.conf\"")


async def ensure_git_repo_cloned(host: Host, repo_url, target_dir):
    host.log_info_msg(f"Ensuring repo {target_dir}...")
    if not await path_exists(host, target_dir, expected_type="directory"):
        host.log_info_msg(f"Cloning repo {repo_url}")
        await host.run(f'git clone "{repo_url}" "{target_dir}"')


async def ensure_conductress(host: Host, pull=False):
    conductress_path = host.get_home_path() / "conductress"

    if not await path_exists(host, conductress_path, expected_type="directory"):
        await ensure_git_repo_cloned(
            host,
            "https://github.com/SoftlyRaining/valkey-conductress.git",
            conductress_path,
        )
    if pull:
        host.log_info_msg("pulling conductress")
        await host.run(f"cd {conductress_path} && git pull")

    if not all(
        await asyncio.gather(
            path_exists(host, conductress_path / config.VALKEY_CLI, "file"),
            path_exists(host, conductress_path / config.VALKEY_BENCHMARK, "file"),
        )
    ):
        host.log_info_msg("retrieving and building needed binaries")
        valkey_path = conductress_path / "valkey"
        await ensure_git_repo_cloned(host, "https://github.com/valkey-io/valkey.git", valkey_path)

        await host.run(
            f"cd {valkey_path} && git fetch && git checkout {config.VALKEY_BENCHMARK_COMMIT} && make distclean && make -j"
        )

        await asyncio.gather(
            host.run(f"cp {valkey_path / 'src/valkey-cli'} {conductress_path / config.VALKEY_CLI}"),
            host.run(f"cp {valkey_path / 'src/valkey-benchmark'} {conductress_path / config.VALKEY_BENCHMARK}"),
        )


async def cleanup_legacy_build_cache(host: Host) -> None:
    """Remove old cache files from deprecated caching strategy.

    Old: build_cache/{source}/{commit_hash}/
    New: build_cache/{source}/{commit_hash}/{make_args_hash}/
    """
    old_cache_path = host.get_home_path() / "build_cache"

    if await path_exists(host, old_cache_path, expected_type="directory"):
        host.log_info_msg("Cleaning up old build cache files")
        # Remove old cache structure that didn't include make_args in path
        await host.run(f"find {old_cache_path} -name 'valkey-server' -delete", check=False)
        await host.run(f"find {old_cache_path} -type d -empty -delete", check=False)


SYSTEMD_SERVICE_TEMPLATE = """\
[Unit]
Description=Conductress benchmark runner
After=network.target

[Service]
Type=simple
User={user}
WorkingDirectory={workdir}
ExecStart=/usr/bin/python3 -m conductress run --sweep{publish_arg}
Restart=on-failure
RestartSec=5
# cachecannon at 400 connections x 8 threads on io_uring can require ~128k fds
# (it fails fast with an explicit RLIMIT_NOFILE error on kernel-5.14 hosts;
# observed on RHEL 9 Graviton 3). 65536 is not enough.
LimitNOFILE=1048576
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""


async def ensure_memtier(host: Host) -> None:
    """Ensure memtier_benchmark is installed at the pinned commit.

    Skips entirely if the binary already exists (user-provided or previously built).
    Installs to ~/conductress/memtier_benchmark alongside valkey-benchmark.
    """
    conductress_dir = host.get_home_path() / "conductress"
    binary_path = conductress_dir / "memtier_benchmark"

    if await path_exists(host, binary_path):
        host.log_info_msg("memtier_benchmark already exists, skipping install")
        return

    # Build in a temp location, copy binary to conductress dir
    build_dir = host.get_home_path() / "memtier_benchmark_build"
    if not await path_exists(host, build_dir, expected_type="directory"):
        host.log_info_msg("Cloning memtier_benchmark...")
        await host.run(f'git clone https://github.com/Redis/memtier_benchmark.git "{build_dir}"')

    host.log_info_msg(f"Building memtier_benchmark at {config.MEMTIER_COMMIT}...")
    await host.run(f"cd {build_dir} && git fetch && git checkout {config.MEMTIER_COMMIT}")
    await host.run(f"cd {build_dir} && autoreconf -ivf && ./configure && make -j$(nproc)")
    await host.run(f"cp {build_dir}/memtier_benchmark {binary_path}")


IO_URING_SYSCTL_DROPIN = "/etc/sysctl.d/99-conductress-io-uring.conf"

IO_URING_SYSCTL_CONTENT = """\
# Installed by conductress bootstrap.
# The cachecannon load generator uses io_uring on every benchmark host; a host
# where io_uring is disabled would run its generator on a different I/O engine
# (ringline's mio fallback), which is a cross-platform measurement confound.
# RHEL 9 ships kernel.io_uring_disabled=2 (disabled for all users) by default.
kernel.io_uring_disabled = 0
"""


async def ensure_io_uring_enabled(host: Host) -> None:
    """Ensure io_uring is enabled and the setting persists across reboots.

    The generator fleet must run one I/O engine everywhere: cachecannon uses
    io_uring, and a host with io_uring disabled silently falls back to mio,
    making its results a measurement confound rather than a failure. RHEL 9
    disables io_uring by default (``kernel.io_uring_disabled=2``); Amazon
    Linux 2023 leaves it enabled and often has no such sysctl at all.

    Kernels that predate the sysctl (< 6.6 mainline; RHEL 9's 5.14 backports
    it) simply have io_uring enabled unconditionally, so a missing sysctl is
    treated as enabled and left alone.
    """
    current = await host.run("cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo absent", check=False)
    current = current.strip()

    if current == "absent":
        host.log_info_msg("io_uring sysctl not present (kernel predates it) -- io_uring is enabled")
        return
    if current == "0":
        host.log_info_msg("io_uring already enabled")
    else:
        host.log_warn_msg(f"io_uring_disabled={current} -- enabling (required for cachecannon engine parity)")
        await host.run("sudo sysctl -w kernel.io_uring_disabled=0")

    # Persist regardless of the runtime value: a host that boots back to the
    # distro default would silently change the generator's I/O engine.
    existing = await host.run(f"cat {IO_URING_SYSCTL_DROPIN} 2>/dev/null || echo ''", check=False)
    if existing.strip() != IO_URING_SYSCTL_CONTENT.strip():
        host.log_info_msg(f"Persisting io_uring enablement to {IO_URING_SYSCTL_DROPIN}")
        escaped = IO_URING_SYSCTL_CONTENT.replace("'", "'\\''")
        await host.run(f"echo '{escaped}' | sudo tee {IO_URING_SYSCTL_DROPIN} > /dev/null")


async def ensure_cachecannon(host: Host) -> None:
    """Ensure cachecannon is built at the pinned commit.

    cachecannon is the canonical generator for GET/SET/DELETE throughput and
    mixed ratios (the v3 sweep epoch). The binary lands at
    ``~/cachecannon/target/release/cachecannon``, matching
    ``DEFAULT_CACHECANNON_BINARY`` in the cachecannon task.

    If a binary exists but its repo is at the wrong commit, this rebuilds:
    a generator-version drift across the fleet is exactly the kind of silent
    comparability break the pin exists to prevent.

    Safe at bootstrap time: this runs before the runner service starts. Never
    run a cargo build on a host mid-measurement -- the build contaminates the
    running cell.
    """
    cachecannon_dir = host.get_home_path() / "cachecannon"
    binary_path = cachecannon_dir / "target/release/cachecannon"
    pinned = config.SWEEP_V3_CACHECANNON_COMMIT

    if not await path_exists(host, cachecannon_dir, expected_type="directory"):
        host.log_info_msg("Cloning cachecannon...")
        await host.run(f'git clone https://github.com/cachecannon/cachecannon.git "{cachecannon_dir}"')

    current = await host.run(f"cd {cachecannon_dir} && git rev-parse HEAD", check=False)
    at_pin = current.strip() == pinned

    if at_pin and await path_exists(host, binary_path, expected_type="file"):
        host.log_info_msg(f"cachecannon already built at pinned {pinned[:12]}")
        return

    # Rust toolchain: required for the build, distro package is sufficient.
    has_cargo = await host.run("command -v cargo >/dev/null 2>&1 && echo yes || echo no", check=False)
    if "yes" not in has_cargo:
        host.log_info_msg("Installing rust/cargo...")
        await host.run("sudo dnf install -y cargo rust")

    host.log_info_msg(f"Building cachecannon at pinned {pinned[:12]} (aarch64 takes ~5min)...")
    await host.run(f"cd {cachecannon_dir} && git fetch && git checkout {pinned}")
    await host.run(f"cd {cachecannon_dir} && cargo build --release")

    if not await path_exists(host, binary_path, expected_type="file"):
        raise RuntimeError(f"cachecannon build completed but binary missing at {binary_path}")
    host.log_info_msg("cachecannon build complete")


# openssl config for the server cert's SANs: the herd targets loopback aliases
# (127.0.0.1, 127.0.0.2, ...) and the two names Valkey's own gen-test-certs.sh
# uses. check_hostname is off on the client side (see runner._tls_context), so
# these names only need to exist, not to match every alias the herd binds to.
_TLS_SERVER_EXT = (
    "[v3_req]\n"
    "basicConstraints = CA:FALSE\n"
    "keyUsage = digitalSignature, keyEncipherment\n"
    "extendedKeyUsage = serverAuth\n"
    "subjectAltName = @alt_names\n"
    "[alt_names]\n"
    "DNS.1 = localhost\n"
    "IP.1 = 127.0.0.1\n"
)


async def ensure_tls_test_certs(host: Host) -> None:
    """Generate the connection-storm TLS test certificates once per runner.

    Creates ``~/conductress/tls/{ca.crt, ca.key, server.crt, server.key}`` --
    a self-signed CA plus a server certificate for ``127.0.0.1``/``localhost``
    signed by it, 10-year validity, RSA 2048 -- mirroring what Valkey's own
    ``utils/gen-test-certs.sh`` produces, but without depending on the Valkey
    source tree (the binary cache has no checkout). These are throwaway test
    certificates for a loopback benchmark; they never leave the runner and
    authenticate nothing real.

    Idempotent: if all four files already exist the function returns without
    running openssl, so a re-bootstrap does not rotate the certs (which would
    invalidate a herd already pinned to the old CA on an in-flight cell).
    """
    tls_dir = host.get_home_path() / "conductress" / "tls"
    ca_crt = tls_dir / "ca.crt"
    ca_key = tls_dir / "ca.key"
    server_crt = tls_dir / "server.crt"
    server_key = tls_dir / "server.key"

    if all(
        await asyncio.gather(
            path_exists(host, ca_crt, "file"),
            path_exists(host, ca_key, "file"),
            path_exists(host, server_crt, "file"),
            path_exists(host, server_key, "file"),
        )
    ):
        host.log_info_msg("TLS test certificates already present, skipping generation")
        return

    host.log_info_msg(f"Generating connection-storm TLS test certificates in {tls_dir}")
    await host.run(f"mkdir -p {tls_dir}")

    # 1. CA key + self-signed CA cert (10-year).
    await host.run(f"openssl genrsa -out {ca_key} 2048")
    await host.run(
        f"openssl req -x509 -new -nodes -key {ca_key} -sha256 -days 3650 "
        f'-subj "/CN=Conductress Test CA" -out {ca_crt}'
    )
    # 2. Server key + CSR.
    await host.run(f"openssl genrsa -out {server_key} 2048")
    csr = tls_dir / "server.csr"
    await host.run(f'openssl req -new -key {server_key} -subj "/CN=127.0.0.1" -out {csr}')
    # 3. Sign the server cert with the CA, carrying the SANs from an ext file.
    ext_file = tls_dir / "server.ext"
    escaped_ext = _TLS_SERVER_EXT.replace("'", "'\\''")
    await host.run(f"printf '%s' '{escaped_ext}' > {ext_file}")
    await host.run(
        f"openssl x509 -req -in {csr} -CA {ca_crt} -CAkey {ca_key} -CAcreateserial "
        f"-out {server_crt} -days 3650 -sha256 -extensions v3_req -extfile {ext_file}"
    )
    await host.run(f"rm -f {csr} {ext_file} {tls_dir / 'ca.srl'}", check=False)
    host.log_info_msg("TLS test certificates generated")


async def install_systemd_service(host: Host) -> None:
    """Install and enable the conductress systemd service.

    Skips gracefully on systems without systemd (e.g., macOS, containers).
    """
    # Check if systemd is available
    has_systemd = await host.run("command -v systemctl >/dev/null 2>&1 && echo yes || echo no", check=False)
    if "yes" not in has_systemd:
        host.log_info_msg("Skipping systemd service (systemctl not available)")
        return

    workdir = host.get_home_path() / "conductress"
    user = host.username or "ec2-user"
    service_content = SYSTEMD_SERVICE_TEMPLATE.format(
        user=user, workdir=workdir, publish_arg=f" --publish {PUBLISH_TARGET}"
    )
    service_path = "/etc/systemd/system/conductress.service"

    # Check if service file already exists with correct content
    existing = await host.run(f"cat {service_path} 2>/dev/null || echo ''", check=False)
    if existing.strip() == service_content.strip():
        host.log_info_msg("Conductress systemd service already up to date")
    else:
        host.log_info_msg("Installing conductress systemd service")
        # Write service file via sudo tee
        escaped = service_content.replace("'", "'\\''")
        await host.run(f"echo '{escaped}' | sudo tee {service_path} > /dev/null")
        await host.run("sudo systemctl daemon-reload")

    # Enable (auto-start on boot) and start if not running
    await host.run("sudo systemctl enable conductress", check=False)
    status = await host.run("systemctl is-active conductress", check=False)
    if "active" not in status.strip():
        host.log_info_msg("Starting conductress service")
        await host.run("sudo systemctl start conductress")
    else:
        host.log_info_msg("Conductress service already running")


STATUS_TIMER_UNITS = (
    "/etc/systemd/system/conductress-status.service",
    "/etc/systemd/system/conductress-status.timer",
)


async def retire_status_timer(host: Host) -> None:
    """Disable and remove the per-minute ``conductress-status.timer`` if a host still has it.

    The runner publishes status itself at task boundaries (``TaskRunner``
    ``starting``/final/idle snapshots), which replaced the networked
    60-second timer. The timer was never removed from bootstrap, so a
    re-bootstrapped host got it back: ``python3 -m conductress status-export
    --publish`` every minute, ~12 CPU-seconds per run across 64 threads on
    whatever cores the scheduler picked, which the replica-read bottleneck
    verdict reported as a busy unallocated core on five cells in a row
    (g4bench, Sep 19 2026) before the attribution named it. Idempotent: a
    host without the unit files issues no systemd commands.
    """
    present = [path for path in STATUS_TIMER_UNITS if await path_exists(host, Path(path), expected_type="file")]
    if not present:
        return
    host.log_info_msg("Retiring the per-minute status timer (boundary publication replaced it)")
    await host.run("sudo systemctl disable --now conductress-status.timer", check=False)
    await host.run("sudo rm -f " + " ".join(present), check=False)
    await host.run("sudo systemctl daemon-reload", check=False)


async def update_host(server_info: config.ServerInfo):
    """Perform all updates on host at specified connection"""
    host = await Host.from_server_info(server_info)
    distro = await host.get_linux_distro()
    if distro == "Red Hat Enterprise Linux":
        await update_rhel_packages(host)
    elif distro == "Ubuntu":
        await update_ubuntu_packages(host)
    elif distro == "Amazon Linux":
        await update_amazon_packages(host)

    # The editable pip install below targets ~/conductress, so the checkout
    # must exist first. On a fresh host nothing else creates it.
    await ensure_conductress(host, pull=(server_info != "localhost"))
    await update_pip_packages(host)
    await ensure_file_descriptor_limits(host)
    await ensure_io_uring_enabled(host)
    await ensure_git_repo_cloned(host, "https://github.com/brendangregg/FlameGraph.git", "FlameGraph")
    await ensure_memtier(host)
    await ensure_cachecannon(host)
    await ensure_tls_test_certs(host)

    # Clean up deprecated/legacy files from older versions
    await cleanup_legacy_build_cache(host)

    # The runner service belongs on runner hosts only. A benchmark server
    # (servers.json entry with role "server", the default for a remote host)
    # never gets it: a runner unit there has no local server to drive, crash
    # loops, and its restarts take CPU from the measured server.
    if server_info.effective_role() == "runner":
        await install_systemd_service(host)
    else:
        host.log_info_msg(f"Skipping the runner service: {server_info.name or server_info.ip} has role 'server'")
    await retire_status_timer(host)

    host.log_info_msg("Ensuring config repos cloned...")
    await asyncio.gather(*(ensure_git_repo_cloned(host, repo_url, target_dir) for repo_url, target_dir in REPOSITORIES))
    host.log_info_msg("Done.")


async def update_host_list(servers: list[config.ServerInfo]) -> None:
    await asyncio.gather(*(update_host(server) for server in servers))


if __name__ == "__main__":
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)
    logger.info("⊹˚₊‧───Starting update/setup───‧₊˚⊹")

    ensure_ssh_key()
    asyncio.run(ensure_server_ssh_fingerprints())

    update_servers = SERVERS.copy()
    # add localhost if not already present
    if config.ServerInfo("localhost", "", "localhost") not in update_servers:
        update_servers.append(config.ServerInfo("localhost", "", "localhost"))

    asyncio.run(update_host_list(update_servers))

    logger.info("Update/setup complete!")
