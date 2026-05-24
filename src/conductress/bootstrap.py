"""Updates/installs all packages and dependencies and sets up servers for use."""

import asyncio
import logging
import subprocess
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


def subprocess_command(command: str) -> None:
    cmd_list = command.split()
    result = subprocess.run(
        cmd_list,
        check=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.stderr:
        logger.error(repr(result.stderr))


# ======== ensure asyncssh installed and imported ========
try:
    import asyncssh
except ImportError:
    subprocess_command("sudo dnf install -y python3-pip")
    subprocess_command("python3 -m pip install --upgrade pip")
    subprocess_command("pip install asyncssh")
    try:
        import asyncssh
    except ImportError:
        logger.error("asyncssh is not available even after installation. Try again - python may need to be restarted.")
        sys.exit(1)
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
    packages = load_requirements("pip-requirements")
    if DEV:
        packages += load_requirements("pip-requirements-dev")
    # Quote each package spec to prevent shell interpretation of < and > in version ranges
    quoted_packages = " ".join(f"'{p}'" for p in packages)
    distro = await host.get_linux_distro()
    if distro == "Ubuntu":
        # ensure virtual environment
        venv_path = Path("./python-venv")
        if not await path_exists(host, venv_path, expected_type="directory"):
            await host.run(f"python3 -m venv {venv_path}")
        pip = venv_path / "bin/pip"
        await host.run(f"{pip} install --upgrade pip")
        await host.run(f"{pip} install {quoted_packages}")
    else:
        await host.run("python3 -m pip install --upgrade pip")
        await host.run(f"pip install {quoted_packages}")


async def update_rhel_packages(host: Host):
    packages = load_requirements("rhel-requirements")
    host.log_info_msg("Updating RHEL packages")
    await host.run("sudo dnf update -y", check=False)
    devtools_task = host.run('sudo dnf groupinstall -y "Development Tools"')
    packages_task = host.run(f"sudo dnf install -y {' '.join(packages)}")
    await asyncio.gather(devtools_task, packages_task)


async def update_amazon_packages(host: Host) -> None:
    # same as rhel, just uses a different package list
    packages = load_requirements("amz_requirements")
    host.log_info_msg("Updating Amazon Linux packages")
    await host.run("sudo dnf update -y")
    devtools_task = host.run("sudo dnf install -y gcc gcc-c++ make automake autoconf libtool")
    packages_task = host.run(f"sudo dnf install -y {' '.join(packages)}")
    await asyncio.gather(devtools_task, packages_task)


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
            f"cd {valkey_path} && git fetch && git reset --hard origin/unstable && make distclean && make -j"
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
ExecStart=/usr/bin/python3 -m conductress run --sweep --memory-sweep
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""


SYSTEMD_STATUS_SERVICE_TEMPLATE = """\
[Unit]
Description=Conductress status export

[Service]
Type=oneshot
User={user}
WorkingDirectory={workdir}
ExecStart=/usr/bin/python3 -m conductress status-export
"""

SYSTEMD_STATUS_TIMER_TEMPLATE = """\
[Unit]
Description=Export Conductress status every 60s

[Timer]
OnBootSec=10s
OnUnitActiveSec=60s
AccuracySec=5s

[Install]
WantedBy=timers.target
"""


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
    service_content = SYSTEMD_SERVICE_TEMPLATE.format(user=user, workdir=workdir)
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


async def install_status_timer(host: Host) -> None:
    """Install the status export timer (writes status.json every 60s)."""
    has_systemd = await host.run("command -v systemctl >/dev/null 2>&1 && echo yes || echo no", check=False)
    if "yes" not in has_systemd:
        return

    workdir = host.get_home_path() / "conductress"
    user = host.username or "ec2-user"

    service_content = SYSTEMD_STATUS_SERVICE_TEMPLATE.format(user=user, workdir=workdir)
    timer_content = SYSTEMD_STATUS_TIMER_TEMPLATE

    service_path = "/etc/systemd/system/conductress-status.service"
    timer_path = "/etc/systemd/system/conductress-status.timer"

    # Install service
    existing = await host.run(f"cat {service_path} 2>/dev/null || echo ''", check=False)
    if existing.strip() != service_content.strip():
        escaped = service_content.replace("'", "'\\''")
        await host.run(f"echo '{escaped}' | sudo tee {service_path} > /dev/null")

    # Install timer
    existing = await host.run(f"cat {timer_path} 2>/dev/null || echo ''", check=False)
    if existing.strip() != timer_content.strip():
        escaped = timer_content.replace("'", "'\\''")
        await host.run(f"echo '{escaped}' | sudo tee {timer_path} > /dev/null")
        await host.run("sudo systemctl daemon-reload")

    await host.run("sudo systemctl enable --now conductress-status.timer", check=False)
    host.log_info_msg("Status export timer installed and running")


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

    await update_pip_packages(host)
    await ensure_file_descriptor_limits(host)
    await ensure_conductress(host, pull=(server_info != "localhost"))
    await ensure_git_repo_cloned(host, "https://github.com/brendangregg/FlameGraph.git", "FlameGraph")

    # Clean up deprecated/legacy files from older versions
    await cleanup_legacy_build_cache(host)

    # Install and enable systemd service for the runner
    await install_systemd_service(host)
    await install_status_timer(host)

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
