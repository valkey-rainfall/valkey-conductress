import ast
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from conductress.bootstrap import (
    Host,
    ensure_file_descriptor_limits,
    load_requirements,
    path_exists,
    retire_status_timer,
    update_amazon_packages,
    update_pip_packages,
    update_rhel_packages,
)


class TestPackageInstallSerialized:
    """dnf must never be invoked concurrently: parallel installs race on the
    shared /var/cache/dnf cache and fail on a cold cache with a spurious
    "No such file or directory: <pkg>.rpm" error (observed on a fresh
    c8g.metal). These tests fail against the old asyncio.gather() code."""

    @staticmethod
    def _overlap_tracking_host():
        """Return (host, get_max_concurrency) where host.run tracks how many
        dnf commands are in-flight simultaneously."""
        host = MagicMock(spec=Host)
        host.log_info_msg = MagicMock()
        state = {"active": 0, "max": 0}

        async def fake_run(command, *args, **kwargs):
            if "dnf install" in command or "groupinstall" in command:
                state["active"] += 1
                state["max"] = max(state["max"], state["active"])
                await asyncio.sleep(0)  # yield so any concurrent task can interleave
                state["active"] -= 1
            return ""

        host.run = AsyncMock(side_effect=fake_run)
        return host, lambda: state["max"]

    @pytest.mark.asyncio
    @patch("conductress.bootstrap.load_requirements", return_value=["pkg-a", "pkg-b"])
    async def test_amazon_packages_not_concurrent(self, _mock_reqs):
        host, get_max = self._overlap_tracking_host()
        await update_amazon_packages(host)
        assert get_max() == 1, "dnf installs must not run concurrently"

    @pytest.mark.asyncio
    @patch("conductress.bootstrap.load_requirements", return_value=["pkg-a", "pkg-b"])
    async def test_rhel_packages_not_concurrent(self, _mock_reqs):
        host, get_max = self._overlap_tracking_host()
        await update_rhel_packages(host)
        assert get_max() == 1, "dnf installs must not run concurrently"


class TestPipInstallEditable:
    """pip must install conductress editable (-e) from the repo's absolute
    path, not a bare '.' relative to $HOME. asyncssh runs commands from
    $HOME, and a non-editable install breaks config.PROJECT_ROOT. These
    tests fail against the old `pip install '.[dev]'` code."""

    @staticmethod
    def _host(distro):
        host = MagicMock(spec=Host)
        host.log_info_msg = MagicMock()
        host.get_home_path = MagicMock(return_value=Path("/home/tester"))
        host.get_linux_distro = AsyncMock(return_value=distro)
        host.run = AsyncMock(return_value="")
        return host

    @pytest.mark.asyncio
    async def test_amazon_installs_editable_absolute(self):
        host = self._host("Amazon Linux")
        await update_pip_packages(host)
        joined = " ".join(call.args[0] for call in host.run.await_args_list)
        assert "install -e" in joined, f"expected editable install: {joined}"
        assert "/home/tester/conductress" in joined
        assert "'.[dev]'" not in joined  # old CWD-relative, non-editable target

    @pytest.mark.asyncio
    @patch("conductress.bootstrap.path_exists", new_callable=AsyncMock, return_value=True)
    async def test_ubuntu_installs_editable_in_repo_venv(self, _mock_pe):
        host = self._host("Ubuntu")
        await update_pip_packages(host)
        joined = " ".join(call.args[0] for call in host.run.await_args_list)
        assert "install -e" in joined, f"expected editable install: {joined}"
        assert "/home/tester/conductress" in joined
        # the venv must live under the repo, never in $HOME
        assert "python-venv" not in joined or "/home/tester/conductress/python-venv" in joined


class TestFileDescriptorLimits:

    @pytest.mark.asyncio
    async def test_limit_already_sufficient(self):
        """Test when current limit is already >= 65536"""
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="65536\n")

        await ensure_file_descriptor_limits(host)

        host.run.assert_called_once_with("ulimit -n")

    @pytest.mark.asyncio
    async def test_limit_already_configured(self):
        """Test when limit is configured in limits.conf but not active"""
        host = MagicMock(spec=Host)
        limits_conf = """# /etc/security/limits.conf
#<domain>      <type>  <item>         <value>
#*               soft    core            0
* soft nofile 65536
* hard nofile 65536
# End of file
"""
        host.run = AsyncMock(side_effect=["1024\n", limits_conf])
        host.log_info_msg = MagicMock()

        await ensure_file_descriptor_limits(host)

        assert host.run.call_count == 2
        host.log_info_msg.assert_called_once()

    @pytest.mark.asyncio
    async def test_insufficient_configured_limit(self):
        """Test when configured limit is too low"""
        host = MagicMock(spec=Host)
        limits_conf = """# /etc/security/limits.conf
#<domain>      <type>  <item>         <value>
* soft nofile 4096
* hard nofile 4096
# End of file
"""
        host.run = AsyncMock(side_effect=["1024\n", limits_conf])

        with pytest.raises(RuntimeError, match="Insufficient file limit"):
            await ensure_file_descriptor_limits(host)

    @pytest.mark.asyncio
    async def test_configure_new_limits(self):
        """Test when limits need to be configured"""
        host = MagicMock(spec=Host)
        limits_conf = """# /etc/security/limits.conf
#<domain>      <type>  <item>         <value>
#*               soft    core            0
# End of file
"""
        host.run = AsyncMock(side_effect=["1024\n", limits_conf, "", ""])
        host.log_info_msg = MagicMock()

        await ensure_file_descriptor_limits(host)

        assert host.run.call_count == 4
        assert "* soft nofile 65536" in host.run.call_args_list[2][0][0]
        assert "* hard nofile 65536" in host.run.call_args_list[3][0][0]


class TestLoadRequirements:

    def test_basic_requirements(self):
        content = "package1\npackage2\npackage3\n"
        with patch("builtins.open", mock_open(read_data=content)):
            result = load_requirements("test")
        assert result == ["package1", "package2", "package3"]

    def test_with_comments(self):
        content = "package1\n# comment\npackage2  # inline comment\n"
        with patch("builtins.open", mock_open(read_data=content)):
            result = load_requirements("test")
        assert result == ["package1", "", "package2"]


class TestMemtierBuildDeps:
    """memtier_benchmark's configure needs libevent (openssl), pcre2 and zlib
    dev packages; without them fresh-host bootstrap fails at ensure_memtier.
    Guard that the package requirement lists include them."""

    MEMTIER_DEPS = {"libevent-devel", "openssl-devel", "pcre2-devel", "zlib-devel"}

    @pytest.mark.parametrize("reqs", ["amz_requirements", "rhel-requirements"])
    def test_memtier_deps_present(self, reqs):
        pkgs = set(load_requirements(reqs))
        missing = self.MEMTIER_DEPS - pkgs
        assert not missing, f"{reqs} missing memtier build deps: {missing}"


class TestClangToolchain:
    """``--make-args CC=clang`` is a documented layout control (docs/layout-noise.md).
    A runner without clang turns that cell into a build failure, and a fleet where
    only some runners have it cannot run the same A/B everywhere. Guard that every
    package list installs it."""

    @pytest.mark.parametrize("reqs", ["amz_requirements", "rhel-requirements", "ubuntu-requirements"])
    def test_clang_present(self, reqs):
        pkgs = set(load_requirements(reqs))
        assert "clang" in pkgs, f"{reqs} does not install clang"


class TestHostGetHomePath:

    def test_with_username(self):
        host = Host(ip="1.2.3.4", username="testuser", name="test", conn=MagicMock())
        assert host.get_home_path() == Path("/home/testuser")

    def test_without_username(self):
        host = Host(ip="1.2.3.4", username="", name="test", conn=MagicMock())
        assert host.get_home_path() == Path.home()


class TestHostGetLinuxDistro:

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "os_release,expected_name",
        [
            (
                'PRETTY_NAME="Ubuntu 22.04.3 LTS"\nNAME="Ubuntu"\nVERSION_ID="22.04"\nVERSION="22.04.3 LTS (Jammy Jellyfish)"\n',
                "Ubuntu",
            ),
            (
                'NAME="Red Hat Enterprise Linux"\nVERSION="9.2 (Plow)"\nID="rhel"\nID_LIKE="fedora"\n',
                "Red Hat Enterprise Linux",
            ),
            (
                'NAME="Amazon Linux"\nVERSION="2023"\nID="amzn"\nID_LIKE="fedora"\nVERSION_ID="2023"\n',
                "Amazon Linux",
            ),
        ],
    )
    async def test_parse_distro(self, os_release, expected_name):
        host = MagicMock(spec=Host)
        host.distro = ""
        host.run = AsyncMock(return_value=os_release)

        result = await Host.get_linux_distro(host)

        assert result == expected_name
        assert host.distro == expected_name

    @pytest.mark.asyncio
    async def test_distro_name_cached(self):
        host = MagicMock(spec=Host)
        host.distro = "Cached Distro"
        host.run = AsyncMock()

        result = await Host.get_linux_distro(host)

        assert result == "Cached Distro"
        host.run.assert_not_called()


class TestBootstrapImportOrdering:
    """Guards against importing non-stdlib modules before they're installed.

    This prevents the common packaging mistake where setup.py (or similar packaging files)
    imports dependencies at module level before pip has installed them, causing installation
    to fail for users installing from source.
    """

    def test_detects_non_stdlib_import(self):
        """Verify detection logic works for non-stdlib imports"""
        code = "import asyncssh\nfrom setuptools import setup"
        tree = ast.parse(code)
        stdlib_modules = {"os", "sys", "setuptools"}

        with pytest.raises(AssertionError, match="non-stdlib module: asyncssh"):
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        module = alias.name.split(".")[0]
                        assert module in stdlib_modules, f"non-stdlib module: {module}"

    def test_detects_non_stdlib_from_import(self):
        """Verify detection logic works for non-stdlib from imports"""
        code = "from asyncssh import connect"
        tree = ast.parse(code)
        stdlib_modules = {"os", "sys", "setuptools"}

        with pytest.raises(AssertionError, match="non-stdlib module: asyncssh"):
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    if node.module:
                        module = node.module.split(".")[0]
                        assert module in stdlib_modules, f"non-stdlib module: {module}"

    def test_bootstrap_imports_non_stdlib(self):
        """Verify bootstrap.py imports non-stdlib modules (expected behavior for runtime script)"""
        bootstrap_path = Path(__file__).parent.parent.parent / "src" / "conductress" / "bootstrap.py"

        with open(bootstrap_path) as f:
            tree = ast.parse(f.read())

        found_asyncssh = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "asyncssh":
                        found_asyncssh = True

        assert found_asyncssh, "bootstrap.py should import asyncssh (it's a runtime script, not packaging)"


class TestImportHasNoSideEffects:
    """Importing bootstrap must never install software or escalate privileges.

    The module is imported by the CLI dispatcher and by tests on machines that
    are not benchmark runners. A missing dependency is an install error for the
    caller, never a reason for the module to run sudo or pip on its own."""

    @staticmethod
    def _module_source() -> str:
        bootstrap_path = Path(__file__).parent.parent.parent / "src" / "conductress" / "bootstrap.py"
        return bootstrap_path.read_text(encoding="utf-8")

    def test_module_level_code_does_not_spawn_processes(self):
        """No module-level statement may call subprocess; only functions and
        coroutines (which run on an explicit `conductress setup`) may act."""
        tree = ast.parse(self._module_source())
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call):
                    name = ast.unparse(sub.func)
                    assert "subprocess" not in name, f"module-level subprocess call: {name}"
                    assert name != "subprocess_command", "module-level subprocess_command call"

    def test_no_sudo_on_import_path(self):
        """No module-level string may carry a privilege escalation, so a
        missing asyncssh cannot trigger one."""
        tree = ast.parse(self._module_source())
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                continue  # the module docstring describes sudo; it does not run it
            for sub in ast.walk(node):
                if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                    assert "sudo " not in sub.value, f"sudo command in module-level code: {sub.value!r}"

    def test_missing_asyncssh_raises_import_error_with_hint(self):
        """A missing asyncssh surfaces as ImportError naming the fix, not as
        an attempted install or a sys.exit."""
        tree = ast.parse(self._module_source())
        handlers = [
            h
            for node in tree.body
            if isinstance(node, ast.Try)
            for h in node.handlers
            if h.type is not None and ast.unparse(h.type) == "ImportError"
        ]
        assert handlers, "expected a module-level try/except ImportError around `import asyncssh`"
        for handler in handlers:
            assert len(handler.body) == 1 and isinstance(
                handler.body[0], ast.Raise
            ), "the ImportError handler must only re-raise with a hint"
            raised = handler.body[0].exc
            assert raised is not None and ast.unparse(raised.func) == "ImportError"
            message = "".join(ast.unparse(a) for a in raised.args)
            assert "pip install" in message


class TestPathExists:

    @pytest.mark.asyncio
    async def test_path_not_exists(self):
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="1\n1\n1\n1\n")

        result = await path_exists(host, "/nonexistent")

        assert result is False

    @pytest.mark.asyncio
    async def test_file_exists(self):
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="0\n0\n1\n1\n")

        result = await path_exists(host, "/some/file", expected_type="file")

        assert result is True

    @pytest.mark.asyncio
    async def test_directory_exists(self):
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="0\n1\n0\n1\n")

        result = await path_exists(host, "/some/dir", expected_type="directory")

        assert result is True

    @pytest.mark.asyncio
    async def test_symlink_exists(self):
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="0\n1\n1\n0\n")

        result = await path_exists(host, "/some/link", expected_type="symlink")

        assert result is True

    @pytest.mark.asyncio
    async def test_expected_file_but_is_directory(self):
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="0\n1\n0\n1\n")

        with pytest.raises(RuntimeError, match="Expected .* to be a file"):
            await path_exists(host, "/some/dir", expected_type="file")

    @pytest.mark.asyncio
    async def test_expected_directory_but_is_file(self):
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="0\n0\n1\n1\n")

        with pytest.raises(RuntimeError, match="Expected .* to be a directory"):
            await path_exists(host, "/some/file", expected_type="directory")

    @pytest.mark.asyncio
    async def test_expected_symlink_but_is_file(self):
        host = MagicMock(spec=Host)
        host.run = AsyncMock(return_value="0\n0\n1\n1\n")

        with pytest.raises(RuntimeError, match="Expected .* to be a symlink"):
            await path_exists(host, "/some/file", expected_type="symlink")


class TestRetireStatusTimer:
    """The per-minute status timer must be removed where present and left alone where absent."""

    @staticmethod
    def _host(present: set):
        host = MagicMock(spec=Host)
        host.log_info_msg = MagicMock()
        commands = []

        async def fake_run(command, *args, **kwargs):
            commands.append(command)
            if command.startswith("test -e"):
                # path_exists probe: quoted path is the first test's argument
                path = command.split('"')[1]
                exists = path in present
                # e f d L -> exists, is file, is dir, is symlink
                return "0\n0\n1\n1\n" if exists else "1\n1\n1\n1\n"
            return ""

        host.run = fake_run
        return host, commands

    @pytest.mark.asyncio
    async def test_removes_units_and_disables_timer_when_present(self):
        host, commands = self._host(
            {"/etc/systemd/system/conductress-status.service", "/etc/systemd/system/conductress-status.timer"}
        )
        await retire_status_timer(host)
        actions = [c for c in commands if not c.startswith("test -e")]
        assert actions[0] == "sudo systemctl disable --now conductress-status.timer"
        assert actions[1].startswith("sudo rm -f ")
        assert "/etc/systemd/system/conductress-status.service" in actions[1]
        assert "/etc/systemd/system/conductress-status.timer" in actions[1]
        assert actions[2] == "sudo systemctl daemon-reload"
        assert len(actions) == 3
        host.log_info_msg.assert_called_once()

    @pytest.mark.asyncio
    async def test_removes_only_the_unit_that_is_left(self):
        host, commands = self._host({"/etc/systemd/system/conductress-status.service"})
        await retire_status_timer(host)
        rm = next(c for c in commands if c.startswith("sudo rm -f "))
        assert rm == "sudo rm -f /etc/systemd/system/conductress-status.service"

    @pytest.mark.asyncio
    async def test_no_systemd_commands_when_already_retired(self):
        host, commands = self._host(set())
        await retire_status_timer(host)
        assert all(c.startswith("test -e") for c in commands), commands
        host.log_info_msg.assert_not_called()

    def test_bootstrap_no_longer_installs_the_timer(self):
        import conductress.bootstrap as bootstrap_module  # pylint: disable=import-outside-toplevel

        source = Path(bootstrap_module.__file__).read_text(encoding="utf-8")
        assert "install_status_timer" not in source
        assert "OnUnitActiveSec" not in source
        assert "enable --now conductress-status.timer" not in source
        assert "await retire_status_timer(host)" in source


class TestRunnerServiceOnlyOnRunnerHosts:
    """update_host installs the runner unit on runner hosts only.

    A servers.json entry for another machine is a benchmark server (role
    "server" by default); a runner unit there crash loops and takes CPU from
    the measured server, so the bootstrap must not install it.
    """

    STEPS = (
        "update_rhel_packages",
        "update_ubuntu_packages",
        "update_amazon_packages",
        "ensure_conductress",
        "update_pip_packages",
        "ensure_file_descriptor_limits",
        "ensure_io_uring_enabled",
        "ensure_git_repo_cloned",
        "ensure_memtier",
        "ensure_cachecannon",
        "ensure_tls_test_certs",
        "cleanup_legacy_build_cache",
        "install_systemd_service",
        "retire_status_timer",
    )

    async def _update(self, server_info):
        import conductress.bootstrap as bootstrap_module  # pylint: disable=import-outside-toplevel
        from conductress import config  # pylint: disable=import-outside-toplevel

        host = MagicMock()
        host.get_linux_distro = AsyncMock(return_value="Amazon Linux")
        mocks = {name: AsyncMock() for name in self.STEPS}
        with (
            patch.object(bootstrap_module.Host, "from_server_info", AsyncMock(return_value=host)),
            patch.multiple(bootstrap_module, **mocks),
        ):
            await bootstrap_module.update_host(config.ServerInfo(**server_info))
        return mocks, host

    @pytest.mark.asyncio
    async def test_remote_benchmark_server_gets_no_runner_unit(self):
        mocks, host = await self._update({"ip": "198.51.100.44", "username": "operator", "name": "g4net-server"})
        mocks["install_systemd_service"].assert_not_awaited()
        mocks["ensure_cachecannon"].assert_awaited_once()  # the rest of the provisioning still runs
        mocks["retire_status_timer"].assert_awaited_once()
        assert any("role 'server'" in str(c.args[0]) for c in host.log_info_msg.call_args_list)

    @pytest.mark.asyncio
    async def test_runner_hosts_get_the_unit(self):
        for info in (
            {"ip": "localhost", "username": "", "name": "localhost"},
            {"ip": "127.0.0.1", "username": "", "name": "localhost"},
            {"ip": "198.51.100.45", "username": "operator", "name": "runner-b", "role": "runner"},
        ):
            mocks, _host = await self._update(info)
            mocks["install_systemd_service"].assert_awaited_once(), info

    @pytest.mark.asyncio
    async def test_explicit_server_role_on_loopback_gets_no_unit(self):
        mocks, _host = await self._update({"ip": "127.0.0.1", "username": "", "name": "x", "role": "server"})
        mocks["install_systemd_service"].assert_not_awaited()
