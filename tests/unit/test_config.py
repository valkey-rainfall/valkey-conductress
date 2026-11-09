import json

import pytest

import conductress.config as config_module
from conductress.config import ServerInfo, get_servers, load_server_ips


class TestServerInfo:
    def test_disabled_default_false(self):
        s = ServerInfo(ip="1.2.3.4")
        assert s.disabled is False

    def test_disabled_true(self):
        s = ServerInfo(ip="1.2.3.4", disabled=True)
        assert s.disabled is True

    def test_role_defaults_to_runner_on_loopback_and_server_elsewhere(self):
        assert ServerInfo(ip="127.0.0.1").effective_role() == "runner"
        assert ServerInfo(ip="localhost").effective_role() == "runner"
        assert ServerInfo(ip="198.51.100.44", username="operator", name="g4net-server").effective_role() == "server"

    def test_explicit_role_wins_and_is_validated(self):
        assert ServerInfo(ip="198.51.100.44", role="runner").effective_role() == "runner"
        assert ServerInfo(ip="127.0.0.1", role="server").effective_role() == "server"
        with pytest.raises(ValueError, match="role must be one of"):
            ServerInfo(ip="198.51.100.44", role="replica")

    def test_role_loads_from_servers_json(self, tmp_path, monkeypatch):
        import json

        (tmp_path / "servers.json").write_text(
            json.dumps(
                {
                    "valkey_servers": [
                        {"ip": "198.51.100.44", "username": "operator", "name": "srv"},
                        {"ip": "198.51.100.45", "username": "operator", "name": "runner-b", "role": "runner"},
                    ]
                }
            )
        )
        monkeypatch.setattr(config_module, "PROJECT_ROOT", tmp_path)
        roles = {s.name: s.effective_role() for s in load_server_ips()}
        assert roles == {"srv": "server", "runner-b": "runner"}


class TestLoadServerIps:
    def test_filters_disabled_servers(self, tmp_path, monkeypatch):
        config = {
            "valkey_servers": [
                {"ip": "10.0.0.1", "name": "active"},
                {"ip": "10.0.0.2", "name": "off", "disabled": True},
                {"ip": "10.0.0.3", "name": "also-active", "disabled": False},
            ]
        }
        config_file = tmp_path / "servers.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)

        servers = load_server_ips()
        assert len(servers) == 2
        assert servers[0].ip == "10.0.0.1"
        assert servers[1].ip == "10.0.0.3"

    def test_all_disabled(self, tmp_path, monkeypatch):
        config = {
            "valkey_servers": [
                {"ip": "10.0.0.1", "disabled": True},
            ]
        }
        config_file = tmp_path / "servers.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)

        servers = load_server_ips()
        assert len(servers) == 0

    def test_none_disabled(self, tmp_path, monkeypatch):
        config = {
            "valkey_servers": [
                {"ip": "10.0.0.1"},
                {"ip": "10.0.0.2"},
            ]
        }
        config_file = tmp_path / "servers.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)

        servers = load_server_ips()
        assert len(servers) == 2


class TestGetServers:
    """Tests for the lazy-load get_servers() accessor."""

    def test_get_servers_returns_servers_when_config_exists(self, tmp_path, monkeypatch):
        """get_servers() should return the server list when servers.json exists."""
        config = {
            "valkey_servers": [
                {"ip": "10.0.0.1", "name": "server1"},
                {"ip": "10.0.0.2", "name": "server2"},
            ]
        }
        config_file = tmp_path / "servers.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        # Reset the cached _SERVERS so get_servers() re-loads
        monkeypatch.setattr(config_module, "_SERVERS", None)

        servers = get_servers()
        assert len(servers) == 2
        assert servers[0].ip == "10.0.0.1"
        assert servers[1].ip == "10.0.0.2"

    def test_get_servers_raises_when_no_config(self, tmp_path, monkeypatch):
        """get_servers() should raise FileNotFoundError when no config file exists."""
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        monkeypatch.setattr(config_module, "_SERVERS", None)

        with pytest.raises(FileNotFoundError):
            get_servers()

    def test_get_servers_caches_result(self, tmp_path, monkeypatch):
        """get_servers() should cache the result after the first call."""
        config = {
            "valkey_servers": [
                {"ip": "10.0.0.1", "name": "server1"},
            ]
        }
        config_file = tmp_path / "servers.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        monkeypatch.setattr(config_module, "_SERVERS", None)

        first_call = get_servers()
        second_call = get_servers()
        # Should return the same object (cached)
        assert first_call is second_call

    def test_get_servers_falls_back_to_default(self, tmp_path, monkeypatch):
        """get_servers() should fall back to servers.default.json when servers.json is missing."""
        config = {
            "valkey_servers": [
                {"ip": "10.0.0.5", "name": "default-server"},
            ]
        }
        default_file = tmp_path / "servers.default.json"
        default_file.write_text(json.dumps(config))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        monkeypatch.setattr(config_module, "_SERVERS", None)

        servers = get_servers()
        assert len(servers) == 1
        assert servers[0].ip == "10.0.0.5"


class TestPinnedCommits:
    def test_valkey_benchmark_commit_is_full_sha(self):
        """VALKEY_BENCHMARK_COMMIT must be a full 40-char hex SHA. A short or
        invalid SHA (e.g. '0e09824') silently breaks fresh-host bootstrap at
        `git checkout <sha>` because the object may not resolve."""
        sha = config_module.VALKEY_BENCHMARK_COMMIT
        assert len(sha) == 40, f"expected full 40-char SHA, got {sha!r} (len {len(sha)})"
        assert all(c in "0123456789abcdef" for c in sha), f"non-hex chars in {sha!r}"


class TestSweepEpochEnvironmentToggle:
    def test_default_is_disabled(self, monkeypatch):
        monkeypatch.delenv("CONDUCTRESS_SWEEP_V3_ENABLED", raising=False)
        assert config_module._env_bool("CONDUCTRESS_SWEEP_V3_ENABLED", False) is False

    @pytest.mark.parametrize("value", ["1", "true", "TRUE", " yes ", "on", "ON"])
    def test_true_values(self, monkeypatch, value):
        monkeypatch.setenv("CONDUCTRESS_SWEEP_V3_ENABLED", value)
        assert config_module._env_bool("CONDUCTRESS_SWEEP_V3_ENABLED", False) is True

    @pytest.mark.parametrize("value", ["0", "false", "FALSE", " no ", "off", "OFF"])
    def test_false_values(self, monkeypatch, value):
        monkeypatch.setenv("CONDUCTRESS_SWEEP_V3_ENABLED", value)
        assert config_module._env_bool("CONDUCTRESS_SWEEP_V3_ENABLED", True) is False

    @pytest.mark.parametrize("value", ["", "enabled", "2", "maybe"])
    def test_invalid_values_fail_closed(self, monkeypatch, value):
        monkeypatch.setenv("CONDUCTRESS_SWEEP_V3_ENABLED", value)
        with pytest.raises(ValueError, match="CONDUCTRESS_SWEEP_V3_ENABLED must be one of"):
            config_module._env_bool("CONDUCTRESS_SWEEP_V3_ENABLED", False)

    def test_production_default_remains_disabled(self):
        assert config_module.SWEEP_V3_ENABLED is False


class TestHoistedDefaults:
    """Hoisted path/endpoint defaults keep their literal values when the env
    override is unset. A drift here silently repoints the generators or the
    publish target on every runner that relies on the defaults."""

    def test_remote_memtier_benchmark_default(self):
        assert config_module.REMOTE_MEMTIER_BENCHMARK == "~/conductress/memtier_benchmark"

    def test_local_memtier_benchmark_under_project_root(self):
        assert config_module.MEMTIER_BENCHMARK == config_module.PROJECT_ROOT / "memtier_benchmark"

    def test_cachecannon_binary_default(self, monkeypatch):
        monkeypatch.delenv("CONDUCTRESS_CACHECANNON_BINARY", raising=False)
        # Re-read the same expression the module evaluates at import time.
        import os

        value = os.environ.get(
            "CONDUCTRESS_CACHECANNON_BINARY", "/home/ec2-user/cachecannon/target/release/cachecannon"
        )
        assert value == "/home/ec2-user/cachecannon/target/release/cachecannon"
        # And the module-level constant matches when the env var is unset in CI.
        assert config_module.CACHECANNON_BINARY == "/home/ec2-user/cachecannon/target/release/cachecannon"

    def test_cachecannon_binary_reexported_from_cachecannon_module(self):
        from conductress.cachecannon import DEFAULT_CACHECANNON_BINARY

        assert DEFAULT_CACHECANNON_BINARY == config_module.CACHECANNON_BINARY

    def test_publish_target_default(self, monkeypatch):
        monkeypatch.delenv("CONDUCTRESS_PUBLISH_TARGET", raising=False)
        import os

        value = os.environ.get("CONDUCTRESS_PUBLISH_TARGET", "ec2-user@data.conductress.rainsupreme.net:/var/www/data")
        assert value == "ec2-user@data.conductress.rainsupreme.net:/var/www/data"
        assert config_module.PUBLISH_TARGET == "ec2-user@data.conductress.rainsupreme.net:/var/www/data"


class TestLoadRepositories:
    """The repositories.json loader must default to the built-in list and
    apply extend/replace semantics, keeping REPOSITORIES/REPO_NAMES working."""

    def test_missing_file_returns_builtin_defaults(self, tmp_path, monkeypatch):
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        repos = config_module.load_repositories()
        assert repos == config_module._BUILTIN_REPOSITORIES
        # A fresh call returns an independent list (not the module's own object).
        assert repos is not config_module._BUILTIN_REPOSITORIES

    def test_extend_appends_new_entries(self, tmp_path, monkeypatch):
        (tmp_path / "repositories.json").write_text(
            json.dumps(
                {
                    "repositories": [
                        {"url": "https://example.com/extra.git", "name": "extra"},
                    ]
                }
            )
        )
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        repos = config_module.load_repositories()
        assert repos[: len(config_module._BUILTIN_REPOSITORIES)] == config_module._BUILTIN_REPOSITORIES
        assert ("https://example.com/extra.git", "extra") in repos
        assert len(repos) == len(config_module._BUILTIN_REPOSITORIES) + 1

    def test_extend_skips_duplicate_directory_names(self, tmp_path, monkeypatch):
        (tmp_path / "repositories.json").write_text(
            json.dumps(
                {
                    "repositories": [
                        {"url": "https://example.com/other-valkey.git", "name": "valkey"},
                    ]
                }
            )
        )
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        repos = config_module.load_repositories()
        # The built-in 'valkey' entry is preserved; the duplicate name is skipped.
        assert repos == config_module._BUILTIN_REPOSITORIES

    def test_replace_uses_only_file_entries(self, tmp_path, monkeypatch):
        (tmp_path / "repositories.json").write_text(
            json.dumps(
                {
                    "replace": True,
                    "repositories": [
                        {"url": "https://github.com/valkey-io/valkey.git", "name": "valkey"},
                    ],
                }
            )
        )
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        repos = config_module.load_repositories()
        assert repos == [("https://github.com/valkey-io/valkey.git", "valkey")]

    def test_replace_with_empty_list_is_error(self, tmp_path, monkeypatch):
        (tmp_path / "repositories.json").write_text(json.dumps({"replace": True, "repositories": []}))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        with pytest.raises(ValueError, match="lists no repositories"):
            config_module.load_repositories()

    def test_malformed_json_is_error(self, tmp_path, monkeypatch):
        (tmp_path / "repositories.json").write_text("{ not valid json")
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        with pytest.raises(ValueError, match="not valid JSON"):
            config_module.load_repositories()

    def test_non_object_root_is_error(self, tmp_path, monkeypatch):
        (tmp_path / "repositories.json").write_text(json.dumps(["valkey"]))
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        with pytest.raises(ValueError, match="must be a JSON object"):
            config_module.load_repositories()

    def test_entry_missing_name_is_error(self, tmp_path, monkeypatch):
        (tmp_path / "repositories.json").write_text(
            json.dumps({"repositories": [{"url": "https://example.com/x.git"}]})
        )
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        with pytest.raises(ValueError, match="missing string 'name'"):
            config_module.load_repositories()

    def test_repositories_default_json_present_and_valkey_io_only(self, tmp_path, monkeypatch):
        """repositories.default.json ships the upstream-only replacement set."""
        default = json.loads((config_module.PROJECT_ROOT / "repositories.default.json").read_text())
        assert default["replace"] is True
        assert default["repositories"] == [{"url": "https://github.com/valkey-io/valkey.git", "name": "valkey"}]

    def test_builtin_repositories_unchanged(self):
        """The shipped default keeps every built-in source. Guards against an
        accidental trim: runners that queue with a fork source break if its
        entry disappears."""
        assert config_module._BUILTIN_REPOSITORIES == [
            ("https://github.com/valkey-io/valkey.git", "valkey"),
            ("https://github.com/rainsupreme/valkey.git", "rainsupreme"),
            ("https://github.com/valkey-io/valkey.git", "zuiderkwast"),
            ("https://github.com/JimB123/valkey.git", "JimB123"),
            ("https://github.com/valkey-rainfall/valkey.git", "valkey-rainfall"),
            ("https://github.com/redis/redis.git", "redis"),
        ]

    def test_repo_names_derived_from_loaded_repositories(self, tmp_path, monkeypatch):
        """REPO_NAMES is the directory-name projection of the resolved list.
        (Asserted against a freshly resolved list, since other test modules
        mutate the module-level config.REPO_NAMES at import time.)"""
        monkeypatch.setattr("conductress.config.PROJECT_ROOT", tmp_path)
        repos = config_module.load_repositories()
        assert [name for _, name in repos] == [name for _, name in config_module._BUILTIN_REPOSITORIES]
