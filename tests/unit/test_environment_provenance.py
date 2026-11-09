"""Environment provenance (schema v2) and sweep environment periods.

Covers the four new provenance fields, the per-task cachecannon identity, the
server-host environment block, the shared environment diff, the point
annotation in the coordinators, and the exporter's period markers.
"""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from conductress import runner_identity as ri
from conductress.control.drift_analyzer import boot_only, change_label, environment_changes
from conductress.sweep import exporter
from conductress.sweep.planner import BenchmarkPoint, PointStatus, SweepState, SweepTask

X86_CPUINFO = """processor\t: 0
vendor_id\t: GenuineIntel
model name\t: Intel(R) Xeon(R) Platinum 8488C
microcode\t: 0x2b000685
cpu MHz\t\t: 2400.000

processor\t: 1
model name\t: Intel(R) Xeon(R) Platinum 8488C
microcode\t: 0x2b000685
"""
ARM_CPUINFO = """processor\t: 0
BogoMIPS\t: 2100.00
CPU implementer\t: 0x41
CPU part\t: 0xd40

processor\t: 1
BogoMIPS\t: 2100.00
"""


class TestHostFields:
    def test_cpuinfo_x86_has_model_and_microcode(self, tmp_path):
        path = tmp_path / "cpuinfo"
        path.write_text(X86_CPUINFO)
        fields = ri._cpuinfo_fields(path)
        assert ri._cpu_model(fields) == "Intel(R) Xeon(R) Platinum 8488C"
        assert ri._cpu_microcode(fields) == "0x2b000685"

    def test_cpuinfo_arm_has_no_microcode(self, tmp_path):
        path = tmp_path / "cpuinfo"
        path.write_text(ARM_CPUINFO)
        fields = ri._cpuinfo_fields(path)
        assert ri._cpu_model(fields) == "0xd40"
        with patch("conductress.runner_identity._read_first", return_value=None):
            assert ri._cpu_microcode(fields) is None

    def test_missing_cpuinfo_is_unknown(self, tmp_path):
        fields = ri._cpuinfo_fields(tmp_path / "none")
        assert fields == {} and ri._cpu_model(fields) == "unknown"

    def test_glibc_version_from_the_running_libc_and_fallbacks(self):
        value = ri._glibc_version()
        assert value is None or value.split(".")[0].isdigit()
        with patch("ctypes.CDLL", side_effect=OSError("no libc")):
            with patch("conductress.runner_identity.platform.libc_ver", return_value=("glibc", "2.99")):
                assert ri._glibc_version() == "2.99"
            with patch("conductress.runner_identity.platform.libc_ver", return_value=("", "")):
                assert ri._glibc_version() is None

    RPM_OUTPUT = (
        "kernel=6.1.170-213.321.amzn2023\nkernel=6.1.189-236.400.amzn2023\n"
        "glibc=2.34-231.amzn2023.0.5\nmicrocode_ctl=2.1-53.amzn2023.0.16\npackage tuned is not installed\n"
    )

    def test_installed_packages_pick_the_running_kernel_and_skip_missing_ones(self):
        completed = subprocess.CompletedProcess(ri._RPM_QUERY, 1, stdout=self.RPM_OUTPUT, stderr="")
        with patch.object(ri.subprocess, "run", return_value=completed) as run:
            packages = ri._installed_packages("6.1.189-236.400.amzn2023.x86_64")
        assert run.call_args.args[0] == ["rpm", "-q", "--qf", "%{NAME}=%{VERSION}-%{RELEASE}\n", *ri.PACKAGE_NAMES]
        assert run.call_args.kwargs["timeout"] == 2.0
        assert packages == {
            "kernel": "6.1.189-236.400.amzn2023",
            "glibc": "2.34-231.amzn2023.0.5",
            "microcode_ctl": "2.1-53.amzn2023.0.16",
            "tuned": None,
        }
        # No running-kernel match: the last listed kernel.
        assert ri.parse_package_versions(self.RPM_OUTPUT.splitlines(), "5.10.0")["kernel"] == "6.1.189-236.400.amzn2023"

    def test_installed_packages_without_rpm_or_on_timeout_is_none(self):
        with patch.object(ri.subprocess, "run", side_effect=FileNotFoundError("rpm")):
            assert ri._installed_packages("6.1") is None
        with patch.object(ri.subprocess, "run", side_effect=subprocess.TimeoutExpired("rpm", 2.0)):
            assert ri._installed_packages("6.1") is None
        empty = subprocess.CompletedProcess(ri._RPM_QUERY, 1, stdout="", stderr="rpm: command not found")
        with patch.object(ri.subprocess, "run", return_value=empty):
            assert ri._installed_packages("6.1") is None

    def test_boot_id_reads_the_file(self, tmp_path):
        path = tmp_path / "boot_id"
        path.write_text("790c2972-0361-4349-b4e5-65d59f734c2e\n")
        assert ri._boot_id(path) == "790c2972-0361-4349-b4e5-65d59f734c2e"
        assert ri._boot_id(tmp_path / "missing") is None


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout.strip()


class TestCachecannonIdentity:
    @pytest.fixture
    def checkout(self, tmp_path):
        repo = tmp_path / "cachecannon"
        (repo / "target" / "release").mkdir(parents=True)
        _git(tmp_path, "init", "-q", str(repo))
        (repo / "src.rs").write_text("fn main() {}\n")
        _git(repo, "add", "src.rs")
        _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "init")
        binary = repo / "target" / "release" / "cachecannon"
        binary.write_bytes(b"\x7fELF-fake")
        ri.clear_identity_caches()
        return repo, binary

    def test_commit_and_dirty_flag_from_the_checkout_above_the_binary(self, checkout):
        repo, binary = checkout
        identity = ri.cachecannon_identity(str(binary))
        assert identity == {"cachecannon_commit": _git(repo, "rev-parse", "HEAD"), "cachecannon_dirty": False}
        (repo / "src.rs").write_text("fn main() { changed }\n")
        # The cache key is the binary's mtime: an unchanged binary keeps the cached answer...
        assert ri.cachecannon_identity(str(binary))["cachecannon_dirty"] is False
        # ...and a rebuilt one is read again.
        binary.write_bytes(b"\x7fELF-fake-2")
        import os

        os.utime(binary, ns=(binary.stat().st_atime_ns, binary.stat().st_mtime_ns + 10**9))
        assert ri.cachecannon_identity(str(binary))["cachecannon_dirty"] is True

    def test_binary_outside_a_checkout_gets_a_content_hash(self, tmp_path):
        binary = tmp_path / "cachecannon"
        binary.write_bytes(b"copied binary")
        ri.clear_identity_caches()
        with patch("conductress.runner_identity.subprocess.run", side_effect=OSError("no git")):
            identity = ri.cachecannon_identity(str(binary))
        assert identity["cachecannon_commit"] is None and identity["cachecannon_dirty"] is None
        assert len(identity["cachecannon_sha256"]) == 16

    def test_no_binary_or_missing_file(self, tmp_path):
        assert ri.cachecannon_identity(None) == {"cachecannon_commit": None, "cachecannon_dirty": None}
        assert ri.cachecannon_identity(str(tmp_path / "nope")) == {
            "cachecannon_commit": None,
            "cachecannon_dirty": None,
        }

    def test_result_provenance_carries_the_per_task_identity(self, checkout, monkeypatch):
        repo, binary = checkout
        monkeypatch.setenv("CONDUCTRESS_RUNNER_ID", "intelbench")
        ri.clear_identity_caches()
        with patch("conductress.runner_identity.get_local_platform_info", return_value=("intel", "intel/x", ["intel"])):
            with_binary = ri.get_result_provenance(str(binary))
            without = ri.get_result_provenance()
        assert with_binary["provenance_schema_version"] == 3
        assert with_binary["environment"]["cachecannon_commit"] == _git(repo, "rev-parse", "HEAD")
        assert without["environment"]["cachecannon_commit"] is None
        for key in ("glibc_version", "cpu_microcode", "boot_id", "kernel_release"):
            assert key in with_binary["environment"]
        ri.clear_identity_caches()


class TestRemoteEnvironment:
    OUTPUT = (
        "kernel_release=6.18.56-125.164.amzn2023.aarch64\nmachine=aarch64\nboot_id=1111-2222\n"
        "machine_id=abc\nhostname=g4net-server\nglibc_version=2.34\ncpu_model=0xd4f\ncpu_microcode=\n"
        "instance_id=i-0server\n"
    )

    def test_parse_remote_environment(self):
        env = ri.parse_remote_environment(self.OUTPUT)
        assert env["kernel_release"] == "6.18.56-125.164.amzn2023.aarch64"
        assert env["glibc_version"] == "2.34" and env["boot_id"] == "1111-2222"
        assert env["cpu_microcode"] is None and env["instance_id"] == "i-0server"
        assert len(env["host_fingerprint"]) == 16
        # The fingerprint depends on the machine id and instance id, not on the order of lines.
        assert (
            ri.parse_remote_environment(self.OUTPUT.replace("hostname=g4net-server", "hostname=other"))[
                "host_fingerprint"
            ]
            != env["host_fingerprint"]
        )

    def test_remote_packages_line_is_parsed_and_missing_rpm_is_none(self):
        env = ri.parse_remote_environment(self.OUTPUT)
        assert env["packages"] is None  # a host without rpm prints an empty value
        with_packages = self.OUTPUT + (
            "\npackages=kernel=6.1.170-213.321.amzn2023,kernel=6.18.56-125.164.amzn2023,"
            "glibc=2.34-231.amzn2023.0.5,package tuned is not installed,"
        )
        env = ri.parse_remote_environment(with_packages)
        assert env["packages"] == {
            "kernel": "6.18.56-125.164.amzn2023",  # the running kernel, not the newest listed
            "glibc": "2.34-231.amzn2023.0.5",
            "microcode_ctl": None,
            "tuned": None,
        }

    def test_truncated_output_never_raises(self):
        env = ri.parse_remote_environment("kernel_release=6.1\nmach")
        assert env["kernel_release"] == "6.1" and env["machine"] is None and env["boot_id"] is None

    @pytest.mark.asyncio
    async def test_server_collects_with_one_host_command(self):
        from conductress.server import Server

        server = Server.__new__(Server)
        server.run_host_command = AsyncMock(return_value=(self.OUTPUT, ""))
        env = await server.collect_host_environment()
        assert env["kernel_release"].startswith("6.18")
        command = server.run_host_command.await_args.args[0]
        assert "uname -r" in command and "boot_id" in command and "getconf GNU_LIBC_VERSION" in command
        assert server.run_host_command.await_args.kwargs == {"check": False}

    @pytest.mark.asyncio
    async def test_task_runner_marks_same_host_by_boot_id_and_survives_failure(self, monkeypatch):
        from types import SimpleNamespace

        from conductress.task_runner import TaskRunner

        info = {"environment": {"boot_id": "1111-2222"}}
        monkeypatch.setattr("conductress.runner_identity.get_runner_info", lambda: info)
        with patch("conductress.task_runner.Server") as server_cls:
            server_cls.return_value.collect_host_environment = AsyncMock(
                return_value={"boot_id": "1111-2222", "kernel_release": "6.1"}
            )
            env = await TaskRunner._server_environment(SimpleNamespace(ip="127.0.0.1", username="ec2-user"))
            assert env["same_host"] is True
            server_cls.return_value.collect_host_environment = AsyncMock(
                return_value={"boot_id": "9999", "kernel_release": "6.1"}
            )
            env = await TaskRunner._server_environment(SimpleNamespace(ip="172.31.44.174", username="ec2-user"))
            assert env["same_host"] is False
            server_cls.return_value.collect_host_environment = AsyncMock(side_effect=OSError("ssh down"))
            assert (
                await TaskRunner._server_environment(SimpleNamespace(ip="172.31.44.174", username="ec2-user")) is None
            )


class TestEnvironmentChanges:
    V1 = {"provenance_schema_version": 1, "kernel_release": "6.1.170", "cpu_model": "x"}
    V2_SAME = {
        "provenance_schema_version": 2,
        "kernel_release": "6.1.170",
        "cpu_model": "x",
        "glibc_version": "2.34",
        "cpu_microcode": None,
        "boot_id": "a",
    }

    def test_new_fields_of_a_newer_schema_are_not_changes(self):
        assert environment_changes(self.V1, self.V2_SAME) == []

    def test_real_changes_are_listed_sorted(self):
        after = dict(
            self.V2_SAME,
            kernel_release="6.1.189",
            glibc_version="2.34-231.0.5",
            cpu_microcode="0x2b000685",
            boot_id="b",
        )
        changes = environment_changes(self.V2_SAME, after)
        assert changes == [
            "boot_id: 'a' -> 'b'",
            "cpu_microcode: None -> '0x2b000685'",
            "glibc_version: '2.34' -> '2.34-231.0.5'",
            "kernel_release: '6.1.170' -> '6.1.189'",
        ]
        assert not boot_only(changes)
        label = change_label(changes)
        assert label.startswith(
            "reboot; cpu_microcode: None -> 0x2b000685; glibc_version: 2.34 -> 2.34-231.0.5; kernel_release"
        )

    def test_boot_only_and_server_reboot(self):
        reboot = environment_changes(self.V2_SAME, dict(self.V2_SAME, boot_id="b"))
        assert reboot == ["boot_id: 'a' -> 'b'"] and boot_only(reboot) and change_label(reboot) == "reboot"
        server = environment_changes(dict(self.V2_SAME, server_boot_id="s1"), dict(self.V2_SAME, server_boot_id="s2"))
        assert boot_only(server) and change_label(server) == "server reboot"
        assert change_label([]) == "" and not boot_only([])

    def test_meta_keys_never_count(self):
        assert environment_changes(dict(self.V2_SAME, same_host=True), dict(self.V2_SAME, same_host=False)) == []

    def test_package_release_change_is_one_dotted_change_per_package(self):
        """glibc 2.34 before and after the 2026-11-06 update: only the package release moved."""
        from conductress.public_fields import public_change_label

        before = dict(self.V2_SAME, provenance_schema_version=3, packages={"glibc": "2.34-231.0.4", "kernel": "k1"})
        after = dict(before, packages={"glibc": "2.34-231.0.5", "kernel": "k1", "tuned": None})
        changes = environment_changes(before, after)
        assert changes == ["packages.glibc: '2.34-231.0.4' -> '2.34-231.0.5'"]
        assert public_change_label(changes) == "packages.glibc: 2.34-231.0.4 -> 2.34-231.0.5"
        # A schema-2 row has no packages at all: gaining the block is not a change.
        assert environment_changes(self.V2_SAME, after) == []
        # rpm missing on one side (None) against a block is a change of the block.
        assert environment_changes(dict(after, packages=None), after) == [
            "packages: None -> {'glibc': '2.34-231.0.5', 'kernel': 'k1', 'tuned': None}"
        ]

    def test_environment_for_period_reduces_a_row(self):
        row = {
            "provenance_schema_version": 2,
            "environment": dict(
                self.V2_SAME,
                conductress_revision="r1",
                cachecannon_commit="c1",
                cachecannon_dirty=False,
                host_fingerprint="f",
            ),
            "server_environment": {
                "same_host": False,
                "kernel_release": "6.18",
                "boot_id": "s",
                "glibc_version": "2.34",
                "cpu_microcode": None,
            },
        }
        reduced = ri.environment_for_period(row)
        assert reduced["kernel_release"] == "6.1.170" and reduced["server_kernel_release"] == "6.18"
        assert reduced["cachecannon_commit"] == "c1" and "host_fingerprint" not in reduced
        assert "packages" not in reduced and "server_packages" not in reduced  # schema-2 rows have none
        row["environment"]["packages"] = {"glibc": "2.34-231.0.5"}
        row["server_environment"]["packages"] = {"glibc": "2.34-231.0.4"}
        reduced = ri.environment_for_period(row)
        assert reduced["packages"] == {"glibc": "2.34-231.0.5"} and reduced["server_packages"] == {
            "glibc": "2.34-231.0.4"
        }
        row["server_environment"]["same_host"] = True
        assert "server_kernel_release" not in ri.environment_for_period(row)
        assert ri.environment_for_period({})["provenance_schema_version"] is None


class TestCoordinatorAnnotation:
    @pytest.fixture(autouse=True)
    def _valid_source(self, monkeypatch):
        import conductress.config as cfg

        if "valkey" not in cfg.REPO_NAMES:
            monkeypatch.setattr(cfg, "REPO_NAMES", cfg.REPO_NAMES + ["valkey"])

    def test_completed_task_annotates_the_point(self, tmp_path, monkeypatch):
        from conductress.sweep import coordinator_v3
        from conductress.sweep.coordinator_v3 import CachecannonThroughputSweepCoordinatorV3

        results = tmp_path / "results"
        results.mkdir()
        with (
            patch("conductress.sweep.coordinator_v3._ensure_v3_state_dir"),
            patch("conductress.sweep.coordinator_v3.V3_STATE_DIR", tmp_path),
        ):
            coord = CachecannonThroughputSweepCoordinatorV3(tmp_path, test="get")
        monkeypatch.setattr(coordinator_v3, "CONDUCTRESS_RESULTS", results)
        commit = "a" * 40
        coord.state.merge_commits = [commit]
        coord.state.commit_dates = {commit: "2026-01-01"}
        coord.state.points[commit] = BenchmarkPoint(commit=commit, date="2026-01-01")
        task = coord._create_task(SweepTask(commit=commit, reason="x", date="2026-01-01", priority=1))
        task.sweep_commit = commit
        row = {
            "task_id": task.task_id,
            "score": 1.0,
            "data": {"per_run_rps": [1.0, 1.0]},
            "end_time": "2026.11.08_07.00.00.000000",
            "provenance_schema_version": 2,
            "environment": {"kernel_release": "6.1.189", "boot_id": "b", "glibc_version": "2.34"},
        }
        (results / "output.jsonl").write_text(json.dumps(row) + "\n")
        with patch("conductress.sweep.coordinator.get_head", return_value="z" * 40):
            coord.on_task_completed(task)
        point = coord.state.points[commit]
        assert point.task_id == task.task_id and point.measured_at == "2026.11.08_07.00.00.000000"
        assert point.environment["kernel_release"] == "6.1.189" and point.environment["boot_id"] == "b"
        reloaded = SweepState.load(coord.state_file).points[commit]
        assert reloaded.environment == point.environment and reloaded.measured_at == point.measured_at


def _entry(index, env, measured_at=None, date="2026-11-0%d" % 1):
    entry = {"commit": f"c{index}", "commit_index": index, "date": date, "results": {}}
    if env is not None:
        entry["env"] = env
    if measured_at:
        entry["measured_at"] = measured_at
    return entry


class TestPeriods:
    BASE = {"provenance_schema_version": 2, "kernel_release": "6.1.170", "glibc_version": "2.34", "boot_id": "a"}

    def test_same_environment_is_one_period(self):
        points = [_entry(i, dict(self.BASE)) for i in range(4)]
        periods = exporter._build_periods(points)
        assert len(periods) == 1 and periods[0]["start_commit_index"] == 0 and periods[0]["end_commit_index"] == 3
        assert all(p["period"] == 0 for p in points) and periods[0]["label"] == ""

    def test_kernel_change_opens_a_labelled_period(self):
        after = dict(self.BASE, kernel_release="6.1.189", boot_id="b")
        points = [_entry(0, self.BASE), _entry(1, self.BASE), _entry(2, after), _entry(3, after)]
        periods = exporter._build_periods(points)
        assert [p["period"] for p in points] == [0, 0, 1, 1]
        assert periods[1]["boot_only"] is False and "kernel_release: 6.1.170 -> 6.1.189" in periods[1]["label"]
        # The published period carries the public view of the environment: no boot id.
        assert periods[1]["start_commit_index"] == 2
        assert periods[1]["env"] == {
            "provenance_schema_version": 2,
            "kernel_release": "6.1.189",
            "glibc_version": "2.34",
        }

    def test_private_fields_publish_as_the_type_of_change(self):
        sha_a, sha_b = "a" * 40, "b" * 40
        before = dict(self.BASE, conductress_revision=sha_a, instance_id="i-0123456789abcdef0")
        after = dict(before, conductress_revision=sha_b, instance_id="i-0fedcba9876543210", boot_id="b")
        points = [_entry(0, before), _entry(1, after)]
        periods = exporter._build_periods(points)
        assert periods[1]["label"] == "reboot; harness revision changed; host replaced"
        assert periods[1]["boot_only"] is False
        text = json.dumps(periods)
        assert sha_a not in text and sha_b not in text and "i-0" not in text and "boot_id" not in text

    def test_boot_only_period_is_marked_lightly(self):
        points = [_entry(0, self.BASE), _entry(1, dict(self.BASE, boot_id="b"))]
        periods = exporter._build_periods(points)
        assert len(periods) == 2 and periods[1]["boot_only"] is True and periods[1]["label"] == "reboot"

    def test_old_points_without_env_join_the_previous_period_and_v1_to_v2_is_no_boundary(self):
        v1 = {"provenance_schema_version": 1, "kernel_release": "6.1.170"}
        points = [_entry(0, None), _entry(1, v1), _entry(2, self.BASE), _entry(3, None), _entry(4, self.BASE)]
        periods = exporter._build_periods(points)
        assert len(periods) == 1 and [p["period"] for p in points] == [0] * 5
        assert periods[0]["env"] == v1  # the first recorded environment names the period (public keys only)

    def test_manual_event_opens_a_period(self):
        event = {"at": datetime(2026, 11, 6, 18, 0, tzinfo=timezone.utc), "label": "BIOS update"}
        points = [
            _entry(0, self.BASE, "2026.11.06_10.00.00.000000"),
            _entry(1, self.BASE, "2026.11.06_19.00.00.000000"),
            _entry(2, self.BASE, "2026.11.06_20.00.00.000000"),
        ]
        periods = exporter._build_periods(points, [event])
        assert [p["period"] for p in points] == [0, 1, 1]
        assert periods[1]["label"] == "BIOS update" and periods[1]["boot_only"] is False

    def test_load_environment_events(self, tmp_path):
        path = tmp_path / "environment_events.json"
        assert exporter.load_environment_events(path) == []
        path.write_text(json.dumps({"events": [{"at": "2026-11-06T18:00:00Z", "label": "BIOS update"}]}))
        events = exporter.load_environment_events(path)
        assert events[0]["label"] == "BIOS update" and events[0]["at"].tzinfo is not None

    def test_export_series_publishes_periods_and_flags_crossing_annotations(self, tmp_path):
        state = SweepState(
            merge_commits=["a", "b", "c"], commit_dates={"a": "2026-11-01", "b": "2026-11-02", "c": "2026-11-03"}
        )
        after = dict(self.BASE, kernel_release="6.1.189")
        for commit, value, env in (("a", 100.0, self.BASE), ("b", 100.5, self.BASE), ("c", 120.0, after)):
            state.points[commit] = BenchmarkPoint(
                commit=commit,
                date="2026-11-01",
                value=value,
                cv=0.1,
                reps=5,
                environment=env,
                measured_at="2026.11.08_00.00.00.000000",
                status=PointStatus.COMPLETED,
            )
        out = tmp_path / "series.json"
        with patch("conductress.sweep.exporter.load_environment_events", return_value=[]):
            exporter.export_series(state, out, workload="get")
        payload = json.loads(out.read_text())
        assert [p["period"] for p in payload["points"]] == [0, 0, 1]
        assert payload["points"][2]["env"]["kernel_release"] == "6.1.189" and payload["points"][2]["measured_at"]
        assert len(payload["periods"]) == 2 and "kernel_release" in payload["periods"][1]["label"]
        crossing = [a for a in payload["annotations"] if a["commit"] == "c"]
        assert crossing and crossing[0]["env_period_crossed"] is True
        assert all("_left_commit" not in a for a in payload["annotations"])


def test_write_results_carries_server_environment_and_task_binary(tmp_path, monkeypatch):
    from datetime import datetime as dt

    from conductress import file_protocol as fp

    monkeypatch.setattr(fp, "CONDUCTRESS_OUTPUT", tmp_path / "output.jsonl")
    monkeypatch.setattr(fp, "CONDUCTRESS_QUEUE", tmp_path / "queue")
    (tmp_path / "queue").mkdir()
    task_id = "2026.11.08_07.00.00.000000"
    (tmp_path / "queue" / f"task_{task_id}.json").write_text(
        json.dumps({"task_type": "CachecannonTaskData", "cachecannon_binary": "/opt/cc/target/release/cachecannon"})
    )
    seen = {}

    def fake_provenance(binary=None):
        seen["binary"] = binary
        return {"provenance_schema_version": 2, "environment": {"boot_id": "x"}}

    monkeypatch.setattr(fp, "get_result_provenance", fake_provenance)
    protocol = fp.FileProtocol(task_id, "client", base_dir=tmp_path / "tmp")
    protocol.server_environment = {"boot_id": "x", "same_host": True}
    results = fp.BenchmarkResults(
        method="cachecannon",
        source="valkey",
        specifier="unstable",
        commit_hash="abc",
        score=1.0,
        end_time=dt(2026, 11, 8, 7),
        data={},
        make_args="",
    )
    protocol.write_results(results)
    row = json.loads((tmp_path / "output.jsonl").read_text())
    assert seen["binary"] == "/opt/cc/target/release/cachecannon"
    assert row["server_environment"] == {"boot_id": "x", "same_host": True} and row["provenance_schema_version"] == 2
