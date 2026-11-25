"""Tests for conductress.memory_record: what a memory task records and how it is exported."""

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from conductress import memory_record as mr

# Made-up /proc/<pid>/status text (values in kB, as the kernel prints them).
PROC_STATUS = (
    "Name:\tserver\n"
    "VmPeak:\t  900000 kB\n"
    "VmRSS:\t  204800 kB\n"
    "RssAnon:\t  200000 kB\n"
    "RssFile:\t    4800 kB\n"
    "RssShmem:\t       0 kB\n"
    "Threads:\t4\n"
)

MALLOC_STATS_PROF = '___ Begin jemalloc statistics ___\nVersion: "5.3.0-0-g0"\n  config.prof: true\n  opt.prof: true\n'
MALLOC_STATS_NOPROF = "___ Begin jemalloc statistics ___\n  config.prof: true\n  opt.prof: false\n"
MALLOC_STATS_NO_SUPPORT = "___ Begin jemalloc statistics ___\n  config.prof: false\n"


class TestProcStatus:
    def test_reads_rss_fields_in_bytes(self):
        fields = mr.parse_proc_status(PROC_STATUS)
        assert fields == {
            "vm_rss": 204800 * 1024,
            "rss_anon": 200000 * 1024,
            "rss_file": 4800 * 1024,
            "rss_shmem": 0,
        }

    def test_missing_rss_anon_is_absent(self):
        # A kernel without RssAnon (older than 4.5) still gives VmRSS.
        fields = mr.parse_proc_status("VmRSS:\t 100 kB\n")
        assert fields == {"vm_rss": 100 * 1024}

    @pytest.mark.parametrize("text", [None, "", "garbage", "VmRSS:\tabc kB\n", 5])
    def test_unreadable_text_gives_nothing(self, text):
        assert mr.parse_proc_status(text) == {}


class TestEngineIdentity:
    def test_valkey_style_server_section(self):
        info = {
            "redis_version": "7.2.4",
            "valkey_version": "9.1.0",
            "server_name": "valkey",
            "executable": "/opt/builds/abc/valkey-server",
            "redis_git_sha1": "00000000",
        }
        ident = mr.engine_identity(info)
        assert ident["server_name"] == "valkey"
        assert ident["version"] == "9.1.0"  # not the compatibility redis_version
        assert ident["executable"] == "valkey-server"  # file name only, no host path
        assert ident["git_sha1"] == "00000000"

    def test_server_without_name_or_valkey_version(self):
        ident = mr.engine_identity({"redis_version": "8.2.1", "executable": "/x/y/some-server"})
        assert ident == {"server_name": None, "version": "8.2.1", "executable": "some-server", "git_sha1": None}

    @pytest.mark.parametrize("info", [None, {}, "text"])
    def test_missing_section_gives_none_fields(self, info):
        assert mr.engine_identity(info) == {"server_name": None, "version": None, "executable": None, "git_sha1": None}


class TestVersionAndProfiling:
    def test_version_text_is_first_line(self):
        assert mr.parse_version_text("\n  Example server v=1.2.3 sha=0 bits=64\nsecond\n") == (
            "Example server v=1.2.3 sha=0 bits=64"
        )

    def test_version_text_missing(self):
        assert mr.parse_version_text("") is None
        assert mr.parse_version_text(None) is None

    def test_profiling_active(self):
        assert mr.profiling_active(MALLOC_STATS_PROF) is True

    def test_profiling_inactive(self):
        assert mr.profiling_active(MALLOC_STATS_NOPROF) is False

    def test_build_without_profiling_support_is_inactive(self):
        assert mr.profiling_active(MALLOC_STATS_NO_SUPPORT) is False

    @pytest.mark.parametrize("text", [None, "", "ERR unknown command", "Stats not supported for the current allocator"])
    def test_engine_that_cannot_say_is_unknown(self, text):
        assert mr.profiling_active(text) is None


class TestConfig:
    def test_parses_alternating_lines(self):
        text = "hash-max-listpack-entries\n128\nhash-max-listpack-value\n64\n"
        assert mr.parse_config_get(text) == {"hash-max-listpack-entries": "128", "hash-max-listpack-value": "64"}

    def test_empty_or_odd_output(self):
        assert mr.parse_config_get("") == {}
        assert mr.parse_config_get(None) == {}
        assert mr.parse_config_get("only-a-name") == {}


def _sample(used, rss_info=None, vm_rss=None):
    return {"used_memory": used, "used_memory_rss": rss_info, "vm_rss": vm_rss, "rss_anon": None}


class TestRssPerItem:
    def test_proc_delta_when_unprofiled(self):
        rss, source = mr.rss_per_item(_sample(0, vm_rss=1000), _sample(0, vm_rss=1000 + 50 * 10), 10, False)
        assert (rss, source) == (50.0, mr.RSS_SOURCE_PROC)

    def test_falls_back_to_info_when_proc_unreadable(self):
        rss, source = mr.rss_per_item(_sample(0, rss_info=100), _sample(0, rss_info=300), 10, False)
        assert (rss, source) == (20.0, mr.RSS_SOURCE_INFO)

    def test_no_source_gives_none(self):
        assert mr.rss_per_item(_sample(0), _sample(0), 10, False) == (None, None)

    @pytest.mark.parametrize("profiled", [True, None])
    def test_profiled_or_unknown_gives_none(self, profiled):
        assert mr.rss_per_item(_sample(0, vm_rss=0), _sample(0, vm_rss=1000), 10, profiled) == (None, None)


def _record(**overrides):
    args = dict(
        engine={"server_name": "valkey", "version": "9.1.0", "executable": "valkey-server", "git_sha1": None},
        binary_sha256="a" * 64,
        version_text="Example v=9.1.0",
        profiling_requested=False,
        profiling_active_at_runtime=False,
        settle_requested=True,
        settle_passes=1,
        settle_key_check="hashtable",
        settle_stability="stable",
        encoding="hashtable",
        config={"hz": "10"},
        item_count=100,
        samples={
            "before_fill": _sample(1_000, vm_rss=10_000),
            "after_fill": _sample(6_000, vm_rss=20_000),
            "after_settle": _sample(5_000, vm_rss=18_000),
        },
        conductress_revision="0123456789abcdef0123456789abcdef01234567",
    )
    args.update(overrides)
    return mr.build_memory_record(**args)


class TestBuildMemoryRecord:
    def test_unprofiled_run_has_total_and_rss(self):
        rec = _record()
        assert rec["total_bytes_per_item"] == 40.0  # (5000 - 1000) / 100, after settle
        assert rec["rss_bytes_per_item"] == 80.0  # (18000 - 10000) / 100
        assert rec["rss_source"] == mr.RSS_SOURCE_PROC
        assert rec["profiling"] == {"requested": False, "active": False, "profiled": False}
        assert rec["settle"] == {
            "requested": True,
            "settled": True,
            "passes": 1,
            "key_check": "hashtable",
            "stability": "stable",
        }
        assert rec["item_count"] == 100
        assert rec["encoding"] == "hashtable"
        assert rec["binary"] == {"sha256": "a" * 64, "version_text": "Example v=9.1.0"}

    def test_profiled_run_keeps_total_but_not_rss(self):
        rec = _record(profiling_requested=True, profiling_active_at_runtime=True)
        assert rec["total_bytes_per_item"] == 40.0
        assert rec["rss_bytes_per_item"] is None
        assert rec["rss_source"] is None
        assert rec["profiling"]["profiled"] is True

    def test_unknown_profiling_follows_the_request(self):
        unprofiled = _record(profiling_active_at_runtime=None, profiling_requested=False)
        assert unprofiled["profiling"] == {"requested": False, "active": None, "profiled": False}
        assert unprofiled["rss_bytes_per_item"] == 80.0
        profiled = _record(profiling_active_at_runtime=None, profiling_requested=True)
        assert profiled["profiling"]["profiled"] is True
        assert profiled["rss_bytes_per_item"] is None

    def test_runtime_state_beats_the_request(self):
        # Requested profiling, but the allocator says it was off: the RSS is valid.
        rec = _record(profiling_requested=True, profiling_active_at_runtime=False)
        assert rec["profiling"]["profiled"] is False
        assert rec["rss_bytes_per_item"] == 80.0

    def test_without_settle_the_fill_sample_is_the_last_one(self):
        samples = {"before_fill": _sample(1_000, vm_rss=10_000), "after_fill": _sample(6_000, vm_rss=20_000)}
        rec = _record(settle_requested=False, settle_passes=None, samples=samples)
        assert rec["total_bytes_per_item"] == 50.0
        assert rec["settle"]["settled"] is False

    def test_missing_readings_give_none_not_errors(self):
        samples = {"before_fill": _sample(None), "after_fill": _sample(None), "after_settle": _sample(None)}
        rec = _record(samples=samples)
        assert rec["total_bytes_per_item"] is None
        assert rec["rss_bytes_per_item"] is None


class FakeServer:
    """A server handle whose commands answer from a table; a value that is an Exception is raised."""

    def __init__(self, answers=None, host=None, info=None, pid=4242):
        self.valkey_pid = pid
        self.answers = answers or {}
        self.host = host or {}
        self._info = info or {}

    async def run_valkey_command(self, command):
        value = self.answers.get(command)
        if isinstance(value, Exception):
            raise value
        return value

    async def run_host_command(self, command, check=True, pin_cpus=""):
        for prefix, value in self.host.items():
            if command.startswith(prefix):
                if isinstance(value, Exception):
                    raise value
                return value, ""
        return "", ""

    async def info(self, section):
        value = self._info.get(section)
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise RuntimeError(f"Failed to run 'info {section}'")
        return value


def run(coro):
    return asyncio.run(coro)


class TestCollectors:
    def test_proc_status_is_read_for_the_server_pid(self):
        server = FakeServer(host={"cat /proc/4242/status": PROC_STATUS})
        assert run(mr.read_proc_status(server))["vm_rss"] == 204800 * 1024

    def test_unreadable_proc_gives_empty(self):
        server = FakeServer(host={"cat /proc/4242/status": RuntimeError("ssh down")})
        assert run(mr.read_proc_status(server)) == {}

    def test_server_not_started_gives_empty(self):
        assert run(mr.read_proc_status(FakeServer(pid=-1))) == {}

    def test_take_sample_without_used_memory_rss(self):
        # An engine that does not report used_memory_rss still gets a /proc RSS.
        server = FakeServer(host={"cat /proc/": PROC_STATUS}, info={"memory": {"used_memory": "5000"}})
        sample = run(mr.take_sample(server))
        assert sample["used_memory"] == 5000
        assert sample["used_memory_rss"] is None
        assert sample["vm_rss"] == 204800 * 1024
        assert sample["rss_anon"] == 200000 * 1024

    def test_take_sample_when_everything_fails(self):
        server = FakeServer(host={"cat /proc/": RuntimeError("x")}, info={"memory": RuntimeError("y")})
        assert run(mr.take_sample(server)) == {
            "used_memory": None,
            "used_memory_rss": None,
            "vm_rss": None,
            "rss_anon": None,
        }

    def test_identity_collects_server_sha_and_version(self):
        server = FakeServer(
            info={"server": {"valkey_version": "9.1.0", "server_name": "valkey", "executable": "/p/valkey-server"}},
            host={"sha256sum": "b" * 64 + "  /x/valkey-server\n", "LD_LIBRARY_PATH": "Example v=9.1.0\n"},
        )
        ident = run(mr.collect_identity(server, Path("/x/valkey-server")))
        assert ident["engine"]["server_name"] == "valkey"
        assert ident["binary_sha256"] == "b" * 64
        assert ident["version_text"] == "Example v=9.1.0"

    def test_identity_degrades_to_none(self):
        server = FakeServer(info={"server": RuntimeError("no")}, host={"sha256sum": RuntimeError("no sha256sum")})
        ident = run(mr.collect_identity(server, Path("/x/server")))
        assert ident["engine"]["server_name"] is None
        assert ident["binary_sha256"] is None
        assert ident["version_text"] is None

    def test_identity_ignores_a_garbled_hash(self):
        server = FakeServer(info={"server": {}}, host={"sha256sum": "not-a-hash  /x\n"})
        assert run(mr.collect_identity(server, Path("/x/server")))["binary_sha256"] is None

    def test_profiling_state_from_malloc_stats(self):
        assert run(mr.collect_profiling_state(FakeServer({"MEMORY MALLOC-STATS": MALLOC_STATS_PROF}))) is True
        assert run(mr.collect_profiling_state(FakeServer({"MEMORY MALLOC-STATS": MALLOC_STATS_NOPROF}))) is False
        assert run(mr.collect_profiling_state(FakeServer({"MEMORY MALLOC-STATS": None}))) is None
        assert run(mr.collect_profiling_state(FakeServer({"MEMORY MALLOC-STATS": RuntimeError("x")}))) is None

    def test_config_collects_what_the_engine_knows(self):
        answers = {"CONFIG GET hz": "hz\n10\n", "CONFIG GET maxmemory*": "maxmemory\n0\nmaxmemory-policy\nnoeviction\n"}
        config = run(mr.collect_config(FakeServer(answers)))
        assert config == {"hz": "10", "maxmemory": "0", "maxmemory-policy": "noeviction"}

    def test_config_with_an_engine_that_has_none(self):
        assert run(mr.collect_config(FakeServer({}))) == {}

    def test_encoding_of_collection_key(self):
        server = FakeServer({"OBJECT ENCODING myset": '"hashtable"'})
        assert run(mr.collect_encoding(server, "sadd")) == "hashtable"

    def test_encoding_none_for_set_and_for_errors(self):
        assert run(mr.collect_encoding(FakeServer({}), "set")) is None
        assert run(mr.collect_encoding(FakeServer({"OBJECT ENCODING myhash": "ERR no such key"}), "hset")) is None


class TestExportFields:
    def test_current_point(self):
        fields = mr.export_memory_fields(_record(), overhead=35.0, user_data_bytes=5)
        assert fields == {
            "total_bytes_per_item": 40.0,
            "rss_bytes_per_item": 80.0,
            "rss_source": mr.RSS_SOURCE_PROC,
            "n": 100,
            "settled": True,
            "settle_passes": 1,
            "profiled": False,
            "engine_server_name": "valkey",
            "engine_version": "9.1.0",
            "binary_sha256": "a" * 64,
            "conductress_revision": "0123456789ab",  # a 12-character prefix only
        }

    def test_old_point_is_unknown_not_false(self):
        fields = mr.export_memory_fields(None, overhead=30.9, user_data_bytes=80)
        assert fields["settled"] is None
        assert fields["profiled"] is None
        assert fields["total_bytes_per_item"] == pytest.approx(110.9)  # overhead + user data
        for key in ("rss_bytes_per_item", "rss_source", "n", "engine_server_name", "engine_version", "binary_sha256"):
            assert fields[key] is None
        assert fields["conductress_revision"] is None

    def test_record_without_revision(self):
        fields = mr.export_memory_fields(_record(conductress_revision=None), overhead=1.0, user_data_bytes=0)
        assert fields["conductress_revision"] is None

    def test_no_full_revision_or_executable_path_is_published(self):
        text = str(mr.export_memory_fields(_record(), overhead=1.0, user_data_bytes=0))
        assert "0123456789abcdef0123456789abcdef01234567" not in text
        assert "/" not in text


def test_conductress_revision_unknown_is_none(monkeypatch):
    import conductress.runner_identity as ri

    monkeypatch.setattr(
        ri, "get_runner_info", MagicMock(return_value={"environment": {"conductress_revision": "unknown"}})
    )
    assert mr.conductress_revision() is None
    monkeypatch.setattr(ri, "get_runner_info", MagicMock(side_effect=RuntimeError("no runner config")))
    assert mr.conductress_revision() is None
