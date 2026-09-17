import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from conductress import config
from conductress.tasks.task_perf_benchmark import (
    BASE_KEY_PATTERN,
    BASE_KEY_SIZE,
    HGETALL_FIELDS,
    HGETALL_KEYSPACE,
    CanaryPerfTaskData,
    PerfTaskData,
    PerfTaskRunner,
    compute_aggregated_stats,
    generate_padded_key,
    hgetall_preload_command,
)


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    """Patch config values for test isolation instead of mutating module globals."""
    monkeypatch.setattr(config, "MANUALLY_UPLOADED", "manual")
    monkeypatch.setattr(config, "REPO_NAMES", ["repo1", "repo2"])


def _make_perf_task_data(test: str = "set", **overrides) -> PerfTaskData:
    """Helper to create a PerfTaskData with sensible defaults."""
    defaults = dict(
        source="manual",
        specifier="test",
        make_args="",
        replicas=1,
        note="test",
        requirements={},
        test=test,
        val_size=1024,
        io_threads=4,
        pipelining=1,
        warmup=5,
        duration=10,
        perf_stat_enabled=False,
        has_expire=False,
        preload_keys=True,
    )
    defaults.update(overrides)
    return PerfTaskData(**defaults)


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d)


class TestPerfTaskData:
    """Tests for PerfTaskData class."""

    def test_keyspace_and_seed_propagate_to_runner(self):
        task = CanaryPerfTaskData(
            source="manual",
            specifier="test",
            make_args="",
            replicas=0,
            note="canary",
            requirements={},
            test="get",
            val_size=512,
            io_threads=9,
            pipelining=10,
            warmup=30,
            duration=300,
            perf_stat_enabled=False,
            has_expire=False,
            preload_keys=True,
            keyspace=12345,
            seed=42,
        )
        runner = task.prepare_task_runner([])
        assert runner.keyspace == 12345
        assert runner.seed == 42

        client = MagicMock()
        client.ip = "127.0.0.1"
        client._cpu_allocator.get_net_interface_numa.return_value = 0
        command = runner._build_benchmark_command(client, "127.0.0.1", None)
        assert "-r 12345" in command
        assert "--seed 42" in command

    def test_converts_float_to_int(self):
        """Test that warmup and duration floats are converted to ints."""
        task = PerfTaskData(
            source="manual",
            specifier="test",
            make_args="",
            replicas=1,
            note="test",
            requirements={},
            test="set",
            val_size=1024,
            io_threads=4,
            pipelining=1,
            warmup=5.5,
            duration=10.7,
            perf_stat_enabled=False,
            has_expire=False,
            preload_keys=True,
        )
        assert isinstance(task.warmup, int)
        assert isinstance(task.duration, int)
        assert task.warmup == 5
        assert task.duration == 10

    def test_loads_floats_from_json(self, temp_dir):
        """Test that loading from JSON with float values converts them to ints."""
        task_file = temp_dir / "task.json"

        # Write JSON with float values for warmup and duration
        task_data = {
            "source": "manual",
            "specifier": "test",
            "make_args": "",
            "replicas": 1,
            "note": "test",
            "requirements": {},
            "test": "set",
            "val_size": 1024,
            "io_threads": 4,
            "pipelining": 1,
            "warmup": 5.5,
            "duration": 10.7,
            "perf_stat_enabled": False,
            "has_expire": False,
            "preload_keys": True,
            "task_type": "PerfTaskData",
            "timestamp": "2024-01-01T00:00:00",
        }

        with task_file.open("w") as f:
            json.dump(task_data, f)

        # Load the task
        from conductress.task_queue import BaseTaskData

        loaded_task = BaseTaskData.from_file(task_file)

        assert isinstance(loaded_task, PerfTaskData)
        assert isinstance(loaded_task.warmup, int)
        assert isinstance(loaded_task.duration, int)
        assert loaded_task.warmup == 5
        assert loaded_task.duration == 10


class TestNewTestTypes:
    """Tests for sismember, ping, and mget test type definitions."""

    def test_sismember_exists_in_tests(self):
        """Verify sismember entry exists with correct preload and test commands."""
        assert "sismember" in PerfTaskRunner.tests
        entry = PerfTaskRunner.tests["sismember"]
        assert entry.name == "sismember"
        assert entry.preload_command == "-t sadd"
        assert entry.test_command == " -- SISMEMBER myset element:__rand_int__"

    def test_ping_exists_in_tests(self):
        """Verify ping entry exists with no preload and correct test command."""
        assert "ping" in PerfTaskRunner.tests
        entry = PerfTaskRunner.tests["ping"]
        assert entry.name == "ping"
        assert entry.preload_command is None
        assert entry.test_command == "-t ping"

    def test_ping_preload_command_is_none(self):
        """Explicitly verify that ping test has preload_command=None."""
        entry = PerfTaskRunner.tests["ping"]
        assert entry.preload_command is None

    def test_mget_exists_in_tests(self):
        """Verify mget entry exists with correct preload and test commands."""
        assert "mget" in PerfTaskRunner.tests
        entry = PerfTaskRunner.tests["mget"]
        assert entry.name == "mget"
        assert entry.preload_command == "-t set"
        assert entry.test_command == " -- MGET key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__"


class TestZsetBatteryTests:
    """Tests for the zset steady-state battery (zrange, zrangebyscore,
    zrandmember, zrem, zpop) added for the fbtree comparison."""

    def test_zset_read_entries(self):
        """zrange/zrangebyscore use --count range placeholders; zrandmember uses a literal count."""
        zr = PerfTaskRunner.tests["zrange"]
        assert zr.preload_command == "-t zadd"
        assert zr.test_command == "--count 100 -- ZRANGE myzset __rand_beg__ __rand_end__"
        assert zr.keyspace is None

        zrbs = PerfTaskRunner.tests["zrangebyscore"]
        assert zrbs.test_command == "--count 100 -- ZRANGEBYSCORE myzset __rand_beg__ __rand_end__"

        zrand = PerfTaskRunner.tests["zrandmember"]
        assert zrand.test_command == " -- ZRANDMEMBER myzset 100"

    def test_zrem_is_balanced_add_remove_sequence(self):
        """zrem pairs a random add and a random remove over the same keyspace,
        with a shell-quoted ';' sequence separator, so it self-balances at ~K/2."""
        entry = PerfTaskRunner.tests["zrem"]
        assert entry.preload_command == "-t zadd"
        assert entry.test_command == (
            " -- ZADD myzset __rand_int__ element:__rand_int__ ';' ZREM myzset element:__rand_int__"
        )
        assert " ';' " in entry.test_command  # semicolon quoted for the shell
        assert entry.keyspace is None

    def test_zpop_is_sliding_window_with_wide_append_keyspace(self):
        """zpop pops the min and appends a distinct-prefix member; its timed -r
        is overridden to a huge namespace so the resident set never drains empty."""
        entry = PerfTaskRunner.tests["zpop"]
        assert entry.preload_command == "-t zadd"
        assert entry.test_command == " -- ZPOPMIN myzset ';' ZADD myzset __rand_int__ nm:__rand_int__"
        assert " ';' " in entry.test_command
        assert entry.keyspace == PerfTaskRunner.ZPOP_APPEND_KEYSPACE
        # Append namespace must dwarf the resident keyspace to bound the drain.
        assert entry.keyspace > config.PERF_BENCH_KEYSPACE * 100

    def test_only_zpop_overrides_keyspace(self):
        """No other test overrides the timed keyspace (they share PERF_BENCH_KEYSPACE)."""
        overriding = {name for name, t in PerfTaskRunner.tests.items() if t.keyspace is not None}
        assert overriding == {"zpop"}


class TestHgetallManyHashes:
    """Tests for the multi-key CPU-heavy hgetall test: HGETALL over many
    independent 100-field hashes (the regime per-slot dispatch designs target,
    as opposed to the single-key zrange/zscore tests)."""

    def test_hgetall_entry_shape(self):
        entry = PerfTaskRunner.tests["hgetall"]
        assert entry.name == "hgetall"
        assert entry.test_command == " -- HGETALL hash:__rand_int__"
        assert entry.keyspace is None  # timed -r == resident set, not widened
        assert entry.resident_keyspace == HGETALL_KEYSPACE == 100_000

    def test_preload_is_one_variadic_hset_with_100_data_fields(self):
        """Preload uses only legacy-generator placeholders (__rand_int__, __data__)
        and fixed field names so every hash has an identical, deterministic shape."""
        cmd = PerfTaskRunner.tests["hgetall"].preload_command
        assert cmd is not None
        assert cmd.startswith(" -- HSET hash:__rand_int__ ")
        assert cmd.count("__data__") == HGETALL_FIELDS == 100
        assert cmd.count("__rand_int__") == 1  # only the key varies
        assert " f000 __data__ " in cmd and cmd.endswith(" f099 __data__")
        assert "__rand_field__" not in cmd and "--count" not in cmd  # not in d2eee78a
        assert cmd == hgetall_preload_command()

    def test_only_hgetall_sets_resident_keyspace(self):
        overriding = {name for name, t in PerfTaskRunner.tests.items() if t.resident_keyspace is not None}
        assert overriding == {"hgetall"}

    def test_resident_keyspace_defaults_to_task_keyspace(self):
        runner = _make_runner("get")
        assert runner.resident_keyspace == runner.keyspace == config.PERF_BENCH_KEYSPACE
        runner.keyspace = 4321  # prepare_task_runner assigns after construction
        assert runner.resident_keyspace == 4321

    def test_hgetall_resident_keyspace_wins_over_task_keyspace(self):
        runner = _make_runner("hgetall")
        runner.keyspace = config.PERF_BENCH_KEYSPACE
        assert runner.resident_keyspace == HGETALL_KEYSPACE

    def test_hgetall_timed_command_uses_resident_keyspace(self):
        """The timed -r must equal the preloaded set: a wider -r would make most
        HGETALLs miss (empty replies), a narrower one would under-cover it."""
        runner = _make_runner("hgetall")
        client = MagicMock()
        client.ip = "127.0.0.1"
        client._cpu_allocator.get_net_interface_numa.return_value = 0
        command = runner._build_benchmark_command(client, "127.0.0.1", None)
        assert f"-r {HGETALL_KEYSPACE} " in command
        assert f"-r {config.PERF_BENCH_KEYSPACE} " not in command
        assert command.rstrip().endswith("-- HGETALL hash:__rand_int__")

    def test_zpop_timed_keyspace_still_wins_over_resident(self):
        """Regression guard: the zpop widening override is unaffected."""
        runner = _make_runner("zpop")
        client = MagicMock()
        client.ip = "127.0.0.1"
        client._cpu_allocator.get_net_interface_numa.return_value = 0
        command = runner._build_benchmark_command(client, "127.0.0.1", None)
        assert f"-r {PerfTaskRunner.ZPOP_APPEND_KEYSPACE} " in command

    def test_hgetall_padded_key_commands(self):
        """key_size>0 pads the hash key in both preload and timed commands."""
        runner = _make_runner("hgetall", key_size=64)
        padded_key = generate_padded_key(64)
        assert runner.preload_command == hgetall_preload_command(padded_key)
        assert runner.test_command == f" -- HGETALL {padded_key}"
        assert runner.preload_command is not None
        assert runner.preload_command.count("__data__") == HGETALL_FIELDS

    def test_hgetall_preload_helper_respects_field_count(self):
        cmd = hgetall_preload_command("k", fields=3)
        assert cmd == " -- HSET k f000 __data__ f001 __data__ f002 __data__"


class TestPerfTaskDataSerialization:
    """Tests for PerfTaskData serialization/deserialization with new test names."""

    @pytest.mark.parametrize("test_name", ["sismember", "ping", "mget"])
    def test_serialize_deserialize_new_test_types(self, test_name, temp_dir):
        """Verify PerfTaskData round-trips correctly for each new test type."""
        from conductress.task_queue import BaseTaskData

        task = _make_perf_task_data(test=test_name)
        task_file = temp_dir / f"task_{test_name}.json"
        task.save_to_file(task_file)

        loaded = BaseTaskData.from_file(task_file)
        assert isinstance(loaded, PerfTaskData)
        assert loaded.test == test_name
        assert loaded.source == "manual"
        assert loaded.specifier == "test"
        assert loaded.val_size == 1024

    @pytest.mark.parametrize("test_name", ["sismember", "ping", "mget"])
    def test_deserialize_new_test_types_from_raw_json(self, test_name, temp_dir):
        """Verify PerfTaskData deserializes from raw JSON for new test types."""
        from conductress.task_queue import BaseTaskData

        task_data = {
            "source": "manual",
            "specifier": "unstable",
            "make_args": "",
            "replicas": 1,
            "note": "",
            "requirements": {},
            "test": test_name,
            "val_size": 512,
            "io_threads": 1,
            "pipelining": 1,
            "warmup": 60,
            "duration": 300,
            "perf_stat_enabled": False,
            "has_expire": False,
            "preload_keys": True,
            "task_type": "PerfTaskData",
            "timestamp": "2025-01-15T12:00:00",
        }
        task_file = temp_dir / f"task_{test_name}.json"
        with task_file.open("w") as f:
            json.dump(task_data, f)

        loaded = BaseTaskData.from_file(task_file)
        assert isinstance(loaded, PerfTaskData)
        assert loaded.test == test_name
        assert loaded.val_size == 512
        assert loaded.warmup == 60
        assert loaded.duration == 300


class TestPaddedKeyProperties:
    """Property-based tests for padded key generation."""

    @settings(max_examples=100)
    @given(key_size=st.integers(min_value=17, max_value=10000))
    def test_padded_key_length_correctness(self, key_size):
        """**Validates: Requirements 2.2**

        Feature: benchmark-tooling-enhancements, Property 1: Padded key length correctness

        For any key_size > BASE_KEY_SIZE, the generated padded key must have
        exactly key_size bytes and start with the base key pattern.
        """
        result = generate_padded_key(key_size)
        assert len(result) == key_size, f"Expected padded key length {key_size}, got {len(result)}"
        assert result.startswith(
            BASE_KEY_PATTERN
        ), f"Padded key must start with '{BASE_KEY_PATTERN}', got '{result[:20]}...'"

    @settings(max_examples=100)
    @given(key_size=st.integers(min_value=17, max_value=10000))
    def test_custom_command_generation_uses_padded_key(self, key_size):
        """**Validates: Requirements 2.3, 2.4**

        Feature: benchmark-tooling-enhancements, Property 2: Custom command generation uses padded key

        For each test type with a preload command (not None) and any key_size > BASE_KEY_SIZE,
        both the preload_command and test_command on the runner must contain the '--' custom
        command syntax prefix and the padded key string from generate_padded_key(key_size).
        """
        padded_key = generate_padded_key(key_size)

        # Test types that have a preload_command (not None)
        tests_with_preload = [name for name, test in PerfTaskRunner.tests.items() if test.preload_command is not None]

        for test_name in tests_with_preload:
            runner = PerfTaskRunner(
                task_name="prop_test",
                server_infos=[],
                binary_source="manual",
                specifier="test",
                io_threads=1,
                valsize=64,
                pipelining=1,
                test=test_name,
                warmup=1,
                duration=1,
                preload_keys=True,
                has_expire=False,
                make_args="",
                key_size=key_size,
            )

            # Both commands must contain '--' custom command syntax
            assert "--" in runner.preload_command, (
                f"Test '{test_name}' with key_size={key_size}: "
                f"preload_command must contain '--', got: {runner.preload_command}"
            )
            assert "--" in runner.test_command, (
                f"Test '{test_name}' with key_size={key_size}: "
                f"test_command must contain '--', got: {runner.test_command}"
            )

            # Both commands must contain the padded key string
            assert padded_key in runner.preload_command, (
                f"Test '{test_name}' with key_size={key_size}: "
                f"preload_command must contain padded key '{padded_key[:30]}...', "
                f"got: {runner.preload_command}"
            )
            assert padded_key in runner.test_command, (
                f"Test '{test_name}' with key_size={key_size}: "
                f"test_command must contain padded key '{padded_key[:30]}...', "
                f"got: {runner.test_command}"
            )

    def test_key_size_zero_produces_unmodified_commands(self):
        """**Validates: Requirements 2.5**

        Feature: benchmark-tooling-enhancements, Property 3: key_size=0 produces unmodified commands

        For each test type in PerfTaskRunner.tests, when key_size is 0,
        the generated test_command and preload_command must equal the
        test's original test_command and preload_command (unmodified).
        """
        for test_name, test_def in PerfTaskRunner.tests.items():
            runner = PerfTaskRunner(
                task_name="prop_test",
                server_infos=[],
                binary_source="manual",
                specifier="test",
                io_threads=1,
                valsize=64,
                pipelining=1,
                test=test_name,
                warmup=1,
                duration=1,
                preload_keys=True,
                has_expire=False,
                make_args="",
                key_size=0,
            )

            assert runner.test_command == test_def.test_command, (
                f"Test '{test_name}' with key_size=0: "
                f"test_command should be '{test_def.test_command}', "
                f"got '{runner.test_command}'"
            )
            assert runner.preload_command == test_def.preload_command, (
                f"Test '{test_name}' with key_size=0: "
                f"preload_command should be '{test_def.preload_command}', "
                f"got '{runner.preload_command}'"
            )


def _make_runner(test: str = "set", key_size: int = 0) -> PerfTaskRunner:
    """Helper to create a PerfTaskRunner with sensible defaults."""
    return PerfTaskRunner(
        task_name="unit_test",
        server_infos=[],
        binary_source="manual",
        specifier="test",
        io_threads=1,
        valsize=64,
        pipelining=1,
        test=test,
        warmup=1,
        duration=1,
        preload_keys=True,
        has_expire=False,
        make_args="",
        key_size=key_size,
    )


class TestKeySizeUnitTests:
    """Unit tests for key-size custom command building.

    Requirements: 9.2
    """

    # ── Test 1: generate_padded_key() at various target sizes ──

    def test_generate_padded_key_size_zero(self):
        """key_size=0 returns BASE_KEY_PATTERN unchanged."""
        result = generate_padded_key(0)
        assert result == BASE_KEY_PATTERN

    def test_generate_padded_key_size_16(self):
        """key_size=16 (== BASE_KEY_SIZE) returns BASE_KEY_PATTERN unchanged."""
        result = generate_padded_key(16)
        assert result == BASE_KEY_PATTERN
        assert len(result) == BASE_KEY_SIZE

    def test_generate_padded_key_size_17(self):
        """key_size=17 returns BASE_KEY_PATTERN + 'A' (length 17)."""
        result = generate_padded_key(17)
        assert len(result) == 17
        assert result == BASE_KEY_PATTERN + "A"
        assert result.startswith(BASE_KEY_PATTERN)

    def test_generate_padded_key_size_64(self):
        """key_size=64 returns BASE_KEY_PATTERN + 'A'*48 (length 64)."""
        result = generate_padded_key(64)
        assert len(result) == 64
        assert result == BASE_KEY_PATTERN + "A" * 48
        assert result.startswith(BASE_KEY_PATTERN)

    def test_generate_padded_key_size_256(self):
        """key_size=256 returns BASE_KEY_PATTERN + 'A'*240 (length 256)."""
        result = generate_padded_key(256)
        assert len(result) == 256
        assert result == BASE_KEY_PATTERN + "A" * 240
        assert result.startswith(BASE_KEY_PATTERN)

    def test_generate_padded_key_size_1024(self):
        """key_size=1024 returns BASE_KEY_PATTERN + 'A'*1008 (length 1024)."""
        result = generate_padded_key(1024)
        assert len(result) == 1024
        assert result == BASE_KEY_PATTERN + "A" * 1008
        assert result.startswith(BASE_KEY_PATTERN)

    # ── Test 2: _build_custom_command() for each test type with key_size=64 ──

    def test_build_custom_command_set(self):
        """set with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("set", key_size=64)
        assert runner.preload_command == f" -- SET {padded_key} __rand_field__"
        assert runner.test_command == f" -- SET {padded_key} __rand_field__"

    def test_build_custom_command_get(self):
        """get with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("get", key_size=64)
        assert runner.preload_command == f" -- SET {padded_key} __rand_field__"
        assert runner.test_command == f" -- GET {padded_key}"

    def test_build_custom_command_sadd(self):
        """sadd with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("sadd", key_size=64)
        assert runner.preload_command == f" -- SADD {padded_key} element:__rand_int__"
        assert runner.test_command == f" -- SADD {padded_key} element:__rand_int__"

    def test_build_custom_command_sismember(self):
        """sismember with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("sismember", key_size=64)
        assert runner.preload_command == f" -- SADD {padded_key} element:__rand_int__"
        assert runner.test_command == f" -- SISMEMBER {padded_key} element:__rand_int__"

    def test_build_custom_command_hset(self):
        """hset with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("hset", key_size=64)
        assert runner.preload_command == f" -- HSET {padded_key} field:__rand_int__ __rand_field__"
        assert runner.test_command == f" -- HSET {padded_key} field:__rand_int__ __rand_field__"

    def test_build_custom_command_zadd(self):
        """zadd with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("zadd", key_size=64)
        assert runner.preload_command == f" -- ZADD {padded_key} __rand_int__ element:__rand_int__"
        assert runner.test_command == f" -- ZADD {padded_key} __rand_int__ element:__rand_int__"

    def test_build_custom_command_mget(self):
        """mget with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("mget", key_size=64)
        assert runner.preload_command == f" -- SET {padded_key} __rand_field__"
        assert runner.test_command == f" -- MGET {padded_key} {padded_key} {padded_key} {padded_key}"

    def test_build_custom_command_ping(self):
        """ping with key_size=64: preload is None, test command is '-t ping'."""
        runner = _make_runner("ping", key_size=64)
        assert runner.preload_command is None
        assert runner.test_command == "-t ping"

    def test_build_custom_command_zrank(self):
        """zrank with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("zrank", key_size=64)
        assert runner.preload_command == f" -- ZADD {padded_key} __rand_int__ element:__rand_int__"
        assert runner.test_command == f" -- ZRANK {padded_key} element:__rand_int__"

    def test_build_custom_command_zcount(self):
        """zcount with key_size=64 produces correct custom commands."""
        padded_key = generate_padded_key(64)
        runner = _make_runner("zcount", key_size=64)
        assert runner.preload_command == f" -- ZADD {padded_key} __rand_int__ element:__rand_int__"
        assert runner.test_command == f" -- ZCOUNT {padded_key} __rand_int__ __rand_int__"

    # ── Test 3: key_size=0 produces unmodified commands for all test types ──

    @pytest.mark.parametrize("test_name", list(PerfTaskRunner.tests.keys()))
    def test_key_size_zero_unmodified_commands(self, test_name):
        """key_size=0 produces the original unmodified commands for all test types."""
        test_def = PerfTaskRunner.tests[test_name]
        runner = _make_runner(test_name, key_size=0)
        assert (
            runner.test_command == test_def.test_command
        ), f"{test_name}: test_command should be unmodified when key_size=0"
        assert (
            runner.preload_command == test_def.preload_command
        ), f"{test_name}: preload_command should be unmodified when key_size=0"

    # ── Test 4: key_size attribute is stored on the runner ──

    def test_key_size_stored_on_runner_zero(self):
        """key_size=0 is stored on the runner instance."""
        runner = _make_runner("set", key_size=0)
        assert runner.key_size == 0

    def test_key_size_stored_on_runner_nonzero(self):
        """key_size=64 is stored on the runner instance."""
        runner = _make_runner("set", key_size=64)
        assert runner.key_size == 64

    def test_key_size_stored_on_runner_large(self):
        """key_size=1024 is stored on the runner instance."""
        runner = _make_runner("get", key_size=1024)
        assert runner.key_size == 1024


class TestStatisticalAggregationProperties:
    """Property-based tests for statistical aggregation correctness."""

    @settings(max_examples=100)
    @given(
        per_run_rps=st.lists(
            st.floats(min_value=1.0, max_value=1e9, allow_nan=False, allow_infinity=False),
            min_size=2,
            max_size=100,
        )
    )
    def test_statistical_aggregation_correctness(self, per_run_rps):
        """**Validates: Requirements 4.4**

        Feature: benchmark-tooling-enhancements, Property 6: Statistical aggregation correctness

        For any list of N > 1 positive floats, the computed mean_rps must equal
        the arithmetic mean and ci_95 must equal t_critical * (stdev / sqrt(N)).
        """
        import math
        import statistics

        from scipy.stats import t as scipy_t

        n = len(per_run_rps)
        mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)

        # Verify mean equals arithmetic mean
        expected_mean = statistics.mean(per_run_rps)
        assert math.isclose(
            mean_rps, expected_mean, rel_tol=1e-9
        ), f"mean_rps={mean_rps} != expected arithmetic mean={expected_mean}"

        # Verify CI equals t_critical * (stdev / sqrt(N))
        expected_ci = scipy_t.ppf(0.975, n - 1) * (statistics.stdev(per_run_rps) / math.sqrt(n))
        assert math.isclose(ci_95, expected_ci, rel_tol=1e-9), f"ci_95={ci_95} != expected CI={expected_ci}"


class TestRepetitionsUnitTests:
    """Unit tests for repetitions logic.

    Requirements: 9.4
    """

    # ── Test 1: Single-run behavior (repetitions=1) produces same result format ──

    def test_compute_aggregated_stats_single_run_raises(self):
        """compute_aggregated_stats raises ValueError with fewer than 2 values."""
        with pytest.raises(ValueError, match="Need at least 2 values"):
            compute_aggregated_stats([100.0])

    def test_compute_aggregated_stats_empty_raises(self):
        """compute_aggregated_stats raises ValueError with empty list."""
        with pytest.raises(ValueError, match="Need at least 2 values"):
            compute_aggregated_stats([])

    def test_perf_task_data_default_repetitions(self):
        """PerfTaskData defaults to repetitions=1."""
        task = _make_perf_task_data()
        assert task.repetitions == 1

    def test_perf_task_data_repetitions_1_serialization(self, temp_dir):
        """PerfTaskData with repetitions=1 serializes and deserializes correctly."""
        from conductress.task_queue import BaseTaskData

        task = _make_perf_task_data(repetitions=1)
        task_file = temp_dir / "task_rep1.json"
        task.save_to_file(task_file)

        loaded = BaseTaskData.from_file(task_file)
        assert isinstance(loaded, PerfTaskData)
        assert loaded.repetitions == 1

    def test_perf_task_data_repetitions_3_serialization(self, temp_dir):
        """PerfTaskData with repetitions=3 serializes and deserializes correctly."""
        from conductress.task_queue import BaseTaskData

        task = _make_perf_task_data(repetitions=3)
        task_file = temp_dir / "task_rep3.json"
        task.save_to_file(task_file)

        loaded = BaseTaskData.from_file(task_file)
        assert isinstance(loaded, PerfTaskData)
        assert loaded.repetitions == 3

    def test_perf_task_data_backward_compat_no_repetitions_field(self, temp_dir):
        """PerfTaskData deserializes from JSON without repetitions field (defaults to 1)."""
        from conductress.task_queue import BaseTaskData

        task_data = {
            "source": "manual",
            "specifier": "test",
            "make_args": "",
            "replicas": 1,
            "note": "",
            "requirements": {},
            "test": "set",
            "val_size": 512,
            "io_threads": 1,
            "pipelining": 1,
            "warmup": 60,
            "duration": 300,
            "perf_stat_enabled": False,
            "has_expire": False,
            "preload_keys": True,
            "task_type": "PerfTaskData",
            "timestamp": "2025-01-15T12:00:00",
        }
        task_file = temp_dir / "task_no_rep.json"
        with task_file.open("w") as f:
            json.dump(task_data, f)

        loaded = BaseTaskData.from_file(task_file)
        assert isinstance(loaded, PerfTaskData)
        assert loaded.repetitions == 1

    # ── Test 2: Multi-run behavior (repetitions>1) produces aggregated result ──

    def test_compute_aggregated_stats_two_values(self):
        """compute_aggregated_stats with 2 values produces mean and CI."""
        per_run_rps = [100.0, 200.0]
        mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)
        assert isinstance(mean_rps, float)
        assert isinstance(ci_95, float)
        assert mean_rps == pytest.approx(150.0)
        assert ci_95 > 0

    def test_compute_aggregated_stats_three_values(self):
        """compute_aggregated_stats with 3 values produces correct aggregated result."""
        per_run_rps = [100.0, 200.0, 300.0]
        mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)
        assert isinstance(mean_rps, float)
        assert isinstance(ci_95, float)
        assert mean_rps == pytest.approx(200.0)
        assert ci_95 > 0

    def test_compute_aggregated_stats_identical_values(self):
        """compute_aggregated_stats with identical values produces zero CI."""
        per_run_rps = [150.0, 150.0, 150.0]
        mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)
        assert mean_rps == pytest.approx(150.0)
        assert ci_95 == pytest.approx(0.0)

    # ── Test 3: Correct statistical computation against known values ──

    def test_compute_aggregated_stats_known_values(self):
        """Verify exact statistical computation against hand-calculated known values.

        per_run_rps = [100.0, 200.0, 300.0]
        Expected mean = 200.0
        Expected stdev = 100.0
        n = 3, t_critical = scipy.stats.t.ppf(0.975, 2) ≈ 4.3027
        Expected ci_95 = 4.3027 * (100.0 / sqrt(3)) ≈ 248.41
        """
        import math

        from scipy.stats import t as scipy_t

        per_run_rps = [100.0, 200.0, 300.0]
        mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)

        # Verify mean
        assert mean_rps == pytest.approx(200.0, abs=1e-9)

        # Verify CI against hand-calculated values
        n = 3
        expected_stdev = 100.0
        t_critical = scipy_t.ppf(0.975, n - 1)  # ≈ 4.3027
        expected_ci = t_critical * (expected_stdev / math.sqrt(n))  # ≈ 248.41

        assert ci_95 == pytest.approx(expected_ci, rel=1e-4)
        # Also verify the approximate value
        assert ci_95 == pytest.approx(248.41, rel=1e-2)

    def test_compute_aggregated_stats_known_values_five_runs(self):
        """Verify statistical computation with 5 known values.

        per_run_rps = [10.0, 20.0, 30.0, 40.0, 50.0]
        Expected mean = 30.0
        Expected stdev = sqrt(250) ≈ 15.8114
        n = 5, t_critical = scipy.stats.t.ppf(0.975, 4) ≈ 2.7764
        Expected ci_95 = 2.7764 * (15.8114 / sqrt(5)) ≈ 19.63
        """
        import math
        import statistics

        from scipy.stats import t as scipy_t

        per_run_rps = [10.0, 20.0, 30.0, 40.0, 50.0]
        mean_rps, ci_95 = compute_aggregated_stats(per_run_rps)

        # Verify mean
        assert mean_rps == pytest.approx(30.0, abs=1e-9)

        # Verify CI
        n = 5
        expected_stdev = statistics.stdev(per_run_rps)
        t_critical = scipy_t.ppf(0.975, n - 1)
        expected_ci = t_critical * (expected_stdev / math.sqrt(n))

        assert ci_95 == pytest.approx(expected_ci, rel=1e-9)

    # ── Test: PerfTaskRunner stores repetitions attribute correctly ──

    def test_runner_stores_repetitions_default(self):
        """PerfTaskRunner stores repetitions=1 by default."""
        runner = PerfTaskRunner(
            task_name="test",
            server_infos=[],
            binary_source="manual",
            specifier="test",
            io_threads=1,
            valsize=64,
            pipelining=1,
            test="set",
            warmup=1,
            duration=1,
            preload_keys=True,
            has_expire=False,
            make_args="",
        )
        assert runner.repetitions == 1

    def test_runner_stores_repetitions_3(self):
        """PerfTaskRunner stores repetitions=3 when explicitly set."""
        runner = PerfTaskRunner(
            task_name="test",
            server_infos=[],
            binary_source="manual",
            specifier="test",
            io_threads=1,
            valsize=64,
            pipelining=1,
            test="set",
            warmup=1,
            duration=1,
            preload_keys=True,
            has_expire=False,
            make_args="",
            repetitions=3,
        )
        assert runner.repetitions == 3

    def test_runner_stores_repetitions_10(self):
        """PerfTaskRunner stores repetitions=10 when explicitly set."""
        runner = PerfTaskRunner(
            task_name="test",
            server_infos=[],
            binary_source="manual",
            specifier="test",
            io_threads=1,
            valsize=64,
            pipelining=1,
            test="get",
            warmup=1,
            duration=1,
            preload_keys=True,
            has_expire=False,
            make_args="",
            repetitions=10,
        )
        assert runner.repetitions == 10

    def test_prepare_task_runner_passes_repetitions(self):
        """PerfTaskData.prepare_task_runner passes repetitions to PerfTaskRunner."""
        task = _make_perf_task_data(repetitions=5)
        runner = task.prepare_task_runner(server_infos=[])
        assert runner.repetitions == 5

    def test_prepare_task_runner_default_repetitions(self):
        """PerfTaskData.prepare_task_runner passes default repetitions=1."""
        task = _make_perf_task_data()
        runner = task.prepare_task_runner(server_infos=[])
        assert runner.repetitions == 1


class TestStorePerfCounters:
    """Unit tests for _store_perf_counters bucket splitting (per-thread feature)."""

    def test_splits_bucketed_counters(self):
        runner = _make_runner()
        detailed: dict = {}
        bucketed = {
            "all": {"instructions": 60, "cycles": 20},
            "main": {"instructions": 14, "cycles": 5},
            "io": {"instructions": 46, "cycles": 15},
        }
        runner._store_perf_counters(detailed, bucketed)
        assert detailed["perf_counters"] == {"instructions": 60, "cycles": 20}
        assert detailed["perf_counters_main"] == {"instructions": 14, "cycles": 5}
        assert detailed["perf_counters_io"] == {"instructions": 46, "cycles": 15}
        assert detailed["perf_duration_seconds"] == float(runner.duration)

    def test_empty_per_thread_buckets_omitted(self):
        runner = _make_runner()
        detailed: dict = {}
        bucketed = {"all": {"instructions": 60}, "main": {}, "io": {}}
        runner._store_perf_counters(detailed, bucketed)
        assert detailed["perf_counters"] == {"instructions": 60}
        assert "perf_counters_main" not in detailed
        assert "perf_counters_io" not in detailed

    def test_legacy_flat_dict_treated_as_process_wide(self):
        runner = _make_runner()
        detailed: dict = {}
        # A flat dict (no bucket keys) is interpreted as the process-wide total
        runner._store_perf_counters(detailed, {"instructions": 99, "cycles": 33})
        assert detailed["perf_counters"] == {"instructions": 99, "cycles": 33}
        assert "perf_counters_main" not in detailed
        assert "perf_counters_io" not in detailed
