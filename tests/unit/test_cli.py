"""Property-based and unit tests for the CLI module.

Tests cover:
- Property 4: Cartesian product task count (Requirement 3.3)
- Property 5: Source validation (Requirements 3.4, 3.5)
- Unit tests for argument parsing, validation, and task submission (Requirement 9.3)
"""

from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from conductress import config
from conductress.cli import generate_task_combinations, main, validate_source

# Patch config for tests (consistent with other test modules)
config.MANUALLY_UPLOADED = "manual"
config.REPO_NAMES = ["repo1", "repo2"]


class TestCartesianProductProperties:
    """Property-based tests for Cartesian product task count."""

    @settings(max_examples=100)
    @given(
        tests=st.lists(
            st.text(
                alphabet=st.characters(whitelist_categories=("L", "N")),
                min_size=1,
                max_size=10,
            ),
            min_size=1,
            max_size=5,
            unique=True,
        ),
        sizes=st.lists(
            st.integers(min_value=1, max_value=1_000_000),
            min_size=1,
            max_size=5,
            unique=True,
        ),
        io_threads=st.lists(
            st.integers(min_value=1, max_value=32),
            min_size=1,
            max_size=5,
            unique=True,
        ),
        pipelining=st.lists(
            st.integers(min_value=1, max_value=64),
            min_size=1,
            max_size=5,
            unique=True,
        ),
        key_sizes=st.lists(
            st.integers(min_value=0, max_value=10_000),
            min_size=1,
            max_size=5,
            unique=True,
        ),
    )
    def test_cartesian_product_task_count(self, tests, sizes, io_threads, pipelining, key_sizes):
        """**Validates: Requirements 3.3**

        Feature: benchmark-tooling-enhancements, Property 4: Cartesian product task count

        For any non-empty lists of tests, sizes, io_threads, pipelining, and key_sizes,
        the number of generated task combinations must equal the product of all list lengths,
        and every unique combination of (test, size, io_thread, pipeline, key_size) must
        appear exactly once.
        """
        result = generate_task_combinations(tests, sizes, io_threads, pipelining, key_sizes)

        # Verify count equals product of lengths
        expected_count = len(tests) * len(sizes) * len(io_threads) * len(pipelining) * len(key_sizes)
        assert len(result) == expected_count, f"Expected {expected_count} combinations, got {len(result)}"

        # Verify every unique combination appears exactly once (no duplicates)
        assert len(result) == len(
            set(result)
        ), f"Found duplicate combinations: {len(result)} total vs {len(set(result))} unique"

        # Verify every combination is a valid tuple from the input lists
        for combo in result:
            test, size, io_thread, pipeline, key_size = combo
            assert test in tests, f"Test '{test}' not in input tests"
            assert size in sizes, f"Size {size} not in input sizes"
            assert io_thread in io_threads, f"IO thread {io_thread} not in input io_threads"
            assert pipeline in pipelining, f"Pipeline {pipeline} not in input pipelining"
            assert key_size in key_sizes, f"Key size {key_size} not in input key_sizes"


class TestSourceValidationProperties:
    """Property-based tests for source validation."""

    @settings(max_examples=100)
    @given(s=st.text(min_size=0, max_size=50))
    def test_source_validation(self, s):
        """**Validates: Requirements 3.4, 3.5**

        Feature: benchmark-tooling-enhancements, Property 5: Source validation

        For any string s, validate_source(s) must return True if and only if
        s is in config.REPO_NAMES or s equals config.MANUALLY_UPLOADED.
        All other strings must be rejected.
        """
        valid_set = set(config.REPO_NAMES) | {config.MANUALLY_UPLOADED}
        expected = s in valid_set
        result = validate_source(s)
        assert result == expected, (
            f"validate_source({s!r}) returned {result}, expected {expected}. " f"Valid sources: {valid_set}"
        )


# ---------------------------------------------------------------------------
# Unit tests for CLI (Requirement 9.3)
# ---------------------------------------------------------------------------


class TestGenerateTaskCombinations:
    """Unit tests for generate_task_combinations()."""

    def test_single_combination(self):
        """A single value per parameter produces exactly 1 combination."""
        result = generate_task_combinations(["get"], [512], [1], [1], [0])
        assert len(result) == 1
        assert result[0] == ("get", 512, 1, 1, 0)

    def test_full_cartesian_product(self):
        """Multiple values per parameter produce the full Cartesian product."""
        result = generate_task_combinations(["get", "set"], [512, 1024], [1, 9], [1, 4], [0, 64])
        assert len(result) == 2 * 2 * 2 * 2 * 2  # 32 combinations


class TestValidateSource:
    """Unit tests for validate_source()."""

    def test_valid_repo_name(self):
        assert validate_source("repo1") is True

    def test_valid_manually_uploaded(self):
        assert validate_source("manual") is True

    def test_invalid_source(self):
        assert validate_source("nonexistent") is False

    def test_empty_string(self):
        assert validate_source("") is False


class TestQueueAddSubcommand:
    """Unit tests for the 'queue add' subcommand via main()."""

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_correct_number_of_tasks(self, mock_queue_cls):
        """main(["queue", "add", ...]) with 2 tests, 2 sizes, 2 io-threads, 2 pipelining, 1 key-size
        should queue 2*2*2*2*1 = 16 tasks."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add",
                "--source",
                "repo1",
                "--specifier",
                "unstable",
                "--tests",
                "get,set",
                "--sizes",
                "512,1KB",
                "--io-threads",
                "1,9",
                "--pipelining",
                "1,4",
            ]
        )

        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 16

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_invalid_source_returns_exit_code_1(self, mock_queue_cls):
        """main() with an invalid source should return exit code 1."""
        exit_code = main(
            [
                "queue",
                "add",
                "--source",
                "invalid_source",
                "--tests",
                "get",
            ]
        )

        assert exit_code == 1
        mock_queue_cls.return_value.submit_task.assert_not_called()

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_empty_tests_returns_exit_code_1(self, mock_queue_cls):
        """main() with empty --tests should return exit code 1."""
        exit_code = main(
            [
                "queue",
                "add",
                "--source",
                "repo1",
                "--tests",
                "",
            ]
        )

        assert exit_code == 1
        mock_queue_cls.return_value.submit_task.assert_not_called()

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_repetitions_zero_returns_exit_code_1(self, mock_queue_cls):
        """main() with --repetitions 0 should return exit code 1."""
        exit_code = main(
            [
                "queue",
                "add",
                "--source",
                "repo1",
                "--tests",
                "get",
                "--repetitions",
                "0",
            ]
        )

        assert exit_code == 1
        mock_queue_cls.return_value.submit_task.assert_not_called()

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_key_sizes_produces_tasks_with_correct_values(self, mock_queue_cls):
        """main() with --key-sizes "0,64" should produce tasks with key_size 0 and 64."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add",
                "--source",
                "repo1",
                "--tests",
                "get",
                "--key-sizes",
                "0,64",
            ]
        )

        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 2

        submitted_key_sizes = sorted(call.args[0].key_size for call in mock_queue.submit_task.call_args_list)
        assert submitted_key_sizes == [0, 64]

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_defaults(self, mock_queue_cls):
        """Defaults: source=valkey, specifier=unstable, sizes=512, io-threads=9, pipeline=10, reps=5."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add",
                "--source",
                "repo1",
                "--tests",
                "set",
            ]
        )

        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 1
        task = mock_queue.submit_task.call_args[0][0]
        assert task.source == "repo1"
        assert task.specifier == "unstable"
        assert task.val_size == 512
        assert task.io_threads == 9
        assert task.pipelining == 10
        assert task.repetitions == 3
        assert task.make_args == ""

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_server_args_passthrough(self, mock_queue_cls):
        """--server-args flows verbatim into the queued task; default is empty."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add",
                "--source",
                "repo1",
                "--tests",
                "get",
                "--server-args",
                "--io-threads-ownership yes",
            ]
        )

        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.server_args == "--io-threads-ownership yes"

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_bench_params_passthrough(self, mock_queue_cls):
        """--bench-threads/--bench-clients flow into the task; default 0."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add", "--source", "repo1", "--tests", "get", "--bench-threads", "32", "--bench-clients", "2400"]
        )

        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.bench_threads == 32
        assert task.bench_clients == 2400

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_access_hotset_passthrough(self, mock_queue_cls):
        """--access hotset flows into the task for a test that supports it; the default is uniform."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        assert main(["queue", "add", "--source", "repo1", "--tests", "zscore,zrank"]) == 0
        assert {c[0][0].access for c in mock_queue.submit_task.call_args_list} == {"uniform"}

        mock_queue.reset_mock()
        assert main(["queue", "add", "--source", "repo1", "--tests", "zscore,zrank", "--access", "hotset"]) == 0
        assert {c[0][0].access for c in mock_queue.submit_task.call_args_list} == {"hotset"}

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_access_hotset_rejects_unsupported_tests(self, mock_queue_cls, capsys):
        for tests in ("zadd", "zscore,get", "nosuchtest"):
            exit_code = main(["queue", "add", "--source", "repo1", "--tests", tests, "--access", "hotset"])
            assert exit_code == 1, tests
        assert "does not support test(s)" in capsys.readouterr().err
        mock_queue_cls.return_value.submit_task.assert_not_called()

    def test_queue_add_access_has_no_zipf_choice(self, capsys):
        with pytest.raises(SystemExit):
            main(["queue", "add", "--source", "repo1", "--tests", "zscore", "--access", "zipf"])
        assert "invalid choice" in capsys.readouterr().err

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_range_tests_get_the_range_generator(self, mock_queue_cls):
        """Tests that need range mode select range-v1; the others keep the default generator."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        assert main(["queue", "add", "--source", "repo1", "--tests", "zrange,zrangebyscore,zrevrange,zscore"]) == 0

        profiles = {c[0][0].test: c[0][0].generator_profile for c in mock_queue.submit_task.call_args_list}
        assert profiles == {"zrange": "range-v1", "zrangebyscore": "range-v1", "zrevrange": "range-v1", "zscore": ""}

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_generator_profile_passthrough(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        assert main(["queue", "add", "--source", "repo1", "--tests", "zscore", "--generator-profile", "range-v1"]) == 0
        assert mock_queue.submit_task.call_args[0][0].generator_profile == "range-v1"

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_refuses_range_test_with_a_profile_lacking_range_mode(self, mock_queue_cls, capsys):
        for profile in ("legacy-v1", "scalable-v2"):
            code = main(["queue", "add", "--source", "repo1", "--tests", "zrange", "--generator-profile", profile])
            assert code == 1, profile
        assert "lacks range support" in capsys.readouterr().err
        mock_queue_cls.return_value.submit_task.assert_not_called()

    def test_queue_add_unknown_generator_profile_is_rejected(self, capsys):
        with pytest.raises(SystemExit):
            main(["queue", "add", "--source", "repo1", "--tests", "zscore", "--generator-profile", "nope"])
        assert "invalid choice" in capsys.readouterr().err

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_bench_binary_skips_the_profile_choice(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        assert main(["queue", "add", "--source", "repo1", "--tests", "zrange", "--bench-binary", "/x/vb"]) == 0
        assert mock_queue.submit_task.call_args[0][0].generator_profile == ""

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_older_redis_release_is_an_ordinary_ad_hoc_task(self, mock_queue_cls, monkeypatch):
        """The sweep floor (8.0.0) only bounds the sweep's commit range. An ad hoc task names any ref."""
        monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "redis"])
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add", "--source", "redis", "--specifier", "7.2.9", "--tests", "zscore"])

        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert (task.source, task.specifier) == ("redis", "7.2.9")
        assert config.get_sweep_engine("redis").floor_tag == "8.0.0"  # sweep scope is unchanged

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_bench_params_default_zero(self, mock_queue_cls):
        """Without the flags the task carries 0 (= use config defaults)."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add", "--source", "repo1", "--tests", "get"])

        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.bench_threads == 0
        assert task.bench_clients == 0

    @patch("conductress.cli.TaskQueue")
    def test_queue_add_server_args_default_empty(self, mock_queue_cls):
        """Without --server-args the task carries an empty string (schema-compatible default)."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add", "--source", "repo1", "--tests", "get"])

        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.server_args == ""


class TestQueueListSubcommand:
    """Unit tests for the 'queue list' subcommand via main()."""

    @patch("conductress.cli.TaskQueue")
    def test_queue_list_returns_exit_code_0(self, mock_queue_cls):
        """main(["queue", "list"]) should return exit code 0."""
        mock_queue = MagicMock()
        mock_queue.get_all_tasks.return_value = []
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "list"])
        assert exit_code == 0

    def test_queue_no_subcommand_defaults_to_list(self, monkeypatch):
        """main(["queue"]) with no subcommand preserves default-to-list behavior."""
        called = {}
        import conductress.cli as cli_mod

        def fake_list(args):
            called["list"] = True
            return 0

        monkeypatch.setattr(cli_mod, "handle_queue_list", fake_list)
        exit_code = main(["queue"])
        assert exit_code == 0
        assert called.get("list") is True

    def test_no_subcommand_returns_exit_code_1(self):
        """main([]) with no subcommand should return exit code 1."""
        exit_code = main([])
        assert exit_code == 1

    def test_queue_help_shows_subcommands(self, capsys):
        """build_parser() queue --help lists all subcommands with descriptions."""
        from conductress.cli import build_parser

        parser = build_parser()
        # Simulate --help by formatting the queue subparser
        queue_parser = parser._subparsers._actions[1].choices["queue"]
        queue_parser.print_help()
        captured = capsys.readouterr()
        for subcmd in ("list", "add", "add-insertion", "add-memory", "add-latency", "remove", "clear"):
            assert subcmd in captured.out


class TestQueueRemoveSubcommand:
    """Unit tests for the 'queue remove' subcommand."""

    @patch("conductress.cli.TaskQueue")
    def test_queue_remove_success(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_queue.remove_task.return_value = True
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "remove", "2026.05.17_10.08.33.876407"])
        assert exit_code == 0
        mock_queue.remove_task.assert_called_once_with("2026.05.17_10.08.33.876407")

    @patch("conductress.cli.TaskQueue")
    def test_queue_remove_not_found(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_queue.remove_task.return_value = False
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "remove", "nonexistent"])
        assert exit_code == 1


class TestQueueClearSubcommand:
    """Unit tests for the 'queue clear' subcommand."""

    @patch("conductress.cli.TaskQueue")
    def test_queue_clear_removes_all(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_task1 = MagicMock()
        mock_task1.task_id = "task1"
        mock_task2 = MagicMock()
        mock_task2.task_id = "task2"
        mock_queue.get_all_tasks.return_value = [mock_task1, mock_task2]
        mock_queue.remove_task.return_value = True
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "clear"])
        assert exit_code == 0
        assert mock_queue.remove_task.call_count == 2


class TestQueueAddMemorySubcommand:
    """Unit tests for the 'queue add-memory' subcommand."""

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_defaults(self, mock_queue_cls):
        """All 4 types queued without --expire gives 4 tasks."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add-memory", "--source", "repo1", "--specifier", "abc123"])
        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 4

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_with_expire(self, mock_queue_cls):
        """--expire adds expire variants (5 total for default types)."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add-memory", "--source", "repo1", "--specifier", "abc123", "--expire"])
        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 5

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_single_type(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set"])
        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 1

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_one_task_per_type_despite_roster_shapes(self, mock_queue_cls):
        """The sweep roster carries several shapes per type; a request for a type queues its first shape only."""
        from conductress.sweep.memory_coordinator import MEMORY_WORKLOADS

        assert sum(1 for wl in MEMORY_WORKLOADS if wl.command == "set" and not wl.has_expire) > 1
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set,hset"])
        assert exit_code == 0
        tasks = [c[0][0] for c in mock_queue.submit_task.call_args_list]
        assert [(t.type, t.key_size, t.field_size, t.val_sizes) for t in tasks] == [
            ("set", 16, 0, [64]),
            ("hset", 0, 64, [64]),
        ]

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_sizes_do_not_multiply_by_roster_shapes(self, mock_queue_cls):
        """--sizes derives from the one base shape, so two roster string shapes give one task per size."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set", "--sizes", "16,32"]
        )
        assert exit_code == 0
        tasks = [c[0][0] for c in mock_queue.submit_task.call_args_list]
        assert [t.val_sizes for t in tasks] == [[16], [32]]

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_manually_uploaded(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add-memory", "--source", "manual", "--specifier", "/path/to/binary"])
        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 4

    def test_add_memory_invalid_source(self):
        exit_code = main(["queue", "add-memory", "--source", "bogus", "--specifier", "x"])
        assert exit_code == 1

    def test_add_memory_invalid_type(self):
        exit_code = main(["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "badtype"])
        assert exit_code == 1

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_custom_sizes_task_count(self, mock_queue_cls):
        """--sizes queues one task per type per size."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add-memory",
                "--source",
                "repo1",
                "--specifier",
                "x",
                "--types",
                "zadd,sadd",
                "--sizes",
                "12,20,64",
            ]
        )
        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 6

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_custom_sizes_user_data_bytes(self, mock_queue_cls):
        """user_data_bytes is derived per type at each custom size (zadd adds score bytes)."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "zadd", "--sizes", "40"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.val_sizes == [40]
        assert task.user_data_bytes == 40 + config.MEM_TEST_SCORE_SIZE

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_custom_sizes_set_includes_key(self, mock_queue_cls):
        """set user data at a custom size includes the workload's key size."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set", "--sizes", "100"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.user_data_bytes == task.key_size + 100

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_field_size_overrides_hset(self, mock_queue_cls):
        """--field-size replaces the hset workload's field size and recomputes user data."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "hset", "--field-size", "16"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.field_size == 16
        assert task.val_sizes == [64]
        assert task.user_data_bytes == 16 + 64

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_field_size_combines_with_sizes(self, mock_queue_cls):
        """--field-size and --sizes together give a symmetric hset workload."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add-memory",
                "--source",
                "repo1",
                "--specifier",
                "x",
                "--types",
                "hset",
                "--field-size",
                "16",
                "--sizes",
                "16",
            ]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.field_size == 16
        assert task.val_sizes == [16]
        assert task.user_data_bytes == 32

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_field_size_ignored_for_non_hset(self, mock_queue_cls):
        """--field-size leaves set/zadd/sadd workloads unchanged."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set", "--field-size", "16"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.field_size == 0
        assert task.user_data_bytes == task.key_size + 64

    def test_add_memory_field_size_rejects_list(self):
        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "hset", "--field-size", "8,16"]
        )
        assert exit_code == 1

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_key_size_overrides_set(self, mock_queue_cls):
        """--key-size replaces the set workload's key size and recomputes user data."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set", "--key-size", "32"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.key_size == 32
        assert task.val_sizes == [64]
        assert task.user_data_bytes == 32 + 64

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_key_size_applies_to_expire_variant(self, mock_queue_cls):
        """--key-size and --sizes shape both the plain and the --expire set workloads."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add-memory",
                "--source",
                "repo1",
                "--specifier",
                "x",
                "--types",
                "set",
                "--key-size",
                "24",
                "--sizes",
                "8",
                "--expire",
            ]
        )
        assert exit_code == 0
        tasks = [c[0][0] for c in mock_queue.submit_task.call_args_list]
        assert len(tasks) == 2
        assert {t.has_expire for t in tasks} == {False, True}
        assert all(t.key_size == 24 and t.val_sizes == [8] and t.user_data_bytes == 32 for t in tasks)

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_key_size_ignored_for_non_set(self, mock_queue_cls):
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "hset", "--key-size", "32"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.key_size == 0
        assert task.field_size == 64

    def test_add_memory_rejects_sizes_below_populator_floor(self):
        """A key/field/member too short to hold a unique index for every item is refused."""
        for extra in (
            ["--types", "set", "--key-size", "8"],
            ["--types", "hset", "--field-size", "4"],
            ["--types", "zadd", "--sizes", "6"],
        ):
            exit_code = main(["queue", "add-memory", "--source", "repo1", "--specifier", "x", *extra])
            assert exit_code == 1, extra

    def test_memory_workload_label_names_every_dimension(self):
        from conductress.cli import _memory_workload_label
        from conductress.sweep.memory_coordinator import MemoryWorkload

        assert _memory_workload_label(MemoryWorkload(command="set", key_size=16, value_size=16)) == "set-k16-v16"
        assert (
            _memory_workload_label(MemoryWorkload(command="set", key_size=16, value_size=16, has_expire=True))
            == "set-k16-v16-expire"
        )
        assert (
            _memory_workload_label(MemoryWorkload(command="hset", key_size=0, value_size=16, field_size=16))
            == "hset-f16-v16"
        )
        assert _memory_workload_label(MemoryWorkload(command="zadd", key_size=0, value_size=20)) == "zadd-m20"

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_builds_with_jemalloc_prof(self, mock_queue_cls):
        """enable_profiling=True is only useful if the binary is built with --enable-prof.

        Regression: add-memory used to forward bare --make-args, so jemalloc wrote no heap
        dump and every manual memory task recorded breakdown=None.
        """
        from conductress.heap_profiler import JEMALLOC_PROF_CONFIGURE_OPTS

        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set", "--breakdown"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.enable_profiling is True
        assert JEMALLOC_PROF_CONFIGURE_OPTS in task.make_args

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_default_is_unprofiled(self, mock_queue_cls):
        """Without --breakdown the task runs unprofiled, so used_memory and RSS are both valid."""
        from conductress.heap_profiler import JEMALLOC_PROF_CONFIGURE_OPTS

        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "set"])
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.enable_profiling is False
        assert JEMALLOC_PROF_CONFIGURE_OPTS not in task.make_args

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_user_make_args_kept_alongside_prof_flag(self, mock_queue_cls):
        """A user-supplied --make-args is preserved, with the profiling flag added once."""
        from conductress.heap_profiler import JEMALLOC_PROF_CONFIGURE_OPTS

        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add-memory",
                "--source",
                "repo1",
                "--specifier",
                "x",
                "--types",
                "set",
                "--make-args",
                "MALLOC=jemalloc",
                "--breakdown",
            ]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.make_args.startswith("MALLOC=jemalloc")
        assert task.make_args.count(JEMALLOC_PROF_CONFIGURE_OPTS) == 1

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_custom_sizes_with_expire(self, mock_queue_cls):
        """--sizes composes with --expire (expire variants also get each size)."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            [
                "queue",
                "add-memory",
                "--source",
                "repo1",
                "--specifier",
                "x",
                "--types",
                "set",
                "--sizes",
                "16,64",
                "--expire",
            ]
        )
        assert exit_code == 0
        assert mock_queue.submit_task.call_count == 4

    @patch("conductress.cli.TaskQueue")
    def test_add_memory_custom_sizes_human_units(self, mock_queue_cls):
        """--sizes accepts human-readable byte units like 1KB."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "sadd", "--sizes", "1KB"]
        )
        assert exit_code == 0
        task = mock_queue.submit_task.call_args[0][0]
        assert task.val_sizes == [1024]
        assert task.user_data_bytes == 1024

    def test_add_memory_invalid_sizes(self):
        exit_code = main(
            ["queue", "add-memory", "--source", "repo1", "--specifier", "x", "--types", "zadd", "--sizes", "abc"]
        )
        assert exit_code == 1


class TestQueueRemoteCommands:
    """The queue cancel/mine/pending/approve/reject thin clients."""

    def test_queue_cancel_reports_changed_tasks(self, capsys):
        fake = MagicMock()
        fake.cancel_task.return_value = {
            "tasks": [{"task_id": "t1", "state": "cancelled"}],
            "changed": 1,
        }
        with patch("conductress.cli._remote_client", return_value=fake):
            assert main(["queue", "cancel", "b1"]) == 0
        fake.cancel_task.assert_called_once_with("b1")
        assert "1 task(s) changed" in capsys.readouterr().out

    def test_queue_mine_lists_tasks(self, capsys):
        fake = MagicMock()
        fake.list_mine.return_value = {
            "tasks": [{"task_id": "t1", "state": "queued", "batch_id": "b1", "submitted_at": "2026-01-01T00:00:00Z"}]
        }
        with patch("conductress.cli._remote_client", return_value=fake):
            assert main(["queue", "mine"]) == 0
        assert "t1" in capsys.readouterr().out

    def test_queue_pending_lists_tasks(self, capsys):
        fake = MagicMock()
        fake.list_pending.return_value = {
            "tasks": [
                {
                    "task_id": "t1",
                    "runner_id": "armbench",
                    "submitter_login": "ghost",
                    "batch_id": None,
                    "submitted_at": "2026-01-01T00:00:00Z",
                }
            ]
        }
        with patch("conductress.cli._remote_client", return_value=fake):
            assert main(["queue", "pending"]) == 0
        assert "ghost" in capsys.readouterr().out

    def test_queue_approve_and_reject(self, capsys):
        fake = MagicMock()
        fake.approve_tasks.return_value = {"changed": 2}
        fake.reject_tasks.return_value = {"changed": 1}
        with patch("conductress.cli._remote_client", return_value=fake):
            assert main(["queue", "approve", "b1"]) == 0
            assert main(["queue", "reject", "t9"]) == 0
        fake.approve_tasks.assert_called_once_with("b1")
        fake.reject_tasks.assert_called_once_with("t9")
        out = capsys.readouterr().out
        assert "2 task(s) queued" in out
        assert "1 task(s) rejected" in out
