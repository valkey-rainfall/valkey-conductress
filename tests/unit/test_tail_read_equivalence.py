"""The bounded readers give the same answer as the whole-file reads they replaced.

Each ``_old_*`` function is the logic the sweep coordinators, ``confirmation.find_row`` and
``load_duration_calibration`` used before (``read_text().splitlines()``). Each test builds one
file and checks that the old and the new logic agree on it.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conductress.duration_estimator import load_duration_calibration
from conductress.sweep.confirmation import find_row
from conductress.tail_reader import CONTINUE, find_newest_row

# --- the old whole-file logic -------------------------------------------------------------


def _old_scan(path, task_id, take):
    if not path.exists():
        return None
    for line in reversed(path.read_text().strip().splitlines()):
        try:
            entry = json.loads(line)
            if entry.get("task_id") == task_id:
                value = take(entry)
                if value is not CONTINUE:
                    return value
        except (ValueError, KeyError, TypeError, AttributeError):
            # AttributeError: the old code crashed on a line that is JSON but not an object
            # (for example ``42``). The new code skips such a line; see the test below.
            continue
    return None


def _take_entry(entry):
    return entry


def _take_score(entry):
    score = entry.get("score")
    return (score, 0.0, 1) if score and score > 0 else CONTINUE


def _take_memory_record(entry):
    results = entry.get("data", {}).get("results", [])
    record = results[0].get("memory_record") if results else None
    return record if isinstance(record, dict) else None


def _take_breakdown(entry):
    results = entry.get("data", {}).get("results", [])
    return results[0]["breakdown"] if results and results[0].get("breakdown") else CONTINUE


TAKERS = [_take_entry, _take_score, _take_memory_record, _take_breakdown]


def _old_find_row(path, task_id):
    for line in reversed(path.read_text(encoding="utf-8").splitlines()):
        if task_id not in line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict) and row.get("task_id") == task_id:
            return row
    return None


# --- files ------------------------------------------------------------------------------------


def _row(task_id, score=1.0, **extra):
    row = {
        "task_id": task_id,
        "score": score,
        "data": {"results": [{"memory_record": {"used": score}, "breakdown": {"a": score}}]},
        "pad": "x" * 200,
    }
    row.update(extra)
    return json.dumps(row)


def _filler(count, start=0):
    return [_row(f"other-{start + i}") for i in range(count)]


# name -> (lines, ends_with_newline)
FILES = {
    "last_line": (_filler(30) + [_row("wanted", 5.0)], True),
    "near_the_end": (_filler(30) + [_row("wanted", 5.0)] + _filler(3, 100), True),
    "far_from_the_end": ([_row("wanted", 5.0)] + _filler(400), True),
    "absent": (_filler(50), True),
    "newest_of_two": ([_row("wanted", 1.0)] + _filler(100) + [_row("wanted", 2.0)] + _filler(5, 200), True),
    "partial_last_line": (_filler(5) + [_row("wanted", 5.0)] + ['{"task_id": "wanted", "sco'], False),
    "no_final_newline": (_filler(5) + [_row("wanted", 5.0)], False),
    "blank_and_junk_lines": (
        [_row("wanted", 5.0), "", "not json", "[1, 2]", "42", '{"task_id": "wanted", "score": ']
        + _filler(20)
        + ["", ""],
        True,
    ),
    "newest_row_not_accepted": (
        [_row("wanted", 5.0)] + _filler(60) + [_row("wanted", 0), _row("wanted", None, data={"results": []})],
        True,
    ),
    "crlf": ([line + "\r" for line in _filler(10)] + [_row("wanted", 5.0) + "\r"], True),
    "empty": ([], True),
}

# window sizes that put the wanted row inside, at the edge of, and outside the tail window
WINDOWS = [(64, 64), (1024, 256), (4096, 1000), (1 << 20, 64 * 1024)]


def _write(tmp_path, name):
    lines, newline = FILES[name]
    path = tmp_path / "output.jsonl"
    path.write_text("\n".join(lines) + ("\n" if newline and lines else ""))
    return path


@pytest.mark.parametrize("name", sorted(FILES))
@pytest.mark.parametrize("take", TAKERS, ids=lambda t: t.__name__)
@pytest.mark.parametrize("max_bytes,chunk_size", WINDOWS)
def test_find_newest_row_matches_the_whole_file_scan(tmp_path, name, take, max_bytes, chunk_size):
    path = _write(tmp_path, name)
    expected = _old_scan(path, "wanted", take)
    assert find_newest_row(path, "wanted", take, max_bytes=max_bytes, chunk_size=chunk_size) == expected


@pytest.mark.parametrize("name", sorted(FILES))
@pytest.mark.parametrize("max_bytes,chunk_size", WINDOWS)
def test_find_newest_row_default_accept_returns_the_row(tmp_path, name, max_bytes, chunk_size):
    path = _write(tmp_path, name)
    expected = _old_scan(path, "wanted", _take_entry)
    assert find_newest_row(path, "wanted", max_bytes=max_bytes, chunk_size=chunk_size) == expected


def test_a_json_line_that_is_not_an_object_is_skipped(tmp_path):
    """The old code raised AttributeError on such a line; skipping it is the only deliberate difference."""
    path = tmp_path / "output.jsonl"
    path.write_text(_row("wanted", 4.0) + "\n42\n[1, 2]\n")
    assert find_newest_row(path, "wanted")["score"] == 4.0


def test_missing_file_is_none(tmp_path):
    assert find_newest_row(tmp_path / "nope.jsonl", "wanted") is None


def test_row_older_than_the_window_is_found_by_the_streaming_fallback(tmp_path):
    path = _write(tmp_path, "far_from_the_end")
    assert path.stat().st_size > 4096
    row = find_newest_row(path, "wanted", max_bytes=4096)
    assert row is not None and row["score"] == 5.0


def test_the_fallback_never_reads_the_whole_file_at_once(tmp_path):
    path = _write(tmp_path, "far_from_the_end")
    with (
        patch.object(Path, "read_text", side_effect=AssertionError("read_text")),
        patch.object(Path, "read_bytes", side_effect=AssertionError("read_bytes")),
    ):
        assert find_newest_row(path, "wanted", max_bytes=2048)["score"] == 5.0
        assert find_newest_row(path, "absent-id", max_bytes=2048) is None


def test_the_fallback_keeps_the_newest_of_two_old_rows(tmp_path):
    lines = [_row("wanted", 1.0)] + _filler(50) + [_row("wanted", 2.0)] + _filler(200, 100)
    path = tmp_path / "output.jsonl"
    path.write_text("\n".join(lines) + "\n")
    assert find_newest_row(path, "wanted", max_bytes=2048)["score"] == 2.0


def test_task_id_with_json_escapes_is_still_found(tmp_path):
    task_id = 'odd"id\\1'
    path = tmp_path / "output.jsonl"
    path.write_text("\n".join(_filler(5) + [json.dumps({"task_id": task_id, "score": 3})] + _filler(5, 9)) + "\n")
    assert find_newest_row(path, task_id)["score"] == 3
    assert find_newest_row(path, task_id, max_bytes=300)["score"] == 3


# --- the callers ------------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(FILES))
def test_find_row_matches_the_old_version(tmp_path, name):
    path = _write(tmp_path, name)
    assert find_row(path, "wanted") == _old_find_row(path, "wanted")
    assert find_row(path, "absent-id") is None


def test_find_row_on_a_missing_file(tmp_path):
    assert find_row(tmp_path / "nope.jsonl", "wanted") is None


def _calibration_lines(count):
    lines = []
    for i in range(count):
        lines.append(
            json.dumps(
                {
                    "task_id": f"t{i}",
                    "observed_duration_sec": 100 + i % 7,
                    "expected_duration_sec": 100,
                    "duration_family": "perf" if i % 2 else "memory",
                }
            )
        )
    return lines


@pytest.mark.parametrize("count", [0, 3, 399, 400, 401, 900])
def test_load_duration_calibration_matches_the_whole_file_read(tmp_path, count):
    from conductress.duration_estimator import load_duration_calibration_from_lines

    lines = _calibration_lines(count)
    lines[1:1] = ["", "not json"] if count > 5 else []
    path = tmp_path / "output.jsonl"
    path.write_text("\n".join(lines) + "\n" if lines else "")
    expected = load_duration_calibration_from_lines(path.read_text(encoding="utf-8").splitlines())
    assert load_duration_calibration(path) == expected


def test_load_duration_calibration_on_a_missing_file(tmp_path):
    assert load_duration_calibration(tmp_path / "nope.jsonl") == {}


def _big_file(tmp_path, wanted_first):
    """A file longer than the default 64 MiB window, with the wanted row before the window."""
    path = tmp_path / "results" / "output.jsonl"
    path.parent.mkdir()
    pad = json.dumps({"task_id": "pad", "blob": "y" * (1 << 20)})
    with path.open("w") as handle:
        if wanted_first:
            handle.write(_row("wanted", 7.0) + "\n")
        for _ in range(66):
            handle.write(pad + "\n")
    return path


def test_coordinators_find_a_row_older_than_the_default_window(tmp_path):
    from conductress.sweep import coordinator as base_mod
    from conductress.sweep import coordinator_v3 as v3_mod
    from conductress.sweep import memory_coordinator as mem_mod

    path = _big_file(tmp_path, wanted_first=True)
    task = MagicMock()
    task.task_id = "wanted"
    with patch.object(Path, "read_text", side_effect=AssertionError("read_text")):
        for module, cls in (
            (base_mod, base_mod.BaseSweepCoordinator),
            (v3_mod, v3_mod.BaseCachecannonSweepCoordinatorV3),
        ):
            with patch.object(module, "CONDUCTRESS_RESULTS", path.parent):
                entry = cls._find_task_entry(MagicMock(), task)
                assert entry is not None and entry["score"] == 7.0
        with patch.object(mem_mod, "CONDUCTRESS_RESULTS", path.parent):
            coordinator = MagicMock()
            assert mem_mod.MemorySweepCoordinator._extract_result(coordinator, task) == (7.0, 0.0, 1)
            assert mem_mod.MemorySweepCoordinator._extract_memory_record(coordinator, task) == {"used": 7.0}
            assert mem_mod.MemorySweepCoordinator._extract_breakdown(coordinator, task) == {"a": 7.0}
        assert find_row(path, "wanted")["score"] == 7.0


def test_coordinators_report_absent_for_a_big_file_without_the_row(tmp_path):
    from conductress.sweep import coordinator as base_mod

    path = _big_file(tmp_path, wanted_first=False)
    task = MagicMock()
    task.task_id = "wanted"
    with patch.object(base_mod, "CONDUCTRESS_RESULTS", path.parent):
        assert base_mod.BaseSweepCoordinator._find_task_entry(MagicMock(), task) is None
