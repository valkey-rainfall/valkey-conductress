"""``conductress build-cache prune``: keep set, dry run, idle check and safe deletion. Synthetic directories only."""

import io
import json
import os
import re
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from conductress import build_cache as bc
from conductress import config
from conductress.sweep.planner import BenchmarkPoint, Landmark, SweepState

DAY = 86400
NOW = 2_000_000_000.0  # fixed clock, so ages are exact


def sha(n: int) -> str:
    return f"{n:040x}"


def make_entry(root: Path, source: str, commit: str, age_days: float, args_hash: str = "0123456789abcdef") -> Path:
    leaf = root / source / commit / args_hash
    leaf.mkdir(parents=True)
    for name in ("valkey-server", "libvalkeylua.so"):
        (leaf / name).write_bytes(b"x" * 4096)
    stamp = NOW - age_days * DAY
    for item in (*leaf.iterdir(), leaf):
        os.utime(item, (stamp, stamp))
    return leaf


def write_state(state_dir: Path, name: str, points=(), landmarks=(), head=None, merges=()) -> Path:
    state = SweepState()
    for commit in points:
        state.points[commit] = BenchmarkPoint(commit=commit, date="2026-01-01", value=1.0)
    state.landmarks = [Landmark(commit=c, date="2026-01-01", label="x") for c in landmarks]
    state.last_benchmarked_head = head
    state.merge_commits = list(merges)
    state.commit_dates = {c: "2026-01-01" for c in (*points, *merges, "f" * 40)}  # must not widen the keep set
    path = state_dir / name
    state.save(path)
    return path


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "build_cache"
    root.mkdir()
    state_dir = tmp_path / "sweep_data"
    (state_dir / "v3").mkdir(parents=True)
    queue = tmp_path / "benchmark_queue"
    queue.mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(config, "CONDUCTRESS_QUEUE", queue)
    monkeypatch.setattr(config, "DELIVERY_JOURNAL_PATH", tmp_path / "fleet_delivery.json")
    monkeypatch.setattr(config, "SWEEP_V3_CACHECANNON_COMMIT", sha(999))
    monkeypatch.setattr(config, "REPO_NAMES", ["valkey", "redis"])
    monkeypatch.setattr(bc, "checked_out_heads", lambda home, sources: set())
    write_state(state_dir, "state_x.json")  # at least one state file must exist
    return {"root": root, "state": state_dir, "queue": queue, "proc": proc, "tmp": tmp_path}


def run(env, *flags, capture=None):
    args = bc.build_parser().parse_args(
        [
            "prune",
            "--cache-dir",
            str(env["root"]),
            "--state-dir",
            str(env["state"]),
            "--log-file",
            str(env["tmp"] / "prune.log"),
            *flags,
        ]
    )
    out = capture if capture is not None else io.StringIO()
    code = bc.prune(args, now=NOW, proc_root=env["proc"], out=out)
    return code, out.getvalue()


def tree(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


# --- keep set -----------------------------------------------------------------------------------


def test_an_old_unused_entry_is_removed(env):
    old = make_entry(env["root"], "valkey", sha(1), age_days=100)
    code, _ = run(env, "--apply")
    assert code == bc.EXIT_OK
    assert not old.exists()
    assert not old.parent.exists()  # the empty commit directory goes too


def test_an_entry_newer_than_min_age_is_kept(env):
    fresh = make_entry(env["root"], "valkey", sha(1), age_days=59)
    old = make_entry(env["root"], "valkey", sha(2), age_days=61)
    run(env, "--apply")
    assert fresh.exists() and not old.exists()


def test_min_age_days_is_adjustable(env):
    entry = make_entry(env["root"], "valkey", sha(1), age_days=10)
    run(env, "--apply")
    assert entry.exists()
    run(env, "--apply", "--min-age-days", "5")
    assert not entry.exists()


def test_recent_access_time_keeps_an_entry_with_an_old_modification_time(env):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=100)
    binary = leaf / "valkey-server"
    os.utime(binary, (NOW - 2 * DAY, NOW - 100 * DAY))  # read two days ago, built long ago
    run(env, "--apply")
    assert leaf.exists()


def test_a_series_point_is_kept_even_if_old(env):
    point, landmark, head, tip, plain = (make_entry(env["root"], "valkey", sha(n), age_days=400) for n in range(1, 6))
    write_state(
        env["state"], "state_v1.json", points=[sha(1)], landmarks=[sha(2)], head=sha(3), merges=[sha(0), sha(4)]
    )
    run(env, "--apply")
    assert point.exists() and landmark.exists() and head.exists() and tip.exists()
    assert not plain.exists()


def test_points_in_the_v3_directory_and_memory_state_are_kept(env):
    a = make_entry(env["root"], "valkey", sha(1), age_days=400)
    b = make_entry(env["root"], "redis", sha(2), age_days=400)
    c = make_entry(env["root"], "valkey", sha(3), age_days=400)
    write_state(env["state"] / "v3", "state_cachecannon-v3_get.json", points=[sha(1)])
    write_state(env["state"], "memory_state_sadd.json", points=[sha(2)])
    run(env, "--apply")
    assert a.exists() and b.exists() and not c.exists()


def test_a_commit_known_only_as_a_merge_commit_or_date_is_not_kept(env):
    entry = make_entry(env["root"], "valkey", sha(1), age_days=400)
    write_state(env["state"], "state_v1.json", points=[sha(7)], merges=[sha(1), sha(8)])
    run(env, "--apply")
    assert not entry.exists()


def test_a_nested_value_that_looks_like_a_point_key_is_ignored(env):
    entry = make_entry(env["root"], "valkey", sha(1), age_days=400)
    path = write_state(env["state"], "state_v1.json", points=[sha(7)])
    data = json.loads(path.read_text())
    data["points"][sha(7)]["perf_counters"] = {sha(1): {"commit": sha(1)}}
    path.write_text(json.dumps(data, indent=2))
    run(env, "--apply")
    assert not entry.exists()


def test_the_pinned_cachecannon_commit_and_checked_out_head_are_kept(env, monkeypatch):
    pinned = make_entry(env["root"], "valkey", sha(999), age_days=400)
    head = make_entry(env["root"], "valkey", sha(5), age_days=400)
    other = make_entry(env["root"], "valkey", sha(6), age_days=400)
    monkeypatch.setattr(bc, "checked_out_heads", lambda home, sources: {sha(5)})
    run(env, "--apply")
    assert pinned.exists() and head.exists() and not other.exists()


def test_names_that_are_not_cache_entries_are_kept_and_listed_as_skipped(env):
    root = env["root"]
    (root / "notes.txt").write_text("x")
    (root / "other-engine" / sha(1) / "0123456789abcdef").mkdir(parents=True)
    (root / "valkey" / "not-a-commit" / "0123456789abcdef").mkdir(parents=True)
    (root / "valkey" / sha(2) / "odd-name").mkdir(parents=True)
    legacy = root / "valkey" / sha(3)  # old layout: the binary directly in the commit directory
    legacy.mkdir(parents=True)
    (legacy / "valkey-server").write_bytes(b"x")
    for item in root.rglob("*"):
        os.utime(item, (NOW - 500 * DAY, NOW - 500 * DAY))
    before = tree(root)
    code, text = run(env, "--apply")
    assert code == bc.EXIT_OK
    assert tree(root) == before
    assert "Skipped (not a cache entry, left alone): 5" in text
    for name in ("notes.txt", "other-engine", "valkey/not-a-commit", f"valkey/{sha(2)}/odd-name", f"valkey/{sha(3)}"):
        assert name in text


def test_a_symlink_inside_an_entry_makes_it_skipped(env, tmp_path):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=400)
    target = tmp_path / "elsewhere"
    target.mkdir()
    (leaf / "link").symlink_to(target)
    _, text = run(env, "--apply")
    assert leaf.exists() and target.exists()
    assert "contains a symlink" in text


# --- deletion safety ----------------------------------------------------------------------------


def _entry(path: Path, source="valkey", commit=None, args_hash="0123456789abcdef") -> bc.Entry:
    return bc.Entry(source, commit or sha(1), args_hash, path, 0, 0.0)


def test_a_symlinked_entry_is_refused(env, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("x")
    parent = env["root"] / "valkey" / sha(1)
    parent.mkdir(parents=True)
    (parent / "0123456789abcdef").symlink_to(outside)
    with pytest.raises(ValueError):
        bc.remove_entry(env["root"], _entry(parent / "0123456789abcdef"))
    assert (outside / "keep.txt").exists()


def test_a_symlinked_parent_that_leads_outside_is_refused(env, tmp_path):
    outside = tmp_path / "outside"
    (outside / sha(1) / "0123456789abcdef").mkdir(parents=True)
    (env["root"] / "valkey").symlink_to(outside)
    with pytest.raises(ValueError):
        bc.remove_entry(env["root"], _entry(env["root"] / "valkey" / sha(1) / "0123456789abcdef"))
    assert (outside / sha(1) / "0123456789abcdef").exists()


def test_a_path_outside_the_cache_is_refused(env, tmp_path):
    outside = tmp_path / "home" / "documents" / "0123456789abcdef"
    outside.mkdir(parents=True)
    with pytest.raises(ValueError):
        bc.remove_entry(env["root"], _entry(outside))
    assert outside.exists()


def test_a_name_that_is_not_a_cache_name_is_refused(env):
    make_entry(env["root"], "valkey", sha(1), age_days=1)
    with pytest.raises(ValueError):
        bc.remove_entry(env["root"], _entry(env["root"] / "valkey" / sha(1), args_hash="../.."))
    with pytest.raises(ValueError):
        bc.remove_entry(env["root"], _entry(env["root"], source="../x", commit="..", args_hash="0123456789abcdef"))


def test_only_the_entry_directory_is_removed(env):
    a = make_entry(env["root"], "valkey", sha(1), age_days=400, args_hash="aaaaaaaaaaaaaaaa")
    b = make_entry(env["root"], "valkey", sha(1), age_days=400, args_hash="bbbbbbbbbbbbbbbb")
    bc.remove_entry(env["root"], _entry(a, args_hash="aaaaaaaaaaaaaaaa"))
    assert not a.exists() and b.exists() and b.parent.exists()


def test_an_entry_used_again_after_the_scan_is_left_alone(env):
    leaf = make_entry(env["root"], "valkey", sha(1), age_days=400)
    entries, skipped = bc.scan_cache(env["root"], ["valkey"])
    plan = bc.make_plan(entries, skipped, now=NOW, min_age_days=60, series=set(), pinned=set())
    assert plan.remove
    os.utime(leaf / "valkey-server", (NOW, NOW))  # a task started to use it
    removed, _, code = bc.apply_plan(
        plan,
        env["root"],
        log_path=env["tmp"] / "p.log",
        is_busy=lambda: [],
        now=NOW,
        min_age_days=60,
        out=io.StringIO(),
    )
    assert (removed, code) == (0, bc.EXIT_OK)
    assert leaf.exists()
    assert "kept-used-again" in (env["tmp"] / "p.log").read_text()


# --- dry run, apply and the idle check ----------------------------------------------------------


def test_dry_run_removes_nothing_and_says_what_it_would_do(env):
    for n in range(1, 26):
        make_entry(env["root"], "valkey", sha(n), age_days=100 + n)
    make_entry(env["root"], "redis", sha(50), age_days=5)
    before = tree(env["root"])
    code, text = run(env)
    assert code == bc.EXIT_OK
    assert tree(env["root"]) == before
    assert not (env["tmp"] / "prune.log").exists()
    assert "Would remove: 25 entries" in text
    assert "oldest: valkey/" + sha(25) in text
    assert "newest: valkey/" + sha(1) in text
    assert "... and 5 more" in text  # the first 20 names, then a count
    assert len(re.findall(r"^  valkey/[0-9a-f]{40}/", text, re.M)) == 20
    assert "Dry run: nothing was removed" in text


def test_apply_is_off_by_default():
    args = bc.build_parser().parse_args(["prune"])
    assert args.apply is False and args.min_age_days == 60


@pytest.mark.parametrize("cause", ["process", "queue", "journal", "unreadable journal"])
def test_apply_is_refused_while_a_task_is_running(env, cause):
    old = make_entry(env["root"], "valkey", sha(1), age_days=400)
    if cause == "process":
        (env["proc"] / "123").mkdir()
        (env["proc"] / "123" / "comm").write_text("valkey-server\n")
        (env["proc"] / "123" / "cmdline").write_bytes(b"/home/u/build_cache/x/valkey-server\x00--port\x006379")
    elif cause == "queue":
        (env["queue"] / "task_1.json").write_text("{}")
    elif cause == "journal":
        (env["tmp"] / "fleet_delivery.json").write_text(json.dumps({"active": {"task_id": "t1"}}))
    else:
        (env["tmp"] / "fleet_delivery.json").write_text("{not json")
    code, text = run(env, "--apply")
    assert code == bc.EXIT_BUSY
    assert "refused" in text
    assert old.exists()
    assert not (env["tmp"] / "prune.log").exists()


def test_a_cut_process_name_is_recognised(env):
    (env["proc"] / "7").mkdir()
    (env["proc"] / "7" / "comm").write_text("memtier_benchma\n")  # comm is cut at 15 characters
    (env["proc"] / "7" / "cmdline").write_bytes(b"")
    assert bc.busy_reasons(queue_dir=env["queue"], journal_path=env["tmp"] / "j.json", proc_root=env["proc"])


def test_an_idle_runner_with_an_idle_journal_is_not_busy(env):
    (env["tmp"] / "fleet_delivery.json").write_text(json.dumps({"active": None}))
    (env["proc"] / "5").mkdir()
    (env["proc"] / "5" / "comm").write_text("bash\n")
    (env["proc"] / "5" / "cmdline").write_bytes(b"bash\x00")
    assert (
        bc.busy_reasons(queue_dir=env["queue"], journal_path=env["tmp"] / "fleet_delivery.json", proc_root=env["proc"])
        == []
    )


def test_apply_stops_when_the_runner_becomes_busy(env):
    for n in range(1, 31):
        make_entry(env["root"], "valkey", sha(n), age_days=400 + n)
    entries, skipped = bc.scan_cache(env["root"], ["valkey"])
    plan = bc.make_plan(entries, skipped, now=NOW, min_age_days=60, series=set(), pinned=set())
    calls = []

    def is_busy():
        calls.append(1)
        return ["process valkey-server is running"] if len(calls) > 1 else []

    removed, _, code = bc.apply_plan(
        plan, env["root"], log_path=env["tmp"] / "p.log", is_busy=is_busy, now=NOW, min_age_days=60, out=io.StringIO()
    )
    assert code == bc.EXIT_BUSY
    assert removed == bc.RECHECK_EVERY
    assert len(tree(env["root"])) > 0


def test_every_removal_is_logged(env):
    make_entry(env["root"], "valkey", sha(1), age_days=400)
    make_entry(env["root"], "redis", sha(2), age_days=300)
    run(env, "--apply")
    lines = (env["tmp"] / "prune.log").read_text().splitlines()
    assert len(lines) == 2
    assert all(" removed " in line for line in lines)
    assert f"valkey/{sha(1)}/0123456789abcdef" in lines[0]  # oldest first


# --- size cap ------------------------------------------------------------------------------------


def test_max_size_removes_the_oldest_removable_entries_until_the_cache_fits(env):
    leaves = [make_entry(env["root"], "valkey", sha(n), age_days=100 + n) for n in range(1, 5)]
    entries, _ = bc.scan_cache(env["root"], ["valkey"])
    one = entries[0].size
    # cap of 2.5 entries: the two oldest go, the two newest stay
    code, _ = run(env, "--apply", "--max-size-gb", str(2.5 * one / (1 << 30)))
    assert code == bc.EXIT_OK
    assert [leaf.exists() for leaf in leaves] == [True, True, False, False]


def test_max_size_never_removes_protected_entries(env):
    fresh = make_entry(env["root"], "valkey", sha(1), age_days=1)
    point = make_entry(env["root"], "valkey", sha(2), age_days=500)
    old = make_entry(env["root"], "valkey", sha(3), age_days=500)
    write_state(env["state"], "state_v1.json", points=[sha(2)])
    _, text = run(env, "--apply", "--max-size-gb", "0")
    assert fresh.exists() and point.exists() and not old.exists()
    assert "larger than --max-size-gb" in text


# --- state files ---------------------------------------------------------------------------------


def test_a_state_file_in_another_layout_stops_the_run(env):
    old = make_entry(env["root"], "valkey", sha(1), age_days=400)
    (env["state"] / "state_v1.json").write_text(json.dumps({"points": {sha(1): {}}}))  # compact, not indent=2
    code, text = run(env, "--apply")
    assert code == bc.EXIT_ERROR
    assert "no top-level 'points' key" in text
    assert old.exists()


def test_no_state_files_stops_the_run(env):
    old = make_entry(env["root"], "valkey", sha(1), age_days=400)
    (env["state"] / "state_x.json").unlink()
    code, text = run(env, "--apply")
    assert code == bc.EXIT_ERROR and old.exists()
    assert "no sweep state files" in text


def test_the_state_reader_gives_the_same_commits_as_loading_the_file(env):
    path = write_state(
        env["state"], "state_v1.json", points=[sha(1), sha(2)], landmarks=[sha(3)], head=sha(4), merges=[sha(5), sha(6)]
    )
    loaded = SweepState.load(path)
    expected = {*loaded.points, *(p.commit for p in loaded.points.values()), *(lm.commit for lm in loaded.landmarks)}
    expected |= {loaded.last_benchmarked_head, loaded.merge_commits[-1]}
    assert bc.commits_in_state_file(path) == expected


def test_a_missing_cache_directory_is_an_error(env):
    env["root"].rmdir()
    code, _ = run(env)
    assert code == bc.EXIT_ERROR


# --- never automatic -----------------------------------------------------------------------------


def test_nothing_but_the_cli_entry_point_uses_the_module():
    src = Path(bc.__file__).parent
    users = [
        p.name
        for p in src.rglob("*.py")
        if p.name != "build_cache.py"
        and re.search(r"\bbuild_cache import|import build_cache|conductress\.build_cache", p.read_text())
    ]
    assert users == ["__main__.py"]


def test_the_command_is_reachable_from_the_main_entry_point():
    from conductress.__main__ import main

    with patch("sys.argv", ["conductress", "build-cache", "prune", "--help"]), pytest.raises(SystemExit) as stop:
        main()
    assert stop.value.code == 0
