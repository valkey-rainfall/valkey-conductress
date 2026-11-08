"""Series generations: closing a platform's live v3 history at a manual cut.

Everything runs on tmp_path stand-ins for the v3 state dir, the results file,
the task queue and the two bookkeeping files; no runner is involved.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from conductress.sweep import generations as gen
from conductress.sweep.planner import BenchmarkPoint, PointStatus, SweepPlanner, SweepState

UTC = timezone.utc
CUT = datetime(2026, 11, 6, 16, 14, 23, tzinfo=UTC)
RESUME = datetime(2026, 11, 6, 18, 21, 58, tzinfo=UTC)
LABEL = "get-k16-v16-t7-p1"
COMMITS = [f"{i:040x}" for i in range(1, 9)]


def _task_time(moment: datetime) -> str:
    return moment.strftime("%Y.%m.%d_%H.%M.%S.%f")


def _row(label: str, commit: str, start: datetime, duration: float = 576.1, source: str = "valkey", **extra) -> str:
    end = start + timedelta(seconds=duration)
    row = {
        "method": "cachecannon",
        "score": 1.0,
        "commit_hash": commit,
        "end_time": _task_time(end),
        "note": f"[cachecannon-sweep-v3:{source}/{label}] Backfill: 6 commits in gap",
        "task_id": _task_time(start - timedelta(seconds=7)),
        "observed_duration_sec": duration,
        "big": "x" * 2000,
    }
    row.update(extra)
    return json.dumps(row)


def _write_state(state_dir: Path, label: str, commits, pending=()) -> Path:
    state_dir.mkdir(parents=True, exist_ok=True)
    state = SweepState(
        merge_commits=list(COMMITS), commit_dates={c: f"2026-10-{i + 1:02d}" for i, c in enumerate(COMMITS)}
    )
    for commit in commits:
        state.points[commit] = BenchmarkPoint(
            commit=commit, date="2026-10-01", value=100.0, cv=1.0, reps=3, status=PointStatus.COMPLETED
        )
    for commit in pending:
        state.points[commit] = BenchmarkPoint(commit=commit, date="2026-10-01", status=PointStatus.PENDING)
    path = state_dir / f"{gen.V3_STATE_PREFIX}{label}.json"
    state.save(path)
    return path


@pytest.fixture
def rig(tmp_path):
    """A v3 state dir with one series, a results file, a queue, and bookkeeping paths."""
    state_dir = tmp_path / "v3"
    results = tmp_path / "output.jsonl"
    queue = tmp_path / "queue"
    # Commits 1-2: old rows (before the cut day). 3: before the cut, same day.
    # 4-5: started in the discard window. 6-7: after the resume. 8: pending.
    lines = [
        _row(LABEL, COMMITS[0], datetime(2026, 10, 20, 12, 0, tzinfo=UTC)),
        _row(LABEL, COMMITS[1], datetime(2026, 11, 5, 12, 0, tzinfo=UTC)),
        _row(LABEL, COMMITS[2], CUT - timedelta(minutes=15)),
        _row(LABEL, COMMITS[3], CUT + timedelta(minutes=5)),
        _row(LABEL, COMMITS[4], RESUME - timedelta(minutes=30)),
        _row(LABEL, COMMITS[5], RESUME),
        _row(LABEL, COMMITS[6], RESUME + timedelta(hours=1)),
        # Another series' row at a discard-window time: must not affect LABEL.
        _row("set-k16-v16-t7-p10", COMMITS[0], CUT + timedelta(minutes=10)),
        # An old row for commit 6 too: the LATEST row decides the bucket.
        _row(LABEL, COMMITS[5], datetime(2026, 11, 1, 12, 0, tzinfo=UTC)),
    ]
    results.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _write_state(state_dir, LABEL, COMMITS[:7], pending=[COMMITS[7]])
    _write_state(state_dir, "set-k16-v16-t7-p10", [COMMITS[0]])
    return {
        "state_dir": state_dir,
        "results": results,
        "queue": queue,
        "generations": tmp_path / "generations.json",
        "excluded": tmp_path / "excluded_tasks.json",
    }


def _close(rig, **kwargs):
    defaults = dict(
        cut=CUT,
        resume=RESUME,
        label="kernel + glibc + microcode update",
        reason="canary alarm +12.3%",
        recorded_by="tests",
        state_dir=rig["state_dir"],
        generations_file=rig["generations"],
        excluded_file=rig["excluded"],
        results_file=rig["results"],
        queue_dir=rig["queue"],
        service_active=False,
        now=datetime(2026, 11, 8, 7, 0, tzinfo=UTC),
    )
    defaults.update(kwargs)
    return gen.close_generation(**defaults)


class TestRowReading:
    def test_start_time_is_end_minus_duration(self):
        row = json.loads(_row(LABEL, COMMITS[0], CUT))
        assert gen.row_start_time(row) == CUT
        assert gen.row_start_time({"end_time": "bad"}) is None
        assert gen.row_start_time({"end_time": _task_time(CUT)}) is None  # no duration

    def test_latest_rows_keeps_the_newest_row_per_commit_and_skips_old_lines(self, rig):
        rows = gen.latest_rows(rig["results"], [LABEL], datetime(2026, 11, 6, tzinfo=UTC))
        assert (LABEL, COMMITS[0]) not in rows  # older than the cut day: never parsed
        assert (LABEL, COMMITS[1]) not in rows
        assert rows[(LABEL, COMMITS[5])].start_time == RESUME  # the newer of its two rows
        assert set(k[0] for k in rows) == {LABEL}

    def test_missing_results_file_gives_no_rows(self, tmp_path):
        assert gen.latest_rows(tmp_path / "none.jsonl", [LABEL], CUT) == {}


class TestCloseGeneration:
    def test_splits_points_by_task_start_time(self, rig):
        result = _close(rig)
        assert result.refused_reason is None
        split = next(s for s in result.series if s.label == LABEL)
        assert sorted(split.before) == sorted(COMMITS[:3])
        assert sorted(split.after) == sorted(COMMITS[5:7])
        assert [d["commit"] for d in split.discarded] == sorted(COMMITS[3:5])
        assert all(
            d["reason"].startswith("started between the cut") and "canary alarm" in d["reason"] for d in split.discarded
        )

        archived = SweepState.load(result.generation_dir / f"{gen.V3_STATE_PREFIX}{LABEL}.json")
        live = SweepState.load(rig["state_dir"] / f"{gen.V3_STATE_PREFIX}{LABEL}.json")
        assert set(archived.points) == set(COMMITS[:3])
        assert set(live.points) == {COMMITS[5], COMMITS[6], COMMITS[7]}  # after + the pending point
        assert live.points[COMMITS[7]].status is PointStatus.PENDING
        # Shared history travels with both halves.
        assert archived.merge_commits == live.merge_commits == COMMITS
        assert archived.commit_dates == live.commit_dates

    def test_other_series_is_split_by_its_own_rows(self, rig):
        result = _close(rig)
        other = next(s for s in result.series if s.label == "set-k16-v16-t7-p10")
        assert other.before == [] and other.after == [] and [d["commit"] for d in other.discarded] == [COMMITS[0]]

    def test_records_exclusions_generation_and_backup(self, rig):
        result = _close(rig)
        excluded = gen.load_excluded_tasks(rig["excluded"])
        assert len(excluded) == 3
        entry = excluded[next(iter(excluded))]
        assert entry["recorded_by"] == "tests" and entry["recorded_at"] == "2026-11-08T07:00:00Z"
        assert entry["series"].startswith("cachecannon-sweep-v3/")
        closed = gen.load_generations(rig["generations"])
        assert closed[0]["id"] == "2026-11-06"
        assert closed[0]["until"] == "2026-11-06T16:14:23Z" and closed[0]["live_from"] == "2026-11-06T18:21:58Z"
        assert closed[0]["excluded_tasks"] == 3 and closed[0]["label"].startswith("kernel")
        assert sorted(closed[0]["workloads"]) == [LABEL, "set-k16-v16-t7-p10"]
        # The live file was backed up before it was rewritten; a copy of the
        # exclusion list travels with the archived state.
        backups = list(rig["state_dir"].glob(f"{gen.V3_STATE_PREFIX}{LABEL}.json.bak-*"))
        assert len(backups) == 1 and set(SweepState.load(backups[0]).points) == set(COMMITS[:8])
        assert (result.generation_dir / "excluded_tasks.json").exists()
        # The raw results file is untouched.
        assert rig["results"].read_text().count("\n") == 9

    def test_without_resume_nothing_is_discarded(self, rig):
        result = _close(rig, resume=None)
        split = next(s for s in result.series if s.label == LABEL)
        assert split.discarded == [] and sorted(split.after) == sorted(COMMITS[3:7])
        assert not rig["excluded"].exists()

    def test_dry_run_touches_nothing(self, rig):
        before = {p: p.read_bytes() for p in rig["state_dir"].glob("*.json")}
        result = _close(rig, dry_run=True)
        assert result.discarded_count == 3
        assert {p: p.read_bytes() for p in rig["state_dir"].glob("*.json")} == before
        assert not result.generation_dir.exists()
        assert not rig["generations"].exists() and not rig["excluded"].exists()
        assert any("Would" in line for line in result.summary_lines())

    def test_workloads_filter_and_queued_cells(self, rig):
        rig["queue"].mkdir()
        mine = rig["queue"] / "task_a.json"
        mine.write_text(json.dumps({"note": f"[cachecannon-sweep-v3:valkey/{LABEL}] Backfill"}))
        other = rig["queue"] / "task_b.json"
        other.write_text(json.dumps({"note": "[cachecannon-sweep-v3:valkey/set-k16-v16-t7-p10] Backfill"}))
        result = _close(rig, workloads=[LABEL])
        assert [s.label for s in result.series] == [LABEL]
        assert not mine.exists() and other.exists()
        assert list(rig["queue"].glob("reset-*/task_a.json"))

    def test_refusals(self, rig):
        assert "active" in _close(rig, service_active=True).refused_reason
        assert "no v3 state files" in _close(rig, workloads=["nope"]).refused_reason
        _close(rig)
        assert "already closed" in _close(rig).refused_reason
        with pytest.raises(ValueError, match="before cut"):
            _close(rig, resume=CUT - timedelta(seconds=1))
        with pytest.raises(ValueError, match="timezone"):
            _close(rig, cut=CUT.replace(tzinfo=None))


class TestExclusionEnforcement:
    def test_is_excluded_follows_the_file(self, tmp_path):
        path = tmp_path / "excluded_tasks.json"
        assert gen.is_excluded("t1", path) is False
        gen.append_excluded_tasks([{"task_id": "t1", "reason": "x"}], path)
        assert gen.is_excluded("t1", path) is True and gen.is_excluded("t2", path) is False
        assert gen.is_excluded(None, path) is False
        gen.append_excluded_tasks([{"task_id": "t1", "reason": "second entry is ignored"}], path)
        assert gen.load_excluded_tasks(path)["t1"]["reason"] == "x"

    def test_planner_drops_points_of_excluded_tasks(self, tmp_path, monkeypatch):
        path = tmp_path / "excluded_tasks.json"
        monkeypatch.setattr(gen, "EXCLUDED_TASKS_FILE", path)
        monkeypatch.setattr(gen.excluded_task_ids, "__defaults__", (path,))
        state = SweepState(merge_commits=COMMITS[:3])
        for i, commit in enumerate(COMMITS[:3]):
            state.points[commit] = BenchmarkPoint(
                commit=commit, date="d", value=1.0, task_id=f"task-{i}", status=PointStatus.COMPLETED
            )
        planner = SweepPlanner(state)
        assert len(planner._get_ordered_completed_points()) == 3
        gen.append_excluded_tasks([{"task_id": "task-1", "reason": "discarded"}], path)
        assert [p.commit for p in planner._get_ordered_completed_points()] == [COMMITS[0], COMMITS[2]]

    def test_point_task_id_round_trips_through_state_file(self, tmp_path):
        state = SweepState(merge_commits=[COMMITS[0]])
        state.points[COMMITS[0]] = BenchmarkPoint(
            commit=COMMITS[0], date="d", value=1.0, task_id="t-9", status=PointStatus.COMPLETED
        )
        state.save(tmp_path / "s.json")
        assert SweepState.load(tmp_path / "s.json").points[COMMITS[0]].task_id == "t-9"
        # A state written before the field existed loads with None.
        raw = json.loads((tmp_path / "s.json").read_text())
        del raw["points"][COMMITS[0]]["task_id"]
        (tmp_path / "old.json").write_text(json.dumps(raw))
        assert SweepState.load(tmp_path / "old.json").points[COMMITS[0]].task_id is None


class TestNaming:
    def test_generation_path_suffix(self):
        path = Path("series-intel-get-throughput.epoch-v3.json")
        assert (
            gen.generation_path(path, "2026-11-06").name == "series-intel-get-throughput.epoch-v3.gen-2026-11-06.json"
        )
        assert gen.generation_path(path, None) == path

    def test_label_from_state_file(self):
        assert (
            gen.label_from_state_file(Path("state_cachecannon-v3_redis-get-k16-v16-t7-p1.json"))
            == "redis-get-k16-v16-t7-p1"
        )
        with pytest.raises(ValueError):
            gen.label_from_state_file(Path("memory_state_set-k16-v16.json"))

    def test_rehome_points_a_coordinator_at_the_archived_copy(self, tmp_path):
        class Coord:
            def __init__(self, state_file):
                self.state_file = state_file
                self.state = SweepState()

            def _new_planner(self):
                return SweepPlanner(self.state)

        live = tmp_path / "live" / "state_cachecannon-v3_x.json"
        live.parent.mkdir()
        gen_dir = tmp_path / "gen"
        coord = Coord(live)
        assert gen.rehome(coord, gen_dir) is False
        gen_dir.mkdir()
        SweepState(merge_commits=["a"]).save(gen_dir / live.name)
        assert gen.rehome(coord, gen_dir) is True
        assert coord.state_file == gen_dir / live.name and coord.state.merge_commits == ["a"]
