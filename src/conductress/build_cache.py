"""Prune old entries from the runner's build cache (``conductress build-cache prune``).

The cache is ``~/build_cache/{source}/{commit}/{make_args_hash}/`` (see ``binary_manager.py``). Nothing
removes entries, so it grows by about 20 GB a month. This command is run by an operator on a runner host.
It is never called by the service, the sweep or any scheduler.

Safety rules:

* It prints what it would remove and removes nothing unless ``--apply`` is given.
* ``--apply`` refuses while the runner is busy. The idle test is the one the deploy scripts use: no
  ``valkey-server``, ``cachecannon``, ``memtier_benchmark`` or ``valkey-benchmark`` process (this module also
  checks ``redis-server``), an empty ``benchmark_queue`` and no active task in ``fleet_delivery.json``.
  It is checked again while it deletes.
* An entry stays (the keep set) when it was used within ``--min-age-days``, when its commit is a point,
  landmark or tip in a sweep state file, when it is the pinned cachecannon commit or a checked-out source
  HEAD, or when its name or content is not what the cache layout produces ("skipped").
* Only the leaf directory of an entry is deleted, one entry at a time, after a path check that refuses
  symlinks and anything that does not resolve to ``<cache>/<source>/<commit>/<make_args_hash>``.
  Every removal is appended to a prune log.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Callable, Iterable, Optional

from . import config
from .engines import ENGINES

COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
ARGS_HASH_RE = re.compile(r"^[0-9a-f]{16}$")
_COMMIT_BYTES_RE = re.compile(rb"[0-9a-f]{40}")
_TOP_KEY_RE = re.compile(rb'^  "([^"]+)": ?(.*)$')
_POINT_KEY_RE = re.compile(rb'^    "([0-9a-f]{40})": ')
_LIST_ITEM_RE = re.compile(rb'^    "([0-9a-f]{40})",?\s*$')
_COMMIT_FIELD_RE = re.compile(rb'^      "commit": "([0-9a-f]{40})"')
_HEAD_FIELD_RE = re.compile(rb'"([0-9a-f]{40})"')

# Process names the deploy scripts treat as "a benchmark is running" (plus redis-server, the comparison engine).
BUSY_PROCESS_NAMES = (
    "valkey-server",
    "redis-server",
    "cachecannon",
    "memtier_benchmark",
    "valkey-benchmark",
) + tuple(name for engine in ENGINES.values() for name in engine.kill_names)

DEFAULT_MIN_AGE_DAYS = 60
RECHECK_EVERY = 10  # re-test "idle" after this many deletions
LIST_LIMIT = 20

# Sources whose entries cannot be rebuilt from a git commit. They are never removed.
# (Every name in config.REPO_NAMES is a git clone, so its entries are rebuilt by a build.)
_GIB = 1 << 30

EXIT_OK = 0
EXIT_ERROR = 2
EXIT_BUSY = 3


def not_rebuildable(source: str) -> bool:
    """True for a source whose cache entries cannot be rebuilt by building a commit (a hand-uploaded binary)."""
    return source == config.MANUALLY_UPLOADED


class StateFileError(Exception):
    """A sweep state file is not in the format ``SweepState.save`` writes."""


@dataclass
class Entry:
    """One cache entry: the make-args directory of one commit."""

    source: str
    commit: str
    args_hash: str
    path: Path
    size: int  # allocated bytes (st_blocks * 512), each hard-linked file counted once in the entry
    last_use: float  # newest mtime or atime in the entry, unix seconds
    apparent: int = 0  # sum of file sizes
    # Files with more than one link: (device, inode) -> (allocated bytes, link count, links found in this entry).
    shared: dict = field(default_factory=dict)

    @property
    def name(self) -> str:
        return f"{self.source}/{self.commit}/{self.args_hash}"


@dataclass
class Plan:
    """What a prune run would do."""

    remove: list[Entry] = field(default_factory=list)
    kept: dict[str, list[Entry]] = field(default_factory=dict)  # reason -> entries
    skipped: list[str] = field(default_factory=list)  # names that are not cache entries
    total_entries: int = 0
    total_bytes: int = 0


# --------------------------------------------------------------------------- idle check


def _process_names(proc_root: Path) -> set[str]:
    names: set[str] = set()
    for proc in proc_root.iterdir():
        if not proc.name.isdigit():
            continue
        try:
            names.add((proc / "comm").read_text().strip())
            argv0 = (proc / "cmdline").read_bytes().split(b"\x00", 1)[0].decode(errors="ignore")
        except OSError:
            continue
        if argv0:
            names.add(os.path.basename(argv0))
    return names


def busy_reasons(
    *,
    queue_dir: Path,
    journal_path: Path,
    proc_root: Path = Path("/proc"),
) -> list[str]:
    """Why the runner is not idle; an empty list means idle.

    Same condition as the deploy scripts: a benchmark process, a queued task file, or an active task in
    the fleet delivery journal. A journal that exists but cannot be read counts as busy.
    """
    reasons = []
    running = _process_names(proc_root)
    # ``comm`` is cut at 15 characters, so compare the cut name too.
    for name in BUSY_PROCESS_NAMES:
        if name in running or name[:15] in running:
            reasons.append(f"process {name} is running")
    if queue_dir.is_dir() and any(queue_dir.iterdir()):
        reasons.append(f"{queue_dir.name} is not empty")
    if journal_path.exists():
        try:
            active = json.loads(journal_path.read_text()).get("active")
        except (OSError, ValueError, AttributeError):
            reasons.append(f"{journal_path.name} cannot be read")
        else:
            if active is not None:
                reasons.append(f"{journal_path.name} has an active task")
    return reasons


# --------------------------------------------------------------------------- sweep state


def state_files(state_dir: Path) -> list[Path]:
    """The live sweep state files: ``*state*.json`` in the state directory and its ``v3`` directory."""
    found: list[Path] = []
    for directory in (state_dir, state_dir / "v3"):
        if directory.is_dir():
            found.extend(p for p in sorted(directory.glob("*state*.json")) if p.is_file())
    return found


def commits_in_state_file(path: Path) -> set[str]:
    """Commits that are points, landmarks, the last benchmarked head or the newest merge commit in a state file.

    Reads the file one line at a time, so memory is bounded by the longest line, not by the file (they are
    1.3 to 1.5 GB). It relies on the layout ``SweepState.save`` writes (``json.dumps(indent=2)``): the
    section keys sit at 2 spaces, point keys and list items at 4, a point's own fields at 6. A file that
    does not have a top-level ``points`` key in that layout raises StateFileError, so a changed format
    stops the prune instead of shrinking the keep set.
    """
    found: set[str] = set()
    section = b""
    newest_merge = None
    has_points = False
    with path.open("rb") as handle:
        for raw in handle:
            if raw.startswith(b'  "'):
                match = _TOP_KEY_RE.match(raw)
                if match is None:
                    continue
                section = match.group(1)
                has_points = has_points or section == b"points"
                if section == b"last_benchmarked_head":
                    head = _HEAD_FIELD_RE.search(match.group(2))
                    if head:
                        found.add(head.group(1).decode())
                continue
            if section == b"points":
                match = _POINT_KEY_RE.match(raw) or _COMMIT_FIELD_RE.match(raw)
                if match:
                    found.add(match.group(1).decode())
            elif section == b"landmarks":
                match = _COMMIT_FIELD_RE.match(raw)
                if match:
                    found.add(match.group(1).decode())
            elif section == b"merge_commits":
                match = _LIST_ITEM_RE.match(raw)
                if match:
                    newest_merge = match.group(1).decode()
    if not has_points:
        raise StateFileError(f"{path}: no top-level 'points' key in the expected layout")
    if newest_merge:
        found.add(newest_merge)
    return found


def series_commits(files: Iterable[Path]) -> set[str]:
    commits: set[str] = set()
    for path in files:
        commits |= commits_in_state_file(path)
    return commits


def checked_out_heads(home: Path, sources: Iterable[str]) -> set[str]:
    """HEAD of each source checkout (``~/valkey``, ``~/redis``): the commit built last. Best effort."""
    heads = set()
    for source in sources:
        repo = home / source
        if not (repo / ".git").exists():
            continue
        try:
            out = subprocess.run(
                ["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10, check=False
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            continue
        if COMMIT_RE.match(out):
            heads.add(out)
    return heads


# --------------------------------------------------------------------------- scanning


@dataclass
class Measured:
    apparent: int  # sum of file sizes
    allocated: int  # st_blocks * 512, each hard-linked file once
    last_use: float
    shared: dict  # hard-linked files: (device, inode) -> [allocated bytes, link count, links found]


def _measure(path: Path) -> Optional[Measured]:
    """Size and newest time of a tree, or None if it contains a symlink."""
    apparent = allocated = 0
    newest = 0.0
    shared: dict = {}
    stack = [path]
    while stack:
        with os.scandir(stack.pop()) as it:
            for item in it:
                info = item.stat(follow_symlinks=False)
                if item.is_symlink():
                    return None
                if item.is_dir(follow_symlinks=False):
                    # Listing a directory sets its access time, so only its modification time counts.
                    newest = max(newest, info.st_mtime)
                    allocated += info.st_blocks * 512
                    stack.append(Path(item.path))
                    continue
                newest = max(newest, info.st_mtime, info.st_atime)
                if info.st_nlink > 1:
                    key = (info.st_dev, info.st_ino)
                    if key not in shared:
                        apparent += info.st_size
                    seen = shared.setdefault(key, [info.st_blocks * 512, info.st_nlink, 0])
                    seen[2] += 1
                else:
                    apparent += info.st_size
                    allocated += info.st_blocks * 512
    info = path.lstat()
    allocated += info.st_blocks * 512 + sum(blocks for blocks, _, _ in shared.values())
    return Measured(apparent, allocated, max(newest, info.st_mtime), shared)


def freed_estimate(entries: Iterable[Entry]) -> int:
    """Allocated bytes that removing these entries frees: a hard-linked file counts once, and only when
    every link to it is inside these entries.

    shortcut: files that share disk blocks through reflink or copy-on-write are not detected, so the space
    really freed can be lower. Upgrade to FIEMAP shared-extent checks if the numbers keep differing from df.
    """
    total = 0
    links: dict = {}
    for entry in entries:
        total += entry.size - sum(blocks for blocks, _, _ in entry.shared.values())
        for key, (blocks, nlink, found) in entry.shared.items():
            seen = links.setdefault(key, [blocks, nlink, 0])
            seen[2] += found
    return total + sum(blocks for blocks, nlink, found in links.values() if found >= nlink)


def scan_cache(root: Path, sources: Iterable[str]) -> tuple[list[Entry], list[str]]:
    """List the cache entries and the names that are not entries (reported as skipped)."""
    allowed = set(sources)
    entries: list[Entry] = []
    skipped: list[str] = []
    for source_dir in sorted(root.iterdir()):
        if source_dir.is_symlink() or not source_dir.is_dir() or source_dir.name not in allowed:
            skipped.append(source_dir.name)
            continue
        for commit_dir in sorted(source_dir.iterdir()):
            rel = f"{source_dir.name}/{commit_dir.name}"
            if commit_dir.is_symlink() or not commit_dir.is_dir() or not COMMIT_RE.match(commit_dir.name):
                skipped.append(rel)
                continue
            leaves = sorted(commit_dir.iterdir())
            if not leaves:
                continue  # an empty commit directory holds nothing
            for leaf in leaves:
                if leaf.is_symlink() or not leaf.is_dir() or not ARGS_HASH_RE.match(leaf.name):
                    skipped.append(f"{rel}/{leaf.name}")
                    continue
                measured = _measure(leaf)
                if measured is None:
                    skipped.append(f"{rel}/{leaf.name} (contains a symlink)")
                    continue
                entries.append(
                    Entry(
                        source_dir.name,
                        commit_dir.name,
                        leaf.name,
                        leaf,
                        measured.allocated,
                        measured.last_use,
                        measured.apparent,
                        measured.shared,
                    )
                )
    return entries, skipped


def make_plan(
    entries: list[Entry],
    skipped: list[str],
    *,
    now: float,
    min_age_days: float,
    series: set[str],
    pinned: set[str],
    max_size_bytes: Optional[int] = None,
) -> Plan:
    """Split entries into kept (with the reason) and removable, oldest use first.

    Without ``max_size_bytes`` every removable entry is removed. With it, only the oldest removable entries
    are removed, until the cache is at or under the cap; entries in the keep set stay either way.
    """
    plan = Plan(skipped=list(skipped), total_entries=len(entries), total_bytes=sum(e.size for e in entries))
    cutoff = now - min_age_days * 86400
    removable = []
    for entry in sorted(entries, key=lambda e: e.last_use):
        if not_rebuildable(entry.source):
            plan.kept.setdefault("not rebuildable (manually uploaded)", []).append(entry)
        elif entry.last_use >= cutoff:
            plan.kept.setdefault(f"last use within {min_age_days:g} days", []).append(entry)
        elif entry.commit in series:
            plan.kept.setdefault("a point, landmark or tip of a sweep series", []).append(entry)
        elif entry.commit in pinned:
            plan.kept.setdefault("pinned commit or checked-out HEAD", []).append(entry)
        else:
            removable.append(entry)
    if max_size_bytes is None:
        plan.remove = removable
        return plan
    size = plan.total_bytes
    for entry in removable:
        if size <= max_size_bytes:
            plan.kept.setdefault("removable, but the cache is within --max-size-gb", []).append(entry)
        else:
            plan.remove.append(entry)
            size -= entry.size
    return plan


# --------------------------------------------------------------------------- deleting


def validated_entry_path(root: Path, entry: Entry) -> Path:
    """The entry's path, after checking it is exactly ``<root>/<source>/<commit>/<make_args_hash>``.

    Raises ValueError for a symlink, a name that does not match the layout, or a path that resolves anywhere
    else (including outside the cache).
    """
    if not (COMMIT_RE.match(entry.commit) and ARGS_HASH_RE.match(entry.args_hash)) or "/" in entry.source:
        raise ValueError(f"not a cache entry name: {entry.name}")
    if not_rebuildable(entry.source):
        raise ValueError(f"{entry.name} cannot be rebuilt and is never removed")
    real_root = root.resolve()
    expected = real_root / entry.source / entry.commit / entry.args_hash
    path = entry.path
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"not a plain directory: {entry.name}")
    for part in (path, path.parent, path.parent.parent):
        if part.is_symlink():
            raise ValueError(f"symlink in the path of {entry.name}")
    if path.resolve() != expected or real_root not in expected.parents:
        raise ValueError(f"{entry.name} does not resolve inside the build cache")
    return expected


def remove_entry(root: Path, entry: Entry) -> None:
    """Delete one validated entry directory, then its commit directory if that is now empty."""
    path = validated_entry_path(root, entry)
    shutil.rmtree(path)
    try:
        path.parent.rmdir()
    except OSError:
        pass  # the commit directory still holds other make-args entries


def _log(handle: IO[str], status: str, entry: Entry, detail: str = "") -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    used = datetime.fromtimestamp(entry.last_use, timezone.utc).strftime("%Y-%m-%d")
    handle.write(
        f"{stamp} {status} {entry.name} allocated={entry.size} apparent={entry.apparent} last_use={used}{(' ' + detail) if detail else ''}\n"
    )
    handle.flush()


def apply_plan(
    plan: Plan,
    root: Path,
    *,
    log_path: Path,
    is_busy: Callable[[], list[str]],
    now: float,
    min_age_days: float,
    out=sys.stdout,
) -> tuple[int, int, int]:
    """Delete the planned entries one by one. Returns (removed, allocated bytes freed, exit code).

    Stops with EXIT_BUSY when the runner stops being idle. An entry used again since the scan is left alone.
    """
    removed = 0
    gone: list[Entry] = []
    cutoff = now - min_age_days * 86400
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log:
        for index, entry in enumerate(plan.remove):
            if index % RECHECK_EVERY == 0:
                reasons = is_busy()
                if reasons:
                    print(f"stopped: the runner is busy ({'; '.join(reasons)})", file=out)
                    return removed, freed_estimate(gone), EXIT_BUSY
            try:
                measured = _measure(validated_entry_path(root, entry))
                if measured is None:
                    raise ValueError("contains a symlink")
                if measured.last_use >= cutoff:
                    _log(log, "kept-used-again", entry)
                    continue
                remove_entry(root, entry)
            except (OSError, ValueError) as exc:
                _log(log, "failed", entry, str(exc))
                print(f"not removed: {entry.name}: {exc}", file=out)
                continue
            _log(log, "removed", entry)
            removed += 1
            gone.append(entry)
    return removed, freed_estimate(gone), EXIT_OK


# --------------------------------------------------------------------------- command


def _gib(size: int) -> str:
    return f"{size / _GIB:.1f} GiB" if size >= _GIB else f"{size / (1 << 20):.1f} MiB"


def _day(stamp: float) -> str:
    return datetime.fromtimestamp(stamp, timezone.utc).strftime("%Y-%m-%d")


def _sizes(entries: list[Entry], *, freed: bool = False) -> str:
    """Apparent size (sum of file sizes) and allocated size (disk blocks, hard links once in an entry).

    With ``freed`` the allocated figure is the estimate of the space removing the entries frees.
    """
    allocated = freed_estimate(entries) if freed else sum(e.size for e in entries)
    return f"apparent {_gib(sum(e.apparent for e in entries))}, allocated {_gib(allocated)}"


def mount_options(path: Path, mountinfo: Path = Path("/proc/self/mountinfo")) -> Optional[str]:
    """Mount options of the file system that holds ``path`` (longest matching mount point), or None."""
    try:
        target = str(path.resolve())
        best, options = "", None
        for line in mountinfo.read_text().splitlines():
            fields = line.split()
            point = fields[4].replace("\\040", " ")
            if (target == point or target.startswith(point.rstrip("/") + "/")) and len(point) >= len(best):
                best, options = point, fields[5]
        return options
    except (OSError, IndexError):
        return None


def last_use_note(options: Optional[str]) -> str:
    if options is not None and "noatime" in options.split(","):
        return "newest mtime of any file in the entry. This file system is mounted noatime, so it is the build time, not a use time."
    return "newest mtime or atime of any file in the entry."


def print_plan(plan: Plan, *, apply: bool, list_all: bool = False, mount: Optional[str] = None, out=sys.stdout) -> None:
    verb = "Removing" if apply else "Would remove"
    print(f"Last use = {last_use_note(mount)}", file=out)
    allentries = [e for entries in plan.kept.values() for e in entries] + plan.remove
    print(f"Cache: {plan.total_entries} entries, {_sizes(allentries)}", file=out)
    per_source: dict[str, list[int]] = {}
    for entry in allentries:
        per_source.setdefault(entry.source, [0, 0])[0] += 1
    for entry in plan.remove:
        per_source[entry.source][1] += 1
    for source, (count, removing) in sorted(per_source.items()):
        print(f"  {source}: {count} entries, {removing} to remove", file=out)
    for reason, entries in sorted(plan.kept.items()):
        print(f"Keep ({reason}): {len(entries)} entries, {_sizes(entries)}", file=out)
    if plan.skipped:
        print(f"Skipped (not a cache entry, left alone): {len(plan.skipped)}", file=out)
        for name in plan.skipped[:LIST_LIMIT]:
            print(f"  {name}", file=out)
    print(f"{verb}: {len(plan.remove)} entries, {_sizes(plan.remove, freed=True)}", file=out)
    if plan.remove:
        oldest, newest = plan.remove[0], plan.remove[-1]
        print(f"  oldest: {oldest.name} (last use {_day(oldest.last_use)})", file=out)
        print(f"  newest: {newest.name} (last use {_day(newest.last_use)})", file=out)
        shown = plan.remove if list_all else plan.remove[:LIST_LIMIT]
        for entry in shown:
            print(f"  {entry.name}  apparent {_gib(entry.apparent)}  last use {_day(entry.last_use)}", file=out)
        if len(shown) < len(plan.remove):
            print(f"  ... and {len(plan.remove) - len(shown)} more (--list-all prints every entry)", file=out)
    print("Allocated size is disk blocks, hard links counted once; the space really freed can be lower.", file=out)


def prune(
    args: argparse.Namespace,
    *,
    now: Optional[float] = None,
    proc_root: Path = Path("/proc"),
    out=sys.stdout,
) -> int:
    now = time.time() if now is None else now
    root = Path(args.cache_dir).expanduser()
    if not root.is_dir():
        print(f"error: {root} is not a directory", file=out)
        return EXIT_ERROR
    if args.min_age_days < 0:
        print("error: --min-age-days must not be negative", file=out)
        return EXIT_ERROR
    state_dir = Path(args.state_dir)
    files = state_files(state_dir)
    if not files:
        print(f"error: no sweep state files under {state_dir}; the keep set cannot be built", file=out)
        return EXIT_ERROR
    try:
        series = series_commits(files)
    except StateFileError as exc:
        print(f"error: {exc}", file=out)
        return EXIT_ERROR

    sources = list(config.REPO_NAMES) + [config.MANUALLY_UPLOADED]
    pinned = {config.SWEEP_V3_CACHECANNON_COMMIT} | checked_out_heads(Path.home(), config.REPO_NAMES)
    entries, skipped = scan_cache(root, sources)
    cap = None if args.max_size_gb is None else int(args.max_size_gb * _GIB)
    plan = make_plan(
        entries, skipped, now=now, min_age_days=args.min_age_days, series=series, pinned=pinned, max_size_bytes=cap
    )
    print(f"Sweep state: {len(files)} files, {len(series)} commits in the keep set", file=out)
    print_plan(plan, apply=args.apply, list_all=args.list_all, mount=mount_options(root), out=out)
    if cap is not None and plan.total_bytes - sum(e.size for e in plan.remove) > cap:
        print("Note: the entries that stay are larger than --max-size-gb.", file=out)

    if not args.apply:
        print("Dry run: nothing was removed. Pause sweeps, then add --apply to remove these entries.", file=out)
        return EXIT_OK

    def is_busy() -> list[str]:
        return busy_reasons(
            queue_dir=config.CONDUCTRESS_QUEUE, journal_path=config.DELIVERY_JOURNAL_PATH, proc_root=proc_root
        )

    reasons = is_busy()
    if reasons:
        print(f"refused: the runner is busy ({'; '.join(reasons)}). Nothing was removed.", file=out)
        return EXIT_BUSY
    log_path = Path(args.log_file)
    removed, freed, code = apply_plan(
        plan, root, log_path=log_path, is_busy=is_busy, now=now, min_age_days=args.min_age_days, out=out
    )
    print(f"Removed {removed} entries, allocated {_gib(freed)}. Log: {log_path}", file=out)
    return code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="conductress build-cache", description="Manage the runner's build cache")
    sub = parser.add_subparsers(dest="build_cache_command")
    prune_parser = sub.add_parser(
        "prune",
        help="Remove old build cache entries. Dry run unless --apply is given. Run it on the runner host by hand.",
    )
    prune_parser.add_argument("--apply", action="store_true", help="Remove the entries. Without it nothing is removed.")
    prune_parser.add_argument(
        "--min-age-days",
        type=float,
        default=DEFAULT_MIN_AGE_DAYS,
        help=f"Keep entries used within this many days (default {DEFAULT_MIN_AGE_DAYS})",
    )
    prune_parser.add_argument(
        "--max-size-gb",
        type=float,
        default=None,
        help="Remove only the oldest removable entries, until the cache is at or under this size",
    )
    prune_parser.add_argument(
        "--list-all", action="store_true", help="Print every entry that would be removed, not only the first 20"
    )
    prune_parser.add_argument(
        "--cache-dir", default="~/build_cache", help="Build cache directory (default ~/build_cache)"
    )
    prune_parser.add_argument(
        "--state-dir", default=str(config.SWEEP_STATE_DIR), help="Sweep state directory (default: the configured one)"
    )
    prune_parser.add_argument(
        "--log-file",
        default=str(config.PROJECT_ROOT / "build_cache_prune.log"),
        help="Append one line per removed entry here",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.build_cache_command == "prune":
        return prune(args)
    parser.print_usage()
    return EXIT_ERROR
