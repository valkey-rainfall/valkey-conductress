"""Dry-run a control database migration on a copy and report what changed.

``conductress-control check-migration PATH`` never opens PATH for writing. It
copies the file with the SQLite backup API into a temporary directory, records
every table (row count and a content digest), runs the normal start-up
migration on the copy, and records them again. It then runs the migration a
second time. The check passes when:

- the SQLite integrity check and the foreign key check are clean before and after;
- every table that existed before has the same rows afterwards (``schema_migrations``
  may only gain rows);
- a second migration run changes nothing;
- the schema version is the current one.

It prints counts and digests only, never row values.
"""

from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .db import DATABASE_SCHEMA_VERSION, ControlDatabase


@dataclass
class Snapshot:
    version: int
    counts: dict[str, int]
    digests: dict[str, str]
    integrity: str
    foreign_key_problems: int


@dataclass
class MigrationReport:
    before: Snapshot
    after: Snapshot
    second_run_changed: list[str] = field(default_factory=list)
    migration_rows_kept: bool = True
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems

    def lines(self) -> list[str]:
        out = [f"schema version: {self.before.version} -> {self.after.version}"]
        out.append(f"integrity_check: before={self.before.integrity} after={self.after.integrity}")
        out.append(
            f"foreign_key_check problems: before={self.before.foreign_key_problems} "
            f"after={self.after.foreign_key_problems}"
        )
        out.append(f"{'table':<28}{'rows before':>12}{'rows after':>12}  content")
        for table in sorted(set(self.before.counts) | set(self.after.counts)):
            was = self.before.counts.get(table)
            now = self.after.counts.get(table)
            if was is None:
                content = "new table"
            elif self.before.digests[table] == self.after.digests.get(table):
                content = "unchanged"
            elif table == "schema_migrations" and self.migration_rows_kept:
                content = "old rows kept, new rows added"
            else:
                content = "CHANGED"
            out.append(f"{table:<28}{'-' if was is None else was:>12}{'-' if now is None else now:>12}  {content}")
        out.append(f"second run changed: {', '.join(self.second_run_changed) or 'nothing'}")
        out.extend(f"PROBLEM: {problem}" for problem in self.problems)
        out.append("RESULT: " + ("ok" if self.ok else "FAILED"))
        return out


def _table_names(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return sorted(row[0] for row in rows)


def table_digest(connection: sqlite3.Connection, table: str, *, where: str = "") -> str:
    """SHA-256 over all rows of a table in rowid order. Table names come from sqlite_master, not from input."""
    digest = hashlib.sha256()
    for row in connection.execute(f'SELECT * FROM "{table}" {where} ORDER BY rowid'):
        digest.update(repr(tuple(row)).encode("utf-8"))
    return digest.hexdigest()


def snapshot(path: Path) -> Snapshot:
    connection = sqlite3.connect(path)
    try:
        tables = _table_names(connection)
        counts = {t: connection.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}
        digests = {t: table_digest(connection, t) for t in tables}
        version = 0
        if "schema_migrations" in tables:
            version = connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0]
        integrity = "; ".join(row[0] for row in connection.execute("PRAGMA integrity_check"))
        problems = len(connection.execute("PRAGMA foreign_key_check").fetchall())
    finally:
        connection.close()
    return Snapshot(version, counts, digests, integrity, problems)


def _changed_tables(before: Snapshot, after: Snapshot) -> list[str]:
    return sorted(t for t, digest in before.digests.items() if after.digests.get(t) != digest)


def _copy(source: Path, target: Path) -> None:
    reader = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    writer = sqlite3.connect(target)
    try:
        reader.backup(writer)
    finally:
        writer.close()
        reader.close()


def _migrate(path: Path) -> None:
    ControlDatabase(path, path.with_suffix(".audit.jsonl")).initialize()


def check_migration(source: Path, *, work_dir: Optional[Path] = None) -> MigrationReport:
    if not source.is_file():
        raise FileNotFoundError(f"database not found: {source}")
    with tempfile.TemporaryDirectory(dir=work_dir) as tmp:
        copy = Path(tmp) / "copy.db"
        _copy(source, copy)
        before = snapshot(copy)
        old_migration_rows = _migration_rows(copy)
        _migrate(copy)
        after = snapshot(copy)
        kept_migration_rows = _migration_rows(copy)[: len(old_migration_rows)]
        _migrate(copy)
        again = snapshot(copy)
    report = MigrationReport(before=before, after=after)
    report.second_run_changed = _changed_tables(after, again)
    if before.version > DATABASE_SCHEMA_VERSION:
        report.problems.append(f"database schema {before.version} is newer than this code ({DATABASE_SCHEMA_VERSION})")
    if after.version != DATABASE_SCHEMA_VERSION:
        report.problems.append(f"version after migration is {after.version}, expected {DATABASE_SCHEMA_VERSION}")
    for name, snap in (("before", before), ("after", after)):
        if snap.integrity != "ok":
            report.problems.append(f"integrity_check {name}: {snap.integrity}")
        if snap.foreign_key_problems:
            report.problems.append(f"foreign_key_check {name}: {snap.foreign_key_problems} problem(s)")
    changed = [t for t in _changed_tables(before, after) if t != "schema_migrations"]
    if changed:
        report.problems.append(f"existing rows changed in: {', '.join(changed)}")
    report.migration_rows_kept = kept_migration_rows == old_migration_rows
    if not report.migration_rows_kept:
        report.problems.append("existing schema_migrations rows changed")
    if report.second_run_changed:
        report.problems.append("a second migration run changed data")
    return report


def _migration_rows(path: Path) -> list[tuple]:
    connection = sqlite3.connect(path)
    try:
        return [
            tuple(r) for r in connection.execute("SELECT version, applied_at FROM schema_migrations ORDER BY version")
        ]
    except sqlite3.OperationalError:
        return []
    finally:
        connection.close()
