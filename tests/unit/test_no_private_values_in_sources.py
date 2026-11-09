"""The repository is public: tests and docs must not carry values copied from the fleet.

A fixture copied from a live record carries the real thing (a VPC address, an
EC2 instance id, a boot id). This scan fails on the shapes such a copy has and
accepts the documented placeholders, so a fixture is made up before it lands:

- any address in 172.31.0.0/16 (the fleet's VPC range);
- an address in 10.0.0.0/8 unless it is in 10.0.0.0/16, the placeholder block
  the existing tests use (10.0.0.1, 10.0.0.42, ...); 198.51.100.0/24 (TEST-NET-2)
  is always fine;
- an EC2 instance id other than the two placeholders;
- a UUID (boot ids) unless every group is one repeated character
  (11111111-2222-3333-4444-555555555555, aaaaaaaa-bbbb-...);
- the names of private build lines.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCANNED_DIRS = ("tests", "docs")
SCANNED_SUFFIXES = {".py", ".md", ".json", ".toml", ".txt", ".yml", ".yaml", ".cfg", ".ini"}

PLACEHOLDER_INSTANCE_IDS = {"i-0123456789abcdef0", "i-0fedcba9876543210"}
# The hex-digit sequence, an obvious placeholder (tests/unit/test_confirmation.py).
PLACEHOLDER_UUIDS = {"01234567-89ab-cdef-0123-456789abcdef"}
PRIVATE_WORDS = ("superset", "frontier")

IPV4 = re.compile(r"\b(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\b")
INSTANCE_ID = re.compile(r"\bi-[0-9a-f]{8,17}\b")
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
PRIVATE_WORD = re.compile("|".join(PRIVATE_WORDS), re.IGNORECASE)


def _is_placeholder_uuid(value: str) -> bool:
    return value in PLACEHOLDER_UUIDS or all(len(set(group)) == 1 for group in value.split("-"))


def private_values_in(text: str) -> list[str]:
    """Human-readable findings for one file's text; empty when the text is clean."""
    findings: list[str] = []
    for match in IPV4.finditer(text):
        first, second = int(match.group(1)), int(match.group(2))
        if first == 172 and second == 31:
            findings.append(f"VPC address {match.group(0)}")
        elif first == 10 and second != 0:
            findings.append(f"private address {match.group(0)} (placeholders live in 10.0.0.0/16)")
    for match in INSTANCE_ID.finditer(text):
        if match.group(0) not in PLACEHOLDER_INSTANCE_IDS:
            findings.append(f"instance id {match.group(0)} (use {sorted(PLACEHOLDER_INSTANCE_IDS)[0]})")
    for match in UUID.finditer(text):
        if not _is_placeholder_uuid(match.group(0)):
            findings.append(f"uuid {match.group(0)} (use repeated groups, e.g. 11111111-2222-3333-4444-555555555555)")
    for match in PRIVATE_WORD.finditer(text):
        findings.append(f"private word {match.group(0)!r}")
    return findings


def _scanned_files() -> list[Path]:
    files: list[Path] = []
    for directory in SCANNED_DIRS:
        for path in sorted((REPO_ROOT / directory).rglob("*")):
            if path.is_file() and path.suffix in SCANNED_SUFFIXES and "__pycache__" not in path.parts:
                files.append(path)
    return files


def test_tests_and_docs_carry_no_values_copied_from_the_fleet():
    me = Path(__file__).resolve()
    offenders: dict[str, list[str]] = {}
    for path in _scanned_files():
        if path == me:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        hits = private_values_in(text)
        if hits:
            offenders[str(path.relative_to(REPO_ROOT))] = sorted(set(hits))
    assert offenders == {}, "\n".join(f"{file}: {'; '.join(hits)}" for file, hits in offenders.items())


def test_scan_rules_on_samples():
    assert private_values_in("host 198.51.100.44, 10.0.0.1, 10.0.3.9, 127.0.0.1, i-0123456789abcdef0") == []
    assert private_values_in("11111111-2222-3333-4444-555555555555 aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") == []
    assert private_values_in("ip 172.31.5.6") == ["VPC address 172.31.5.6"]
    assert private_values_in("ip 10.20.30.40")[0].startswith("private address 10.20.30.40")
    assert private_values_in("id i-0abcdef1234567890")[0].startswith("instance id i-0abcdef1234567890")
    assert private_values_in("boot 12345678-1234-1234-1234-123456789abc")[0].startswith("uuid 12345678-")
    assert private_values_in("the Superset build") == ["private word 'Superset'"]
