"""Unit tests for per-platform manifest export."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def tmp_path():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def test_manifest_splits_throughput_and_memory(tmp_path):
    """Manifest separates throughput, memory, and latency workloads by metric_id."""
    from conductress.sweep.exporter import export_manifest

    workloads = [
        ("get-k16-v16-t7-p10", "throughput"),
        ("set-k16-v128-t7-p1", "throughput"),
        ("memory-set-k16-v64", "memory"),
        ("redis-get-k16-v16-t7-p10", "throughput"),
        ("get-k16-v16", "latency"),
    ]
    with patch("conductress.publisher.detect_platform", return_value=("arm64", "arm64/test")):
        export_manifest(tmp_path, platforms=["arm64", "amd64", "intel"], workloads=workloads)

    manifest_file = tmp_path / "manifest-arm64.json"
    assert manifest_file.exists()
    data = json.loads(manifest_file.read_text())

    assert data["version"] == 4
    assert data["epoch"] == "v1"
    assert [epoch["id"] for epoch in data["epochs"]] == ["v1"]
    assert data["platform"] == "arm64"
    assert data["throughput_workloads"] == [
        "get-k16-v16-t7-p10",
        "set-k16-v128-t7-p1",
        "redis-get-k16-v16-t7-p10",
    ]
    assert data["memory_workloads"] == ["memory-set-k16-v64"]
    assert data["latency_workloads"] == ["get-k16-v16"]


def test_manifest_uses_detected_platform_in_filename(tmp_path):
    """Manifest filename reflects the detected platform."""
    from conductress.sweep.exporter import export_manifest

    with patch("conductress.publisher.detect_platform", return_value=("intel", "intel/xeon")):
        export_manifest(
            tmp_path, platforms=["arm64", "amd64", "intel"], workloads=[("get-k16-v16-t7-p10", "throughput")]
        )

    assert (tmp_path / "manifest-intel.json").exists()
    assert not (tmp_path / "manifest-arm64.json").exists()


def test_manifest_empty_workloads(tmp_path):
    """Manifest handles empty workload list gracefully."""
    from conductress.sweep.exporter import export_manifest

    with patch("conductress.publisher.detect_platform", return_value=("amd64", "amd64/epyc")):
        export_manifest(tmp_path, platforms=["amd64"], workloads=[])

    data = json.loads((tmp_path / "manifest-amd64.json").read_text())
    assert data["throughput_workloads"] == []
    assert data["memory_workloads"] == []
    assert data["latency_workloads"] == []
