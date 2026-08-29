"""Unit tests for platform detection."""

from unittest.mock import mock_open, patch

from conductress.platform import aarch64_platform_id, get_local_platform_tag

_G4_CPUINFO = "processor\t: 0\nCPU implementer\t: 0x41\nCPU part\t: 0xd4f\n"
_G3_CPUINFO = "processor\t: 0\nCPU implementer\t: 0x41\nCPU part\t: 0xd40\n"


@patch("platform.machine", return_value="aarch64")
def test_graviton3_detection(mock_machine):
    # Neoverse-V1 (0xd40) stays 'arm64' to preserve armbench history
    with patch("builtins.open", mock_open(read_data=_G3_CPUINFO)):
        assert get_local_platform_tag() == "arm64"


@patch("platform.machine", return_value="aarch64")
def test_graviton4_detection(mock_machine):
    # Neoverse-V2 (0xd4f) -> distinct 'graviton4' platform
    with patch("builtins.open", mock_open(read_data=_G4_CPUINFO)):
        assert get_local_platform_tag() == "graviton4"


def test_aarch64_platform_id_g4():
    with patch("builtins.open", mock_open(read_data=_G4_CPUINFO)):
        assert aarch64_platform_id() == "graviton4"


def test_aarch64_platform_id_g3():
    with patch("builtins.open", mock_open(read_data=_G3_CPUINFO)):
        assert aarch64_platform_id() == "arm64"


def test_aarch64_platform_id_no_cpuinfo():
    # Missing /proc/cpuinfo (e.g. non-linux) falls back to arm64
    with patch("builtins.open", side_effect=OSError):
        assert aarch64_platform_id() == "arm64"


@patch("platform.machine", return_value="aarch64")
def test_arm64_detection(mock_machine):
    assert get_local_platform_tag() == "arm64"


@patch("platform.machine", return_value="x86_64")
def test_intel_detection(mock_machine):
    cpuinfo = "vendor_id\t: GenuineIntel\nmodel name\t: Intel Xeon\n"
    with patch("builtins.open", mock_open(read_data=cpuinfo)):
        assert get_local_platform_tag() == "intel"


@patch("platform.machine", return_value="x86_64")
def test_amd_detection(mock_machine):
    cpuinfo = "vendor_id\t: AuthenticAMD\nmodel name\t: AMD EPYC\n"
    with patch("builtins.open", mock_open(read_data=cpuinfo)):
        assert get_local_platform_tag() == "amd64"


@patch("platform.machine", return_value="x86_64")
def test_x86_fallback_to_amd(mock_machine):
    with patch("builtins.open", side_effect=OSError):
        assert get_local_platform_tag() == "amd64"


def test_local_platform_info_graviton3():
    with patch("conductress.platform.get_local_platform_tag", return_value="arm64"):
        from conductress.platform import get_local_platform_info

        assert get_local_platform_info() == (
            "arm64",
            "arm64/c7g.metal/graviton3",
            ["graviton3", "arm64"],
        )


def test_local_platform_info_intel():
    with patch("conductress.platform.get_local_platform_tag", return_value="intel"):
        from conductress.platform import get_local_platform_info

        assert get_local_platform_info() == (
            "intel",
            "intel/xeon-8488c/sapphire-rapids",
            ["intel"],
        )
