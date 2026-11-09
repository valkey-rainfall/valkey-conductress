"""public_fields: what a public dashboard file may say about a runner's environment."""

import json
import re

from conductress import public_fields as pf

SHA_A = "5466d250765a099f6834ae56acfca7c5950213c4"
SHA_B = "9ba063dee3d204aa4c1ec6d416e40cec8dea806c"

PRIVATE_ENV = {
    "provenance_schema_version": 2,
    "kernel_release": "6.1.189-236.400.amzn2023.x86_64",
    "glibc_version": "2.34",
    "cpu_microcode": "0x2b000685",
    "cpu_model": "Intel(R) Xeon(R) Platinum 8488C",
    "machine": "x86_64",
    "platform": "intel/xeon-8488c/sapphire-rapids",
    "runner_id": "intelbench",
    "conductress_revision": SHA_B,
    "cachecannon_commit": "31a2befaa8bf7b3b7a7f7e03e2a847a2e407a3ce",
    "cachecannon_dirty": False,
    "boot_id": "86bc23db-f6bc-49fa-82b7-f48d801a298b",
    "instance_id": "i-0690c81234ab4ecb8",
    "host_fingerprint": "c4c46be36d50f57d",
    "packages": {"glibc": "2.34-231.0.5.amzn2023", "kernel": "6.1.189-236.400.amzn2023"},
    "server_kernel_release": "6.18.56",
    "server_boot_id": "3024bc08-85b7-4046-918b-67ae3ff77c13",
}

# Shapes a public file must never contain (the live-file grep after a deploy
# uses the same patterns).
PRIVATE_SHAPES = {
    "40-hex sha": re.compile(r"\b[0-9a-f]{40}\b"),
    "instance id": re.compile(r"\bi-[0-9a-f]{8,17}\b"),
    "ipv4": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "uuid": re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"),
    "path": re.compile(r"(?:/home|/etc|/opt|/var)/"),
}


def private_shapes_in(document, allowed_values=()):
    """Names of the private shapes found in a JSON document, ignoring allowed values."""
    text = json.dumps(document, sort_keys=True)
    for value in allowed_values:
        text = text.replace(value, "")
    return sorted(name for name, pattern in PRIVATE_SHAPES.items() if pattern.search(text))


class TestPublicEnvironment:
    def test_keeps_versions_and_drops_identifiers(self):
        public = pf.public_environment(PRIVATE_ENV)
        assert public == {
            "cachecannon_dirty": False,
            "cpu_microcode": "0x2b000685",
            "cpu_model": "Intel(R) Xeon(R) Platinum 8488C",
            "glibc_version": "2.34",
            "kernel_release": "6.1.189-236.400.amzn2023.x86_64",
            "machine": "x86_64",
            "packages": {"glibc": "2.34-231.0.5.amzn2023", "kernel": "6.1.189-236.400.amzn2023"},
            "platform": "intel/xeon-8488c/sapphire-rapids",
            "provenance_schema_version": 2,
            "server_kernel_release": "6.18.56",
        }
        assert private_shapes_in(public) == []

    def test_none_and_unknown_keys(self):
        assert pf.public_environment(None) is None
        assert pf.public_environment({"brand_new_secret": "x", "kernel_release": "6.1"}) == {"kernel_release": "6.1"}


class TestPublicChangeLabel:
    def test_versions_keep_values_private_fields_become_types(self):
        changes = [
            f"boot_id: 'a' -> 'b'",
            f"conductress_revision: '{SHA_A}' -> '{SHA_B}'",
            "cachecannon_commit: 'aaaa' -> 'bbbb'",
            "glibc_version: '2.34' -> '2.35'",
            "host_fingerprint: 'c4c46be36d50f57d' -> 'ffff'",
            "instance_id: 'i-0690c81234ab4ecb8' -> 'i-0fedcba9876543210'",
            "kernel_release: '6.1.170-213' -> '6.1.189-236'",
            "packages.glibc: '2.34-231.0.4' -> '2.34-231.0.5'",
            "some_new_field: 'x' -> 'y'",
            "event: BIOS update",
        ]
        label = pf.public_change_label(changes)
        assert label == (
            "reboot; harness revision changed; load generator changed; glibc_version: 2.34 -> 2.35; "
            "host replaced; kernel_release: 6.1.170-213 -> 6.1.189-236; packages.glibc: 2.34-231.0.4 -> 2.34-231.0.5; "
            "some_new_field changed; BIOS update"
        )
        assert private_shapes_in(label) == []

    def test_annotation_string_round_trip(self):
        stored = f"conductress_revision: '{SHA_A}' -> '{SHA_B}'; kernel_release: '6.1.170' -> '6.1.189'"
        assert pf.public_annotation(stored) == "harness revision changed; kernel_release: 6.1.170 -> 6.1.189"
        assert pf.public_annotation(None) is None and pf.public_annotation("") == ""
        assert pf.public_change_label([]) == ""


class TestPublicGeneration:
    def test_drops_the_operator_name(self):
        generation = {"id": "2026-11-06", "epoch": "v3", "label": "kernel 6.1.170 -> 6.1.189", "closed_by": "rain"}
        assert pf.public_generation(generation) == {
            "epoch": "v3",
            "id": "2026-11-06",
            "label": "kernel 6.1.170 -> 6.1.189",
        }


def _observation(**extra):
    base = {
        "runner_id": "intelbench",
        "utc_date": "2026-11-09",
        "score": 686477.2,
        "ref_median": 689952.4,
        "delta_pct": -0.5,
        "candidate_signal": "within",
        "phase": "observation",
        "task_id": "canary:intelbench:throughput-get-v1:2026-11-09",
        "env_change_annotation": f"conductress_revision: '{SHA_A}' -> '{SHA_B}'",
        "environment": dict(PRIVATE_ENV),
        "environment_json": json.dumps(PRIVATE_ENV),
    }
    base.update(extra)
    return base


def _entry():
    return {
        "runner_id": "intelbench",
        "display_name": "Intel Sapphire Rapids",
        "platform": "intel/xeon-8488c/sapphire-rapids",
        "enabled": True,
        "canary_profile": "throughput-get-v1",
        "archived": False,
        "archives": [{"runner_id": "intelbench-pre-2026-11-07", "archived_until": "2026-11-06", "archive_note": "n"}],
        "profile": {"profile_id": "throughput-get-v1", "pinned_commit": "fcd8bc3ee40f5d7841b7d5a8f3cd12252fec14e4"},
        "schedule": [{"utc_date": "2026-11-09", "state": "created", "task_state": "completed"}],
        "series": {"runner_id": "intelbench", "phase": "observation", "latest_observation": _observation()},
        "latest_observation": _observation(),
        "calibration_report": {"status": "ready-for-review", "median_score": 1.0, "runner_id": "intelbench"},
        "semantics": "observation-only",
    }


class TestPublicCanaryStatus:
    def test_public_files_carry_no_private_shapes(self):
        status = {"generated_at": "2026-11-09T07:00:00Z", "runners": [_entry()]}
        public = pf.public_canary_status(status)
        # The pinned canary commit is a public release commit and may stay.
        assert private_shapes_in(public, allowed_values=["fcd8bc3ee40f5d7841b7d5a8f3cd12252fec14e4"]) == []
        runner = public["runners"][0]
        latest = runner["latest_observation"]
        assert latest["env_change_annotation"] == "harness revision changed"
        assert "environment_json" not in runner["series"]["latest_observation"]
        assert runner["series"]["latest_observation"]["env_change_annotation"] == "harness revision changed"
        assert "boot_id" not in latest["environment"] and latest["environment"]["kernel_release"].startswith("6.1.189")
        # What the dashboard reads is still there.
        assert latest["candidate_signal"] == "within" and latest["delta_pct"] == -0.5 and latest["utc_date"]
        assert runner["series"]["phase"] == "observation" and runner["archives"][0]["runner_id"]

    def test_unknown_keys_are_dropped_and_reported(self):
        entry = _entry()
        entry["operator_login"] = "rain"
        entry["latest_observation"]["raw_row"] = {"host": "10.0.0.1"}
        entry["series"]["latest_observation"]["db_path"] = "/var/lib/x.db"
        status = {"generated_at": "t", "runners": [entry], "debug": {"pid": 1}}
        public = pf.public_canary_status(status)
        assert "debug" not in public and "operator_login" not in public["runners"][0]
        assert "raw_row" not in public["runners"][0]["latest_observation"]
        assert private_shapes_in(public, allowed_values=["fcd8bc3ee40f5d7841b7d5a8f3cd12252fec14e4"]) == []
        assert pf.unknown_canary_keys(status) == [
            "status.debug",
            "runners[intelbench].operator_login",
            "runners[intelbench].series.latest_observation.db_path",
            "runners[intelbench].latest_observation.raw_row",
        ]

    def test_every_known_key_is_classified(self):
        assert pf.unknown_canary_keys({"generated_at": "t", "runners": [_entry()]}) == []


class TestFieldClassification:
    def test_public_value_key(self):
        assert pf.public_value_key("kernel_release") and pf.public_value_key("packages.tuned")
        assert pf.public_value_key("server_packages.glibc")
        assert not pf.public_value_key("conductress_revision") and not pf.public_value_key("boot_id")

    def test_change_type_label(self):
        assert pf.change_type_label("conductress_revision") == "harness revision changed"
        assert pf.change_type_label("cachecannon_sha256") == "load generator changed"
        assert pf.change_type_label("server_boot_id") == "server reboot"
        assert pf.change_type_label("brand_new") == "brand_new changed"


class TestPublicText:
    def test_withholds_private_shapes(self):
        for text in (SHA_A, "on 10.0.0.1", "see /home/someone/x", "mail a@b.example", "i-0690c81234ab4ecb8"):
            assert pf.public_text(f"note {text}") == pf.WITHHELD
        assert pf.private_detail("plain words") is None

    def test_normalises_and_cuts(self):
        assert pf.public_text("  two   words ") == "two words"
        assert pf.public_text("") is None and pf.public_text(None) is None
        assert len(pf.public_text("x" * 500)) == 300

    def test_check_public_text_names_the_problem(self):
        import pytest

        assert pf.check_public_text(" a  b ", "label") == "a b"
        with pytest.raises(ValueError, match="IPv4"):
            pf.check_public_text("on 10.0.0.1", "label")
        with pytest.raises(ValueError, match="empty"):
            pf.check_public_text(" ", "label")
        with pytest.raises(ValueError, match="longer"):
            pf.check_public_text("x" * 301, "label")

    def test_generation_free_text_is_checked(self):
        generation = {"id": "g", "label": "moved to 10.0.0.9", "reason": "canary step", "closed_by": "someone"}
        assert pf.public_generation(generation) == {"id": "g", "label": pf.WITHHELD, "reason": "canary step"}
