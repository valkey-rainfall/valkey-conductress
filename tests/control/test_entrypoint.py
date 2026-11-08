from unittest.mock import MagicMock, patch

import pytest

from conductress.control.__main__ import main


def test_serve_is_hard_bound_to_localhost(monkeypatch):
    monkeypatch.setattr("sys.argv", ["conductress-control", "serve", "--port", "9001"])
    config = MagicMock()
    app = MagicMock()
    with (
        patch("conductress.control.__main__.ControlConfig.from_env", return_value=config),
        patch("conductress.control.__main__.create_app", return_value=app),
        patch("conductress.control.__main__.web.run_app") as run_app,
    ):
        main()

    run_app.assert_called_once_with(app, host="127.0.0.1", port=9001)


def test_hash_token_reads_secret_without_command_line(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["conductress-control", "hash-token"])
    with patch("conductress.control.__main__.getpass.getpass", return_value="secret"):
        main()

    assert capsys.readouterr().out.strip() == ("2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b")


def test_body_limit_default_is_four_mib_and_env_overrides(monkeypatch, tmp_path):
    from conductress.control.config import DEFAULT_MAX_BODY_BYTES, ControlConfig

    for name in ("MAX_BODY_BYTES", "PUBLISHED_TASKS_DIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CONTROL_DB_PATH", str(tmp_path / "control.db"))
    assert DEFAULT_MAX_BODY_BYTES == 4 * 1024 * 1024
    assert ControlConfig.from_env().max_body_bytes == DEFAULT_MAX_BODY_BYTES
    monkeypatch.setenv("MAX_BODY_BYTES", "65536")
    assert ControlConfig.from_env().max_body_bytes == 65536


def test_archive_runner_subcommand_dry_run(monkeypatch, tmp_path, capsys):
    import json

    from conductress.control import __main__ as entry
    from conductress.control.db import ControlDatabase

    from .helpers import fleet_manifest

    manifest = tmp_path / "fleet.json"
    manifest.write_text(json.dumps(fleet_manifest()), encoding="utf-8")
    db = ControlDatabase(tmp_path / "control.db", tmp_path / "audit.jsonl")
    db.initialize()
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO canary_observations(runner_id, profile_id, profile_version, utc_date, task_id, score, "
            "phase, accepted, created_at) VALUES ('armbench', 'p', 1, '2026-09-01', 't1', 1.0, 'observation', 1, 'now')"
        )
    monkeypatch.setenv("CONTROL_DB_PATH", str(tmp_path / "control.db"))
    monkeypatch.setenv("FLEET_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("TOKENS_PATH", str(tmp_path / "tokens.json"))
    monkeypatch.setenv("AUDIT_JSONL_PATH", str(tmp_path / "audit.jsonl"))
    monkeypatch.setattr(entry, "_service_is_active", lambda *_: True)  # dry run never asks

    monkeypatch.setattr(
        "sys.argv",
        [
            "conductress-control",
            "archive-runner",
            "--runner",
            "armbench",
            "--archive-id",
            "armbench-pre",
            "--until",
            "2026-09-01",
            "--note",
            "test",
            "--dry-run",
        ],
    )
    entry.main()
    out = capsys.readouterr().out
    assert "Would rename 1 row(s) in canary_observations" in out
    assert '"archive_of": "armbench"' in out
    # Nothing moved.
    conn = db.connect()
    assert conn.execute("SELECT COUNT(*) FROM canary_observations WHERE runner_id='armbench'").fetchone()[0] == 1
    conn.close()

    # A live service blocks the real run unless forced.
    monkeypatch.setattr(
        "sys.argv",
        [
            "conductress-control",
            "archive-runner",
            "--runner",
            "armbench",
            "--archive-id",
            "armbench-pre",
            "--until",
            "2026-09-01",
            "--note",
            "test",
        ],
    )
    with pytest.raises(SystemExit):
        entry.main()
    assert "is active" in capsys.readouterr().err
