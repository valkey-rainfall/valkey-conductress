from unittest.mock import MagicMock, patch

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
