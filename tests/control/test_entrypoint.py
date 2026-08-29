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
