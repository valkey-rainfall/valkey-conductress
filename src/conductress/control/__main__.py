"""Command-line entry point for the fleet control service."""

import argparse
import getpass

from aiohttp import web

from .app import create_app
from .auth import hash_token
from .config import ControlConfig


def main() -> None:
    parser = argparse.ArgumentParser(prog="conductress-control")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="Run the localhost-only control service")
    serve.add_argument("--port", type=int, default=8390)
    subparsers.add_parser("hash-token", help="Read a token securely and print its SHA-256 hash")
    args = parser.parse_args()

    if args.command == "hash-token":
        token = getpass.getpass("Token: ")
        if not token:
            parser.error("token must not be empty")
        print(hash_token(token))
        return

    config = ControlConfig.from_env()
    # Deliberately hardcoded: the reverse proxy is the only network listener.
    web.run_app(create_app(config), host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
