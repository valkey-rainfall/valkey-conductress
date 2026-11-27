#!/usr/bin/env python3
"""A tiny stand-in for a Redis-protocol server, for tests. It is not an engine and measures nothing real.

Speaks just the commands a Conductress memory task sends: PING, INFO, ZADD, SADD, HSET, SET, the read commands
the settle's read pass uses, ZCARD, SCARD, HLEN, DBSIZE, CONFIG GET and SHUTDOWN. DEBUG, OBJECT and MEMORY answer an
error, like an engine that lacks them. Memory is a made-up function of the item count: used_memory is
1,000,000 + 40 bytes per item, so a test can check the per-item arithmetic exactly.

usage: fake_engine.py --port N [any other flags, ignored]
"""

import asyncio
import os
import sys

BASE_MEMORY = 1_000_000
BYTES_PER_ITEM = 40


class Store:
    def __init__(self):
        self.data = {}  # key -> dict (zset, set, hash) or str

    def items(self):
        total = 0
        for value in self.data.values():
            total += len(value) if isinstance(value, dict) else 1
        return total


def simple(text):
    return b"+" + text.encode() + b"\r\n"


def error(text):
    return b"-" + text.encode() + b"\r\n"


def integer(number):
    return b":" + str(number).encode() + b"\r\n"


def bulk(value):
    if value is None:
        return b"$-1\r\n"
    raw = value if isinstance(value, bytes) else str(value).encode()
    return b"$" + str(len(raw)).encode() + b"\r\n" + raw + b"\r\n"


def info(store, section):
    sections = {
        "server": "# Server\r\ndragonfly_version:df-fake\r\nredis_mode:standalone\r\n",
        "memory": f"# Memory\r\nused_memory:{BASE_MEMORY + BYTES_PER_ITEM * store.items()}\r\n",
        "keyspace": "# Keyspace\r\n" + (f"db0:keys={len(store.data)},expires=0,avg_ttl=0\r\n" if store.data else ""),
    }
    return sections.get(section.lower(), "")


def execute(store, args):
    name = args[0].decode().upper()
    if name == "PING":
        return simple("PONG")
    if name == "INFO":
        return bulk(info(store, args[1].decode() if len(args) > 1 else "server"))
    if name == "SET":
        store.data[args[1]] = args[2]
        return simple("OK")
    if name == "EXISTS":
        return integer(1 if args[1] in store.data else 0)
    if name in ("ZADD", "HSET"):
        table = store.data.setdefault(args[1], {})
        added = 0
        pairs = args[2:]
        for i in range(0, len(pairs) - 1, 2):
            member, other = (pairs[i + 1], pairs[i]) if name == "ZADD" else (pairs[i], pairs[i + 1])
            added += member not in table
            table[member] = other
        return integer(added)
    if name == "SADD":
        table = store.data.setdefault(args[1], {})
        added = sum(1 for member in args[2:] if member not in table)
        table.update({member: b"" for member in args[2:]})
        return integer(added)
    if name == "ZSCORE":
        return bulk(store.data.get(args[1], {}).get(args[2]))
    if name in ("SISMEMBER", "HEXISTS"):
        return integer(1 if args[2] in store.data.get(args[1], {}) else 0)
    if name in ("ZCARD", "SCARD", "HLEN"):
        return integer(len(store.data.get(args[1], {})))
    if name == "DBSIZE":
        return integer(len(store.data))
    if name == "CONFIG":
        return b"*0\r\n"
    return error(f"ERR unknown command '{args[0].decode()}'")


async def read_command(reader):
    line = await reader.readline()
    if not line:
        return None
    count = int(line[1:])
    args = []
    for _ in range(count):
        size = int((await reader.readline())[1:])
        args.append((await reader.readexactly(size + 2))[:-2])
    return args


def main(argv):
    port = int(argv[argv.index("--port") + 1])
    store = Store()

    async def serve(reader, writer):
        while True:
            args = await read_command(reader)
            if args is None:
                break
            if args[0].upper() == b"SHUTDOWN":
                os._exit(0)
            writer.write(execute(store, args))
            await writer.drain()

    async def run():
        server = await asyncio.start_server(serve, "127.0.0.1", port)
        async with server:
            await server.serve_forever()

    asyncio.run(run())


if __name__ == "__main__":
    main(sys.argv[1:])
