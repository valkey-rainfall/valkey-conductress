"""Keyspace counting: the shared parser keeps its old answers and tolerates other engines' fields."""

from collections import defaultdict
from unittest.mock import AsyncMock

import pytest

from conductress.server import Server, keyspace_counts


def _old_count(info):
    """The parser Server.count_items_expires used before keyspace_counts existed."""
    counts = defaultdict(int)
    for line in info.values():
        for item in line.split(","):
            name, val = item.split("=")
            counts[name] += int(val)
    return (counts["keys"], counts["expires"])


@pytest.mark.parametrize(
    "info",
    [
        {},
        {"db0": "keys=98331,expires=0,avg_ttl=0"},
        {"db0": "keys=10,expires=2,avg_ttl=500", "db1": "keys=5,expires=5,avg_ttl=0"},
        {"db0": "keys=7,expires=0,avg_ttl=0,subexpiry=0"},
    ],
)
def test_valid_keyspace_gives_the_old_answer(info):
    assert keyspace_counts(info) == _old_count(info)


def test_unknown_and_broken_pairs_are_skipped():
    info = {"db0": "keys=5,expires=1,avg_ttl=0,extra=abc,novalue,=3,keys2=x=y,subexpiry=9"}
    assert keyspace_counts(info) == (5, 1)


@pytest.mark.asyncio
async def test_count_items_expires_uses_the_keyspace_section():
    server = Server.__new__(Server)
    server.info = AsyncMock(return_value={"db0": "keys=42,expires=3,avg_ttl=0"})
    server.run_valkey_command = AsyncMock()
    assert await server.count_items_expires() == (42, 3)
    server.run_valkey_command.assert_not_called()


@pytest.mark.asyncio
async def test_an_empty_keyspace_section_falls_back_to_dbsize():
    server = Server.__new__(Server)
    server.info = AsyncMock(return_value={})
    server.run_valkey_command = AsyncMock(return_value="(integer) 17")
    assert await server.count_items_expires() == (17, 0)
    server.run_valkey_command.assert_awaited_once_with("dbsize")


@pytest.mark.asyncio
@pytest.mark.parametrize("reply", [None, "", "ERR unknown command"])
async def test_an_unreadable_dbsize_reply_counts_zero(reply):
    server = Server.__new__(Server)
    server.info = AsyncMock(return_value={})
    server.run_valkey_command = AsyncMock(return_value=reply)
    assert await server.count_items_expires() == (0, 0)
