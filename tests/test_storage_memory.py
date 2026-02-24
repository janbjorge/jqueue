import asyncio

import pytest

from jqueue.adapters.storage.memory import InMemoryStorage
from jqueue.domain.errors import CASConflictError


async def test_read_empty_returns_empty():
    storage = InMemoryStorage()
    content, etag = await storage.read()
    assert content == b""
    assert etag is None


async def test_write_first_with_none_etag():
    storage = InMemoryStorage()
    new_etag = await storage.write(b"data", if_match=None)
    assert new_etag is not None
    assert isinstance(new_etag, str)


async def test_read_after_write_returns_content():
    storage = InMemoryStorage()
    await storage.write(b"hello", if_match=None)
    content, etag = await storage.read()
    assert content == b"hello"
    assert etag is not None


async def test_etag_changes_on_each_write():
    storage = InMemoryStorage()
    etag1 = await storage.write(b"first", if_match=None)
    _, etag_after_first = await storage.read()
    etag2 = await storage.write(b"second", if_match=etag_after_first)
    assert etag1 != etag2


async def test_cas_conflict_on_wrong_etag():
    storage = InMemoryStorage()
    await storage.write(b"data", if_match=None)
    with pytest.raises(CASConflictError):
        await storage.write(b"new", if_match="wrong-etag")


async def test_cas_conflict_none_etag_when_content_exists():
    storage = InMemoryStorage()
    await storage.write(b"data", if_match=None)
    with pytest.raises(CASConflictError):
        await storage.write(b"new", if_match=None)


async def test_content_unchanged_after_failed_write():
    storage = InMemoryStorage()
    await storage.write(b"original", if_match=None)
    with pytest.raises(CASConflictError):
        await storage.write(b"corrupted", if_match="bad-etag")
    content, _ = await storage.read()
    assert content == b"original"


async def test_sequential_writes():
    storage = InMemoryStorage()
    await storage.write(b"v1", if_match=None)
    _, etag1 = await storage.read()
    await storage.write(b"v2", if_match=etag1)
    _, etag2 = await storage.read()
    await storage.write(b"v3", if_match=etag2)
    content, _ = await storage.read()
    assert content == b"v3"


async def test_initial_content_constructor():
    storage = InMemoryStorage(initial_content=b"pre-populated")
    content, etag = await storage.read()
    assert content == b"pre-populated"
    assert etag == "0"


async def test_initial_content_empty_has_none_etag():
    storage = InMemoryStorage()
    _, etag = await storage.read()
    assert etag is None


async def test_concurrent_writes_exactly_one_wins():
    storage = InMemoryStorage()
    _, etag = await storage.read()
    successes = []
    failures = []

    async def attempt(data: bytes) -> None:
        try:
            await storage.write(data, if_match=etag)
            successes.append(data)
        except CASConflictError:
            failures.append(data)

    await asyncio.gather(attempt(b"a"), attempt(b"b"))
    assert len(successes) == 1
    assert len(failures) == 1


async def test_read_is_consistent_under_concurrent_reads():
    storage = InMemoryStorage()
    await storage.write(b"consistent", if_match=None)
    results = await asyncio.gather(storage.read(), storage.read(), storage.read())
    for content, _ in results:
        assert content == b"consistent"
