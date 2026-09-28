import asyncio
import os
import resource
import signal
import stat
from pathlib import Path

import pytest

from jqueue.adapters.storage.filesystem import LocalFileSystemStorage
from jqueue.domain.errors import CASConflictError


async def test_read_nonexistent_file(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    content, etag = await storage.read()
    assert content == b""
    assert etag is None


async def test_write_creates_file(tmp_path):
    path = tmp_path / "queue.json"
    storage = LocalFileSystemStorage(path)
    await storage.write(b'{"version": 0, "jobs": []}', if_match=None)
    assert path.exists()


async def test_write_returns_etag(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    etag = await storage.write(b"data", if_match=None)
    assert isinstance(etag, str)
    assert len(etag) > 0


async def test_read_after_write_returns_content(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    await storage.write(b"hello", if_match=None)
    content, etag = await storage.read()
    assert content == b"hello"
    assert etag is not None


async def test_cas_write_with_correct_etag(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    await storage.write(b"v1", if_match=None)
    _, etag = await storage.read()
    etag2 = await storage.write(b"v2", if_match=etag)
    content, _ = await storage.read()
    assert content == b"v2"
    assert etag2 != etag


async def test_cas_conflict_on_stale_etag(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    await storage.write(b"v1", if_match=None)
    with pytest.raises(CASConflictError):
        await storage.write(b"v2", if_match="stale-etag")


async def test_cas_conflict_none_when_file_exists(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    await storage.write(b"v1", if_match=None)
    with pytest.raises(CASConflictError):
        await storage.write(b"v2", if_match=None)


async def test_content_unchanged_after_failed_cas(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    await storage.write(b"original", if_match=None)
    with pytest.raises(CASConflictError):
        await storage.write(b"corrupted", if_match="bad-etag")
    content, _ = await storage.read()
    assert content == b"original"


async def test_write_creates_parent_directories(tmp_path):
    path = tmp_path / "deep" / "nested" / "queue.json"
    storage = LocalFileSystemStorage(path)
    await storage.write(b"data", if_match=None)
    assert path.exists()


async def test_multiple_sequential_writes(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    await storage.write(b"v1", if_match=None)
    _, etag1 = await storage.read()
    await storage.write(b"v2", if_match=etag1)
    _, etag2 = await storage.read()
    await storage.write(b"v3", if_match=etag2)
    content, _ = await storage.read()
    assert content == b"v3"


async def test_etag_changes_after_write(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "queue.json")
    await storage.write(b"v1", if_match=None)
    _, etag1 = await storage.read()
    await storage.write(b"v2", if_match=etag1)
    _, etag2 = await storage.read()
    assert etag1 != etag2


async def test_path_accepts_string(tmp_path):
    storage = LocalFileSystemStorage(str(tmp_path / "queue.json"))
    await storage.write(b"data", if_match=None)
    content, _ = await storage.read()
    assert content == b"data"


# ---------------------------------------------------------------------------
# Crash safety: a failed write never leaves a partial file
# ---------------------------------------------------------------------------


def _leftovers(tmp_path: Path) -> list[str]:
    return sorted(p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp"))


async def test_short_write_leaves_previous_content_intact(tmp_path):
    path = tmp_path / "state.json"
    storage = LocalFileSystemStorage(path)
    old = b'{"version": 1, "jobs": []}'
    etag = await storage.write(old)
    new = b"x" * 4096

    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    previous = signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    resource.setrlimit(resource.RLIMIT_FSIZE, (1024, hard))
    try:
        try:
            await storage.write(new, if_match=etag)
        except OSError:
            pass
    finally:
        resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
        signal.signal(signal.SIGXFSZ, previous)

    assert path.read_bytes() in (old, new)
    assert _leftovers(tmp_path) == []


async def test_failed_fsync_keeps_old_content_and_cleans_up(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    storage = LocalFileSystemStorage(path)
    etag = await storage.write(b"old")

    def boom(fd):
        raise OSError("EIO")

    monkeypatch.setattr(os, "fsync", boom)
    with pytest.raises(OSError, match="EIO"):
        await storage.write(b"new", if_match=etag)
    monkeypatch.undo()

    assert path.read_bytes() == b"old"
    assert _leftovers(tmp_path) == []
    content, still = await storage.read()
    assert (content, still) == (b"old", etag)


async def test_successful_write_leaves_no_temp_files(tmp_path):
    path = tmp_path / "state.json"
    storage = LocalFileSystemStorage(path)
    etag = await storage.write(b"one")
    await storage.write(b"two", if_match=etag)

    assert path.read_bytes() == b"two"
    assert _leftovers(tmp_path) == []
    assert stat.S_IMODE(path.stat().st_mode) == 0o644


async def test_failed_cas_leaves_no_temp_files(tmp_path):
    storage = LocalFileSystemStorage(tmp_path / "state.json")
    await storage.write(b"one")
    with pytest.raises(CASConflictError):
        await storage.write(b"two", if_match="stale")
    assert _leftovers(tmp_path) == []


async def test_concurrent_writers_same_etag_exactly_one_wins(tmp_path):
    path = tmp_path / "state.json"
    etag = await LocalFileSystemStorage(path).write(b"base")

    async def attempt(i: int) -> bool:
        try:
            await LocalFileSystemStorage(path).write(f"w{i}".encode(), etag)
        except CASConflictError:
            return False
        return True

    results = await asyncio.gather(*(attempt(i) for i in range(16)))

    assert results.count(True) == 1
    winner = results.index(True)
    assert path.read_bytes() == f"w{winner}".encode()
