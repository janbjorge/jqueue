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
