from unittest.mock import MagicMock, patch

import pytest

from jqueue.adapters.storage.gcs import GCSStorage
from jqueue.domain.errors import CASConflictError, StorageError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_storage() -> tuple[GCSStorage, MagicMock, MagicMock]:
    """Return (storage, blob_mock, client_mock) with wired-up fakes."""
    blob = MagicMock()
    bucket = MagicMock()
    bucket.blob.return_value = blob
    client = MagicMock()
    client.bucket.return_value = bucket
    storage = GCSStorage(bucket_name="my-bucket", blob_name="state.json", client=client)
    return storage, blob, client


# ---------------------------------------------------------------------------
# async read() — patches _sync_read to bypass asyncio.to_thread
# ---------------------------------------------------------------------------

async def test_read_returns_content_and_generation():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_read", return_value=(b'{"version": 0, "jobs": []}', "42")):
        content, etag = await storage.read()
    assert content == b'{"version": 0, "jobs": []}'
    assert etag == "42"


async def test_read_not_found_returns_empty():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_read", return_value=(b"", None)):
        content, etag = await storage.read()
    assert content == b""
    assert etag is None


async def test_read_exception_becomes_storage_error():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_read", side_effect=RuntimeError("network")):
        with pytest.raises(StorageError):
            await storage.read()


async def test_read_cas_conflict_propagated():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_read", side_effect=CASConflictError("mismatch")):
        with pytest.raises(CASConflictError):
            await storage.read()


# ---------------------------------------------------------------------------
# async write() — patches _sync_write to bypass asyncio.to_thread
# ---------------------------------------------------------------------------

async def test_write_returns_generation():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_write", return_value="43"):
        etag = await storage.write(b"data")
    assert etag == "43"


async def test_write_passes_content_and_if_match():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_write", return_value="50") as mock_sw:
        await storage.write(b"content", if_match="49")
    mock_sw.assert_called_once_with(b"content", "49")


async def test_write_cas_conflict_propagated():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_write", side_effect=CASConflictError("mismatch")):
        with pytest.raises(CASConflictError):
            await storage.write(b"data", if_match="42")


async def test_write_exception_becomes_storage_error():
    storage, _, _ = _make_storage()
    with patch.object(storage, "_sync_write", side_effect=RuntimeError("io error")):
        with pytest.raises(StorageError):
            await storage.write(b"data")


# ---------------------------------------------------------------------------
# _sync_read() — tests the synchronous implementation directly
# ---------------------------------------------------------------------------

def test_sync_read_success():
    pytest.importorskip("google.api_core.exceptions")
    storage, blob, _ = _make_storage()
    blob.download_as_bytes.return_value = b"content"
    blob.generation = 99
    content, etag = storage._sync_read()
    assert content == b"content"
    assert etag == "99"


def test_sync_read_not_found_returns_empty():
    gapi_exc = pytest.importorskip("google.api_core.exceptions")
    storage, blob, _ = _make_storage()
    blob.download_as_bytes.side_effect = gapi_exc.NotFound("blob not found")
    content, etag = storage._sync_read()
    assert content == b""
    assert etag is None


def test_sync_read_calls_correct_bucket_and_blob():
    pytest.importorskip("google.api_core.exceptions")
    storage, blob, client = _make_storage()
    blob.download_as_bytes.return_value = b"data"
    blob.generation = 1
    storage._sync_read()
    client.bucket.assert_called_once_with("my-bucket")
    client.bucket.return_value.blob.assert_called_once_with("state.json")


# ---------------------------------------------------------------------------
# _sync_write() — tests the synchronous implementation directly
# ---------------------------------------------------------------------------

def test_sync_write_none_if_match_uses_generation_zero():
    pytest.importorskip("google.api_core.exceptions")
    storage, blob, _ = _make_storage()
    blob.generation = 100
    storage._sync_write(b"content", if_match=None)
    blob.upload_from_string.assert_called_once_with(
        b"content",
        content_type="application/json",
        if_generation_match=0,
    )


def test_sync_write_with_if_match_uses_int_generation():
    pytest.importorskip("google.api_core.exceptions")
    storage, blob, _ = _make_storage()
    blob.generation = 101
    storage._sync_write(b"content", if_match="50")
    blob.upload_from_string.assert_called_once_with(
        b"content",
        content_type="application/json",
        if_generation_match=50,
    )


def test_sync_write_returns_generation_as_string():
    pytest.importorskip("google.api_core.exceptions")
    storage, blob, _ = _make_storage()
    blob.generation = 77
    etag = storage._sync_write(b"content", if_match=None)
    assert etag == "77"


def test_sync_write_reloads_blob_after_upload():
    pytest.importorskip("google.api_core.exceptions")
    storage, blob, _ = _make_storage()
    blob.generation = 55
    storage._sync_write(b"content", if_match=None)
    blob.reload.assert_called_once()


def test_sync_write_precondition_failed_raises_cas_conflict():
    gapi_exc = pytest.importorskip("google.api_core.exceptions")
    storage, blob, _ = _make_storage()
    blob.upload_from_string.side_effect = gapi_exc.PreconditionFailed("mismatch")
    with pytest.raises(CASConflictError):
        storage._sync_write(b"content", if_match="42")  # GCS etags are int strings
