from unittest.mock import AsyncMock, MagicMock

import pytest

from jqueue.adapters.storage.s3 import S3Storage, _s3_error_code
from jqueue.domain.errors import CASConflictError, StorageError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _AsyncCM:
    """Minimal async context manager wrapping a return value."""

    def __init__(self, value: object) -> None:
        self._value = value

    async def __aenter__(self) -> object:
        return self._value

    async def __aexit__(self, *args: object) -> None:
        pass


def _make_storage(s3: AsyncMock | None = None) -> tuple[S3Storage, AsyncMock]:
    if s3 is None:
        s3 = AsyncMock()
    session = MagicMock()
    session.client.return_value = _AsyncCM(s3)
    storage = S3Storage(bucket="my-bucket", key="state.json", session=session)
    return storage, s3


def _client_error(code: str) -> Exception:
    exc = Exception(f"ClientError: {code}")
    exc.response = {"Error": {"Code": code}}  # type: ignore[attr-defined]
    return exc


# ---------------------------------------------------------------------------
# read()
# ---------------------------------------------------------------------------

async def test_read_returns_content_and_etag():
    storage, s3 = _make_storage()
    body = AsyncMock()
    body.read.return_value = b'{"version": 0, "jobs": []}'
    s3.get_object.return_value = {"Body": body, "ETag": '"abc123"'}

    content, etag = await storage.read()

    assert content == b'{"version": 0, "jobs": []}'
    assert etag == '"abc123"'


async def test_read_no_such_key_returns_empty():
    storage, s3 = _make_storage()
    s3.get_object.side_effect = _client_error("NoSuchKey")

    content, etag = await storage.read()

    assert content == b""
    assert etag is None


async def test_read_404_returns_empty():
    storage, s3 = _make_storage()
    s3.get_object.side_effect = _client_error("404")

    content, etag = await storage.read()

    assert content == b""
    assert etag is None


async def test_read_other_s3_error_raises_storage_error():
    storage, s3 = _make_storage()
    s3.get_object.side_effect = _client_error("InternalError")

    with pytest.raises(StorageError):
        await storage.read()


async def test_read_generic_exception_raises_storage_error():
    storage, s3 = _make_storage()
    s3.get_object.side_effect = RuntimeError("network failure")

    with pytest.raises(StorageError):
        await storage.read()


# ---------------------------------------------------------------------------
# write()
# ---------------------------------------------------------------------------

async def test_write_without_if_match_no_condition_header():
    storage, s3 = _make_storage()
    s3.put_object.return_value = {"ETag": '"new-etag"'}

    etag = await storage.write(b"data")

    assert etag == '"new-etag"'
    call_kwargs = s3.put_object.call_args.kwargs
    assert "IfMatch" not in call_kwargs


async def test_write_with_if_match_sends_condition():
    storage, s3 = _make_storage()
    s3.put_object.return_value = {"ETag": '"new-etag"'}

    etag = await storage.write(b"data", if_match='"old-etag"')

    assert etag == '"new-etag"'
    call_kwargs = s3.put_object.call_args.kwargs
    assert call_kwargs["IfMatch"] == '"old-etag"'


async def test_write_sends_correct_bucket_and_key():
    storage, s3 = _make_storage()
    s3.put_object.return_value = {"ETag": '"etag"'}

    await storage.write(b"data")

    call_kwargs = s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "my-bucket"
    assert call_kwargs["Key"] == "state.json"
    assert call_kwargs["Body"] == b"data"


async def test_write_precondition_failed_raises_cas_conflict():
    storage, s3 = _make_storage()
    s3.put_object.side_effect = _client_error("PreconditionFailed")

    with pytest.raises(CASConflictError):
        await storage.write(b"data", if_match='"etag"')


async def test_write_other_error_raises_storage_error():
    storage, s3 = _make_storage()
    s3.put_object.side_effect = RuntimeError("network failure")

    with pytest.raises(StorageError):
        await storage.write(b"data")


# ---------------------------------------------------------------------------
# _client_kwargs()
# ---------------------------------------------------------------------------

def test_client_kwargs_empty_by_default():
    storage = S3Storage(bucket="b", key="k")
    assert storage._client_kwargs() == {}


def test_client_kwargs_with_region():
    storage = S3Storage(bucket="b", key="k", region_name="us-west-2")
    assert storage._client_kwargs()["region_name"] == "us-west-2"


def test_client_kwargs_with_endpoint_url():
    storage = S3Storage(bucket="b", key="k", endpoint_url="http://localhost:9000")
    assert storage._client_kwargs()["endpoint_url"] == "http://localhost:9000"


def test_client_kwargs_with_both():
    storage = S3Storage(
        bucket="b",
        key="k",
        region_name="eu-central-1",
        endpoint_url="http://minio:9000",
    )
    kwargs = storage._client_kwargs()
    assert kwargs["region_name"] == "eu-central-1"
    assert kwargs["endpoint_url"] == "http://minio:9000"


# ---------------------------------------------------------------------------
# _s3_error_code()
# ---------------------------------------------------------------------------

def test_s3_error_code_extracts_code():
    exc = Exception()
    exc.response = {"Error": {"Code": "NoSuchKey"}}  # type: ignore[attr-defined]
    assert _s3_error_code(exc) == "NoSuchKey"


def test_s3_error_code_precondition_failed():
    exc = Exception()
    exc.response = {"Error": {"Code": "PreconditionFailed"}}  # type: ignore[attr-defined]
    assert _s3_error_code(exc) == "PreconditionFailed"


def test_s3_error_code_no_response_attr():
    exc = Exception("plain error")
    assert _s3_error_code(exc) == ""


def test_s3_error_code_response_not_dict():
    exc = Exception()
    exc.response = "not a dict"  # type: ignore[attr-defined]
    assert _s3_error_code(exc) == ""


def test_s3_error_code_missing_error_key():
    exc = Exception()
    exc.response = {}  # type: ignore[attr-defined]
    assert _s3_error_code(exc) == ""


def test_s3_error_code_empty_code():
    exc = Exception()
    exc.response = {"Error": {"Code": ""}}  # type: ignore[attr-defined]
    assert _s3_error_code(exc) == ""
