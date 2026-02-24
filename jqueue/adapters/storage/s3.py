"""
S3Storage — AWS S3 adapter using aioboto3 and If-Match conditional writes.

Install extras: pip install "jqueue[s3]"

CAS semantics
-------------
S3 supports conditional PutObject via the IfMatch parameter (added Aug 2024).

  read()  → returns (content, ETag) where ETag is the S3 object's entity tag
  write() → passes IfMatch=etag; S3 raises PreconditionFailed on mismatch
            → CASConflictError

First write (if_match=None):
  IfMatch is omitted — unconditional put.

Compatible with S3-compatible storage that supports conditional writes:
  MinIO, Cloudflare R2, Tigris, etc.
"""
from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from jqueue.domain.errors import CASConflictError, StorageError

if TYPE_CHECKING:
    from aioboto3 import Session as AioBoto3Session


@dataclasses.dataclass
class S3Storage:
    """
    AWS S3 storage adapter.

    Parameters
    ----------
    bucket       : S3 bucket name
    key          : object key (e.g. "queues/my-queue/state.json")
    session      : aioboto3.Session — created lazily from env vars if omitted
    region_name  : AWS region passed to the S3 client
    endpoint_url : custom endpoint for S3-compatible backends (e.g. MinIO)
    """

    bucket: str
    key: str
    session: AioBoto3Session | None = None
    region_name: str | None = None
    endpoint_url: str | None = None

    def _get_session(self) -> AioBoto3Session:
        if self.session is not None:
            return self.session
        try:
            import aioboto3  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "S3Storage requires aioboto3. Install with: pip install 'jqueue[s3]'"
            ) from exc
        return aioboto3.Session()  # type: ignore[return-value]

    def _client_kwargs(self) -> dict[str, str]:
        """Build kwargs forwarded to the S3 client constructor."""
        kwargs: dict[str, str] = {}
        if self.region_name:
            kwargs["region_name"] = self.region_name
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return kwargs

    async def read(self) -> tuple[bytes, str | None]:
        """Read the state object. Returns (b"", None) if the key does not exist."""
        session = self._get_session()
        try:
            async with session.client("s3", **self._client_kwargs()) as s3:  # type: ignore[attr-defined]
                try:
                    response = await s3.get_object(Bucket=self.bucket, Key=self.key)
                    content: bytes = await response["Body"].read()
                    etag: str = response["ETag"]
                    return content, etag
                except Exception as exc:
                    if _s3_error_code(exc) in ("NoSuchKey", "404"):
                        return b"", None
                    raise
        except (CASConflictError, StorageError):
            raise
        except Exception as exc:
            raise StorageError("S3 read failed", exc) from exc

    async def write(
        self,
        content: bytes,
        if_match: str | None = None,
    ) -> str:
        """CAS write. Raises CASConflictError on ETag mismatch (PreconditionFailed)."""
        session = self._get_session()
        try:
            async with session.client("s3", **self._client_kwargs()) as s3:  # type: ignore[attr-defined]
                put_kwargs: dict[str, str | bytes] = {
                    "Bucket": self.bucket,
                    "Key": self.key,
                    "Body": content,
                    "ContentType": "application/json",
                }
                if if_match is not None:
                    put_kwargs["IfMatch"] = if_match

                try:
                    response = await s3.put_object(**put_kwargs)
                    return str(response["ETag"])
                except Exception as exc:
                    if _s3_error_code(exc) == "PreconditionFailed":
                        raise CASConflictError(
                            "S3 ETag mismatch (PreconditionFailed)"
                        ) from exc
                    raise
        except (CASConflictError, StorageError):
            raise
        except Exception as exc:
            raise StorageError("S3 write failed", exc) from exc


def _s3_error_code(exc: Exception) -> str:
    """Extract the error code from a botocore ClientError, or return ''."""
    try:
        response = getattr(exc, "response", None)
        if isinstance(response, dict):
            error = response.get("Error", {})
            if isinstance(error, dict):
                code = error.get("Code", "")
                return str(code) if code else ""
    except Exception:  # noqa: BLE001
        pass
    return ""
