"""
GCSStorage — Google Cloud Storage adapter using google-cloud-storage.

Install extras: pip install "jqueue[gcs]"

CAS semantics
-------------
GCS supports conditional writes via object generation numbers.

  read()  → returns (content, generation_string) where generation is the
            integer GCS object generation, stringified to match the etag type
  write() → uses if_generation_match=int(etag); GCS raises PreconditionFailed
            on mismatch → CASConflictError

First write (if_match=None):
  Uses if_generation_match=0 — GCS convention for "blob must not exist yet".

Note: google-cloud-storage is synchronous. All operations are wrapped in
asyncio.to_thread to avoid blocking the event loop.
"""
from __future__ import annotations

import asyncio
import dataclasses
from typing import TYPE_CHECKING

from jqueue.domain.errors import CASConflictError, StorageError

if TYPE_CHECKING:
    from google.cloud.storage import Client as GCSClient


@dataclasses.dataclass
class GCSStorage:
    """
    Google Cloud Storage adapter.

    Parameters
    ----------
    bucket_name : GCS bucket name
    blob_name   : blob path (e.g. "queues/my-queue/state.json")
    client      : google.cloud.storage.Client — created lazily if omitted
    """

    bucket_name: str
    blob_name: str
    client: GCSClient | None = None

    def _get_client(self) -> GCSClient:
        if self.client is not None:
            return self.client
        try:
            from google.cloud import storage  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "GCSStorage requires google-cloud-storage. "
                "Install with: pip install 'jqueue[gcs]'"
            ) from exc
        return storage.Client()  # type: ignore[return-value]

    async def read(self) -> tuple[bytes, str | None]:
        """Read the state blob. Returns (b"", None) if the blob does not exist."""
        try:
            return await asyncio.to_thread(self._sync_read)
        except (CASConflictError, StorageError):
            raise
        except Exception as exc:
            raise StorageError("GCS read failed", exc) from exc

    async def write(
        self,
        content: bytes,
        if_match: str | None = None,
    ) -> str:
        """CAS write. Raises CASConflictError on generation mismatch."""
        try:
            return await asyncio.to_thread(self._sync_write, content, if_match)
        except (CASConflictError, StorageError):
            raise
        except Exception as exc:
            raise StorageError("GCS write failed", exc) from exc

    # ------------------------------------------------------------------ #
    # Synchronous implementations (executed in a thread-pool worker)      #
    # ------------------------------------------------------------------ #

    def _sync_read(self) -> tuple[bytes, str | None]:
        try:
            from google.api_core import exceptions as gapi_exc  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "GCSStorage requires google-cloud-storage. "
                "Install with: pip install 'jqueue[gcs]'"
            ) from exc

        client = self._get_client()
        blob = client.bucket(self.bucket_name).blob(self.blob_name)  # type: ignore[attr-defined]
        try:
            content: bytes = blob.download_as_bytes()
            return content, str(blob.generation)
        except gapi_exc.NotFound:
            return b"", None

    def _sync_write(self, content: bytes, if_match: str | None) -> str:
        try:
            from google.api_core import exceptions as gapi_exc  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "GCSStorage requires google-cloud-storage. "
                "Install with: pip install 'jqueue[gcs]'"
            ) from exc

        client = self._get_client()
        blob = client.bucket(self.bucket_name).blob(self.blob_name)  # type: ignore[attr-defined]

        # if_generation_match=0 → "blob must not exist yet"
        gen_match: int = 0 if if_match is None else int(if_match)

        try:
            blob.upload_from_string(  # type: ignore[attr-defined]
                content,
                content_type="application/json",
                if_generation_match=gen_match,
            )
        except gapi_exc.PreconditionFailed as exc:
            raise CASConflictError("GCS generation mismatch") from exc

        blob.reload()  # type: ignore[attr-defined]
        return str(blob.generation)
