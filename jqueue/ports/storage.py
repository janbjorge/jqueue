"""
ObjectStoragePort — the single port in jqueue.

Any object satisfying this structural Protocol can act as the storage backend.
No base class or registration is required — Python's structural subtyping
(duck typing + Protocol) is sufficient.

CAS write contract
------------------
write(content, if_match=None)
  - if if_match is None  → unconditional put (used for the very first write)
  - if if_match is given → conditional put
      succeeds → storage returns the new etag (opaque str)
      fails    → raises CASConflictError

read()
  - Returns (content_bytes, etag_string)
  - If the object does not exist, returns (b"", None)
    (the caller treats this as an empty queue)
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ObjectStoragePort(Protocol):
    """
    Minimal interface required by jqueue core.

    Two async methods. The etag returned by read() must be passed back
    as if_match on the next write() to achieve compare-and-set semantics.

    Implementing adapters (built-in):
      - InMemoryStorage       — asyncio.Lock-based, for testing
      - LocalFileSystemStorage — fcntl.flock-based, POSIX single-machine
      - S3Storage             — AWS S3 If-Match conditional write (aioboto3)
      - GCSStorage            — GCS if_generation_match (google-cloud-storage)

    Custom adapters need only implement these two methods with the contract
    described in their docstrings.
    """

    async def read(self) -> tuple[bytes, str | None]:
        """
        Read the current state object.

        Returns
        -------
        content : bytes
            Raw bytes. Empty bytes (b"") if the object does not exist yet.
        etag : str | None
            Opaque version token. Pass this to write() as if_match.
            None if the object does not exist.
        """
        ...

    async def write(
        self,
        content: bytes,
        if_match: str | None = None,
    ) -> str:
        """
        Atomically write the state object.

        Parameters
        ----------
        content  : new object body
        if_match : etag from the previous read(), or None for unconditional write

        Returns
        -------
        str : new etag for the written object

        Raises
        ------
        CASConflictError   if if_match is provided but does not match the current etag
        StorageError       for any other I/O failure
        """
        ...
