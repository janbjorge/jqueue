"""
LocalFileSystemStorage — fcntl.flock-based CAS for POSIX systems.

Suitable for local development, single-machine deployments, or integration
tests that need a persistent file rather than in-memory state.

NOT suitable for multi-machine deployments — use S3Storage or GCSStorage
for distributed workloads.

Etag strategy
-------------
The etag is a SHA-256 hex digest of the file contents. This is stable,
deterministic, and always changes when content changes — unlike mtime which
can be identical across rapid successive writes on fast machines.
A file that is absent or empty is treated as non-existent; its etag is None.
The jqueue codec always produces non-empty JSON, so a 0-byte file only occurs
transiently before the first write completes.

CAS semantics
-------------
write(content, if_match) acquires an exclusive flock, re-reads the current
etag while holding the lock, and raises CASConflictError if it differs from
if_match. The write is performed atomically within the same lock scope.

POSIX-only (Linux, macOS). Not compatible with NFS or distributed filesystems.
"""

from __future__ import annotations

import asyncio
import dataclasses
import fcntl
import hashlib
import os
from pathlib import Path

from jqueue.domain.errors import CASConflictError


@dataclasses.dataclass
class LocalFileSystemStorage:
    """
    Stores the queue state in a local file.

    Parameters
    ----------
    path : path to the JSON state file (parent directory created if absent)
    """

    path: Path

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def read(self) -> tuple[bytes, str | None]:
        """Return (content, etag). Returns (b"", None) if the file does not exist."""
        return await asyncio.to_thread(self._sync_read)

    async def write(
        self,
        content: bytes,
        if_match: str | None = None,
    ) -> str:
        """CAS write. Raises CASConflictError on etag mismatch."""
        return await asyncio.to_thread(self._sync_write, content, if_match)

    # ------------------------------------------------------------------ #
    # Synchronous implementations (executed in a thread-pool worker)      #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _etag(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _sync_read(self) -> tuple[bytes, str | None]:
        if not self.path.exists():
            return b"", None
        with open(self.path, "rb") as fh:
            fcntl.flock(fh, fcntl.LOCK_SH)
            try:
                content = fh.read()
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
        etag: str | None = self._etag(content) if content else None
        return content, etag

    def _sync_write(self, content: bytes, if_match: str | None) -> str:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)

            existing = os.read(fd, os.fstat(fd).st_size)
            real_etag: str | None = self._etag(existing) if existing else None

            if real_etag != if_match:
                raise CASConflictError(
                    f"ETag mismatch: expected {if_match!r}, got {real_etag!r}"
                )

            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, content)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

        return self._etag(content)
