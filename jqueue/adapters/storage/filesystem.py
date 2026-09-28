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
The jqueue codec always produces non-empty JSON.

CAS semantics
-------------
write(content, if_match) takes an exclusive flock on ``<path>.lock``,
re-checks the etag, then writes a temp file, fsyncs it and os.replace()s it
over ``path``. A failed write leaves the old state intact, and readers never
see a partial file.

POSIX-only (Linux, macOS). Not compatible with NFS or distributed filesystems.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import fcntl
import hashlib
import os
import tempfile
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

    @property
    def _lock_path(self) -> Path:
        return self.path.with_name(self.path.name + ".lock")

    def _sync_read(self) -> tuple[bytes, str | None]:
        try:
            content = self.path.read_bytes()
        except FileNotFoundError:
            return b"", None
        etag: str | None = self._etag(content) if content else None
        return content, etag

    def _sync_write(self, content: bytes, if_match: str | None) -> str:
        directory = self.path.parent
        directory.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(str(self._lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            _, real_etag = self._sync_read()
            if real_etag != if_match:
                raise CASConflictError(
                    f"ETag mismatch: expected {if_match!r}, got {real_etag!r}"
                )

            fd, tmp_name = tempfile.mkstemp(
                dir=directory, prefix=f".{self.path.name}.", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(content)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.chmod(tmp_name, 0o644)
                os.replace(tmp_name, self.path)
            except BaseException:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(tmp_name)
                raise
            _fsync_dir(directory)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

        return self._etag(content)


def _fsync_dir(directory: Path) -> None:
    """Persist a rename by fsyncing its directory."""
    dir_fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
