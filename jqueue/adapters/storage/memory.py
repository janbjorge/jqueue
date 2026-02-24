"""
InMemoryStorage — asyncio.Lock-based CAS for testing and development.

Stores the queue state as bytes in memory. Uses an asyncio.Lock to serialize
reads and writes, faithfully simulating the CAS semantics of real object
storage backends.

The etag is a simple monotonic integer counter (stringified) that increments
on every successful write.

Zero external dependencies. Safe for multiple concurrent coroutines in a
single event loop. NOT safe across processes or threads.
"""
from __future__ import annotations

import asyncio
import dataclasses

from jqueue.domain.errors import CASConflictError


@dataclasses.dataclass
class InMemoryStorage:
    """
    In-process object storage backed by a bytes buffer.

    Parameters
    ----------
    initial_content : optional pre-populated bytes (useful for test setup)
    """

    initial_content: bytes = b""

    def __post_init__(self) -> None:
        self._content: bytes = self.initial_content
        self._etag: str | None = "0" if self.initial_content else None
        self._counter: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    async def read(self) -> tuple[bytes, str | None]:
        """Return (content, etag). etag is None until the first write."""
        async with self._lock:
            return self._content, self._etag

    async def write(
        self,
        content: bytes,
        if_match: str | None = None,
    ) -> str:
        """
        CAS write. Raises CASConflictError if if_match differs from the current etag.
        """
        async with self._lock:
            if if_match != self._etag:
                raise CASConflictError(
                    f"ETag mismatch: expected {if_match!r}, got {self._etag!r}"
                )
            self._counter += 1
            self._etag = str(self._counter)
            self._content = content
            return self._etag
