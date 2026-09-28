"""
DirectQueue — one CAS write per operation.

Every enqueue, dequeue, ack, nack, or heartbeat call does:
  1. read current state + etag from storage
  2. mutate state in memory
  3. CAS write back with if_match=etag (retries on CASConflictError)

Suitable for ~1-5 ops/sec workloads depending on storage backend latency.
Use BrokerQueue for higher throughput.

Retry policy
------------
Operations retry up to `max_retries` times (default 10) on CASConflictError
with linear back-off (10ms × attempt). Raises CASConflictError if all retries
are exhausted.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from jqueue.core import codec
from jqueue.core.queries import StateQueries
from jqueue.domain.errors import CASConflictError
from jqueue.domain.models import Job, QueueState
from jqueue.ports.storage import ObjectStoragePort


@dataclasses.dataclass
class DirectQueue:
    """
    Thin stateless service around ObjectStoragePort.

    All methods are async and safe to call from multiple coroutines;
    each operation performs a full CAS cycle independently. Each public
    method is one unit of work: it either commits entirely or raises.

    Parameters
    ----------
    storage     : any ObjectStoragePort implementation
    max_retries : CAS attempts before CASConflictError is re-raised
    """

    storage: ObjectStoragePort
    max_retries: int = 10

    # ------------------------------------------------------------------ #
    # Write operations                                                     #
    # ------------------------------------------------------------------ #

    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes,
        priority: int = 0,
    ) -> Job:
        """Add a new job to the queue. Returns the committed Job."""
        job = Job.new(entrypoint, payload, priority)
        return await self._transaction(lambda q: q.add(job))

    async def dequeue(
        self,
        entrypoint: str | None = None,
        *,
        batch_size: int = 1,
    ) -> list[Job]:
        """
        Claim up to `batch_size` QUEUED jobs and mark them IN_PROGRESS.

        Optionally filter by entrypoint. Returns the list of claimed jobs.
        Returns an empty list if no jobs are available.
        """
        return await self._transaction(
            lambda q: q.claim(entrypoint, batch_size, datetime.now(UTC))
        )

    async def ack(self, job_id: str) -> None:
        """Mark a job as done and remove it from the queue."""
        await self._transaction(lambda q: q.remove(job_id))

    async def nack(self, job_id: str) -> None:
        """Return a job to QUEUED status (worker failed or declined it)."""
        await self._transaction(lambda q: q.release(job_id))

    async def heartbeat(self, job_id: str) -> None:
        """Update the heartbeat timestamp of an IN_PROGRESS job."""
        await self._transaction(lambda q: q.touch(job_id, datetime.now(UTC)))

    async def requeue_stale(self, timeout: timedelta) -> int:
        """
        Re-queue any IN_PROGRESS jobs whose heartbeat is older than `timeout`.

        Returns the number of jobs re-queued.
        """
        cutoff = datetime.now(UTC) - timeout
        return await self._transaction(lambda q: q.requeue_stale(cutoff))

    # ------------------------------------------------------------------ #
    # Read operations (no CAS needed)                                     #
    # ------------------------------------------------------------------ #

    async def read_state(self) -> QueueState:
        """Read-only snapshot of the current queue state."""
        content, _ = await self.storage.read()
        return codec.decode(content)

    # ------------------------------------------------------------------ #
    # Internal CAS loop                                                   #
    # ------------------------------------------------------------------ #

    async def _transaction[T](self, fn: Callable[[StateQueries], T]) -> T:
        """
        Read-modify-write with CAS retry loop.

        fn(queries) -> result  (synchronous, re-run on a fresh snapshot per retry)
        Retries up to self.max_retries on CASConflictError. If fn leaves the
        state unchanged (e.g. dequeue on an empty queue), no write is made:
        the snapshot just read is a valid linearization point.
        """
        for attempt in range(self.max_retries):
            content, etag = await self.storage.read()
            snapshot = codec.decode(content)
            queries = StateQueries(snapshot)
            result = fn(queries)
            if queries.state is snapshot:
                return result
            try:
                await self.storage.write(codec.encode(queries.state), if_match=etag)
                return result
            except CASConflictError:
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(0.01 * (attempt + 1))
        raise CASConflictError("Max CAS retries exceeded")
