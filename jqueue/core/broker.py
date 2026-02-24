"""
BrokerQueue — high-throughput queue backed by GroupCommitLoop.

BrokerQueue is an async context manager that starts a GroupCommitLoop on
__aenter__ and performs a clean shutdown (drain pending ops) on __aexit__.

Usage
-----
    from jqueue import BrokerQueue, HeartbeatManager, InMemoryStorage

    async with BrokerQueue(InMemoryStorage()) as q:
        job = await q.enqueue("send_email", b'{"to": "user@example.com"}')

        [job] = await q.dequeue("send_email")
        async with HeartbeatManager(q, job.id):
            process(job.payload)
        await q.ack(job.id)

Throughput
----------
All concurrent callers share a single writer task. While a CAS write is
in-flight (typically 50–300 ms against real object storage), every enqueue
and dequeue call that arrives is buffered and committed in the *next* write —
collapsing N concurrent writes down to O(1) storage round-trips.
"""
from __future__ import annotations

import dataclasses
from datetime import timedelta
from types import TracebackType

from jqueue.core.group_commit import GroupCommitLoop
from jqueue.domain.models import Job, QueueState
from jqueue.ports.storage import ObjectStoragePort


@dataclasses.dataclass
class BrokerQueue:
    """
    Async context manager wrapping a GroupCommitLoop.

    Parameters
    ----------
    storage       : any ObjectStoragePort implementation
    stale_timeout : IN_PROGRESS jobs with a heartbeat older than this are
                    automatically re-queued on each write cycle (default 5 min)
    """

    storage: ObjectStoragePort
    stale_timeout: timedelta = timedelta(minutes=5)

    _loop: GroupCommitLoop = dataclasses.field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._loop = GroupCommitLoop(
            storage=self.storage,
            stale_timeout=self.stale_timeout,
        )

    async def __aenter__(self) -> "BrokerQueue":
        await self._loop.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._loop.stop()

    # ------------------------------------------------------------------ #
    # Queue operations (delegated to GroupCommitLoop)                     #
    # ------------------------------------------------------------------ #

    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes,
        priority: int = 0,
    ) -> Job:
        """Add a new job. Returns the committed Job."""
        return await self._loop.enqueue(entrypoint, payload, priority)

    async def dequeue(
        self,
        entrypoint: str | None = None,
        *,
        batch_size: int = 1,
    ) -> list[Job]:
        """Claim up to batch_size QUEUED jobs and mark them IN_PROGRESS."""
        return await self._loop.dequeue(entrypoint, batch_size=batch_size)

    async def ack(self, job_id: str) -> None:
        """Remove a completed job from the queue."""
        await self._loop.ack(job_id)

    async def nack(self, job_id: str) -> None:
        """Return a job to QUEUED status."""
        await self._loop.nack(job_id)

    async def heartbeat(self, job_id: str) -> None:
        """Refresh the heartbeat timestamp for an IN_PROGRESS job."""
        await self._loop.heartbeat(job_id)

    async def read_state(self) -> QueueState:
        """Read-only snapshot of the current queue state."""
        return await self._loop.read_state()
