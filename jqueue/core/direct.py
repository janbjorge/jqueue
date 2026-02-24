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
from datetime import datetime, timedelta, timezone
from typing import Callable

from jqueue.core import codec
from jqueue.domain.errors import CASConflictError, JobNotFoundError
from jqueue.domain.models import Job, JobStatus, QueueState
from jqueue.ports.storage import ObjectStoragePort

MutationFn = Callable[[QueueState], QueueState]


@dataclasses.dataclass
class DirectQueue:
    """
    Thin stateless wrapper around ObjectStoragePort.

    All methods are async and safe to call from multiple coroutines;
    each operation performs a full CAS cycle independently.
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
        await self._mutate(lambda state: state.with_job_added(job))
        return job

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
        claimed: list[Job] = []

        def _fn(state: QueueState) -> QueueState:
            nonlocal claimed
            available = state.queued_jobs(entrypoint)[:batch_size]
            claimed = []
            new_state = state
            for job in available:
                updated = job.with_status(JobStatus.IN_PROGRESS).with_heartbeat(
                    datetime.now(timezone.utc)
                )
                new_state = new_state.with_job_replaced(updated)
                claimed.append(updated)
            return new_state

        await self._mutate(_fn)
        return claimed

    async def ack(self, job_id: str) -> None:
        """Mark a job as done and remove it from the queue."""
        await self._mutate(lambda state: state.with_job_removed(job_id))

    async def nack(self, job_id: str) -> None:
        """Return a job to QUEUED status (worker failed or declined it)."""

        def _fn(state: QueueState) -> QueueState:
            job = state.find(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            return state.with_job_replaced(
                job.with_status(JobStatus.QUEUED).with_heartbeat(None)
            )

        await self._mutate(_fn)

    async def heartbeat(self, job_id: str) -> None:
        """Update the heartbeat timestamp of an IN_PROGRESS job."""

        def _fn(state: QueueState) -> QueueState:
            job = state.find(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            return state.with_job_replaced(
                job.with_heartbeat(datetime.now(timezone.utc))
            )

        await self._mutate(_fn)

    async def requeue_stale(self, timeout: timedelta) -> int:
        """
        Re-queue any IN_PROGRESS jobs whose heartbeat is older than `timeout`.

        Returns the number of jobs re-queued.
        """
        cutoff = datetime.now(timezone.utc) - timeout
        requeued = 0

        def _fn(state: QueueState) -> QueueState:
            nonlocal requeued
            old_in_progress = {j.id for j in state.in_progress_jobs()}
            new_state = state.requeue_stale(cutoff)
            new_in_progress = {j.id for j in new_state.in_progress_jobs()}
            requeued = len(old_in_progress - new_in_progress)
            return new_state

        await self._mutate(_fn)
        return requeued

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

    async def _mutate(self, fn: MutationFn) -> None:
        """
        Read-modify-write with CAS retry loop.

        fn(state) -> new_state  (synchronous)
        Retries up to self.max_retries on CASConflictError.
        """
        for attempt in range(self.max_retries):
            content, etag = await self.storage.read()
            state = codec.decode(content)
            new_state = fn(state)
            new_content = codec.encode(new_state)
            try:
                await self.storage.write(new_content, if_match=etag)
                return
            except CASConflictError:
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(0.01 * (attempt + 1))
