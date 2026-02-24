"""
HeartbeatManager — async context manager for background heartbeats.

A worker holding a long-running job wraps its work in HeartbeatManager
to send periodic heartbeats, preventing the broker from re-queueing the
job as stale.

Usage
-----
    async with BrokerQueue(storage) as q:
        [job] = await q.dequeue("process_video")

        async with HeartbeatManager(q, job.id, interval=timedelta(seconds=30)):
            result = await do_long_work(job.payload)

        await q.ack(job.id)

If the worker raises, the heartbeat task is cancelled. Callers should
nack() the job in an except/finally block.

HeartbeatManager is typed against the structural Protocol _HasHeartbeat, so
it works with BrokerQueue, DirectQueue, and GroupCommitLoop without any
shared base class.
"""
from __future__ import annotations

import asyncio
import dataclasses
from datetime import timedelta
from types import TracebackType
from typing import Protocol

from jqueue.domain.errors import JQueueError


class _HasHeartbeat(Protocol):
    """Structural Protocol — any object with an async heartbeat(job_id) method."""

    async def heartbeat(self, job_id: str) -> None: ...


@dataclasses.dataclass
class HeartbeatManager:
    """
    Sends periodic heartbeats for a single job.

    Parameters
    ----------
    queue    : any object with async heartbeat(job_id: str) -> None
    job_id   : the job to keep alive
    interval : time between heartbeat sends (default 60 seconds)
    """

    queue: _HasHeartbeat
    job_id: str
    interval: timedelta = timedelta(seconds=60)

    _task: asyncio.Task[None] | None = dataclasses.field(
        default=None, init=False, repr=False
    )

    async def __aenter__(self) -> HeartbeatManager:
        self._task = asyncio.create_task(
            self._beat(), name=f"jqueue-heartbeat-{self.job_id}"
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _beat(self) -> None:
        while True:
            await asyncio.sleep(self.interval.total_seconds())
            try:
                await self.queue.heartbeat(self.job_id)
            except JQueueError:
                # Job was acked or removed — stop silently.
                return
