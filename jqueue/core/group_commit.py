"""
GroupCommitLoop — serialize all mutations through a single asyncio writer task.

Algorithm (from the turbopuffer blog post)
------------------------------------------
When a write is in-flight, incoming operations accumulate in the pending buffer.
As soon as the write finishes, the buffer is flushed as the next CAS write.
This collapses N concurrent operations into O(1) storage writes.

Concretely:

  Caller 1: enqueue()    ──────────────────────────────────────────> [future]
  Caller 2: enqueue()    ──────────────────────────────────────────> [future]
  Caller 3: dequeue()    ──────────────────────────────────────────> [future]
                           ↓ batch = [op1, op2, op3]
  Writer:              read → apply op1, op2, op3 → CAS write → resolve futures

If the CAS write fails (concurrent external writer), the whole batch is
re-applied on a fresh state and retried.

Per-operation error isolation
------------------------------
If one mutation in a batch raises (e.g., JobNotFoundError), that future gets
the exception but the other mutations in the batch still commit normally.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from jqueue.core import codec
from jqueue.domain.errors import CASConflictError, JQueueError
from jqueue.domain.models import Job, JobStatus, QueueState
from jqueue.ports.storage import ObjectStoragePort

MutationFn = Callable[[QueueState], QueueState]

_MAX_RETRIES: int = 20


@dataclasses.dataclass
class _PendingOp:
    """A buffered mutation waiting to be committed to object storage."""

    fn: MutationFn
    future: asyncio.Future[None]


@dataclasses.dataclass
class GroupCommitLoop:
    """
    Serializes all storage mutations through a single asyncio writer task.

    Usage
    -----
        loop = GroupCommitLoop(storage=my_storage)
        await loop.start()
        try:
            job  = await loop.enqueue("send_email", b"payload")
            jobs = await loop.dequeue("send_email", batch_size=5)
        finally:
            await loop.stop()   # drains pending ops before shutting down
    """

    storage: ObjectStoragePort
    stale_timeout: timedelta = timedelta(minutes=5)

    _pending: list[_PendingOp] = dataclasses.field(
        default_factory=list, init=False, repr=False
    )
    _wakeup: asyncio.Event = dataclasses.field(
        default_factory=asyncio.Event, init=False, repr=False
    )
    _task: asyncio.Task[None] | None = dataclasses.field(
        default=None, init=False, repr=False
    )
    _stopped: bool = dataclasses.field(default=False, init=False, repr=False)

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Start the background writer task."""
        if self._task is not None:
            raise RuntimeError("GroupCommitLoop is already running")
        self._task = asyncio.create_task(
            self._writer_loop(), name="jqueue-group-commit-writer"
        )

    async def stop(self) -> None:
        """Signal shutdown and wait for the writer to drain pending ops."""
        self._stopped = True
        self._wakeup.set()
        if self._task is not None:
            await self._task
            self._task = None

    # ------------------------------------------------------------------ #
    # Public mutation API                                                  #
    # ------------------------------------------------------------------ #

    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes,
        priority: int = 0,
    ) -> Job:
        """Add a new job. Returns the committed Job (UUID stable across retries)."""
        job = Job.new(entrypoint, payload, priority)

        def _fn(state: QueueState) -> QueueState:
            return state.with_job_added(job)

        await self._submit(_fn)
        return job

    async def dequeue(
        self,
        entrypoint: str | None = None,
        *,
        batch_size: int = 1,
    ) -> list[Job]:
        """Claim up to batch_size QUEUED jobs and mark them IN_PROGRESS."""
        claimed: list[Job] = []

        def _fn(state: QueueState) -> QueueState:
            nonlocal claimed
            available = state.queued_jobs(entrypoint)[:batch_size]
            claimed = []
            new_state = state
            for job in available:
                updated = job.with_status(JobStatus.IN_PROGRESS).with_heartbeat(
                    datetime.now(UTC)
                )
                new_state = new_state.with_job_replaced(updated)
                claimed.append(updated)
            return new_state

        await self._submit(_fn)
        return claimed

    async def ack(self, job_id: str) -> None:
        """Remove a completed job from the queue."""
        await self._submit(lambda state: state.with_job_removed(job_id))

    async def nack(self, job_id: str) -> None:
        """Return a job to QUEUED status."""
        from jqueue.domain.errors import JobNotFoundError

        def _fn(state: QueueState) -> QueueState:
            job = state.find(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            return state.with_job_replaced(
                job.with_status(JobStatus.QUEUED).with_heartbeat(None)
            )

        await self._submit(_fn)

    async def heartbeat(self, job_id: str) -> None:
        """Refresh the heartbeat timestamp for an IN_PROGRESS job."""
        from jqueue.domain.errors import JobNotFoundError

        def _fn(state: QueueState) -> QueueState:
            job = state.find(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            return state.with_job_replaced(job.with_heartbeat(datetime.now(UTC)))

        await self._submit(_fn)

    async def read_state(self) -> QueueState:
        """Read-only snapshot of current queue state (bypasses the write pipeline)."""
        content, _ = await self.storage.read()
        return codec.decode(content)

    # ------------------------------------------------------------------ #
    # Internal machinery                                                   #
    # ------------------------------------------------------------------ #

    async def _submit(self, fn: MutationFn) -> None:
        """
        Enqueue a mutation and block until it is committed.

        Appends the op to _pending, wakes the writer, then awaits the future
        that resolves when the batch containing this op successfully commits.
        """
        if self._stopped:
            raise JQueueError("GroupCommitLoop is stopped")
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._pending.append(_PendingOp(fn=fn, future=future))
        self._wakeup.set()
        await future

    async def _writer_loop(self) -> None:
        """Background coroutine — runs until stopped and all pending ops drain."""
        while not self._stopped or self._pending:
            if not self._pending:
                self._wakeup.clear()
                await self._wakeup.wait()

            if not self._pending:
                continue

            batch = list(self._pending)
            self._pending.clear()
            await self._commit_batch(batch)

    async def _commit_batch(self, batch: list[_PendingOp]) -> None:
        """
        Apply all ops in `batch` to the current state and CAS write.

        Retries on CASConflictError. Per-mutation exceptions only fail that
        op's future; the rest of the batch still commits on the same write.
        """
        for attempt in range(_MAX_RETRIES):
            try:
                content, etag = await self.storage.read()
                state = codec.decode(content)

                # Sweep stale jobs on every write cycle (free — no extra I/O)
                cutoff = datetime.now(UTC) - self.stale_timeout
                state = state.requeue_stale(cutoff)

                per_op_errors: dict[int, Exception] = {}
                for i, op in enumerate(batch):
                    try:
                        state = op.fn(state)
                    except Exception as exc:
                        per_op_errors[i] = exc

                await self.storage.write(codec.encode(state), if_match=etag)

                for i, op in enumerate(batch):
                    if op.future.done():
                        continue
                    if i in per_op_errors:
                        op.future.set_exception(per_op_errors[i])
                    else:
                        op.future.set_result(None)
                return

            except CASConflictError:
                if attempt == _MAX_RETRIES - 1:
                    _fail_batch(batch, CASConflictError("Max CAS retries exceeded"))
                    return
                # Exponential back-off, capped at ~320 ms
                await asyncio.sleep(0.005 * (2 ** min(attempt, 6)))

            except Exception as exc:
                _fail_batch(batch, exc)
                return


def _fail_batch(batch: list[_PendingOp], exc: Exception) -> None:
    """Set exception on all unresolved futures in the batch."""
    for op in batch:
        if not op.future.done():
            op.future.set_exception(exc)
