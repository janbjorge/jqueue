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

Cancellation
------------
An op whose caller is cancelled before the batch is applied is dropped. A
dequeue whose caller is cancelled after its claim was applied has its claim
handed back (jobs returned to QUEUED) in the next batch, so the jobs do not
sit IN_PROGRESS with no worker until the stale timeout.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from jqueue.core import codec
from jqueue.core.queries import StateQueries, check_batch_size
from jqueue.domain.errors import CASConflictError, JQueueError
from jqueue.domain.models import Job, QueueState
from jqueue.ports.storage import ObjectStoragePort

_MAX_RETRIES: int = 20


@dataclasses.dataclass
class _PendingOp[T]:
    """A buffered operation waiting to be committed to object storage."""

    fn: Callable[[StateQueries], T]
    future: asyncio.Future[T]
    undo: Callable[[StateQueries, T], object] | None = None


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

    _pending: list[_PendingOp[Any]] = dataclasses.field(
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
        return await self._submit(lambda q: q.add(job))

    async def dequeue(
        self,
        entrypoint: str | None = None,
        *,
        batch_size: int = 1,
    ) -> list[Job]:
        """
        Claim up to batch_size QUEUED jobs and mark them IN_PROGRESS.

        Raises ValueError if ``batch_size`` is less than 1.
        """
        check_batch_size(batch_size)
        return await self._submit(
            lambda q: q.claim(entrypoint, batch_size, datetime.now(UTC)),
            undo=lambda q, claimed: q.release_claims(claimed),
        )

    async def ack(self, job_id: str) -> None:
        """Remove a completed job from the queue."""
        await self._submit(lambda q: q.remove(job_id))

    async def nack(self, job_id: str) -> None:
        """Return a job to QUEUED status."""
        await self._submit(lambda q: q.release(job_id))

    async def heartbeat(self, job_id: str) -> None:
        """Refresh the heartbeat timestamp for an IN_PROGRESS job."""
        await self._submit(lambda q: q.touch(job_id, datetime.now(UTC)))

    async def read_state(self) -> QueueState:
        """Read-only snapshot of current queue state (bypasses the write pipeline)."""
        content, _ = await self.storage.read()
        return codec.decode(content)

    # ------------------------------------------------------------------ #
    # Internal machinery                                                   #
    # ------------------------------------------------------------------ #

    async def _submit[T](
        self,
        fn: Callable[[StateQueries], T],
        undo: Callable[[StateQueries, T], object] | None = None,
    ) -> T:
        """
        Enqueue an operation and block until it is committed.

        Appends the op to _pending, wakes the writer, then awaits the future
        that resolves when the batch containing this op successfully commits.
        If the caller is cancelled after the op committed, ``undo(queries,
        result)`` is submitted as a follow-up op.
        """
        if self._stopped:
            raise JQueueError("GroupCommitLoop is stopped")
        if self._task is None or self._task.done():
            # Nobody would ever resolve the future — fail instead of hanging.
            raise JQueueError("GroupCommitLoop is not running; call start() first")
        future: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        self._pending.append(_PendingOp(fn=fn, future=future, undo=undo))
        self._wakeup.set()
        return await future

    def _submit_undo[T](
        self, undo: Callable[[StateQueries, T], object], result: T
    ) -> None:
        """Queue a compensating op nobody awaits (bypasses the stopped check)."""
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        # Mark any failure as retrieved; the stale sweep is the fallback.
        future.add_done_callback(lambda f: f.cancelled() or f.exception())
        self._pending.append(_PendingOp(fn=lambda q: undo(q, result), future=future))
        self._wakeup.set()

    async def _writer_loop(self) -> None:
        """
        Background coroutine — runs until stopped and all pending ops drain.

        If the task dies (e.g. it is cancelled), every op still waiting —
        in flight or pending — is failed so no caller hangs.
        """
        batch: list[_PendingOp[Any]] = []
        try:
            while not self._stopped or self._pending:
                if not self._pending:
                    self._wakeup.clear()
                    await self._wakeup.wait()

                if not self._pending:
                    continue

                batch = list(self._pending)
                self._pending.clear()
                await self._commit_batch(batch)
                batch = []
        finally:
            orphaned = batch + self._pending
            self._pending.clear()
            _fail_batch(orphaned, JQueueError("GroupCommitLoop writer stopped"))

    async def _commit_batch(self, batch: list[_PendingOp[Any]]) -> None:
        """
        Apply all ops in `batch` to the current state and CAS write.

        Retries on CASConflictError. Per-mutation exceptions only fail that
        op's future; the rest of the batch still commits on the same write.
        If the batch leaves the state unchanged, no write is made.
        """
        for attempt in range(_MAX_RETRIES):
            try:
                content, etag = await self.storage.read()
                snapshot = codec.decode(content)
                queries = StateQueries(snapshot)

                # Sweep stale jobs on every write cycle (free — no extra I/O)
                queries.requeue_stale(datetime.now(UTC) - self.stale_timeout)

                results: dict[int, Any] = {}
                per_op_errors: dict[int, Exception] = {}
                for i, op in enumerate(batch):
                    if op.future.done():
                        # Caller cancelled before we applied it — drop it.
                        continue
                    try:
                        results[i] = op.fn(queries)
                    except Exception as exc:
                        per_op_errors[i] = exc

                # Nothing changed (e.g. only empty dequeues / failed ops):
                # skip the write, the snapshot is a valid linearization point.
                if queries.state is not snapshot:
                    await self.storage.write(codec.encode(queries.state), if_match=etag)

                for i, op in enumerate(batch):
                    if op.future.done():
                        # Cancelled while the write was in flight: the op
                        # committed but nobody will receive its result.
                        if op.undo is not None and i in results:
                            self._submit_undo(op.undo, results[i])
                        continue
                    if i in per_op_errors:
                        op.future.set_exception(per_op_errors[i])
                    else:
                        op.future.set_result(results[i])
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


def _fail_batch(batch: list[_PendingOp[Any]], exc: Exception) -> None:
    """Set exception on all unresolved futures in the batch."""
    for op in batch:
        if not op.future.done():
            op.future.set_exception(exc)
