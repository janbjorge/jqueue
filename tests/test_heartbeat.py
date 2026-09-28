import asyncio
from datetime import timedelta

import pytest

from jqueue.adapters.storage.memory import InMemoryStorage
from jqueue.core.direct import DirectQueue
from jqueue.core.heartbeat import HeartbeatManager
from jqueue.domain.errors import (
    CASConflictError,
    JobNotFoundError,
    JobNotInProgressError,
    JQueueError,
    StorageError,
)

# ---------------------------------------------------------------------------
# Minimal queue stub
# ---------------------------------------------------------------------------


class _MockQueue:
    def __init__(self, side_effect: Exception | None = None) -> None:
        self.calls: list[str] = []
        self._side_effect = side_effect

    async def heartbeat(self, job_id: str) -> None:
        self.calls.append(job_id)
        if self._side_effect is not None:
            raise self._side_effect


# ---------------------------------------------------------------------------
# Basic operation
# ---------------------------------------------------------------------------


async def test_heartbeat_sends_at_interval() -> None:
    queue = _MockQueue()
    async with HeartbeatManager(
        queue=queue, job_id="job-1", interval=timedelta(milliseconds=10)
    ):
        await asyncio.sleep(0.08)  # ~8 intervals

    assert len(queue.calls) >= 3


async def test_heartbeat_calls_correct_job_id() -> None:
    queue = _MockQueue()
    async with HeartbeatManager(
        queue=queue, job_id="specific-job", interval=timedelta(milliseconds=10)
    ):
        await asyncio.sleep(0.05)

    assert all(jid == "specific-job" for jid in queue.calls)


async def test_heartbeat_stops_after_context_exit() -> None:
    queue = _MockQueue()
    async with HeartbeatManager(
        queue=queue, job_id="job-1", interval=timedelta(milliseconds=10)
    ):
        await asyncio.sleep(0.04)

    calls_at_exit = len(queue.calls)
    await asyncio.sleep(0.04)
    # No new calls after exit
    assert len(queue.calls) == calls_at_exit


# ---------------------------------------------------------------------------
# Task lifecycle
# ---------------------------------------------------------------------------


async def test_task_is_running_inside_context() -> None:
    queue = _MockQueue()
    hb = HeartbeatManager(queue=queue, job_id="j", interval=timedelta(seconds=100))
    async with hb:
        assert hb._task is not None
        assert not hb._task.done()


async def test_task_is_none_after_exit() -> None:
    queue = _MockQueue()
    hb = HeartbeatManager(queue=queue, job_id="j", interval=timedelta(seconds=100))
    async with hb:
        pass
    assert hb._task is None


async def test_exception_in_body_still_cancels_task() -> None:
    queue = _MockQueue()
    hb = HeartbeatManager(queue=queue, job_id="j", interval=timedelta(seconds=100))
    with pytest.raises(ValueError):
        async with hb:
            raise ValueError("worker error")
    assert hb._task is None


# ---------------------------------------------------------------------------
# JQueueError handling
# ---------------------------------------------------------------------------


async def test_job_not_found_stops_heartbeat_silently() -> None:
    """_beat() exits without raising once the job is gone."""
    queue = _MockQueue(side_effect=JobNotFoundError("job-1"))
    async with HeartbeatManager(
        queue=queue, job_id="job-1", interval=timedelta(milliseconds=5)
    ):
        await asyncio.sleep(0.04)

    assert len(queue.calls) == 1


async def test_job_not_in_progress_stops_heartbeat_silently() -> None:
    """_beat() exits once the claim is lost (job re-queued)."""
    queue = _MockQueue(side_effect=JobNotInProgressError("job-1", "queued"))
    async with HeartbeatManager(
        queue=queue, job_id="job-1", interval=timedelta(milliseconds=5)
    ):
        await asyncio.sleep(0.04)

    assert len(queue.calls) == 1


async def test_heartbeat_stops_after_real_stale_requeue() -> None:
    """End to end: a swept job stops its HeartbeatManager instead of reviving it."""
    queue = DirectQueue(InMemoryStorage())
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    await queue.requeue_stale(timedelta(seconds=-1))

    hb = HeartbeatManager(
        queue=queue, job_id=job.id, interval=timedelta(milliseconds=5)
    )
    async with hb:
        await asyncio.sleep(0.04)
        assert hb._task is not None
        assert hb._task.done()

    stored = (await queue.read_state()).find(job.id)
    assert stored is not None
    assert stored.heartbeat_at is None


@pytest.mark.parametrize(
    "error",
    [
        StorageError("S3 write failed", RuntimeError("timeout")),
        CASConflictError("Max CAS retries exceeded"),
        JQueueError("generic"),
    ],
)
async def test_transient_error_does_not_stop_heartbeat(error: Exception) -> None:
    """A transient JQueueError skips one beat; later beats are still sent."""

    class _FailOnce(_MockQueue):
        async def heartbeat(self, job_id: str) -> None:
            self.calls.append(job_id)
            if len(self.calls) == 1:
                raise error

    queue = _FailOnce()
    async with HeartbeatManager(
        queue=queue, job_id="job-1", interval=timedelta(milliseconds=5)
    ):
        await asyncio.sleep(0.05)

    assert len(queue.calls) >= 3


async def test_persistent_transient_error_keeps_retrying() -> None:
    """Heartbeats keep being attempted while storage stays unavailable."""
    queue = _MockQueue(side_effect=StorageError("down", RuntimeError("503")))
    async with HeartbeatManager(
        queue=queue, job_id="job-1", interval=timedelta(milliseconds=5)
    ):
        await asyncio.sleep(0.05)

    assert len(queue.calls) >= 3


async def test_non_jqueue_error_propagates_through_exit() -> None:
    """Non-JQueueError raised by queue.heartbeat propagates out of __aexit__."""
    queue = _MockQueue(side_effect=RuntimeError("unexpected"))
    hb = HeartbeatManager(
        queue=queue, job_id="job-1", interval=timedelta(milliseconds=5)
    )
    with pytest.raises(RuntimeError, match="unexpected"):
        async with hb:
            await asyncio.sleep(0.03)


# ---------------------------------------------------------------------------
# Default interval
# ---------------------------------------------------------------------------


def test_default_interval_is_60_seconds() -> None:
    queue = _MockQueue()
    hb = HeartbeatManager(queue=queue, job_id="j")
    assert hb.interval == timedelta(seconds=60)
