import asyncio

import pytest

from jqueue.adapters.storage.memory import InMemoryStorage
from jqueue.core.broker import BrokerQueue
from jqueue.domain.errors import JobNotFoundError
from jqueue.domain.models import JobStatus

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

async def test_context_manager_starts_and_stops_loop() -> None:
    storage = InMemoryStorage()
    async with BrokerQueue(storage) as q:
        assert q._loop._task is not None
    assert q._loop._task is None


async def test_can_use_as_async_context_manager() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        job = await q.enqueue("task", b"data")
        assert job is not None


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------

async def test_enqueue_returns_job() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        job = await q.enqueue("send_email", b'{"to": "user@example.com"}')
        assert job.entrypoint == "send_email"
        assert job.status == JobStatus.QUEUED


async def test_enqueue_with_priority() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        job = await q.enqueue("task", b"data", priority=7)
        assert job.priority == 7


# ---------------------------------------------------------------------------
# dequeue
# ---------------------------------------------------------------------------

async def test_dequeue_returns_in_progress() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        await q.enqueue("task", b"data")
        result = await q.dequeue("task")
        assert len(result) == 1
        assert result[0].status == JobStatus.IN_PROGRESS


async def test_dequeue_empty_returns_empty_list() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        result = await q.dequeue("task")
        assert result == []


async def test_dequeue_batch_size() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        await asyncio.gather(
            q.enqueue("task", b"1"),
            q.enqueue("task", b"2"),
            q.enqueue("task", b"3"),
        )
        result = await q.dequeue("task", batch_size=2)
        assert len(result) == 2


async def test_dequeue_filters_by_entrypoint() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        await q.enqueue("email", b"1")
        await q.enqueue("sms", b"2")
        result = await q.dequeue("email")
        assert len(result) == 1
        assert result[0].entrypoint == "email"


# ---------------------------------------------------------------------------
# ack
# ---------------------------------------------------------------------------

async def test_ack_removes_job() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        job = await q.enqueue("task", b"data")
        [dequeued] = await q.dequeue("task")
        await q.ack(dequeued.id)
        state = await q.read_state()
        assert state.find(job.id) is None


# ---------------------------------------------------------------------------
# nack
# ---------------------------------------------------------------------------

async def test_nack_returns_to_queued() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        await q.enqueue("task", b"data")
        [job] = await q.dequeue("task")
        await q.nack(job.id)
        state = await q.read_state()
        stored = state.find(job.id)
        assert stored is not None
        assert stored.status == JobStatus.QUEUED
        assert stored.heartbeat_at is None


async def test_nack_missing_job_raises() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        with pytest.raises(JobNotFoundError):
            await q.nack("nonexistent")


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------

async def test_heartbeat_updates_timestamp() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        await q.enqueue("task", b"data")
        [job] = await q.dequeue("task")
        await asyncio.sleep(0.01)
        await q.heartbeat(job.id)
        state = await q.read_state()
        stored = state.find(job.id)
        assert stored is not None
        assert stored.heartbeat_at is not None
        assert stored.heartbeat_at > job.heartbeat_at  # type: ignore[operator]


async def test_heartbeat_missing_job_raises() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        with pytest.raises(JobNotFoundError):
            await q.heartbeat("nonexistent")


# ---------------------------------------------------------------------------
# read_state
# ---------------------------------------------------------------------------

async def test_read_state_empty() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        state = await q.read_state()
        assert state.jobs == ()


async def test_read_state_reflects_enqueue() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        await q.enqueue("task", b"data")
        state = await q.read_state()
        assert len(state.jobs) == 1


# ---------------------------------------------------------------------------
# End-to-end workflow
# ---------------------------------------------------------------------------

async def test_full_workflow() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        # Enqueue
        job = await q.enqueue("process_video", b'{"file": "video.mp4"}')
        assert job.status == JobStatus.QUEUED

        # Dequeue
        [claimed] = await q.dequeue("process_video")
        assert claimed.id == job.id
        assert claimed.status == JobStatus.IN_PROGRESS

        # Heartbeat
        await q.heartbeat(claimed.id)

        # Ack
        await q.ack(claimed.id)
        state = await q.read_state()
        assert state.find(job.id) is None


async def test_enqueue_dequeue_nack_requeue_workflow() -> None:
    async with BrokerQueue(InMemoryStorage()) as q:
        await q.enqueue("task", b"data")
        [job] = await q.dequeue("task")

        # Worker fails — nack
        await q.nack(job.id)

        # Job re-available
        [retry] = await q.dequeue("task")
        assert retry.id == job.id
        assert retry.status == JobStatus.IN_PROGRESS
