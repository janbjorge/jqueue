import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from jqueue.adapters.storage.memory import InMemoryStorage
from jqueue.core.direct import DirectQueue
from jqueue.domain.errors import (
    CASConflictError,
    JobNotFoundError,
    JobNotInProgressError,
)
from jqueue.domain.models import JobStatus

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def storage() -> InMemoryStorage:
    return InMemoryStorage()


@pytest.fixture
def queue(storage: InMemoryStorage) -> DirectQueue:
    return DirectQueue(storage=storage)


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------


async def test_enqueue_returns_job(queue: DirectQueue) -> None:
    job = await queue.enqueue("send_email", b"payload")
    assert job.entrypoint == "send_email"
    assert job.payload == b"payload"
    assert job.status == JobStatus.QUEUED
    assert job.priority == 0


async def test_enqueue_with_priority(queue: DirectQueue) -> None:
    job = await queue.enqueue("task", b"data", priority=5)
    assert job.priority == 5


async def test_enqueue_stores_job_in_state(queue: DirectQueue) -> None:
    job = await queue.enqueue("task", b"data")
    state = await queue.read_state()
    assert state.find(job.id) is not None


async def test_enqueue_increments_version(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"1")
    await queue.enqueue("task", b"2")
    state = await queue.read_state()
    assert state.version == 2


# ---------------------------------------------------------------------------
# dequeue
# ---------------------------------------------------------------------------


async def test_dequeue_empty_queue_returns_empty_list(queue: DirectQueue) -> None:
    result = await queue.dequeue("task")
    assert result == []


async def test_dequeue_returns_job(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    result = await queue.dequeue("task")
    assert len(result) == 1
    assert result[0].status == JobStatus.IN_PROGRESS


async def test_dequeue_sets_heartbeat(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    assert job.heartbeat_at is not None


async def test_dequeue_marks_job_in_progress_in_storage(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    state = await queue.read_state()
    stored = state.find(job.id)
    assert stored is not None
    assert stored.status == JobStatus.IN_PROGRESS


async def test_dequeue_filters_by_entrypoint(queue: DirectQueue) -> None:
    await queue.enqueue("email", b"1")
    await queue.enqueue("sms", b"2")
    result = await queue.dequeue("email")
    assert len(result) == 1
    assert result[0].entrypoint == "email"


async def test_dequeue_wrong_entrypoint_returns_empty(queue: DirectQueue) -> None:
    await queue.enqueue("email", b"1")
    result = await queue.dequeue("sms")
    assert result == []


async def test_dequeue_none_entrypoint_gets_all(queue: DirectQueue) -> None:
    await queue.enqueue("email", b"1")
    await queue.enqueue("sms", b"2")
    result = await queue.dequeue(None, batch_size=10)
    assert len(result) == 2


async def test_dequeue_batch_size(queue: DirectQueue) -> None:
    for i in range(5):
        await queue.enqueue("task", f"data{i}".encode())
    result = await queue.dequeue("task", batch_size=3)
    assert len(result) == 3


async def test_dequeue_default_batch_size_is_one(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"1")
    await queue.enqueue("task", b"2")
    result = await queue.dequeue("task")
    assert len(result) == 1


async def test_dequeue_respects_priority_order(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"low-prio", priority=10)
    await queue.enqueue("task", b"high-prio", priority=1)
    [job] = await queue.dequeue("task")
    assert job.payload == b"high-prio"


# ---------------------------------------------------------------------------
# ack
# ---------------------------------------------------------------------------


async def test_ack_removes_job(queue: DirectQueue) -> None:
    job = await queue.enqueue("task", b"data")
    [dequeued] = await queue.dequeue("task")
    await queue.ack(dequeued.id)
    state = await queue.read_state()
    assert state.find(job.id) is None


async def test_ack_missing_job_raises(queue: DirectQueue) -> None:
    with pytest.raises(JobNotFoundError):
        await queue.ack("nonexistent-id")


# ---------------------------------------------------------------------------
# nack
# ---------------------------------------------------------------------------


async def test_nack_returns_job_to_queued(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    await queue.nack(job.id)
    state = await queue.read_state()
    stored = state.find(job.id)
    assert stored is not None
    assert stored.status == JobStatus.QUEUED
    assert stored.heartbeat_at is None


async def test_nack_missing_job_raises(queue: DirectQueue) -> None:
    with pytest.raises(JobNotFoundError):
        await queue.nack("nonexistent-id")


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------


async def test_heartbeat_updates_timestamp(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    old_hb = job.heartbeat_at
    await asyncio.sleep(0.01)
    await queue.heartbeat(job.id)
    state = await queue.read_state()
    stored = state.find(job.id)
    assert stored is not None
    assert stored.heartbeat_at is not None
    assert stored.heartbeat_at > old_hb  # type: ignore[operator]


async def test_heartbeat_missing_job_raises(queue: DirectQueue) -> None:
    with pytest.raises(JobNotFoundError):
        await queue.heartbeat("nonexistent-id")


async def test_heartbeat_queued_job_raises(queue: DirectQueue) -> None:
    job = await queue.enqueue("task", b"data")
    with pytest.raises(JobNotInProgressError):
        await queue.heartbeat(job.id)
    stored = (await queue.read_state()).find(job.id)
    assert stored is not None
    assert stored.heartbeat_at is None


async def test_heartbeat_after_stale_requeue_raises(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    assert await queue.requeue_stale(timedelta(seconds=-1)) == 1
    with pytest.raises(JobNotInProgressError):
        await queue.heartbeat(job.id)
    stored = (await queue.read_state()).find(job.id)
    assert stored is not None
    assert stored.status == JobStatus.QUEUED
    assert stored.heartbeat_at is None


# ---------------------------------------------------------------------------
# requeue_stale
# ---------------------------------------------------------------------------


async def test_requeue_stale_no_stale_jobs_returns_zero(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    count = await queue.requeue_stale(timedelta(hours=1))
    assert count == 0


async def test_requeue_stale_requeues_old_in_progress(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    count = await queue.requeue_stale(timedelta(seconds=0))
    assert count == 1
    state = await queue.read_state()
    stored = state.find(job.id)
    assert stored is not None
    assert stored.status == JobStatus.QUEUED


async def test_requeue_stale_returns_count(queue: DirectQueue) -> None:
    for _ in range(3):
        await queue.enqueue("task", b"data")
    await queue.dequeue("task", batch_size=3)
    count = await queue.requeue_stale(timedelta(seconds=0))
    assert count == 3


# ---------------------------------------------------------------------------
# read_state
# ---------------------------------------------------------------------------


async def test_read_state_empty(queue: DirectQueue) -> None:
    state = await queue.read_state()
    assert state.jobs == ()
    assert state.version == 0


async def test_read_state_reflects_mutations(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"data")
    state = await queue.read_state()
    assert len(state.jobs) == 1


# ---------------------------------------------------------------------------
# CAS retry behaviour
# ---------------------------------------------------------------------------


async def test_cas_retry_on_conflict_eventually_succeeds() -> None:
    real_storage = InMemoryStorage()
    fail_count = 0
    original_write = real_storage.write

    async def flaky_write(content: bytes, if_match: str | None = None) -> str:
        nonlocal fail_count
        if fail_count < 2:
            fail_count += 1
            raise CASConflictError("simulated conflict")
        return await original_write(content, if_match)

    real_storage.write = flaky_write  # type: ignore[method-assign]
    q = DirectQueue(storage=real_storage)
    job = await q.enqueue("task", b"data")
    assert job is not None
    assert fail_count == 2


async def test_cas_exhausted_raises_conflict() -> None:
    real_storage = InMemoryStorage()
    real_storage.write = AsyncMock(  # type: ignore[method-assign]
        side_effect=CASConflictError("always fails")
    )
    q = DirectQueue(storage=real_storage, max_retries=2)
    with pytest.raises(CASConflictError):
        await q.enqueue("task", b"data")


# ---------------------------------------------------------------------------
# No-op operations skip the write
# ---------------------------------------------------------------------------


class _CountingStorage(InMemoryStorage):
    writes: int = 0

    async def write(self, content: bytes, if_match: str | None = None) -> str:
        etag = await super().write(content, if_match)
        self.writes += 1
        return etag


async def test_empty_dequeue_does_not_write() -> None:
    storage = _CountingStorage()
    queue = DirectQueue(storage)
    await queue.enqueue("task", b"data")
    assert storage.writes == 1
    _, etag_before = await storage.read()

    assert await queue.dequeue("other") == []

    _, etag_after = await storage.read()
    assert storage.writes == 1
    assert etag_after == etag_before


async def test_dequeue_on_missing_object_does_not_create_it() -> None:
    storage = _CountingStorage()
    queue = DirectQueue(storage)
    assert await queue.dequeue() == []
    assert storage.writes == 0
    assert await storage.read() == (b"", None)


async def test_requeue_stale_with_nothing_stale_does_not_write() -> None:
    storage = _CountingStorage()
    queue = DirectQueue(storage)
    await queue.enqueue("task", b"data")
    await queue.dequeue("task")
    writes = storage.writes

    assert await queue.requeue_stale(timedelta(hours=1)) == 0
    assert storage.writes == writes


async def test_failed_op_does_not_write() -> None:
    storage = _CountingStorage()
    queue = DirectQueue(storage)
    await queue.enqueue("task", b"data")
    with pytest.raises(JobNotFoundError):
        await queue.ack("nonexistent-id")
    assert storage.writes == 1


async def test_state_changing_ops_still_write() -> None:
    storage = _CountingStorage()
    queue = DirectQueue(storage)
    await queue.enqueue("task", b"data")
    [job] = await queue.dequeue("task")
    await queue.heartbeat(job.id)
    await queue.nack(job.id)
    await queue.dequeue("task")
    assert await queue.requeue_stale(timedelta(seconds=-1)) == 1
    await queue.ack(job.id)
    assert storage.writes == 7
    assert (await queue.read_state()).jobs == ()


async def test_empty_dequeue_does_not_conflict_with_concurrent_writer() -> None:
    storage = AsyncMock(wraps=InMemoryStorage())
    queue = DirectQueue(storage)
    await queue.enqueue("task", b"data")
    await queue.dequeue("task")
    storage.write.reset_mock()

    assert await queue.dequeue("task") == []
    storage.write.assert_not_called()


# ---------------------------------------------------------------------------
# Cancelled dequeue does not orphan claimed jobs
# ---------------------------------------------------------------------------


class _GatedStorage(InMemoryStorage):
    """InMemoryStorage whose writes wait on ``gate`` once ``armed``."""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.armed = False
        self.gate = asyncio.Event()
        self.write_started = asyncio.Event()

    async def write(self, content: bytes, if_match: str | None = None) -> str:
        if self.armed:
            self.write_started.set()
            await self.gate.wait()
        return await super().write(content, if_match)


async def _settle() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


async def test_dequeue_cancelled_during_write_releases_claim() -> None:
    storage = _GatedStorage()
    queue = DirectQueue(storage)
    job = await queue.enqueue("task", b"data")
    storage.armed = True
    task = asyncio.create_task(queue.dequeue("task"))
    await storage.write_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    storage.armed = False
    storage.gate.set()
    await _settle()

    assert queue._cleanup == set()
    stored = (await queue.read_state()).find(job.id)
    assert stored is not None
    assert stored.status == JobStatus.QUEUED
    assert stored.heartbeat_at is None


async def test_uncancelled_dequeue_keeps_claim_direct() -> None:
    storage = _GatedStorage()
    queue = DirectQueue(storage)
    job = await queue.enqueue("task", b"data")
    storage.armed = True
    task = asyncio.create_task(queue.dequeue("task"))
    await storage.write_started.wait()
    storage.armed = False
    storage.gate.set()

    [claimed] = await task
    await _settle()

    assert claimed.id == job.id
    stored = (await queue.read_state()).find(job.id)
    assert stored is not None
    assert stored.status == JobStatus.IN_PROGRESS


async def test_cancelled_dequeue_with_failed_claim_leaves_state_alone() -> None:
    storage = _GatedStorage()
    queue = DirectQueue(storage, max_retries=1)
    job = await queue.enqueue("task", b"data")
    storage.armed = True
    task = asyncio.create_task(queue.dequeue("task"))
    await storage.write_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # A concurrent writer wins, so the gated claim hits a CAS conflict.
    storage.armed = False
    other = DirectQueue(storage)
    await other.enqueue("task", b"other")
    storage.gate.set()
    await _settle()

    assert queue._cleanup == set()
    state = await queue.read_state()
    assert state.in_progress_jobs() == ()
    assert state.find(job.id) is not None


# ---------------------------------------------------------------------------
# batch_size validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("batch_size", [0, -1, -100])
async def test_dequeue_rejects_non_positive_batch_size(batch_size: int) -> None:
    storage = AsyncMock(wraps=InMemoryStorage())
    queue = DirectQueue(storage)
    await queue.enqueue("task", b"a")
    await queue.enqueue("task", b"b")
    storage.reset_mock()

    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        await queue.dequeue("task", batch_size=batch_size)

    storage.read.assert_not_called()
    storage.write.assert_not_called()
    assert (await queue.read_state()).in_progress_jobs() == ()


async def test_dequeue_batch_size_one_is_accepted(queue: DirectQueue) -> None:
    await queue.enqueue("task", b"a")
    await queue.enqueue("task", b"b")
    assert len(await queue.dequeue("task", batch_size=1)) == 1
