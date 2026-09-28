import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from jqueue.adapters.storage.memory import InMemoryStorage
from jqueue.core.group_commit import GroupCommitLoop
from jqueue.domain.errors import CASConflictError, JobNotFoundError, JQueueError
from jqueue.domain.models import JobStatus

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def loop() -> AsyncIterator[GroupCommitLoop]:
    storage = InMemoryStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    yield gcl
    await gcl.stop()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_creates_task() -> None:
    storage = InMemoryStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    assert gcl._task is not None
    await gcl.stop()


async def test_stop_clears_task() -> None:
    storage = InMemoryStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    await gcl.stop()
    assert gcl._task is None


async def test_double_start_raises() -> None:
    storage = InMemoryStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    try:
        with pytest.raises(RuntimeError):
            await gcl.start()
    finally:
        await gcl.stop()


async def test_submit_after_stop_raises() -> None:
    storage = InMemoryStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    await gcl.stop()
    with pytest.raises(JQueueError):
        await gcl.enqueue("task", b"data")


async def test_stop_drains_pending_ops() -> None:
    storage = InMemoryStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    enqueue_task = asyncio.create_task(gcl.enqueue("task", b"data"))
    # Yield so the task runs until it's waiting on its future (op is in _pending)
    await asyncio.sleep(0)
    await gcl.stop()
    job = await enqueue_task
    state = await gcl.read_state()
    assert state.find(job.id) is not None


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------


async def test_enqueue_returns_job(loop: GroupCommitLoop) -> None:
    job = await loop.enqueue("send_email", b"payload")
    assert job.entrypoint == "send_email"
    assert job.payload == b"payload"
    assert job.status == JobStatus.QUEUED


async def test_enqueue_stores_in_state(loop: GroupCommitLoop) -> None:
    job = await loop.enqueue("task", b"data")
    state = await loop.read_state()
    assert state.find(job.id) is not None


# ---------------------------------------------------------------------------
# dequeue
# ---------------------------------------------------------------------------


async def test_dequeue_returns_in_progress(loop: GroupCommitLoop) -> None:
    await loop.enqueue("task", b"data")
    result = await loop.dequeue("task")
    assert len(result) == 1
    assert result[0].status == JobStatus.IN_PROGRESS


async def test_dequeue_empty_returns_empty_list(loop: GroupCommitLoop) -> None:
    result = await loop.dequeue("task")
    assert result == []


async def test_dequeue_filters_by_entrypoint(loop: GroupCommitLoop) -> None:
    await loop.enqueue("email", b"1")
    await loop.enqueue("sms", b"2")
    result = await loop.dequeue("email")
    assert len(result) == 1
    assert result[0].entrypoint == "email"


async def test_dequeue_batch_size(loop: GroupCommitLoop) -> None:
    for i in range(5):
        await loop.enqueue("task", f"item{i}".encode())
    result = await loop.dequeue("task", batch_size=3)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# ack
# ---------------------------------------------------------------------------


async def test_ack_removes_job(loop: GroupCommitLoop) -> None:
    job = await loop.enqueue("task", b"data")
    [dequeued] = await loop.dequeue("task")
    await loop.ack(dequeued.id)
    state = await loop.read_state()
    assert state.find(job.id) is None


# ---------------------------------------------------------------------------
# nack
# ---------------------------------------------------------------------------


async def test_nack_returns_to_queued(loop: GroupCommitLoop) -> None:
    await loop.enqueue("task", b"data")
    [job] = await loop.dequeue("task")
    await loop.nack(job.id)
    state = await loop.read_state()
    stored = state.find(job.id)
    assert stored is not None
    assert stored.status == JobStatus.QUEUED
    assert stored.heartbeat_at is None


async def test_nack_missing_job_raises(loop: GroupCommitLoop) -> None:
    with pytest.raises(JobNotFoundError):
        await loop.nack("nonexistent")


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------


async def test_heartbeat_updates_timestamp(loop: GroupCommitLoop) -> None:
    await loop.enqueue("task", b"data")
    [job] = await loop.dequeue("task")
    await asyncio.sleep(0.01)
    await loop.heartbeat(job.id)
    state = await loop.read_state()
    stored = state.find(job.id)
    assert stored is not None
    assert stored.heartbeat_at is not None
    assert stored.heartbeat_at > job.heartbeat_at  # type: ignore[operator]


async def test_heartbeat_missing_job_raises(loop: GroupCommitLoop) -> None:
    with pytest.raises(JobNotFoundError):
        await loop.heartbeat("nonexistent")


# ---------------------------------------------------------------------------
# read_state
# ---------------------------------------------------------------------------


async def test_read_state_empty(loop: GroupCommitLoop) -> None:
    state = await loop.read_state()
    assert state.jobs == ()


async def test_read_state_after_enqueue(loop: GroupCommitLoop) -> None:
    await loop.enqueue("task", b"data")
    state = await loop.read_state()
    assert len(state.jobs) == 1


# ---------------------------------------------------------------------------
# Concurrency — batching
# ---------------------------------------------------------------------------


async def test_concurrent_enqueues_all_committed(loop: GroupCommitLoop) -> None:
    jobs = await asyncio.gather(
        loop.enqueue("task", b"1"),
        loop.enqueue("task", b"2"),
        loop.enqueue("task", b"3"),
    )
    assert len(jobs) == 3
    state = await loop.read_state()
    assert len(state.jobs) == 3


async def test_writer_loop_survives_op_error(loop: GroupCommitLoop) -> None:
    """A failing mutation should not crash the writer loop."""
    with pytest.raises(JobNotFoundError):
        await loop.nack("nonexistent")
    # Loop still works after the error
    job = await loop.enqueue("task", b"new")
    assert job is not None


async def test_per_op_error_isolation(loop: GroupCommitLoop) -> None:
    """A failing op should not prevent a valid concurrent op from committing."""
    await loop.enqueue("task", b"seed")
    [seed_job] = await loop.dequeue("task")
    _ = seed_job  # dequeued so state is modified

    nack_task = asyncio.create_task(loop.nack("nonexistent-id"))
    enqueue_task = asyncio.create_task(loop.enqueue("task", b"new"))

    results = await asyncio.gather(nack_task, enqueue_task, return_exceptions=True)
    assert isinstance(results[0], JobNotFoundError)
    assert not isinstance(results[1], Exception)

    state = await loop.read_state()
    new_job = results[1]
    assert not isinstance(new_job, BaseException)
    assert state.find(new_job.id) is not None


# ---------------------------------------------------------------------------
# Stale-job requeue on write cycle
# ---------------------------------------------------------------------------


async def test_stale_jobs_requeued_on_write_cycle() -> None:
    """GroupCommitLoop auto-requeues stale IN_PROGRESS jobs every write cycle."""
    storage = InMemoryStorage()
    # negative timedelta means cutoff is in the future → every job is stale
    gcl = GroupCommitLoop(storage=storage, stale_timeout=timedelta(milliseconds=-100))
    await gcl.start()
    try:
        await gcl.enqueue("task", b"data")
        [job] = await gcl.dequeue("task")
        assert job.status == JobStatus.IN_PROGRESS

        # Trigger another write cycle
        await gcl.enqueue("task", b"trigger")

        state = await gcl.read_state()
        original = state.find(job.id)
        assert original is not None
        assert original.status == JobStatus.QUEUED
    finally:
        await gcl.stop()


# ---------------------------------------------------------------------------
# CAS retry
# ---------------------------------------------------------------------------


async def test_commit_batch_retries_on_cas_conflict() -> None:
    """GroupCommitLoop should retry on CASConflictError up to _MAX_RETRIES times."""
    real_storage = InMemoryStorage()
    fail_count = 0
    original_write = real_storage.write

    async def flaky_write(content: bytes, if_match: str | None = None) -> str:
        nonlocal fail_count
        if fail_count < 3:
            fail_count += 1
            raise CASConflictError("simulated")
        return await original_write(content, if_match)

    real_storage.write = flaky_write  # type: ignore[method-assign]
    gcl = GroupCommitLoop(storage=real_storage)
    await gcl.start()
    try:
        job = await gcl.enqueue("task", b"data")
        assert job is not None
        assert fail_count == 3
    finally:
        await gcl.stop()


# ---------------------------------------------------------------------------
# No-op batches skip the write
# ---------------------------------------------------------------------------


class _CountingStorage(InMemoryStorage):
    """InMemoryStorage that counts successful writes."""

    writes: int = 0

    async def write(self, content: bytes, if_match: str | None = None) -> str:
        etag = await super().write(content, if_match)
        self.writes += 1
        return etag


async def test_noop_batch_does_not_write() -> None:
    storage = _CountingStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    try:
        await gcl.enqueue("task", b"data")
        writes = storage.writes

        results = await asyncio.gather(
            gcl.dequeue("other"),
            gcl.dequeue("other"),
            gcl.ack("nonexistent"),
            return_exceptions=True,
        )

        assert results[0] == []
        assert results[1] == []
        assert isinstance(results[2], JobNotFoundError)
        assert storage.writes == writes
    finally:
        await gcl.stop()


async def test_batch_with_one_change_still_writes_once() -> None:
    storage = _CountingStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    try:
        await gcl.enqueue("task", b"data")
        writes = storage.writes

        empty, claimed = await asyncio.gather(gcl.dequeue("other"), gcl.dequeue("task"))

        assert empty == []
        assert len(claimed) == 1
        assert storage.writes == writes + 1
        state = await gcl.read_state()
        assert state.jobs[0].status == JobStatus.IN_PROGRESS
    finally:
        await gcl.stop()


async def test_stale_sweep_alone_still_writes() -> None:
    """A batch whose only change is the stale sweep must persist it."""
    storage = _CountingStorage()
    gcl = GroupCommitLoop(storage=storage, stale_timeout=timedelta(0))
    await gcl.start()
    try:
        await gcl.enqueue("task", b"data")
        await gcl.dequeue("task")
        writes = storage.writes

        assert await gcl.dequeue("other") == []

        assert storage.writes == writes + 1
        state = await gcl.read_state()
        assert state.jobs[0].status == JobStatus.QUEUED
    finally:
        await gcl.stop()


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
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    try:
        job = await gcl.enqueue("task", b"data")
        storage.armed = True
        task = asyncio.create_task(gcl.dequeue("task"))
        await storage.write_started.wait()

        task.cancel()
        storage.armed = False
        storage.gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _settle()

        stored = (await gcl.read_state()).find(job.id)
        assert stored is not None
        assert stored.status == JobStatus.QUEUED
        assert stored.heartbeat_at is None
        # Nothing was lost: another worker can claim it right away.
        [claimed] = await gcl.dequeue("task")
        assert claimed.id == job.id
    finally:
        await gcl.stop()


async def test_op_cancelled_before_apply_is_dropped() -> None:
    storage = _GatedStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    try:
        await gcl.enqueue("task", b"first")
        storage.armed = True
        # Occupy the writer so the next ops wait in _pending.
        blocker = asyncio.create_task(gcl.enqueue("task", b"blocker"))
        await storage.write_started.wait()
        cancelled_dequeue = asyncio.create_task(gcl.dequeue("task"))
        cancelled_enqueue = asyncio.create_task(gcl.enqueue("task", b"never"))
        await _settle()
        cancelled_dequeue.cancel()
        cancelled_enqueue.cancel()
        await _settle()

        storage.armed = False
        storage.gate.set()
        await blocker
        await _settle()

        state = await gcl.read_state()
        assert state.in_progress_jobs() == ()
        assert sorted(j.payload for j in state.jobs) == [b"blocker", b"first"]
    finally:
        await gcl.stop()


async def test_uncancelled_dequeue_keeps_claim() -> None:
    storage = _GatedStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    try:
        job = await gcl.enqueue("task", b"data")
        storage.armed = True
        task = asyncio.create_task(gcl.dequeue("task"))
        await storage.write_started.wait()
        storage.armed = False
        storage.gate.set()

        [claimed] = await task
        await _settle()

        assert claimed.id == job.id
        stored = (await gcl.read_state()).find(job.id)
        assert stored is not None
        assert stored.status == JobStatus.IN_PROGRESS
    finally:
        await gcl.stop()


async def test_cancelled_dequeue_does_not_release_job_heartbeated_since() -> None:
    """The compensating release only touches jobs still as the claim left them."""
    storage = _GatedStorage()
    gcl = GroupCommitLoop(storage=storage)
    await gcl.start()
    try:
        job = await gcl.enqueue("task", b"data")
        storage.armed = True
        task = asyncio.create_task(gcl.dequeue("task"))
        await storage.write_started.wait()
        task.cancel()
        # Queued behind the in-flight write, ahead of the compensating op.
        hb = asyncio.create_task(gcl.heartbeat(job.id))
        await _settle()
        storage.armed = False
        storage.gate.set()
        await hb
        await _settle()

        stored = (await gcl.read_state()).find(job.id)
        assert stored is not None
        assert stored.status == JobStatus.IN_PROGRESS
    finally:
        await gcl.stop()


# ---------------------------------------------------------------------------
# batch_size validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("batch_size", [0, -1])
async def test_dequeue_rejects_non_positive_batch_size(
    loop: GroupCommitLoop, batch_size: int
) -> None:
    await loop.enqueue("task", b"a")
    await loop.enqueue("task", b"b")

    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        await loop.dequeue("task", batch_size=batch_size)

    assert loop._pending == []
    assert (await loop.read_state()).in_progress_jobs() == ()


async def test_dequeue_batch_size_larger_than_queue_is_accepted(
    loop: GroupCommitLoop,
) -> None:
    await loop.enqueue("task", b"a")
    assert len(await loop.dequeue("task", batch_size=50)) == 1
