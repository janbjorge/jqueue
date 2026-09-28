from datetime import UTC, datetime, timedelta

import pytest

from jqueue.core.queries import StateQueries
from jqueue.domain.errors import JobNotFoundError, JobNotInProgressError
from jqueue.domain.models import Job, JobStatus, QueueState

NOW = datetime(2024, 1, 1, tzinfo=UTC)


def _queries(*jobs: Job) -> StateQueries:
    return StateQueries(QueueState(jobs=jobs))


# ---------------------------------------------------------------------------
# find / add
# ---------------------------------------------------------------------------


def test_find_returns_none_when_absent() -> None:
    assert _queries().find("missing") is None


def test_add_returns_job_and_updates_state() -> None:
    q = _queries()
    job = Job.new("task", b"data")
    assert q.add(job) is job
    assert q.find(job.id) == job
    assert q.state.version == 1


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------


def test_claim_empty_returns_empty_list() -> None:
    q = _queries()
    assert q.claim(None, 5, NOW) == []
    assert q.state.version == 0


def test_claim_marks_in_progress_with_heartbeat() -> None:
    q = _queries(Job.new("task", b"a"))
    [claimed] = q.claim("task", 1, NOW)
    assert claimed.status == JobStatus.IN_PROGRESS
    assert claimed.heartbeat_at == NOW
    assert q.find(claimed.id) == claimed


def test_claim_respects_priority_batch_size_and_entrypoint() -> None:
    low = Job.new("task", b"low", priority=5)
    high = Job.new("task", b"high", priority=0)
    other = Job.new("other", b"x")
    q = _queries(low, high, other)
    claimed = q.claim("task", 1, NOW)
    assert [j.id for j in claimed] == [high.id]
    assert [j.id for j in q.state.queued_jobs()] == [other.id, low.id]


# ---------------------------------------------------------------------------
# remove / release / touch
# ---------------------------------------------------------------------------


def test_remove_returns_removed_job() -> None:
    job = Job.new("task", b"data")
    q = _queries(job)
    assert q.remove(job.id) == job
    assert q.state.jobs == ()


def test_release_returns_job_to_queued() -> None:
    job = Job.new("task", b"data")
    q = _queries(job)
    [claimed] = q.claim(None, 1, NOW)
    released = q.release(claimed.id)
    assert released.status == JobStatus.QUEUED
    assert released.heartbeat_at is None


def test_touch_updates_heartbeat() -> None:
    job = Job.new("task", b"data")
    q = _queries(job)
    q.claim(None, 1, NOW)
    later = NOW + timedelta(seconds=30)
    assert q.touch(job.id, later).heartbeat_at == later


def test_touch_queued_job_raises_and_leaves_state_unchanged() -> None:
    job = Job.new("task", b"data")
    q = _queries(job)
    before = q.state
    with pytest.raises(JobNotInProgressError) as exc_info:
        q.touch(job.id, NOW)
    assert exc_info.value.job_id == job.id
    assert exc_info.value.status == JobStatus.QUEUED
    assert q.state is before


def test_touch_after_stale_requeue_raises() -> None:
    job = Job.new("task", b"data")
    q = _queries(job)
    q.claim(None, 1, NOW)
    q.requeue_stale(NOW + timedelta(seconds=1))
    with pytest.raises(JobNotInProgressError):
        q.touch(job.id, NOW + timedelta(seconds=2))
    stored = q.find(job.id)
    assert stored is not None
    assert stored.heartbeat_at is None


@pytest.mark.parametrize("op", ["remove", "release", "touch"])
def test_missing_job_raises_and_leaves_state_unchanged(op: str) -> None:
    q = _queries(Job.new("task", b"data"))
    before = q.state
    with pytest.raises(JobNotFoundError):
        if op == "touch":
            q.touch("missing", NOW)
        else:
            getattr(q, op)("missing")
    assert q.state is before


# ---------------------------------------------------------------------------
# requeue_stale
# ---------------------------------------------------------------------------


def test_requeue_stale_returns_count() -> None:
    q = _queries(Job.new("task", b"a"), Job.new("task", b"b"))
    q.claim(None, 2, NOW)
    assert q.requeue_stale(NOW + timedelta(seconds=1)) == 2
    assert q.state.in_progress_jobs() == ()


def test_requeue_stale_ignores_fresh_jobs() -> None:
    q = _queries(Job.new("task", b"a"))
    q.claim(None, 1, NOW)
    before = q.state
    assert q.requeue_stale(NOW - timedelta(seconds=1)) == 0
    assert q.state is before
