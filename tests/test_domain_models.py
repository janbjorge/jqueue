import base64
from datetime import UTC, datetime, timedelta

import pytest

from jqueue.domain.errors import JobNotFoundError
from jqueue.domain.models import Job, JobStatus, QueueState

# ---------------------------------------------------------------------------
# JobStatus
# ---------------------------------------------------------------------------


def test_job_status_values():
    assert JobStatus.QUEUED.value == "queued"
    assert JobStatus.IN_PROGRESS.value == "in_progress"
    assert JobStatus.DEAD.value == "dead"


def test_job_status_is_str_enum():
    assert isinstance(JobStatus.QUEUED, str)
    assert JobStatus.QUEUED.value == "queued"


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


def test_job_new_defaults():
    job = Job.new("send_email", b"payload")
    assert job.entrypoint == "send_email"
    assert job.payload == b"payload"
    assert job.status == JobStatus.QUEUED
    assert job.priority == 0
    assert job.id is not None
    assert job.created_at is not None
    assert job.heartbeat_at is None


def test_job_new_with_priority():
    job = Job.new("task", b"data", priority=5)
    assert job.priority == 5


def test_job_new_assigns_unique_ids():
    job1 = Job.new("task", b"data")
    job2 = Job.new("task", b"data")
    assert job1.id != job2.id


def test_job_is_frozen():
    job = Job.new("task", b"data")
    with pytest.raises(Exception):
        job.entrypoint = "other"


def test_job_payload_accepts_bytes():
    job = Job(entrypoint="e", payload=b"hello")
    assert job.payload == b"hello"


def test_job_payload_accepts_base64_string():
    encoded = base64.b64encode(b"hello world").decode("ascii")
    job = Job(entrypoint="e", payload=encoded)  # type: ignore[arg-type]
    assert job.payload == b"hello world"


def test_job_payload_invalid_type_raises():
    with pytest.raises(Exception):
        Job(entrypoint="e", payload=123)  # type: ignore[arg-type]


def test_job_with_status_returns_new_instance():
    job = Job.new("task", b"data")
    updated = job.with_status(JobStatus.IN_PROGRESS)
    assert updated.status == JobStatus.IN_PROGRESS
    assert job.status == JobStatus.QUEUED  # original unchanged
    assert updated.id == job.id


def test_job_with_heartbeat_sets_timestamp():
    job = Job.new("task", b"data")
    ts = datetime.now(UTC)
    updated = job.with_heartbeat(ts)
    assert updated.heartbeat_at == ts
    assert job.heartbeat_at is None  # original unchanged


def test_job_with_heartbeat_none_clears():
    ts = datetime.now(UTC)
    job = Job.new("task", b"data").with_heartbeat(ts)
    cleared = job.with_heartbeat(None)
    assert cleared.heartbeat_at is None


def test_job_serializes_payload_as_base64():
    job = Job.new("task", b"hello")
    data = job.model_dump(mode="json")
    assert data["payload"] == base64.b64encode(b"hello").decode("ascii")


def test_job_roundtrip_via_json():
    job = Job.new("task", b"binary\x00data")
    json_str = job.model_dump_json()
    restored = Job.model_validate_json(json_str)
    assert restored.payload == b"binary\x00data"
    assert restored.id == job.id
    assert restored.entrypoint == job.entrypoint


# ---------------------------------------------------------------------------
# QueueState
# ---------------------------------------------------------------------------


def test_queue_state_defaults():
    state = QueueState()
    assert state.jobs == ()
    assert state.version == 0


def test_queued_jobs_empty():
    assert QueueState().queued_jobs() == ()


def test_queued_jobs_returns_only_queued():
    job_q = Job.new("task", b"1")
    job_ip = Job.new("task", b"2").with_status(JobStatus.IN_PROGRESS)
    job_dead = Job.new("task", b"3").with_status(JobStatus.DEAD)
    state = QueueState(jobs=(job_q, job_ip, job_dead))
    result = state.queued_jobs()
    assert result == (job_q,)


def test_queued_jobs_filtered_by_entrypoint():
    job_email = Job.new("email", b"1")
    job_sms = Job.new("sms", b"2")
    state = QueueState(jobs=(job_email, job_sms))
    assert state.queued_jobs("email") == (job_email,)
    assert state.queued_jobs("sms") == (job_sms,)
    assert len(state.queued_jobs()) == 2


def test_queued_jobs_sorted_by_priority():
    job_low = Job.new("task", b"low", priority=10)
    job_high = Job.new("task", b"high", priority=1)
    state = QueueState(jobs=(job_low, job_high))
    result = state.queued_jobs()
    assert result[0].payload == b"high"
    assert result[1].payload == b"low"


def test_queued_jobs_same_priority_sorted_by_created_at():
    earlier = datetime(2024, 1, 1, tzinfo=UTC)
    later = datetime(2024, 1, 2, tzinfo=UTC)
    job_later = Job(entrypoint="task", payload=b"later", created_at=later)
    job_earlier = Job(entrypoint="task", payload=b"earlier", created_at=earlier)
    state = QueueState(jobs=(job_later, job_earlier))
    result = state.queued_jobs()
    assert result[0].payload == b"earlier"
    assert result[1].payload == b"later"


def test_in_progress_jobs():
    job_q = Job.new("task", b"1")
    job_ip = Job.new("task", b"2").with_status(JobStatus.IN_PROGRESS)
    state = QueueState(jobs=(job_q, job_ip))
    assert state.in_progress_jobs() == (job_ip,)


def test_in_progress_jobs_empty():
    state = QueueState(jobs=(Job.new("task", b"1"),))
    assert state.in_progress_jobs() == ()


def test_find_existing_job():
    job = Job.new("task", b"data")
    state = QueueState(jobs=(job,))
    assert state.find(job.id) is job


def test_find_missing_job_returns_none():
    assert QueueState().find("nonexistent") is None


def test_with_job_added_increments_version():
    state = QueueState()
    job = Job.new("task", b"data")
    new_state = state.with_job_added(job)
    assert len(new_state.jobs) == 1
    assert new_state.version == 1
    assert len(state.jobs) == 0  # original unchanged


def test_with_job_added_multiple():
    state = QueueState()
    for i in range(3):
        state = state.with_job_added(Job.new("task", f"data{i}".encode()))
    assert len(state.jobs) == 3
    assert state.version == 3


def test_with_job_replaced_success():
    job = Job.new("task", b"data")
    state = QueueState(jobs=(job,))
    updated = job.with_status(JobStatus.IN_PROGRESS)
    new_state = state.with_job_replaced(updated)
    assert new_state.jobs[0].status == JobStatus.IN_PROGRESS
    assert new_state.version == state.version + 1
    assert state.jobs[0].status == JobStatus.QUEUED  # original unchanged


def test_with_job_replaced_preserves_other_jobs():
    job1 = Job.new("task", b"1")
    job2 = Job.new("task", b"2")
    state = QueueState(jobs=(job1, job2))
    updated_job2 = job2.with_status(JobStatus.IN_PROGRESS)
    new_state = state.with_job_replaced(updated_job2)
    assert new_state.find(job1.id) == job1
    assert new_state.find(job2.id).status == JobStatus.IN_PROGRESS  # type: ignore[union-attr]


def test_with_job_replaced_not_found_raises():
    state = QueueState()
    job = Job.new("task", b"data")
    with pytest.raises(JobNotFoundError) as exc_info:
        state.with_job_replaced(job)
    assert exc_info.value.job_id == job.id


def test_with_job_removed_success():
    job = Job.new("task", b"data")
    state = QueueState(jobs=(job,))
    new_state = state.with_job_removed(job.id)
    assert len(new_state.jobs) == 0
    assert new_state.version == state.version + 1


def test_with_job_removed_preserves_others():
    job1 = Job.new("task", b"1")
    job2 = Job.new("task", b"2")
    state = QueueState(jobs=(job1, job2))
    new_state = state.with_job_removed(job1.id)
    assert len(new_state.jobs) == 1
    assert new_state.jobs[0].id == job2.id


def test_with_job_removed_not_found_raises():
    state = QueueState()
    with pytest.raises(JobNotFoundError) as exc_info:
        state.with_job_removed("missing-id")
    assert exc_info.value.job_id == "missing-id"


def test_requeue_stale_no_stale_returns_self():
    job = Job.new("task", b"data")
    state = QueueState(jobs=(job,))
    cutoff = datetime(2020, 1, 1, tzinfo=UTC)
    result = state.requeue_stale(cutoff)
    assert result is state


def test_requeue_stale_old_heartbeat():
    old_ts = datetime(2020, 1, 1, tzinfo=UTC)
    job = (
        Job.new("task", b"data")
        .with_status(JobStatus.IN_PROGRESS)
        .with_heartbeat(old_ts)
    )
    state = QueueState(jobs=(job,))
    cutoff = datetime.now(UTC)
    new_state = state.requeue_stale(cutoff)
    assert new_state.jobs[0].status == JobStatus.QUEUED
    assert new_state.jobs[0].heartbeat_at is None
    assert new_state.version == state.version + 1


def test_requeue_stale_none_heartbeat_treated_as_stale():
    job = Job.new("task", b"data").with_status(JobStatus.IN_PROGRESS)
    assert job.heartbeat_at is None
    state = QueueState(jobs=(job,))
    cutoff = datetime.now(UTC)
    new_state = state.requeue_stale(cutoff)
    assert new_state.jobs[0].status == JobStatus.QUEUED


def test_requeue_stale_fresh_heartbeat_not_affected():
    future_ts = datetime.now(UTC) + timedelta(hours=1)
    job = (
        Job.new("task", b"data")
        .with_status(JobStatus.IN_PROGRESS)
        .with_heartbeat(future_ts)
    )
    state = QueueState(jobs=(job,))
    cutoff = datetime.now(UTC)
    result = state.requeue_stale(cutoff)
    assert result is state  # unchanged


def test_requeue_stale_only_affects_in_progress():
    queued = Job.new("task", b"1")
    dead = Job.new("task", b"2").with_status(JobStatus.DEAD)
    state = QueueState(jobs=(queued, dead))
    cutoff = datetime.now(UTC)
    result = state.requeue_stale(cutoff)
    assert result is state  # neither QUEUED nor DEAD are touched


def test_requeue_stale_mixed_batch():
    old_ts = datetime(2020, 1, 1, tzinfo=UTC)
    future_ts = datetime.now(UTC) + timedelta(hours=1)
    stale = (
        Job.new("task", b"stale")
        .with_status(JobStatus.IN_PROGRESS)
        .with_heartbeat(old_ts)
    )
    fresh = (
        Job.new("task", b"fresh")
        .with_status(JobStatus.IN_PROGRESS)
        .with_heartbeat(future_ts)
    )
    state = QueueState(jobs=(stale, fresh))
    cutoff = datetime.now(UTC)
    new_state = state.requeue_stale(cutoff)
    stale_result = new_state.find(stale.id)
    fresh_result = new_state.find(fresh.id)
    assert stale_result is not None
    assert fresh_result is not None
    assert stale_result.status == JobStatus.QUEUED
    assert fresh_result.status == JobStatus.IN_PROGRESS
