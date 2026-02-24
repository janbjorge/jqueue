import base64
import json

from jqueue.core import codec
from jqueue.domain.models import Job, JobStatus, QueueState


def test_decode_empty_bytes_returns_empty_state():
    state = codec.decode(b"")
    assert isinstance(state, QueueState)
    assert state.jobs == ()
    assert state.version == 0


def test_encode_produces_bytes():
    state = QueueState()
    result = codec.encode(state)
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_encode_is_valid_json():
    state = QueueState()
    data = json.loads(codec.encode(state))
    assert "version" in data
    assert "jobs" in data


def test_encode_decode_roundtrip_empty_state():
    state = QueueState()
    assert codec.decode(codec.encode(state)) == state


def test_encode_decode_roundtrip_with_jobs():
    job = Job.new("send_email", b"hello world")
    state = QueueState(jobs=(job,), version=3)
    restored = codec.decode(codec.encode(state))
    assert restored.version == 3
    assert len(restored.jobs) == 1
    assert restored.jobs[0].payload == b"hello world"
    assert restored.jobs[0].entrypoint == "send_email"
    assert restored.jobs[0].id == job.id


def test_encode_preserves_job_status():
    job = Job.new("task", b"data").with_status(JobStatus.IN_PROGRESS)
    state = QueueState(jobs=(job,))
    restored = codec.decode(codec.encode(state))
    assert restored.jobs[0].status == JobStatus.IN_PROGRESS


def test_encode_payload_stored_as_base64():
    job = Job.new("task", b"binary\x00\xff\xfe")
    state = QueueState(jobs=(job,))
    data = json.loads(codec.encode(state))
    encoded_payload = data["jobs"][0]["payload"]
    assert base64.b64decode(encoded_payload) == b"binary\x00\xff\xfe"


def test_encode_preserves_version():
    state = QueueState(version=42)
    data = json.loads(codec.encode(state))
    assert data["version"] == 42


def test_decode_roundtrip_multiple_jobs():
    jobs = tuple(Job.new("task", f"data{i}".encode()) for i in range(5))
    state = QueueState(jobs=jobs, version=5)
    restored = codec.decode(codec.encode(state))
    assert len(restored.jobs) == 5
    assert [j.id for j in restored.jobs] == [j.id for j in jobs]


def test_encode_datetime_in_iso8601():
    job = Job.new("task", b"data")
    state = QueueState(jobs=(job,))
    data = json.loads(codec.encode(state))
    created_at = data["jobs"][0]["created_at"]
    assert "T" in created_at  # ISO-8601 format
    assert "+" in created_at or "Z" in created_at  # timezone present
