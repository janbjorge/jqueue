import pytest

from jqueue.domain.errors import (
    CASConflictError,
    JobNotFoundError,
    JQueueError,
    StorageError,
)


def test_jqueue_error_is_exception():
    err = JQueueError("test message")
    assert isinstance(err, Exception)
    assert str(err) == "test message"


def test_cas_conflict_is_jqueue_error():
    err = CASConflictError("etag mismatch")
    assert isinstance(err, JQueueError)
    assert str(err) == "etag mismatch"


def test_job_not_found_stores_job_id():
    err = JobNotFoundError("abc-123")
    assert isinstance(err, JQueueError)
    assert err.job_id == "abc-123"
    assert "abc-123" in str(err)


def test_storage_error_stores_cause_and_message():
    cause = RuntimeError("disk full")
    err = StorageError("write failed", cause)
    assert isinstance(err, JQueueError)
    assert err.cause is cause
    assert "write failed" in str(err)
    assert "disk full" in str(err)


def test_error_hierarchy():
    assert issubclass(CASConflictError, JQueueError)
    assert issubclass(JobNotFoundError, JQueueError)
    assert issubclass(StorageError, JQueueError)
    assert issubclass(JQueueError, Exception)


def test_can_catch_subclass_as_base():
    with pytest.raises(JQueueError):
        raise CASConflictError("conflict")


def test_job_not_found_message_contains_id():
    err = JobNotFoundError("xyz-999")
    assert "xyz-999" in str(err)


def test_storage_error_message_format():
    cause = OSError("permission denied")
    err = StorageError("GCS read failed", cause)
    msg = str(err)
    assert "GCS read failed" in msg
    assert "permission denied" in msg
