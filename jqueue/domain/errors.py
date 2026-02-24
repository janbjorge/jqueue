"""
Exception hierarchy for jqueue.

JQueueError
├── CASConflictError   — write rejected because etag did not match
├── JobNotFoundError   — job_id not present in current QueueState
└── StorageError       — underlying I/O failure (wraps original exception)
"""

from __future__ import annotations


class JQueueError(Exception):
    """Base class for all jqueue exceptions."""


class CASConflictError(JQueueError):
    """
    Raised when a compare-and-set write is rejected by the storage backend.

    The caller should re-read the current state and retry the operation.
    This is the normal concurrency signal — not an error in the traditional sense.
    """


class JobNotFoundError(JQueueError):
    """Raised when a job_id is not present in the current QueueState."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Job {job_id!r} not found in queue state")


class StorageError(JQueueError):
    """
    Wraps an underlying I/O failure from a storage adapter.

    Attributes
    ----------
    cause : Exception
        The original exception from the storage backend.
    """

    def __init__(self, message: str, cause: Exception) -> None:
        self.cause = cause
        super().__init__(f"{message}: {cause}")
