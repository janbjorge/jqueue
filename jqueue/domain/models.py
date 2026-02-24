"""
Domain models for jqueue — backed by Pydantic v2.

Pydantic handles:
  - JSON serialization / deserialization (via codec.py)
  - bytes ↔ base64 encoding in JSON mode
  - datetime parsing (ISO-8601 with timezone)
  - field validation and type coercion

All models are frozen (immutable). Mutations return new instances via
model_copy(update=...), following a functional-update style.
"""

import base64
import uuid
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


class JobStatus(str, Enum):
    """Lifecycle states for a queued job."""

    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    DEAD = "dead"


class Job(BaseModel):
    """
    A single unit of work stored in the queue.

    id           — stable identifier, assigned at enqueue time
    entrypoint   — logical name used to route the job to a handler
    payload      — arbitrary bytes (serialised as base64 in JSON)
    status       — current lifecycle state
    priority     — lower value = higher priority (default 0)
    created_at   — UTC timestamp set at enqueue time
    heartbeat_at — UTC timestamp of last worker heartbeat (None when QUEUED)
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    entrypoint: str
    payload: bytes
    status: JobStatus = JobStatus.QUEUED
    priority: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    heartbeat_at: datetime | None = None

    @field_validator("payload", mode="before")
    @classmethod
    def _decode_payload(cls, v: str | bytes) -> bytes:
        """Accept base64 strings from JSON; pass bytes through unchanged."""
        match v:
            case bytes():
                return v
            case str():
                return base64.b64decode(v)
            case _:
                raise ValueError(
                    f"payload must be bytes or a base64-encoded str, got {type(v).__name__}"
                )

    @field_serializer("payload")
    def _encode_payload(self, v: bytes) -> str:
        """Encode bytes as base64 ASCII for JSON serialisation."""
        return base64.b64encode(v).decode("ascii")

    @classmethod
    def new(cls, entrypoint: str, payload: bytes, priority: int = 0) -> "Job":
        """Factory — assigns a fresh UUID and sets status to QUEUED."""
        return cls(entrypoint=entrypoint, payload=payload, priority=priority)

    def with_status(self, status: JobStatus) -> "Job":
        """Return a new Job with an updated status."""
        return self.model_copy(update={"status": status})

    def with_heartbeat(self, ts: datetime | None) -> "Job":
        """Return a new Job with an updated heartbeat timestamp."""
        return self.model_copy(update={"heartbeat_at": ts})


class QueueState(BaseModel):
    """
    The complete, authoritative state of the queue.

    This is exactly what lives in the JSON file on object storage.
    Pure value type — all mutations return new instances.

    jobs    — ordered sequence of jobs; order is preserved across ser/de
    version — monotonically increasing counter, incremented on every CAS write
    """

    model_config = ConfigDict(frozen=True)

    jobs: tuple[Job, ...] = ()
    version: int = 0

    # ------------------------------------------------------------------ #
    # Query helpers                                                        #
    # ------------------------------------------------------------------ #

    def queued_jobs(self, entrypoint: str | None = None) -> tuple[Job, ...]:
        """All QUEUED jobs sorted by (priority, created_at), optionally filtered."""
        candidates = (j for j in self.jobs if j.status == JobStatus.QUEUED)
        if entrypoint is not None:
            candidates = (j for j in candidates if j.entrypoint == entrypoint)
        return tuple(sorted(candidates, key=lambda j: (j.priority, j.created_at)))

    def in_progress_jobs(self) -> tuple[Job, ...]:
        """All IN_PROGRESS jobs."""
        return tuple(j for j in self.jobs if j.status == JobStatus.IN_PROGRESS)

    def find(self, job_id: str) -> Job | None:
        """Return the job with the given id, or None if absent."""
        return next((j for j in self.jobs if j.id == job_id), None)

    # ------------------------------------------------------------------ #
    # Mutation helpers — each returns a new QueueState                    #
    # ------------------------------------------------------------------ #

    def with_job_added(self, job: Job) -> "QueueState":
        """Append a job and increment version."""
        return self.model_copy(
            update={"jobs": self.jobs + (job,), "version": self.version + 1}
        )

    def with_job_replaced(self, updated: Job) -> "QueueState":
        """Replace the job with the same id. Raises JobNotFoundError if absent."""
        from jqueue.domain.errors import JobNotFoundError

        found = False
        new_jobs: list[Job] = []
        for j in self.jobs:
            if j.id == updated.id:
                new_jobs.append(updated)
                found = True
            else:
                new_jobs.append(j)
        if not found:
            raise JobNotFoundError(updated.id)
        return self.model_copy(
            update={"jobs": tuple(new_jobs), "version": self.version + 1}
        )

    def with_job_removed(self, job_id: str) -> "QueueState":
        """Remove a job by id. Raises JobNotFoundError if absent."""
        from jqueue.domain.errors import JobNotFoundError

        original_len = len(self.jobs)
        new_jobs = tuple(j for j in self.jobs if j.id != job_id)
        if len(new_jobs) == original_len:
            raise JobNotFoundError(job_id)
        return self.model_copy(
            update={"jobs": new_jobs, "version": self.version + 1}
        )

    def requeue_stale(self, cutoff: datetime) -> "QueueState":
        """
        Reset any IN_PROGRESS job whose heartbeat_at is before `cutoff` back to QUEUED.

        Returns self unchanged when no jobs are stale.
        """
        new_jobs = tuple(
            j.with_status(JobStatus.QUEUED).with_heartbeat(None)
            if (
                j.status == JobStatus.IN_PROGRESS
                and (j.heartbeat_at is None or j.heartbeat_at < cutoff)
            )
            else j
            for j in self.jobs
        )
        if new_jobs == self.jobs:
            return self
        return self.model_copy(
            update={"jobs": new_jobs, "version": self.version + 1}
        )
