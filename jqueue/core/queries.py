"""
StateQueries — pure operations on a single QueueState snapshot.

This is the "query" layer described in ``queries-and-services.md``. A
StateQueries instance wraps the snapshot read at the start of one CAS cycle
and exposes the domain operations (add, claim, release, touch, remove,
requeue_stale) that services compose into a unit of work.

StateQueries never performs I/O and never touches ObjectStoragePort. The
caller (a service such as DirectQueue or GroupCommitLoop) owns the storage
round-trip: it reads the state, builds a StateQueries, runs operations, and
CAS-writes ``queries.state`` back.

Each operation is atomic with respect to ``self.state``: the new snapshot is
computed first and only assigned on success, so an operation that raises
leaves the state untouched. GroupCommitLoop relies on this for per-operation
error isolation within a batch.

Usage
-----
    content, etag = await storage.read()
    queries = StateQueries(codec.decode(content))
    claimed = queries.claim("send_email", batch_size=5, now=datetime.now(UTC))
    await storage.write(codec.encode(queries.state), if_match=etag)
"""

from __future__ import annotations

import dataclasses
from datetime import datetime

from jqueue.domain.errors import JobNotFoundError
from jqueue.domain.models import Job, JobStatus, QueueState


@dataclasses.dataclass
class StateQueries:
    """
    Domain operations over one QueueState snapshot.

    Parameters
    ----------
    state : QueueState
        The snapshot to operate on. Replaced (never mutated) by each
        successful write operation.
    """

    state: QueueState

    # ------------------------------------------------------------------ #
    # Reads                                                                #
    # ------------------------------------------------------------------ #

    def find(self, job_id: str) -> Job | None:
        """Return the job with the given id, or None if absent."""
        return self.state.find(job_id)

    # ------------------------------------------------------------------ #
    # Writes                                                               #
    # ------------------------------------------------------------------ #

    def add(self, job: Job) -> Job:
        """
        Append a job to the queue.

        The job is built by the caller so its id stays stable across CAS
        retries.

        Returns
        -------
        Job
            The added job.
        """
        self.state = self.state.with_job_added(job)
        return job

    def claim(
        self,
        entrypoint: str | None,
        batch_size: int,
        now: datetime,
    ) -> list[Job]:
        """
        Mark up to ``batch_size`` QUEUED jobs IN_PROGRESS.

        Parameters
        ----------
        entrypoint : str | None
            Only claim jobs for this entrypoint; None claims from any.
        batch_size : int
            Maximum number of jobs to claim.
        now : datetime
            Heartbeat timestamp for the claimed jobs.

        Returns
        -------
        list[Job]
            The claimed jobs, in priority order. Empty if none are available.
        """
        state = self.state
        claimed: list[Job] = []
        for job in state.queued_jobs(entrypoint)[:batch_size]:
            updated = job.with_status(JobStatus.IN_PROGRESS).with_heartbeat(now)
            state = state.with_job_replaced(updated)
            claimed.append(updated)
        self.state = state
        return claimed

    def remove(self, job_id: str) -> Job:
        """
        Remove a job from the queue.

        Returns
        -------
        Job
            The removed job.

        Raises
        ------
        JobNotFoundError
            If no job with ``job_id`` exists.
        """
        job = self._require(job_id)
        self.state = self.state.with_job_removed(job_id)
        return job

    def release(self, job_id: str) -> Job:
        """
        Return a job to QUEUED and clear its heartbeat.

        Returns
        -------
        Job
            The updated job.

        Raises
        ------
        JobNotFoundError
            If no job with ``job_id`` exists.
        """
        updated = (
            self._require(job_id).with_status(JobStatus.QUEUED).with_heartbeat(None)
        )
        self.state = self.state.with_job_replaced(updated)
        return updated

    def touch(self, job_id: str, now: datetime) -> Job:
        """
        Set a job's heartbeat timestamp.

        Returns
        -------
        Job
            The updated job.

        Raises
        ------
        JobNotFoundError
            If no job with ``job_id`` exists.
        """
        updated = self._require(job_id).with_heartbeat(now)
        self.state = self.state.with_job_replaced(updated)
        return updated

    def requeue_stale(self, cutoff: datetime) -> int:
        """
        Return IN_PROGRESS jobs with a heartbeat older than ``cutoff`` to QUEUED.

        Returns
        -------
        int
            The number of jobs re-queued.
        """
        before = {j.id for j in self.state.in_progress_jobs()}
        self.state = self.state.requeue_stale(cutoff)
        after = {j.id for j in self.state.in_progress_jobs()}
        return len(before - after)

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _require(self, job_id: str) -> Job:
        job = self.state.find(job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        return job
