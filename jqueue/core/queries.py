"""
StateQueries — pure operations on one QueueState snapshot.

No I/O. Services (DirectQueue, GroupCommitLoop) own the storage read/CAS write.
An operation that raises leaves ``state`` unchanged.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime

from jqueue.domain.errors import JobNotFoundError, JobNotInProgressError
from jqueue.domain.models import Job, JobStatus, QueueState


def check_batch_size(batch_size: int) -> None:
    """
    Validate a dequeue ``batch_size``.

    Raises
    ------
    ValueError
        If ``batch_size`` is less than 1.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")


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
            Maximum number of jobs to claim; must be >= 1.
        now : datetime
            Heartbeat timestamp for the claimed jobs.

        Returns
        -------
        list[Job]
            The claimed jobs, in priority order. Empty if none are available.

        Raises
        ------
        ValueError
            If ``batch_size`` is less than 1.
        """
        check_batch_size(batch_size)
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
        Set the heartbeat timestamp of an IN_PROGRESS job.

        Returns
        -------
        Job
            The updated job.

        Raises
        ------
        JobNotFoundError
            If no job with ``job_id`` exists.
        JobNotInProgressError
            If the job is not IN_PROGRESS (e.g. it went stale and was
            re-queued), so the caller no longer holds it.
        """
        job = self._require(job_id)
        if job.status != JobStatus.IN_PROGRESS:
            raise JobNotInProgressError(job_id, job.status)
        updated = job.with_heartbeat(now)
        self.state = self.state.with_job_replaced(updated)
        return updated

    def release_claims(self, claimed: list[Job]) -> list[Job]:
        """
        Return jobs from an abandoned claim to QUEUED.

        Used when the caller that claimed ``claimed`` went away (was
        cancelled) before receiving them. A job is released only if it is
        still exactly as that claim left it — IN_PROGRESS with the claim's
        heartbeat — so a job that has since been heartbeated, acked, or
        re-claimed by another worker is left alone. Never raises.

        Parameters
        ----------
        claimed : list[Job]
            The jobs as returned by the abandoned ``claim``.

        Returns
        -------
        list[Job]
            The jobs that were released.
        """
        state = self.state
        released: list[Job] = []
        for job in claimed:
            current = state.find(job.id)
            if (
                current is None
                or current.status != JobStatus.IN_PROGRESS
                or current.heartbeat_at != job.heartbeat_at
            ):
                continue
            updated = current.with_status(JobStatus.QUEUED).with_heartbeat(None)
            state = state.with_job_replaced(updated)
            released.append(updated)
        self.state = state
        return released

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
