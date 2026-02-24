"""
jqueue — object-storage queue with compare-and-set semantics.

Implements the turbopuffer object-storage queue pattern:
  https://turbopuffer.com/blog/object-storage-queue

The queue state lives in a single JSON file on object storage. Every mutation
is a compare-and-set (CAS) write — read the file, mutate in memory, write
back with an If-Match guard. Concurrent writers that lose the CAS race retry
automatically.

For higher throughput, BrokerQueue batches concurrent operations into a single
CAS write (group commit), reducing N concurrent writes to O(1) storage
operations per round-trip.

Quick start
-----------
    import asyncio
    from jqueue import BrokerQueue, HeartbeatManager
    from jqueue.adapters.storage.memory import InMemoryStorage

    async def main():
        storage = InMemoryStorage()

        async with BrokerQueue(storage) as q:
            # Enqueue work
            await q.enqueue("send_email", b'{"to": "user@example.com"}')

            # Claim and process
            [job] = await q.dequeue("send_email")
            async with HeartbeatManager(q, job.id):
                print(f"Processing job {job.id}")
            await q.ack(job.id)

    asyncio.run(main())

Storage adapters
----------------
Built-in adapters (no extra deps):
  - InMemoryStorage           — for tests and examples
  - LocalFileSystemStorage    — POSIX single-machine (fcntl.flock)

Optional adapters (install extras):
  - S3Storage        (pip install "jqueue[s3]")
  - GCSStorage       (pip install "jqueue[gcs]")

Custom adapters only need to implement the two-method ObjectStoragePort:
  async def read() -> tuple[bytes, str | None]
  async def write(content, if_match=None) -> str

Architecture
------------
Follows the Ports & Adapters pattern:
  domain/   — pure value types (Job, QueueState, JobStatus)
  ports/    — Protocol interfaces (ObjectStoragePort)
  core/     — business logic (DirectQueue, BrokerQueue, GroupCommitLoop)
  adapters/ — concrete storage implementations
"""
from __future__ import annotations

from jqueue.adapters.storage.filesystem import LocalFileSystemStorage
from jqueue.adapters.storage.memory import InMemoryStorage
from jqueue.core.broker import BrokerQueue
from jqueue.core.direct import DirectQueue
from jqueue.core.heartbeat import HeartbeatManager
from jqueue.domain.errors import (
    CASConflictError,
    JobNotFoundError,
    JQueueError,
    StorageError,
)
from jqueue.domain.models import Job, JobStatus, QueueState
from jqueue.ports.storage import ObjectStoragePort

__all__ = [
    # Domain models
    "Job",
    "JobStatus",
    "QueueState",
    # Errors
    "JQueueError",
    "CASConflictError",
    "JobNotFoundError",
    "StorageError",
    # Port (for typing custom adapters)
    "ObjectStoragePort",
    # High-level queue API
    "BrokerQueue",
    "DirectQueue",
    "HeartbeatManager",
    # Built-in storage adapters
    "InMemoryStorage",
    "LocalFileSystemStorage",
]
