# jqueue

A lightweight, storage-agnostic job queue for Python that runs on top of ordinary object
storage (S3, GCS, a local file, or an in-memory buffer). No message broker, no database,
no sidecar process required.

Inspired by the [turbopuffer object-storage queue pattern](https://turbopuffer.com/blog/object-storage-queue).

## Contents

- [Why object storage?](#why-object-storage)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Use cases](#use-cases)
- [How it works](#how-it-works)
  - [Compare-and-set writes](#compare-and-set-writes)
  - [DirectQueue: one CAS write per operation](#directqueue--one-cas-write-per-operation)
  - [BrokerQueue: group commit](#brokerqueue--group-commit)
  - [Heartbeats and stale-job recovery](#heartbeats-and-stale-job-recovery)
  - [Wire format](#wire-format)
- [Storage adapters](#storage-adapters)
  - [InMemoryStorage](#inmemorystorage)
  - [LocalFileSystemStorage](#localfilesystemstorage)
  - [S3Storage](#s3storage)
  - [GCSStorage](#gcsstorage)
  - [Custom adapters](#custom-adapters)
- [API reference](#api-reference)
- [Error handling](#error-handling)
- [Architecture](#architecture)
- [Limitations and trade-offs](#limitations-and-trade-offs)

## Why object storage?

Traditional job queues add a dependency: Redis, RabbitMQ, SQS, a Postgres table. Each comes
with operational overhead (provisioning, monitoring, capacity planning) and another failure
domain to manage.

For workloads that don't need sub-millisecond latency or thousands of operations per second,
object storage works well:

| Property | Object storage queue |
|---|---|
| **Durability** | 11 nines (S3/GCS), survives entire AZ outages |
| **Cost** | ~$0.004/10 000 operations (S3 PUT pricing) |
| **Infrastructure** | None. Uses storage you already have |
| **Concurrency safety** | CAS writes (If-Match / if_generation_match) |
| **Exactly-once delivery** | Guaranteed by the CAS protocol |
| **Ops/sec** | ~1-100 ops/s depending on backend and batching |

The queue state lives in **a single JSON file**. Every mutation is a conditional write that
only succeeds if the file hasn't changed since you last read it. Concurrent writers that lose
the race retry automatically.

## Installation

```bash
# Core (no optional deps)
pip install jqueue

# With S3 support
pip install "jqueue[s3]"

# With GCS support
pip install "jqueue[gcs]"

# Both
pip install "jqueue[s3,gcs]"
```

Requires Python 3.12+.

## Quick start

```python
import asyncio
from jqueue import BrokerQueue, HeartbeatManager, InMemoryStorage

async def main():
    async with BrokerQueue(InMemoryStorage()) as q:

        # Producer: add jobs to the queue
        await q.enqueue("send_email", b'{"to": "alice@example.com"}')
        await q.enqueue("send_email", b'{"to": "bob@example.com"}')

        # Consumer: claim and process
        jobs = await q.dequeue("send_email", batch_size=2)
        for job in jobs:
            async with HeartbeatManager(q, job.id):
                print(f"Sending email: {job.payload}")
            await q.ack(job.id)

asyncio.run(main())
```

Switch to a real backend by swapping the storage adapter. The queue logic is identical:

```python
# Local file (single machine)
from jqueue import LocalFileSystemStorage
storage = LocalFileSystemStorage("/var/lib/myapp/queue.json")

# AWS S3
from jqueue.adapters.storage.s3 import S3Storage
storage = S3Storage(bucket="my-bucket", key="queues/jobs.json")

# Google Cloud Storage
from jqueue.adapters.storage.gcs import GCSStorage
storage = GCSStorage(bucket_name="my-bucket", blob_name="queues/jobs.json")
```

## Use cases

### Background job processing

Enqueue work from a web request and process it in a separate worker process:

```python
# web handler
await q.enqueue("resize_image", payload=image_bytes, priority=0)

# worker loop
async def worker(q):
    while True:
        jobs = await q.dequeue("resize_image", batch_size=5)
        if not jobs:
            await asyncio.sleep(1)
            continue
        for job in jobs:
            async with HeartbeatManager(q, job.id, interval=timedelta(seconds=30)):
                await resize(job.payload)
            await q.ack(job.id)
```

### Fan-out with multiple queues

Use separate JSON files (keys/blobs) to partition workloads:

```python
email_queue = BrokerQueue(S3Storage(bucket="b", key="queues/email.json"))
sms_queue   = BrokerQueue(S3Storage(bucket="b", key="queues/sms.json"))
```

### Priority work

Lower `priority` value = processed first (same convention as Unix `nice`):

```python
await q.enqueue("report", b"payload", priority=0)   # urgent
await q.enqueue("report", b"payload", priority=10)  # best-effort
```

### Long-running jobs with heartbeats

Workers on long tasks use `HeartbeatManager` to prevent the broker from re-queuing their
job as stale. If the worker dies, the heartbeat stops and the job is automatically
recovered:

```python
[job] = await q.dequeue("transcode_video")
try:
    async with HeartbeatManager(q, job.id, interval=timedelta(seconds=30)):
        result = await transcode(job.payload)   # might take minutes
    await q.ack(job.id)
except Exception:
    await q.nack(job.id)   # return to queue for retry
```

### Testing without infrastructure

`InMemoryStorage` implements the same interface, so no mocking is needed:

```python
async def test_email_worker():
    async with BrokerQueue(InMemoryStorage()) as q:
        await q.enqueue("send_email", b'{"to": "test@example.com"}')
        [job] = await q.dequeue("send_email")
        await process_email(job)
        await q.ack(job.id)
        state = await q.read_state()
        assert len(state.jobs) == 0
```

### MinIO / self-hosted S3-compatible storage

```python
storage = S3Storage(
    bucket="my-bucket",
    key="queue.json",
    endpoint_url="http://minio.internal:9000",
    region_name="us-east-1",
)
```

## How it works

### Compare-and-set writes

The entire queue state is serialized to a single JSON blob on object storage. Every
mutation follows a three-step cycle:

```
1. READ   →  fetch the current blob + its etag
2. MUTATE →  apply the operation in memory (pure function)
3. WRITE  →  PUT the new blob with If-Match: <etag>
             ✓ etag matches → write succeeds, new etag returned
             ✗ etag changed → CASConflictError, retry from step 1
```

The **etag** is an opaque version token returned by the storage backend:

| Backend | Etag source |
|---|---|
| S3 / MinIO / R2 | HTTP `ETag` response header |
| GCS | Object `generation` number (integer, stringified) |
| Filesystem | `st_mtime_ns` (nanosecond mtime) |
| InMemory | Monotonic integer counter |

Because two writers can't both satisfy the same `If-Match` condition, the CAS protocol
provides **exactly-once delivery** without any locks, transactions, or coordination service.

```
Writer A                        Writer B
────────                        ────────
read  → state₀, etag="abc"
                                read  → state₀, etag="abc"
mutate → state₁
write (If-Match: abc) → ✓ "xyz"
                                mutate → state₁′
                                write (If-Match: abc) → ✗ CASConflictError
                                read  → state₁, etag="xyz"   ← re-reads fresh state
                                mutate → state₂
                                write (If-Match: xyz) → ✓ "pqr"
```

### DirectQueue: one CAS write per operation

`DirectQueue` is the simplest implementation. Every call to `enqueue`, `dequeue`, `ack`,
`nack`, or `heartbeat` performs its own independent CAS cycle:

```
enqueue("task", b"payload")
  → read (state₀, etag₀)
  → state₁ = state₀.with_job_added(job)
  → write(state₁, if_match=etag₀)   ← one storage round-trip per operation
```

**Retry policy:** up to 10 retries on `CASConflictError`, with linear back-off
(10 ms x attempt number).

Use `DirectQueue` when:
- throughput is ~1-5 ops/s
- you want the simplest possible code path
- you're running a single worker

```python
from jqueue import DirectQueue, LocalFileSystemStorage

q = DirectQueue(LocalFileSystemStorage("queue.json"))
job = await q.enqueue("task", b"data")
[claimed] = await q.dequeue("task")
await q.ack(claimed.id)
```

### BrokerQueue: group commit

When multiple coroutines (or asyncio tasks) call the queue concurrently, each one would
normally trigger its own storage round-trip. With 100 ms S3 latency, 10 concurrent
enqueues would take 10 x 100 ms = 1 second if serialized naively.

`BrokerQueue` solves this with a **group commit loop** (`GroupCommitLoop`), a single
background writer task that batches all pending operations into one CAS write:

```
Caller 1: enqueue() ──────────────────────────────────────> result
Caller 2: enqueue() ──────────────────────────────────────> result
Caller 3: dequeue() ──────────────────────────────────────> result
                      ↓ batch = [op₁, op₂, op₃]
Writer:           read → apply op₁,op₂,op₃ → CAS write → resolve futures
                  └─────────── one round-trip ───────────┘
```

**The algorithm in detail:**

1. Each caller appends its mutation function to a shared `_pending` list and wakes the
   writer via an `asyncio.Event`.
2. The caller then `await`s a `Future` that will be resolved by the writer.
3. The writer drains `_pending` into a batch, reads the current state once, applies all
   mutations in order, and CAS-writes the new state.
4. On success, the writer resolves each future with its result (or exception).
5. If the CAS write fails (another writer raced ahead), the entire batch is re-applied to
   the fresh state and retried with exponential back-off (up to 20 retries, capped at
   ~320 ms).

**Per-operation error isolation:** if one mutation in a batch raises (e.g. `nack` on a
job that no longer exists), only that caller's future receives the exception. All other
operations in the same batch commit normally.

```
Batch: [valid_enqueue, bad_nack, valid_dequeue]
         ↓                ↓             ↓
       success        JobNotFound    success
```

`BrokerQueue` collapses N concurrent callers into O(1) storage operations per write
cycle, making it suitable for ~10-100 ops/s depending on backend latency.

```python
from jqueue import BrokerQueue, InMemoryStorage

async with BrokerQueue(InMemoryStorage()) as q:
    # These three enqueues are batched into a single storage write
    await asyncio.gather(
        q.enqueue("task", b"1"),
        q.enqueue("task", b"2"),
        q.enqueue("task", b"3"),
    )
```

**Lifecycle:** `BrokerQueue` is an async context manager. On enter it starts the writer
task; on exit it signals shutdown and drains any buffered operations before stopping.

### Heartbeats and stale-job recovery

When a worker claims a job (via `dequeue`), the job's status changes to `IN_PROGRESS`
and its `heartbeat_at` timestamp is set to `now`. If the worker crashes or hangs, the
heartbeat stops updating and the job becomes **stale**.

**`HeartbeatManager`** is an async context manager that sends periodic heartbeat pings
for a single job while work is in progress:

```python
async with HeartbeatManager(q, job.id, interval=timedelta(seconds=30)):
    await long_running_work(job.payload)
# heartbeat task is cancelled on exit
```

**Automatic stale recovery:** On every write cycle, `BrokerQueue` (via `GroupCommitLoop`)
sweeps `IN_PROGRESS` jobs and resets any whose `heartbeat_at` is older than
`stale_timeout` (default: 5 minutes) back to `QUEUED`. This requires zero extra storage
operations; the sweep piggybacks on writes that are already happening.

`DirectQueue` exposes this as an explicit call:

```python
requeued = await q.requeue_stale(timeout=timedelta(minutes=5))
print(f"Recovered {requeued} stale jobs")
```

### Wire format

The queue state is stored as pretty-printed JSON. Here's a complete example:

```json
{
  "version": 3,
  "jobs": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "entrypoint": "send_email",
      "payload": "eyJ0byI6ICJ1c2VyQGV4YW1wbGUuY29tIn0=",
      "status": "queued",
      "priority": 0,
      "created_at": "2024-01-01T12:00:00+00:00",
      "heartbeat_at": null
    },
    {
      "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
      "entrypoint": "resize_image",
      "payload": "...",
      "status": "in_progress",
      "priority": 5,
      "created_at": "2024-01-01T11:59:00+00:00",
      "heartbeat_at": "2024-01-01T12:00:45+00:00"
    }
  ]
}
```

- `version`: monotonically increasing counter, incremented on every successful write.
- `payload`: arbitrary bytes, base64-encoded for JSON compatibility.
- `heartbeat_at`: `null` when `QUEUED`; set by `dequeue` and refreshed by `heartbeat`.

Pydantic v2 handles all serialization, including base64 encoding of `bytes` fields and
ISO-8601 datetime formatting.

## Storage adapters

### InMemoryStorage

```python
from jqueue import InMemoryStorage

storage = InMemoryStorage()
# or with pre-populated state:
storage = InMemoryStorage(initial_content=b'{"version":0,"jobs":[]}')
```

Uses an `asyncio.Lock` to serialise reads and writes. Safe for concurrent coroutines
in a single event loop. Not safe across processes or threads. Good for tests.

### LocalFileSystemStorage

```python
from jqueue import LocalFileSystemStorage

storage = LocalFileSystemStorage("/var/lib/myapp/queue.json")
# accepts str or pathlib.Path; parent directories are created automatically
```

Uses `fcntl.flock` for POSIX exclusive locking. The etag is the file's `st_mtime_ns`.
**POSIX-only** (Linux, macOS). Not safe across machines or on NFS.

### S3Storage

```python
from jqueue.adapters.storage.s3 import S3Storage

# Standard AWS, credentials from environment / IAM role
storage = S3Storage(bucket="my-bucket", key="queues/jobs.json")

# Explicit region
storage = S3Storage(bucket="my-bucket", key="jobs.json", region_name="eu-central-1")

# S3-compatible (MinIO, Cloudflare R2, Tigris, ...)
storage = S3Storage(
    bucket="my-bucket",
    key="jobs.json",
    endpoint_url="http://minio.internal:9000",
)

# Bring your own session
import aioboto3
storage = S3Storage(
    bucket="my-bucket",
    key="jobs.json",
    session=aioboto3.Session(
        aws_access_key_id="...",
        aws_secret_access_key="...",
    ),
)
```

Uses aioboto3 (async) with `IfMatch` conditional PutObject. This is the S3 conditional write
feature [released in August 2024](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/).
The etag is the S3 `ETag` response header.

Requires: `pip install "jqueue[s3]"`

### GCSStorage

```python
from jqueue.adapters.storage.gcs import GCSStorage

# Application Default Credentials
storage = GCSStorage(bucket_name="my-bucket", blob_name="queues/jobs.json")

# Explicit client
from google.cloud import storage as gcs
storage = GCSStorage(
    bucket_name="my-bucket",
    blob_name="jobs.json",
    client=gcs.Client(project="my-project"),
)
```

Uses `if_generation_match` for conditional writes. Generation 0 means "blob must not
exist yet", used for the first write. The etag is the GCS object generation number
(stringified integer).

Since `google-cloud-storage` is synchronous, all GCS operations are wrapped in
`asyncio.to_thread`.

Requires: `pip install "jqueue[gcs]"`

### Custom adapters

Any object with these two async methods satisfies the `ObjectStoragePort` protocol:

```python
from jqueue import ObjectStoragePort  # for type checking only

class MyStorage:
    async def read(self) -> tuple[bytes, str | None]:
        """
        Return (content, etag).
        If the object doesn't exist yet, return (b"", None).
        """
        ...

    async def write(self, content: bytes, if_match: str | None = None) -> str:
        """
        Conditional write.
        - if_match=None  → unconditional put (first write)
        - if_match=etag  → only write if current etag matches; raise CASConflictError otherwise
        Returns the new etag.
        """
        ...
```

No base class, no registration. Structural subtyping (duck typing) is sufficient.
Pass your adapter directly to `DirectQueue` or `BrokerQueue`.

## API reference

### `BrokerQueue` / `DirectQueue`

Both queues expose the same public interface. `BrokerQueue` must be used as an async
context manager; `DirectQueue` can be used directly.

```python
# Enqueue
job: Job = await q.enqueue(
    entrypoint: str,        # logical handler name
    payload: bytes,         # arbitrary bytes
    priority: int = 0,      # lower = processed first
)

# Dequeue: marks jobs IN_PROGRESS, returns empty list if none available
jobs: list[Job] = await q.dequeue(
    entrypoint: str | None = None,   # None = any entrypoint
    *,
    batch_size: int = 1,
)

# Acknowledge: remove a completed job
await q.ack(job_id: str)

# Negative-acknowledge: return a job to QUEUED for retry
await q.nack(job_id: str)

# Heartbeat: refresh the IN_PROGRESS timestamp
await q.heartbeat(job_id: str)

# Read-only snapshot (no CAS, no locking)
state: QueueState = await q.read_state()

# DirectQueue only: explicit stale sweep
requeued: int = await q.requeue_stale(timeout: timedelta)
```

### `HeartbeatManager`

```python
async with HeartbeatManager(
    queue,                              # any object with async heartbeat(job_id)
    job_id: str,
    interval: timedelta = timedelta(seconds=60),
):
    await do_work()
```

Starts a background task that calls `queue.heartbeat(job_id)` every `interval` seconds.
The task is cancelled when the context exits. If `heartbeat` raises `JQueueError`
(e.g., the job was acked by another process), the task stops silently.

### `Job`

```python
job.id           # str, stable UUID assigned at enqueue time
job.entrypoint   # str
job.payload      # bytes
job.status       # JobStatus.QUEUED | IN_PROGRESS | DEAD
job.priority     # int, lower = higher priority
job.created_at   # datetime (UTC)
job.heartbeat_at # datetime | None
```

`Job` is a frozen Pydantic model. All fields are immutable.

### `QueueState`

```python
state.jobs      # tuple[Job, ...]
state.version   # int, incremented on every write

state.queued_jobs(entrypoint=None)  # sorted by (priority, created_at)
state.in_progress_jobs()
state.find(job_id)                  # Job | None
```

## Error handling

```python
from jqueue import (
    JQueueError,       # base class, catches all jqueue errors
    CASConflictError,  # CAS write rejected (etag mismatch), usually retried internally
    JobNotFoundError,  # job_id not in current state; has .job_id attribute
    StorageError,      # I/O failure from the storage backend; has .cause attribute
)
```

`CASConflictError` is retried automatically inside `DirectQueue` and `GroupCommitLoop`.
It only bubbles up to the caller if all retries are exhausted.

`JobNotFoundError` is raised by `ack`, `nack`, and `heartbeat` when the job ID is not
present in the current queue state (e.g., it was already acked by another worker).

```python
try:
    await q.ack(job.id)
except JobNotFoundError:
    pass  # already removed, safe to ignore
```

## Architecture

jqueue follows the **Ports & Adapters** (hexagonal) pattern:

```
┌─────────────────────────────────────────────────────────────┐
│  domain/                                                     │
│  ├── models.py    Job, QueueState, JobStatus                 │
│  └── errors.py    JQueueError hierarchy                      │
├─────────────────────────────────────────────────────────────┤
│  ports/                                                      │
│  └── storage.py   ObjectStoragePort (Protocol)               │
├─────────────────────────────────────────────────────────────┤
│  core/                                                       │
│  ├── codec.py          QueueState ↔ JSON bytes               │
│  ├── direct.py         DirectQueue (one CAS per op)          │
│  ├── group_commit.py   GroupCommitLoop (batched writes)      │
│  ├── broker.py         BrokerQueue (context manager facade)  │
│  └── heartbeat.py      HeartbeatManager                      │
├─────────────────────────────────────────────────────────────┤
│  adapters/storage/                                           │
│  ├── memory.py     InMemoryStorage                           │
│  ├── filesystem.py LocalFileSystemStorage                    │
│  ├── s3.py         S3Storage                                 │
│  └── gcs.py        GCSStorage                                │
└─────────────────────────────────────────────────────────────┘
```

Design notes:

- `Job` and `QueueState` are frozen Pydantic models with no I/O dependencies. All
  mutations return new instances; no side effects.
- `ObjectStoragePort` is a `runtime_checkable` Protocol. Any two-method object satisfies
  it without inheritance.
- `codec.encode` / `codec.decode` are the only place that knows about the JSON wire
  format, keeping it easy to evolve.
- Each CAS cycle reads a fresh snapshot.   There are no in-process caches that can go stale.

## Limitations and trade-offs

| Concern | Detail |
|---|---|
| **Throughput ceiling** | S3 conditional writes have ~50-200 ms round-trip latency. `DirectQueue` tops out around 5-20 ops/s; `BrokerQueue` can reach ~50-100 ops/s by batching. |
| **Single-file bottleneck** | All operations contend on one object. This is fine for moderate workloads; for very high throughput, partition into multiple queues (one file per entrypoint). |
| **Queue size** | The entire state is read and written on every operation. Keep queue depths reasonable (hundreds to low thousands of jobs). |
| **No push / subscribe** | Workers must poll `dequeue`. There's no server-push mechanism. |
| **POSIX only (filesystem)** | `LocalFileSystemStorage` uses `fcntl.flock`. Linux and macOS only, not NFS. |
| **S3 conditional writes** | Requires the August 2024 S3 conditional write feature. Verify your S3-compatible backend supports `IfMatch` on PutObject before using `S3Storage`. |
| **Not a database** | If you need complex queries, scheduling, or priority queues with millions of jobs, a purpose-built system (Postgres, Redis) is a better fit. |
