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
- [Deep dive](#deep-dive)
  - [The group commit algorithm](#the-group-commit-algorithm)
  - [Storage adapters and etag strategies](#storage-adapters-and-etag-strategies)
  - [Performance and scaling](#performance-and-scaling)
  - [Stale job recovery in depth](#stale-job-recovery-in-depth)
  - [Production deployment](#production-deployment)
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
| Filesystem | SHA-256 content hash |
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
(10 ms, 20 ms, 30 ms, ...).

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

Uses `fcntl.flock` for POSIX exclusive locking. The etag is a SHA-256 hex digest of
the file content.
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

## Deep dive

### The group commit algorithm

The fundamental problem `GroupCommitLoop` solves is **amortising storage latency across
concurrent callers**. An S3 PUT takes ~100 ms. If ten coroutines each enqueue a job
independently, they would spend a full second in serialised round-trips. Group commit
folds all ten mutations into a single read-modify-write cycle, bringing the wall-clock
cost down to ~100 ms regardless of how many operations were buffered.

The idea comes from database write-ahead log design: instead of flushing to disk on
every transaction commit, the database groups concurrent commits and flushes once. jqueue
applies the same principle to object storage writes (see `core/group_commit.py`).

**How callers interact with the writer.** Every public method (`enqueue`, `dequeue`,
`ack`, etc.) creates a pure function `QueueState -> QueueState` and hands it to the
internal `_submit` method. `_submit` does three things: appends the function and a
`Future` to a pending buffer, wakes a background writer task via an `asyncio.Event`, and
then suspends the caller on the future. The caller doesn't touch storage at all — it
simply waits for the writer to tell it what happened.

An important detail: `enqueue` allocates the job UUID *before* submitting the mutation
(`core/group_commit.py:113`). This means the caller gets a stable job ID back even if
the batch is retried multiple times due to CAS conflicts. The ID is captured by the
closure and replayed identically on every attempt.

**The writer loop** is a single `asyncio.Task` that runs for the lifetime of the
`BrokerQueue` context. When work arrives it drains the entire pending buffer into a
list, clears the buffer, and calls `_commit_batch`. While `_commit_batch` is running
(blocked on storage I/O), new callers keep appending to the now-empty buffer — they
become the *next* batch. This is what produces the pipelining effect: I/O and mutation
accumulation happen in parallel.

On shutdown, the loop condition `while not self._stopped or self._pending` guarantees
that any operations submitted before `stop()` are flushed before the task exits.

**The commit cycle** (`_commit_batch`) does three things per attempt:

1. **Read and decode.** Fetch the current state blob and its etag. Before applying
   the batch, sweep stale IN_PROGRESS jobs — this piggybacks on the write that's about
   to happen, so stale recovery is free.
2. **Apply mutations sequentially.** Each mutation function is called against the
   evolving state. If one raises (e.g. `nack` on a job that doesn't exist), the
   exception is captured in a per-index map but the remaining mutations still run.
   This is the **per-operation error isolation** guarantee: a bad operation in a batch
   doesn't poison the good ones.
3. **CAS write.** The new state is written with `if_match=etag`. On success, each
   caller's future is resolved — either with `None` (success) or the captured
   exception. On `CASConflictError`, the entire batch is retried on fresh state.

The alternative design would be to reject the whole batch on any single failure, but
that creates a fairness problem: one misbehaving caller could block everyone else in the
same batch window.

**Retry strategies** differ between the two queue implementations. `DirectQueue` uses
linear back-off (`10 ms * (attempt + 1)`, max 10 retries) — simple, predictable, and
sufficient when only one or two writers contend. `GroupCommitLoop` uses exponential
back-off (`5 ms * 2^min(attempt, 6)`, capped at ~320 ms, max 20 retries) because it is the
more likely choice under higher contention where thundering-herd effects matter. The
exponential curve spreads competing writers apart in time more effectively than a
linear ramp.

### Storage adapters and etag strategies

The storage port (`ports/storage.py`) is deliberately minimal: `read` returns bytes and
an opaque etag; `write` accepts bytes and an optional `if_match` etag, raising
`CASConflictError` when the condition fails. Everything else — retry logic, batching,
serialisation — lives outside the adapter. This makes it possible to write a new backend
in roughly 30 lines without understanding any queue internals.

**What is an etag and why does it matter?** An etag is a version token that represents
the state of the stored blob at a specific point in time. By passing it back on the
next write (`if_match`), you tell the storage backend "only accept this write if nobody
else has modified the object since I read it." If someone has, the write is rejected and
the caller retries with fresh state. This is the compare-and-set (CAS) guarantee that
makes the whole system work without locks or coordination.

Each backend produces etags differently because each has different native versioning
primitives:

| Backend | Etag | Why this approach |
|---|---|---|
| InMemory | Monotonic counter | Cheapest possible; perfectly ordered |
| Filesystem | SHA-256 of file content | Content-based — handles rapid writes where mtime can collide |
| S3 | HTTP `ETag` header | S3 computes this natively on every PUT |
| GCS | Object generation number | GCS increments this atomically on every write |

The filesystem choice deserves elaboration. A content hash is used instead of a
timestamp like `mtime` because two rapid writes can produce identical timestamps,
silently breaking CAS. A SHA-256 digest always differs when the content changes. The
trade-off is one hash computation per read and write, but for the file sizes jqueue
produces (kilobytes) this is negligible.

**First-write semantics.** When the queue is brand new and no blob exists yet, the
adapter receives `if_match=None`. S3 and GCS handle this differently. S3 simply omits
the `IfMatch` header, making the first write unconditional — if two processes race to
create the blob, the last one wins silently. GCS uses `if_generation_match=0`, which is
a GCS convention meaning "only succeed if the object does not exist yet." This means
GCS's first write is itself conditional — a useful extra safety net against concurrent
initialisers, but it also means you can get a `CASConflictError` on the very first write
if two processes start simultaneously.

In practice this rarely matters because the retry loop handles it, but it's worth
understanding if you're debugging queue initialisation issues on GCS.

**Error classification.** The adapters distinguish three kinds of failures:
`CASConflictError` (expected, retried automatically), `StorageError` (I/O problem with
the backend, wraps the original exception in `.cause`), and everything else (let
through as-is). Both cloud adapters use the same pattern: catch the specific
precondition-failure error from their SDK and translate it to `CASConflictError`, then
wrap anything unexpected in `StorageError`. The S3 adapter needs a defensive helper
(`s3.py:124-135`) to extract error codes from botocore's `ClientError` because the
SDK's error structure is deeply nested and inconsistent across error types.

**Sync-to-async wrapping.** The GCS Python SDK (`google-cloud-storage`) is fully
synchronous. Calling it directly from an async context would block the event loop. Both
the GCS and filesystem adapters solve this by running their synchronous implementations
in a thread-pool worker via `asyncio.to_thread`. This keeps the event loop free to
process heartbeats, dequeues, and other concurrent work while waiting on file I/O or
HTTP calls.

**Writing your own adapter** requires no base class. `ObjectStoragePort` is a
`runtime_checkable` Protocol: if your class has the right `read` and `write` signatures,
it satisfies the interface via structural subtyping. Pass it directly to `DirectQueue`
or `BrokerQueue`.

### Performance and scaling

**Throughput depends almost entirely on storage round-trip time.** CPU time for
JSON serialisation and in-memory mutation is negligible compared to the I/O.
`DirectQueue` performs one round-trip per operation; `BrokerQueue` performs one
round-trip per batch:

| Backend | Round-trip latency | DirectQueue | BrokerQueue (batch ~10) |
|---|---|---|---|
| InMemory | < 1 ms | ~350 ops/s | ~3 200 ops/s |
| Filesystem | 1-5 ms | ~220 ops/s | ~700 ops/s |
| S3 / GCS (same region) | 50-150 ms | ~5-15 ops/s | ~50-100 ops/s |

**Note:** InMemory and Filesystem numbers are from `tools/benchmark_storage.py` running
1000 operations with concurrency levels 10 and 50 on a development machine. S3/GCS numbers
are estimates based on typical round-trip latencies. Run the benchmark tool to measure
performance on your hardware: `uv run tools/benchmark_storage.py`.

The key multiplier for `BrokerQueue` is the average batch size, which grows naturally
with concurrency: the more callers are waiting while a write is in-flight, the larger
the next batch. Under light load the batch size is 1 and `BrokerQueue` behaves like
`DirectQueue` plus some overhead. Under heavy load it can collapse 50+ operations into
a single write.

**The real scaling bottleneck is the full-state read-write cycle.** Every operation
reads the entire JSON blob, deserialises it, applies the mutation, serialises it back,
and writes the whole blob. As the queue grows, so does the blob. At ~1 000 jobs the
JSON payload is roughly 200-400 KB — still fast to transfer and parse. Beyond that,
serialisation time and transfer size start to matter, and CAS conflict rates climb
because longer writes create wider windows for races.

Three design choices in the domain layer help keep per-operation cost low:

1. **Immutable tuples for the job list** (`domain/models.py:100`). Using
   `tuple[Job, ...]` instead of `list[Job]` prevents accidental in-place mutation,
   which would corrupt shared state during batch application where the same state
   object is transformed by multiple closures in sequence.

2. **Early return on no-change** (`domain/models.py:175-176`). `requeue_stale()` is
   called on every write cycle. Most of the time no jobs are stale. When that's the
   case, it returns `self` without incrementing the version, which avoids a
   pointless JSON encode/write cycle.

3. **Lazy generator chains for queries** (`domain/models.py:109-112`). `queued_jobs()`
   chains generators for filtering by status and entrypoint before sorting. This avoids
   allocating intermediate lists — the only materialised collection is the final sorted
   tuple.

**When to partition.** If your workload exceeds what a single blob can handle, the
simplest fix is to use one JSON file per entrypoint. Each file has its own contention
domain, so an email queue and an SMS queue no longer race against each other:

```python
email_q = BrokerQueue(S3Storage(bucket="b", key="queues/email.json"))
sms_q   = BrokerQueue(S3Storage(bucket="b", key="queues/sms.json"))
```

Partition when queue depth routinely exceeds ~1 000 jobs, when throughput exceeds
~50 ops/s on S3/GCS, or when logically independent workloads share a queue for no
good reason.

### Stale job recovery in depth

When `dequeue` claims a job, it sets `heartbeat_at` to `now`. If the worker then
crashes or gets partitioned from storage, heartbeats stop arriving and the job becomes
**stale** — still marked `IN_PROGRESS` but no longer making progress.

The detection rule (`domain/models.py:160-177`) is: an `IN_PROGRESS` job is stale if
its `heartbeat_at` is either `None` or older than the cutoff. The `None` case matters:
it covers workers that crash between `dequeue` (which sets the initial `heartbeat_at`)
and the first `HeartbeatManager` tick. Without it, such jobs would be stuck in
`IN_PROGRESS` forever.

When no jobs are stale, `requeue_stale` returns the state object unchanged (same
identity, no version increment). This is a deliberate optimisation: the method is
called on *every* write cycle inside `GroupCommitLoop`, so the common-case fast path
must be cheap.

**Automatic vs manual recovery.** `BrokerQueue` sweeps stale jobs inside `_commit_batch`
on every write cycle, just before applying the caller's mutations. This costs zero
extra I/O — it piggybacks on writes that are already happening. If no callers are
submitting operations, no writes happen and no sweep runs, but that also means no jobs
are being created or claimed, so there's nothing to recover.

`DirectQueue` has no background task, so stale recovery is an explicit call:
`await q.requeue_stale(timeout=timedelta(minutes=5))`. You can call this from a cron
job, a health-check endpoint, or a periodic asyncio task. It performs its own CAS
cycle, so it costs one storage round-trip.

**HeartbeatManager and the `_HasHeartbeat` Protocol.** `HeartbeatManager` doesn't know
or care which queue implementation it talks to. It's typed against a structural
Protocol with a single method: `async heartbeat(job_id: str) -> None`. Any object
satisfying that signature works — `BrokerQueue`, `DirectQueue`, `GroupCommitLoop`, or a
test double.

Inside the manager, a background task sleeps for `interval` seconds, then calls
`heartbeat`. If the call raises `JQueueError` (which includes `JobNotFoundError` and
`StorageError`), the task exits silently — the assumption is that the job has already
been handled or the connection is lost and there's no useful action to take. On context
exit, the task is cancelled.

**Tuning guidelines:**

| Job duration | Heartbeat interval | Stale timeout |
|---|---|---|
| < 1 minute | 10 s | 30 s |
| 1 - 10 minutes | 30 s | 2 min |
| > 10 minutes | 60 s | 5 - 10 min |

The rule of thumb is **`stale_timeout >= 3 * heartbeat_interval`**. The multiplier
accounts for transient failures: a single missed heartbeat shouldn't trigger recovery.
Two missed heartbeats might mean a problem. Three is a strong signal the worker is gone.
Setting the timeout too tight causes premature requeueing of healthy jobs during GC
pauses, network blips, or temporary storage outages.

**Failure scenario — network partition.** This is the most important edge case to
understand. A worker is processing a job and sending heartbeats normally. Then a
network partition separates it from storage. What happens next:

1. The `_beat` coroutine tries to send a heartbeat, which fails with `StorageError`.
2. Because `StorageError` is a subclass of `JQueueError` (`domain/errors.py:34`), the
   `except JQueueError` handler catches it and the heartbeat task exits silently.
3. The worker continues processing the job, unaware that heartbeats have stopped.
4. After `stale_timeout`, `BrokerQueue` (on a different machine) requeues the job.
5. Another worker picks it up — now two workers are processing the same job.
6. When the original worker finishes and calls `ack()`, the job may no longer exist
   (the second worker already acked it), raising `JobNotFoundError`.

The implication is clear: **job handlers must be idempotent**, and `ack()` should catch
`JobNotFoundError` as a benign condition.

### Production deployment

**Architecture.** In a distributed setup, web servers enqueue jobs and a pool of
workers dequeue and process them. All processes point their `BrokerQueue` at the same
S3/GCS blob. Each process runs its own `GroupCommitLoop`, which batches operations
*within* that process; the CAS protocol serialises writes *across* processes.

```
Web servers ──enqueue──> ┌─────────────────────┐ <──dequeue── Workers
                         │   S3 / GCS bucket    │
                         │   queue.json blob     │
                         └─────────────────────┘
```

There is no leader election, no coordinator, and no discovery mechanism. Processes
don't even need to know about each other. The CAS write is the only synchronisation
point — if your write succeeds, you held the "lock." If it fails, you re-read and
retry.

**Monitoring.** `read_state()` performs a single storage read without entering the write
pipeline, so it's safe to call from a health-check or metrics endpoint:

```python
async def collect_metrics(q: BrokerQueue) -> dict:
    state = await q.read_state()
    now = datetime.now(UTC)
    queued = state.queued_jobs()
    return {
        "queue_depth": len(queued),
        "in_progress": len(state.in_progress_jobs()),
        "version": state.version,
        "oldest_queued_age_s": (
            (now - min(j.created_at for j in queued)).total_seconds()
            if queued else 0
        ),
    }
```

The metrics worth watching: **queue depth** (is work piling up faster than workers
drain it?), **in-progress count** (are workers keeping busy?), **oldest queued age**
(is any job stuck waiting too long?), and **version** (is the queue making forward
progress at all?).

**Capacity planning:**

| Peak throughput | Recommended queue | Notes |
|---|---|---|
| < 5 ops/s | `DirectQueue` | Simplest code path, no background task |
| 5 - 50 ops/s | `BrokerQueue` | Group commit absorbs contention |
| > 50 ops/s | Multiple `BrokerQueue`s | Partition by entrypoint |

To estimate the number of workers needed: `peak_ops_per_second * avg_job_duration_seconds`.
For example, 10 jobs/s with 5 seconds of processing each requires 50 concurrent workers
to keep up.

**Common pitfalls:**

1. **Missing `HeartbeatManager` on long jobs.** Any job running longer than
   `stale_timeout` (default 5 minutes) will be requeued while the original worker is
   still processing it. Always wrap long-running work in a `HeartbeatManager`.

2. **`stale_timeout` shorter than job duration.** If your jobs routinely take 10
   minutes but `stale_timeout` is 5 minutes, healthy jobs will be requeued even with
   heartbeats disabled. Either increase the timeout or — better — add heartbeats.

3. **Crashing on `JobNotFoundError` during `ack()`.** In any system with multiple
   workers, a job can be processed and acked by someone else before you finish.
   This is normal — treat `JobNotFoundError` on `ack` as a no-op, not a crash.

4. **Unbounded queue growth.** The entire state is serialised on every operation, so
   performance degrades as queue depth increases. Monitor queue depth and ensure
   workers keep pace. If the queue consistently exceeds ~1 000 jobs, partition by
   entrypoint or add workers.

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
