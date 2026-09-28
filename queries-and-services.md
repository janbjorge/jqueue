# Queries and Services

## Status

Accepted

## Context

jqueue has two write paths over the same storage port:

- `DirectQueue` runs one CAS cycle per operation.
- `GroupCommitLoop` (behind `BrokerQueue`) batches many operations into one CAS cycle.

Both used to carry their own copy of the job lifecycle logic (claim, nack, heartbeat,
stale re-queue) as ad-hoc closures of type `QueueState -> QueueState`. Results such as the
list of claimed jobs were smuggled out through `nonlocal` variables. The two copies could
drift apart, and the only way to test the lifecycle logic was through a storage adapter.

This ADR adapts the "Queries and Services" split, originally written for a
Postgres/FastAPI codebase, to jqueue's object-storage model. It builds on
`ports-and-adapters.md`.

## Mapping from the database version

| Database concept         | jqueue equivalent                                    |
|--------------------------|------------------------------------------------------|
| `Pool`                   | `ObjectStoragePort`                                  |
| `Connection`             | one `QueueState` snapshot, read at the start of a CAS cycle |
| Transaction              | read → operate → `write(if_match=etag)`, retried on `CASConflictError` |
| Query class              | `StateQueries` (`jqueue/core/queries.py`)            |
| Service class            | `DirectQueue`, `GroupCommitLoop` (and `BrokerQueue` as its facade) |
| Endpoint / `deps.py`     | the caller's composition root: `BrokerQueue(S3Storage(...))` |
| HTTP-free `errors.py`    | `jqueue/domain/errors.py`                            |

## Decision

### Query class: `StateQueries`

`StateQueries` holds one `QueueState` snapshot and exposes the domain operations on it:

```python
@dataclasses.dataclass
class StateQueries:
    state: QueueState

    def find(self, job_id: str) -> Job | None: ...
    def add(self, job: Job) -> Job: ...
    def claim(self, entrypoint: str | None, batch_size: int, now: datetime) -> list[Job]: ...
    def remove(self, job_id: str) -> Job: ...
    def release(self, job_id: str) -> Job: ...
    def touch(self, job_id: str, now: datetime) -> Job: ...
    def requeue_stale(self, cutoff: datetime) -> int: ...
```

**Rules:**

1. Operate on a `QueueState`, never on `ObjectStoragePort`. No I/O, no `async`.
   The service owns the storage round-trip, just as the database version's service owns
   connection acquisition.
2. Use `@dataclass`. The snapshot is the only field (the database version's Option A:
   "connection at class level").
3. Inputs are basic types or domain models. The clock is an input (`now`, `cutoff`),
   so query behaviour is deterministic and testable without patching time.
4. Return domain values: `Job`, `list[Job]`, `int`. Never return raw bytes or a codec payload.
5. Writes return the affected record: `add`, `remove`, `release`, `touch` return the job;
   `claim` returns the claimed jobs; `requeue_stale` returns the affected count.
   Reads return `None` when nothing is found.
6. Writes on a missing job raise `JobNotFoundError`, not `None`, because the enclosing
   unit of work must fail visibly.
7. Every operation is atomic with respect to `self.state`: compute the new snapshot, then
   assign it. An operation that raises leaves `state` untouched. `GroupCommitLoop` relies
   on this so that one failing operation does not corrupt the rest of its batch.

**Where Pydantic fits.** The database version bans Pydantic below the API boundary.
In jqueue, `QueueState` and `Job` *are* the persisted wire format (see `codec.py`), so
they remain frozen Pydantic models. The rule that carries over is that queries return
immutable domain values, not live or mutable objects.

### Service classes: `DirectQueue`, `GroupCommitLoop`

Services accept the storage port and own the transaction boundary. Each public method is
one unit of work: it either commits entirely or raises.

```python
@dataclasses.dataclass
class DirectQueue:
    storage: ObjectStoragePort
    max_retries: int = 10

    async def dequeue(self, entrypoint: str | None = None, *, batch_size: int = 1) -> list[Job]:
        return await self._transaction(
            lambda q: q.claim(entrypoint, batch_size, datetime.now(UTC))
        )

    async def _transaction[T](self, fn: Callable[[StateQueries], T]) -> T:
        for attempt in range(self.max_retries):
            content, etag = await self.storage.read()
            queries = StateQueries(codec.decode(content))
            result = fn(queries)
            try:
                await self.storage.write(codec.encode(queries.state), if_match=etag)
                return result
            except CASConflictError:
                ...
```

**Rules:**

1. Accept `ObjectStoragePort`, never a `QueueState`. Services decide when to read and write.
2. Use `@dataclass`. Store the port and configuration such as retries and stale timeout.
3. Build a fresh `StateQueries` inside each CAS attempt. The unit-of-work function is
   re-run on the new snapshot after a conflict, so it must be a pure function of that snapshot.
   Build values that must stay stable across retries, such as `Job.new(...)` and its
   UUID, *outside* the function.
4. Return results from the unit-of-work function directly (`Callable[[StateQueries], T]`).
   Do not pass them out through `nonlocal` state.
5. Services supply the clock (`datetime.now(UTC)`) and pass it into queries.
6. Raise domain exceptions (`JobNotFoundError`, `CASConflictError`, `StorageError`).
   A failed method commits nothing.
7. The transaction strategy is what distinguishes services, not the domain logic.
   `DirectQueue` commits one operation per write. `GroupCommitLoop` applies a batch of
   `Callable[[StateQueries], T]` operations to one `StateQueries` and commits them in a
   single write, resolving each caller's future with its own result or exception.

### Wiring (the `deps.py` equivalent)

jqueue is a library and has no DI container. The composition root is the caller:

```python
async with BrokerQueue(S3Storage(bucket="jobs", key="queue.json")) as q:
    ...
```

Adapters are injected through constructors only. Services never construct adapters, and
queries never see them.

### Error handling

Domain errors live in `jqueue/domain/errors.py` and inherit from `JQueueError`. They
describe queue semantics (a job is missing, a CAS write lost, storage failed) and carry
no transport concerns. Applications that expose jqueue over HTTP translate them at their
own boundary.

## Consequences

### Positive

- One implementation of the job lifecycle, shared by both write paths.
- Lifecycle logic is unit-testable synchronously, without storage or event loops
  (`tests/test_queries.py`).
- Transaction boundaries are explicit and live in exactly one method per service
  (`DirectQueue._transaction`, `GroupCommitLoop._commit_batch`).
- Typed results replace `nonlocal` result passing.

### Negative

- One more module in `core/`.
- Contributors must remember that unit-of-work functions may run more than once.

## Alternatives Considered

- **Keep `QueueState -> QueueState` closures in each service.** Rejected because it
  duplicates logic and hides results in `nonlocal` state.
- **Put the operations on `QueueState` itself.** Partly done already (`with_job_added`
  and similar). Operations that need a clock and return a result alongside the new state
  (`claim`, `requeue_stale`) do not fit a pure value type cleanly, and `QueueState` should
  stay a serialisable snapshot.
- **Free functions returning `(new_state, result)` tuples.** Workable, but noisier at every
  call site and harder to compose in a batch than a single stateful `StateQueries`.

## Appendix

```
    ┌──────────────────────────────────────────────────────────┐
    │  Caller / composition root                               │
    │  BrokerQueue(S3Storage(...)), DirectQueue(GCSStorage())  │
    └──────────────┬───────────────────────────────┬───────────┘
                   │                               │
    ┌──────────────┴─────────────┐  ┌──────────────┴───────────┐
    │  DirectQueue               │  │  BrokerQueue             │
    │  1 op  → 1 CAS write       │  │  └─ GroupCommitLoop      │
    │                            │  │     N ops → 1 CAS write  │
    └──────────────┬─────────────┘  └──────────────┬───────────┘
                   │  services: own the CAS cycle  │
                   ├───────────────┬───────────────┤
                   │               │               │
    ┌──────────────┴──────┐  ┌─────┴───────────────┴────────────┐
    │  StateQueries       │  │  ObjectStoragePort (port)        │
    │  pure ops on one    │  │  read() / write(if_match)        │
    │  QueueState         │  │  ← memory, filesystem, s3, gcs   │
    └─────────────────────┘  └──────────────────────────────────┘
```
