# Queries and Services

## Status

Accepted

## Context

`DirectQueue` and `GroupCommitLoop` each duplicated the job lifecycle logic (claim, nack,
heartbeat, stale re-queue) as closures, passing results out via `nonlocal`.

## Decision

Split pure state logic (queries) from storage and transaction handling (services).

| Database term | jqueue                                           |
|---------------|--------------------------------------------------|
| Pool          | `ObjectStoragePort`                              |
| Connection    | one `QueueState` snapshot                        |
| Transaction   | read → operate → `write(if_match=etag)`, retry on conflict |
| Query         | `StateQueries` (`core/queries.py`)               |
| Service       | `DirectQueue`, `GroupCommitLoop`                 |

**Queries (`StateQueries`)**

1. Operate on a `QueueState`. No I/O, no `async`.
2. Take the clock as an argument (`now`, `cutoff`).
3. Writes return the affected job(s) or a count. Reads return `None` if nothing is found.
4. Writes on a missing job raise `JobNotFoundError`.
5. Each operation either fully applies or leaves `state` unchanged.

**Services**

1. Accept `ObjectStoragePort`. Own the CAS cycle.
2. Build a fresh `StateQueries` per attempt. Units of work may re-run, so they must be pure.
   Build values that must be stable across retries (e.g. `Job.new`) outside them.
3. Units of work are `Callable[[StateQueries], T]` and return their result directly.
4. Each public method commits fully or raises a domain error.

`Job` and `QueueState` stay Pydantic, since they are the stored format.

## Consequences

- One lifecycle implementation, unit-testable without storage.
- Transaction boundaries live in one place per service.
- Units of work can run more than once; keep them side-effect free.
