# AGENTS.md — Coding Agent Guidelines for jqueue

This document provides coding agents with essential information about the jqueue codebase: build commands, code style, architecture patterns, and conventions.

## Project Overview

**jqueue** is a storage-agnostic async job queue using object storage with compare-and-set (CAS) semantics. It implements the [turbopuffer object-storage queue pattern](https://turbopuffer.com/blog/object-storage-queue).

- **Language**: Python 3.12+ (strictly typed)
- **Framework**: AsyncIO with async/await throughout
- **Architecture**: Ports & Adapters (Hexagonal Architecture)
- **Package Manager**: `uv` (NOT pip or poetry)

## Build, Test, and Lint Commands

### Development Setup
```bash
# Install dev dependencies
uv sync --extra dev

# Install all extras (s3, gcs, dev)
uv sync --extra s3 --extra gcs --extra dev
```

### Testing
```bash
# Run all tests
uv run pytest tests/ -v

# Run a single test file
uv run pytest tests/test_broker_queue.py -v

# Run a single test function
uv run pytest tests/test_broker_queue.py::test_enqueue_returns_job -v

# Run tests matching a pattern
uv run pytest tests/ -v -k "enqueue"
```

### Linting and Formatting
```bash
# Check formatting (don't modify files)
uv run ruff format --check .

# Apply formatting
uv run ruff format .

# Lint code (checks E, F, I, UP rules)
uv run ruff check .

# Auto-fix lint issues
uv run ruff check --fix .
```

### Type Checking
```bash
# Run strict type checking
uv run mypy .

# Type check a specific file
uv run mypy jqueue/core/broker.py
```

### Building
```bash
# Build distribution packages
uv build
```

## Architecture Pattern

jqueue follows **Ports & Adapters** (Hexagonal Architecture):

```
jqueue/
├── domain/          # Pure domain models and errors (Pydantic-based)
│   ├── models.py    # Job, QueueState, JobStatus
│   └── errors.py    # JQueueError, CASConflictError, JobNotFoundError, StorageError
├── ports/           # Protocol interfaces (structural typing)
│   └── storage.py   # ObjectStoragePort (the single port)
├── core/            # Business logic
│   ├── broker.py    # BrokerQueue (high-throughput with group commit)
│   ├── direct.py    # DirectQueue (simple one-operation-per-write)
│   ├── group_commit.py  # Group commit batching algorithm
│   ├── heartbeat.py # HeartbeatManager for job liveness
│   └── codec.py     # JSON serialization
└── adapters/        # Concrete implementations
    └── storage/
        ├── memory.py      # InMemoryStorage (testing)
        ├── filesystem.py  # LocalFileSystemStorage (POSIX)
        ├── s3.py          # S3Storage (requires aioboto3)
        └── gcs.py         # GCSStorage (requires google-cloud-storage)
```

**Key Principles**:
- **Immutable domain models**: Pydantic with `frozen=True`
- **Protocol-based interfaces**: No inheritance, structural typing only
- **Dependency injection**: Via constructor parameters
- **Async-first**: All I/O operations use async/await
- **CAS semantics**: Compare-and-set for concurrency safety

## Code Style Guidelines

### Imports
- Use absolute imports: `from jqueue.domain.models import Job`
- Group imports: stdlib → third-party → local
- Use `from __future__ import annotations` for forward references
- Ruff enforces isort-style import ordering (rule `I`)

### Type Hints
- **Strict typing**: All functions require type hints (mypy strict mode)
- Use `X | None` (not `Optional[X]`) — Python 3.10+ union syntax
- Use Protocol for interfaces, not abstract base classes
- Use `tuple[X, ...]` for variable-length homogeneous tuples
- Use `list[X]`, `dict[K, V]`, not `List`, `Dict` from typing
- Return type annotations are mandatory, including `-> None`

### Naming Conventions
- **Functions/methods**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private attributes**: prefix with single underscore (`_lock`, `_task`)
- **Domain concepts**: Use domain language (Job, QueueState, entrypoint)

### Docstrings
- **Module-level**: Explain purpose and usage
- **Class-level**: Brief description + Parameters section
- **Public methods**: Use Google/NumPy style with sections:
  ```python
  """
  Brief one-line summary.

  Longer description if needed.

  Parameters
  ----------
  param_name : type
      Description

  Returns
  -------
  return_type
      Description

  Raises
  ------
  ExceptionType
      When this happens
  """
  ```
- Private methods may omit docstrings if purpose is obvious

### Error Handling
- All custom exceptions inherit from `JQueueError`
- Use specific exceptions: `CASConflictError`, `JobNotFoundError`, `StorageError`
- Wrap external exceptions in `StorageError` with cause:
  ```python
  try:
      await external_operation()
  except Exception as e:
      raise StorageError("Operation failed", cause=e) from e
  ```
- Document raised exceptions in docstrings

### Immutability Pattern
Domain models are frozen (immutable). Use functional update style:
```python
# Good: Return new instance
updated_job = job.model_copy(update={"status": JobStatus.IN_PROGRESS})
# or use helper methods
updated_job = job.with_status(JobStatus.IN_PROGRESS)

# Bad: Don't mutate
job.status = JobStatus.IN_PROGRESS  # ❌ Raises FrozenInstanceError
```

### Async/Await
- All I/O operations must be async
- Use `async with` for context managers (BrokerQueue, HeartbeatManager)
- Use `asyncio.gather()` for concurrent operations
- Mark async functions with `async def`, never use sync wrappers

### Data Classes and Models
- Use `@dataclasses.dataclass` for simple containers
- Use Pydantic `BaseModel` for domain models (validation, serialization)
- Pydantic models: set `model_config = ConfigDict(frozen=True)`
- Dataclasses: no frozen required unless immutability needed

## Testing Conventions

### Test File Organization
- Location: `/tests/` (flat directory)
- Naming: `test_<module>.py` (e.g., `test_broker_queue.py`)
- Use section comments to group related tests:
  ```python
  # ---------------------------------------------------------------------------
  # enqueue operations
  # ---------------------------------------------------------------------------
  ```

### Test Function Naming
- Descriptive names: `test_enqueue_returns_job`, `test_dequeue_empty_returns_empty_list`
- Return type annotation: `-> None`

### Test Patterns
```python
async def test_operation_behavior() -> None:
    # Arrange
    storage = InMemoryStorage()
    async with BrokerQueue(storage) as q:
        # Act
        result = await q.enqueue("task", b"data")
        
        # Assert
        assert result.status == JobStatus.QUEUED
```

### Fixtures and Helpers
- Use pytest fixtures for shared setup
- Use `InMemoryStorage()` for tests (zero dependencies)
- Test async code with `pytest-asyncio` (auto mode enabled)

## Common Patterns

### Creating a Queue
```python
from jqueue import BrokerQueue, InMemoryStorage

async with BrokerQueue(InMemoryStorage()) as q:
    job = await q.enqueue("send_email", b'{"to": "user@example.com"}')
```

### Implementing a Storage Adapter
Implement the `ObjectStoragePort` Protocol:
```python
class CustomStorage:
    async def read(self) -> tuple[bytes, str | None]:
        """Return (content, etag). etag is None if object doesn't exist."""
        ...
    
    async def write(self, content: bytes, if_match: str | None = None) -> str:
        """CAS write. Raise CASConflictError if if_match doesn't match."""
        ...
```

## References

- **Architecture**: See `ports-and-adapters.md` for detailed design rationale
- **README**: Comprehensive usage examples and API documentation
- **Repository**: https://github.com/janbjorge/jqueue
- **License**: MIT
