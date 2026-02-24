#!/usr/bin/env -S uv run
"""
Storage Adapter Benchmark Tool for jqueue

Benchmarks InMemoryStorage and LocalFileSystemStorage adapters using
realistic queue operations (enqueue/dequeue/ack).

Usage:
    uv run tools/benchmark_storage.py
    uv run tools/benchmark_storage.py --operations 5000 --concurrency 50
    uv run tools/benchmark_storage.py --quick
    uv run tools/benchmark_storage.py --help
"""
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pydantic>=2.0",
#     "typer>=0.9.0",
#     "rich>=13.0",
# ]
# ///

from __future__ import annotations

import asyncio
import statistics
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter

import typer

# Import jqueue components from the local package
# Add parent directory to path to import jqueue
sys.path.insert(0, str(Path(__file__).parent.parent))

from jqueue import BrokerQueue
from jqueue.adapters.storage.filesystem import LocalFileSystemStorage
from jqueue.adapters.storage.memory import InMemoryStorage

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

app = typer.Typer(
    help="Benchmark jqueue storage adapters",
    add_completion=False,
)


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark runs."""

    operations: int = 1000
    concurrency_levels: list[int] = field(default_factory=lambda: [10, 50])
    payload_size: int = 1000
    adapters: list[str] = field(default_factory=lambda: ["memory", "filesystem"])


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""

    adapter_name: str
    operation: str
    total_ops: int
    total_time: float
    latencies: list[float]  # seconds

    @property
    def ops_per_sec(self) -> float:
        """Calculate operations per second."""
        return self.total_ops / self.total_time if self.total_time > 0 else 0.0

    @property
    def p50(self) -> float:
        """Calculate 50th percentile (median) latency in seconds."""
        return statistics.median(self.latencies) if self.latencies else 0.0

    @property
    def p95(self) -> float:
        """Calculate 95th percentile latency in seconds."""
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]

    @property
    def p99(self) -> float:
        """Calculate 99th percentile latency in seconds."""
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]

    @property
    def max_latency(self) -> float:
        """Get maximum latency in seconds."""
        return max(self.latencies) if self.latencies else 0.0

    @property
    def min_latency(self) -> float:
        """Get minimum latency in seconds."""
        return min(self.latencies) if self.latencies else 0.0

    def format_latency_ms(self, seconds: float) -> str:
        """Format latency in milliseconds."""
        ms = seconds * 1000
        if ms < 1:
            return f"{ms:.3f}ms"
        elif ms < 10:
            return f"{ms:.2f}ms"
        else:
            return f"{ms:.1f}ms"


# ---------------------------------------------------------------------------
# Core Benchmark Functions
# ---------------------------------------------------------------------------


async def benchmark_sequential_enqueue(
    queue: BrokerQueue,
    n: int,
    payload: bytes,
) -> list[float]:
    """
    Benchmark N sequential enqueue operations.

    Parameters
    ----------
    queue : BrokerQueue instance
    n : number of operations
    payload : job payload bytes

    Returns
    -------
    list[float] : latency for each operation in seconds
    """
    latencies = []
    for _ in range(n):
        start = perf_counter()
        await queue.enqueue("benchmark_task", payload)
        latencies.append(perf_counter() - start)
    return latencies


async def benchmark_concurrent_enqueue(
    queue: BrokerQueue,
    n: int,
    concurrency: int,
    payload: bytes,
) -> list[float]:
    """
    Benchmark N enqueue operations with specified concurrency level.

    Parameters
    ----------
    queue : BrokerQueue instance
    n : total number of operations
    concurrency : number of concurrent tasks
    payload : job payload bytes

    Returns
    -------
    list[float] : latency for each operation in seconds
    """
    latencies = []

    async def enqueue_one() -> float:
        start = perf_counter()
        await queue.enqueue("benchmark_task", payload)
        return perf_counter() - start

    # Run in batches of 'concurrency' tasks
    for i in range(0, n, concurrency):
        batch_size = min(concurrency, n - i)
        batch_latencies = await asyncio.gather(
            *[enqueue_one() for _ in range(batch_size)]
        )
        latencies.extend(batch_latencies)

    return latencies


async def benchmark_sequential_dequeue(
    queue: BrokerQueue,
    n: int,
) -> list[float]:
    """
    Benchmark N sequential dequeue operations.

    Assumes queue already has N jobs enqueued.

    Parameters
    ----------
    queue : BrokerQueue instance
    n : number of operations

    Returns
    -------
    list[float] : latency for each operation in seconds
    """
    latencies = []
    for _ in range(n):
        start = perf_counter()
        jobs = await queue.dequeue("benchmark_task", batch_size=1)
        latencies.append(perf_counter() - start)
        if not jobs:
            # Queue empty, stop
            break
    return latencies


async def benchmark_concurrent_dequeue(
    queue: BrokerQueue,
    n: int,
    concurrency: int,
) -> list[float]:
    """
    Benchmark N dequeue operations with specified concurrency level.

    Assumes queue already has N jobs enqueued.

    Parameters
    ----------
    queue : BrokerQueue instance
    n : total number of operations
    concurrency : number of concurrent tasks

    Returns
    -------
    list[float] : latency for each operation in seconds
    """
    latencies = []

    async def dequeue_one() -> float | None:
        start = perf_counter()
        jobs = await queue.dequeue("benchmark_task", batch_size=1)
        if jobs:
            return perf_counter() - start
        return None

    # Run in batches of 'concurrency' tasks until we get n successful dequeues
    completed = 0
    while completed < n:
        batch_size = min(concurrency, n - completed)
        batch_results = await asyncio.gather(
            *[dequeue_one() for _ in range(batch_size)]
        )
        successful_latencies = [lat for lat in batch_results if lat is not None]
        latencies.extend(successful_latencies)
        completed += len(successful_latencies)

    return latencies


async def benchmark_ack(
    queue: BrokerQueue,
    job_ids: list[str],
) -> list[float]:
    """
    Benchmark ack operations for a list of job IDs.

    Parameters
    ----------
    queue : BrokerQueue instance
    job_ids : list of job IDs to ack

    Returns
    -------
    list[float] : latency for each operation in seconds
    """
    latencies = []
    for job_id in job_ids:
        start = perf_counter()
        await queue.ack(job_id)
        latencies.append(perf_counter() - start)
    return latencies


async def benchmark_mixed_workload(
    queue: BrokerQueue,
    n: int,
    concurrency: int,
    payload: bytes,
) -> list[float]:
    """
    Benchmark mixed producer/consumer workload.

    50% of concurrent tasks enqueue jobs, 50% dequeue and ack.

    Parameters
    ----------
    queue : BrokerQueue instance
    n : total number of operations (divided between enqueue and dequeue)
    concurrency : number of concurrent tasks
    payload : job payload bytes

    Returns
    -------
    list[float] : latency for each operation in seconds
    """

    @dataclass
    class WorkloadState:
        """Track progress of mixed workload."""

        enqueued: int = 0
        dequeued: int = 0

    latencies = []
    enqueue_count = n // 2
    dequeue_count = n // 2
    state = WorkloadState()

    # Pre-populate some jobs for consumers
    for _ in range(concurrency):
        await queue.enqueue("benchmark_task", payload)

    async def producer() -> tuple[float | None, bool]:
        """
        Execute one enqueue operation.

        Returns
        -------
        tuple[float | None, bool] : (latency, success)
        """
        if state.enqueued >= enqueue_count:
            return None, False
        start = perf_counter()
        await queue.enqueue("benchmark_task", payload)
        return perf_counter() - start, True

    async def consumer() -> tuple[float | None, bool]:
        """
        Execute one dequeue+ack operation.

        Returns
        -------
        tuple[float | None, bool] : (latency, success)
        """
        if state.dequeued >= dequeue_count:
            return None, False
        start = perf_counter()
        jobs = await queue.dequeue("benchmark_task", batch_size=1)
        if jobs:
            await queue.ack(jobs[0].id)
            return perf_counter() - start, True
        return None, False

    # Run mixed workload
    while state.enqueued < enqueue_count or state.dequeued < dequeue_count:
        # Create mix of producers and consumers
        tasks = []
        producer_count = min(concurrency // 2, enqueue_count - state.enqueued)
        consumer_count = min(concurrency // 2, dequeue_count - state.dequeued)

        tasks.extend([("producer", producer()) for _ in range(producer_count)])
        tasks.extend([("consumer", consumer()) for _ in range(consumer_count)])

        if tasks:
            task_types, task_coros = zip(*tasks)
            results = await asyncio.gather(*task_coros)

            # Update state and collect latencies
            for task_type, (latency, success) in zip(task_types, results):
                if success:
                    if task_type == "producer":
                        state.enqueued += 1
                    else:
                        state.dequeued += 1
                    if latency is not None:
                        latencies.append(latency)
        else:
            break

    return latencies


# ---------------------------------------------------------------------------
# Storage Adapter Setup
# ---------------------------------------------------------------------------


async def create_storage_adapter(
    adapter_name: str,
    temp_dir: Path | None = None,
) -> InMemoryStorage | LocalFileSystemStorage:
    """
    Create a storage adapter instance.

    Parameters
    ----------
    adapter_name : "memory" or "filesystem"
    temp_dir : temporary directory for filesystem storage

    Returns
    -------
    Storage adapter instance
    """
    if adapter_name == "memory":
        return InMemoryStorage()
    elif adapter_name == "filesystem":
        if temp_dir is None:
            raise ValueError("temp_dir required for filesystem storage")
        return LocalFileSystemStorage(temp_dir / "queue_state.json")
    else:
        raise ValueError(f"Unknown adapter: {adapter_name}")


# ---------------------------------------------------------------------------
# Benchmark Runner
# ---------------------------------------------------------------------------


async def run_adapter_benchmark(
    adapter_name: str,
    config: BenchmarkConfig,
    temp_dir: Path | None = None,
) -> list[BenchmarkResult]:
    """
    Run all benchmarks for a single storage adapter.

    Parameters
    ----------
    adapter_name : name of the adapter to benchmark
    config : benchmark configuration
    temp_dir : temporary directory for filesystem storage

    Returns
    -------
    list[BenchmarkResult] : results for all benchmark scenarios
    """
    results = []
    payload = b"x" * config.payload_size

    # Sequential enqueue
    storage = await create_storage_adapter(adapter_name, temp_dir)
    async with BrokerQueue(storage) as queue:
        start = perf_counter()
        latencies = await benchmark_sequential_enqueue(
            queue, config.operations, payload
        )
        total_time = perf_counter() - start

    results.append(
        BenchmarkResult(
            adapter_name=adapter_name,
            operation="enqueue-seq",
            total_ops=config.operations,
            total_time=total_time,
            latencies=latencies,
        )
    )

    # Concurrent enqueue at different concurrency levels
    for concurrency in config.concurrency_levels:
        storage = await create_storage_adapter(adapter_name, temp_dir)
        async with BrokerQueue(storage) as queue:
            start = perf_counter()
            latencies = await benchmark_concurrent_enqueue(
                queue, config.operations, concurrency, payload
            )
            total_time = perf_counter() - start

        results.append(
            BenchmarkResult(
                adapter_name=adapter_name,
                operation=f"enqueue-c{concurrency}",
                total_ops=config.operations,
                total_time=total_time,
                latencies=latencies,
            )
        )

    # Sequential dequeue
    storage = await create_storage_adapter(adapter_name, temp_dir)
    async with BrokerQueue(storage) as queue:
        # Pre-populate queue
        for _ in range(config.operations):
            await queue.enqueue("benchmark_task", payload)

        start = perf_counter()
        latencies = await benchmark_sequential_dequeue(queue, config.operations)
        total_time = perf_counter() - start

    results.append(
        BenchmarkResult(
            adapter_name=adapter_name,
            operation="dequeue-seq",
            total_ops=len(latencies),
            total_time=total_time,
            latencies=latencies,
        )
    )

    # Mixed workload at middle concurrency level
    mixed_concurrency = config.concurrency_levels[len(config.concurrency_levels) // 2]
    storage = await create_storage_adapter(adapter_name, temp_dir)
    async with BrokerQueue(storage) as queue:
        start = perf_counter()
        latencies = await benchmark_mixed_workload(
            queue, config.operations, mixed_concurrency, payload
        )
        total_time = perf_counter() - start

    results.append(
        BenchmarkResult(
            adapter_name=adapter_name,
            operation=f"mixed-c{mixed_concurrency}",
            total_ops=len(latencies),
            total_time=total_time,
            latencies=latencies,
        )
    )

    return results


# ---------------------------------------------------------------------------
# Result Formatting
# ---------------------------------------------------------------------------


def format_results_rich(results: list[BenchmarkResult]) -> None:
    """
    Format and print results using Rich library.

    Parameters
    ----------
    results : list of benchmark results
    """
    console = Console()

    # Group results by adapter
    adapters = {}
    for result in results:
        if result.adapter_name not in adapters:
            adapters[result.adapter_name] = []
        adapters[result.adapter_name].append(result)

    # Print header
    console.print()
    console.print(
        Panel(
            "[bold cyan]Storage Adapter Benchmark Results[/bold cyan]",
            expand=False,
        )
    )

    # Print table for each adapter
    for adapter_name, adapter_results in adapters.items():
        console.print()
        console.print(f"[bold yellow]Adapter: {adapter_name}[/bold yellow]")
        console.print()

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Operation", style="cyan", width=15)
        table.add_column("Ops/sec", justify="right", style="green")
        table.add_column("P50", justify="right")
        table.add_column("P95", justify="right")
        table.add_column("P99", justify="right")
        table.add_column("Max", justify="right")

        for result in adapter_results:
            table.add_row(
                result.operation,
                f"{result.ops_per_sec:.1f}",
                result.format_latency_ms(result.p50),
                result.format_latency_ms(result.p95),
                result.format_latency_ms(result.p99),
                result.format_latency_ms(result.max_latency),
            )

        console.print(table)

    console.print()


def format_results_plain(results: list[BenchmarkResult]) -> None:
    """
    Format and print results using plain text (fallback).

    Parameters
    ----------
    results : list of benchmark results
    """
    # Group results by adapter
    adapters = {}
    for result in results:
        if result.adapter_name not in adapters:
            adapters[result.adapter_name] = []
        adapters[result.adapter_name].append(result)

    print("\n" + "=" * 80)
    print("Storage Adapter Benchmark Results")
    print("=" * 80)

    for adapter_name, adapter_results in adapters.items():
        print(f"\nAdapter: {adapter_name}")
        print("-" * 80)
        print(
            f"{'Operation':<15} | {'Ops/sec':>9} | {'P50':>8} | {'P95':>8} | {'P99':>8} | {'Max':>8}"
        )
        print("-" * 80)

        for result in adapter_results:
            print(
                f"{result.operation:<15} | "
                f"{result.ops_per_sec:>9.1f} | "
                f"{result.format_latency_ms(result.p50):>8} | "
                f"{result.format_latency_ms(result.p95):>8} | "
                f"{result.format_latency_ms(result.p99):>8} | "
                f"{result.format_latency_ms(result.max_latency):>8}"
            )

    print("\n" + "=" * 80 + "\n")


def format_results(results: list[BenchmarkResult]) -> None:
    """
    Format and print benchmark results.

    Uses Rich if available, falls back to plain text.

    Parameters
    ----------
    results : list of benchmark results
    """
    if RICH_AVAILABLE:
        format_results_rich(results)
    else:
        format_results_plain(results)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    operations: int = typer.Option(
        1000,
        "--operations",
        "-n",
        help="Number of operations per benchmark",
    ),
    adapters: str = typer.Option(
        "memory,filesystem",
        "--adapters",
        "-a",
        help="Comma-separated adapters to test",
    ),
) -> None:
    """
    Benchmark jqueue storage adapters.

    Measures throughput (ops/sec) and latency percentiles (p50/p95/p99/max)
    for storage adapters using realistic queue operations.

    Default configuration:
    - 1000 operations per benchmark
    - Concurrency levels: 10, 50
    - Payload size: 1KB
    - Tests both InMemoryStorage and LocalFileSystemStorage
    """
    # Parse adapter list
    adapter_list = [a.strip() for a in adapters.split(",")]

    # Sane defaults
    config = BenchmarkConfig(
        operations=operations,
        concurrency_levels=[10, 50],
        payload_size=1000,
        adapters=adapter_list,
    )

    # Run benchmarks
    all_results = []

    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)

        for adapter_name in config.adapters:
            try:
                results = asyncio.run(
                    run_adapter_benchmark(adapter_name, config, temp_dir)
                )
                all_results.extend(results)
            except Exception as e:
                print(f"\nError benchmarking {adapter_name}: {e}", file=sys.stderr)

    # Format and display results
    if all_results:
        format_results(all_results)
    else:
        print("\nNo benchmark results to display.", file=sys.stderr)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
