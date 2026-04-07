"""Benchmark DBOS queue.enqueue_async + completion throughput at a target rate.

Two-phase: enqueue all workflows at the target rate, then drain all completions.
Reports both enqueue and end-to-end completion throughput.
"""

import argparse
import asyncio
import multiprocessing as mp
import os
import time
from urllib.parse import urlparse

import asyncpg


async def recreate_database() -> None:
    """Drop and recreate the benchmark database via POSTGRES_DATABASE_URL."""
    admin_url = os.environ["POSTGRES_DATABASE_URL"]
    bench_db = urlparse(os.environ["BENCHMARK_DATABASE_URL"]).path.lstrip("/")
    conn = await asyncpg.connect(admin_url)
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{bench_db}" WITH (FORCE)')
        await conn.execute(f'CREATE DATABASE "{bench_db}"')
    finally:
        await conn.close()


def bootstrap_schema_entry() -> None:
    """Pre-create the DBOS system schema in a one-shot subprocess.

    Runs in its own spawned process so the parent never imports DBOS.
    Pre-running migrations eliminates the per-worker advisory-lock serialization
    when many workers launch in parallel.
    """
    from dbos import DBOS, DBOSConfig

    config: DBOSConfig = {
        "name": "dbos-queue-bench-bootstrap",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": 2,
    }
    DBOS(config=config)
    DBOS.launch()
    DBOS.destroy()


def worker_entry(
    target_rps: int,
    duration_s: float,
    batch_size: int,
    pool_size: int,
    executor_threads: int,
    done_barrier,
    result_queue: mp.Queue,
) -> None:
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig, Queue

    @DBOS.workflow()
    async def noop_workflow() -> int:
        return 1

    queue = Queue("bench-queue")

    config: DBOSConfig = {
        "name": "dbos-queue-bench",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
    }
    DBOS(config=config)
    DBOS.launch()

    async def enqueue_batch() -> list:
        # Fire all enqueues in this batch concurrently.
        return await asyncio.gather(
            *(queue.enqueue_async(noop_workflow) for _ in range(batch_size))
        )

    async def run() -> dict:
        batches_per_second = target_rps / batch_size
        interval = 1.0 / batches_per_second
        total_batches = int(batches_per_second * duration_s)

        handles: list = []
        enqueue_failures = 0

        # --- Phase 1: enqueue at target rate ---
        enqueue_start = time.monotonic()
        for i in range(total_batches):
            target_time = enqueue_start + i * interval
            now = time.monotonic()
            if target_time > now:
                await asyncio.sleep(target_time - now)
            try:
                handles.extend(await enqueue_batch())
            except Exception:
                enqueue_failures += 1
        enqueue_end = time.monotonic()

        # --- Phase 2: drain all completions ---
        # Split handles into batches; each batch waits for its handles
        # sequentially, batches run concurrently. This caps the number of
        # simultaneously-polling get_result calls.
        async def drain_batch(batch_handles: list) -> int:
            ok = 0
            for h in batch_handles:
                try:
                    await h.get_result()
                    ok += 1
                except Exception:
                    pass
            return ok

        drain_batches = [
            handles[i : i + batch_size] for i in range(0, len(handles), batch_size)
        ]
        batch_results = await asyncio.gather(
            *(drain_batch(b) for b in drain_batches)
        )
        drain_end = time.monotonic()

        completed = sum(batch_results)

        return {
            "enqueued": len(handles),
            "enqueue_failures": enqueue_failures,
            "completed": completed,
            "enqueue_time": enqueue_end - enqueue_start,
            "drain_time": drain_end - enqueue_end,
            "total_time": drain_end - enqueue_start,
        }

    try:
        result = asyncio.run(run())
        result_queue.put(result)
        # Stay alive (executor still running) until every worker has finished
        # its drain phase, so other workers' workflows can still be picked up
        # by this process's executor.
        done_barrier.wait()
    finally:
        DBOS.destroy()


def run_multiprocess(
    total_rps: int,
    duration_s: float,
    batch_size: int,
    pool_size: int,
    executor_threads: int,
    processes: int,
) -> None:
    asyncio.run(recreate_database())

    per_proc_rps = total_rps // processes

    ctx = mp.get_context("spawn")

    # Pre-create the DBOS schema in a single child so workers don't serialize
    # on the migration advisory lock.
    bootstrap = ctx.Process(target=bootstrap_schema_entry)
    bootstrap.start()
    bootstrap.join()

    result_queue: mp.Queue = ctx.Queue()
    done_barrier = ctx.Barrier(processes)
    workers = []
    for _ in range(processes):
        p = ctx.Process(
            target=worker_entry,
            args=(
                per_proc_rps,
                duration_s,
                batch_size,
                pool_size,
                executor_threads,
                done_barrier,
                result_queue,
            ),
        )
        p.start()
        workers.append(p)

    results = [result_queue.get() for _ in workers]
    for p in workers:
        p.join()

    enqueued = sum(r["enqueued"] for r in results)
    completed = sum(r["completed"] for r in results)
    enqueue_failures = sum(r["enqueue_failures"] for r in results)
    enqueue_time = max(r["enqueue_time"] for r in results)
    total_time = max(r["total_time"] for r in results)
    drain_time = total_time - enqueue_time
    enqueue_rps = enqueued / enqueue_time if enqueue_time > 0 else 0
    completion_rps = completed / total_time if total_time > 0 else 0

    print(f"Processes:        {processes}")
    print(f"Target RPS:       {total_rps}  ({per_proc_rps}/proc)")
    print(f"Batch size:       {batch_size}")
    print(f"Pool size/proc:   {pool_size}")
    print(f"Exec threads/proc:{executor_threads}")
    print(f"Enqueue time:     {enqueue_time:.2f}s")
    print(f"Drain time:       {drain_time:.2f}s")
    print(f"Total time:       {total_time:.2f}s")
    print(f"Enqueued:         {enqueued}")
    print(f"Completed:        {completed}")
    print(f"Enqueue failures: {enqueue_failures}")
    print(f"Enqueue RPS:      {enqueue_rps:.0f}")
    print(f"Completion RPS:   {completion_rps:.0f}   (end-to-end)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rps", type=int, required=True, help="Total target enqueue rate (workflows/sec)"
    )
    parser.add_argument(
        "--duration", type=float, default=30.0, help="Enqueue phase duration in seconds"
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Concurrent enqueues per batch"
    )
    parser.add_argument(
        "--pool-size", type=int, default=16, help="DBOS system DB pool size per process"
    )
    parser.add_argument(
        "--executor-threads",
        type=int,
        default=512,
        help="DBOS max_executor_threads per process",
    )
    parser.add_argument(
        "--processes", type=int, default=4, help="Number of worker processes"
    )
    args = parser.parse_args()
    run_multiprocess(
        args.rps,
        args.duration,
        args.batch_size,
        args.pool_size,
        args.executor_threads,
        args.processes,
    )


if __name__ == "__main__":
    main()
