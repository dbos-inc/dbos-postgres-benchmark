"""Benchmark DBOS queue.enqueue_async + completion throughput at a target rate.

Runs a fixed-duration window during which workers enqueue at the target rate.
At the end of the window, counts workflows that have completed (status=SUCCESS).
Throughput = completed / duration.
"""

import argparse
import asyncio
import multiprocessing as mp
import os
import random
import sys
import time
from urllib.parse import urlparse
import uuid

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
    worker_id: int,
    num_workers: int,
    target_rps: int,
    duration_s: float,
    enqueue_batch_size: int,
    pool_size: int,
    executor_threads: int,
    num_queues: int,
    startup_barrier,
    done_barrier,
    result_queue: mp.Queue,
) -> None:
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig, Queue

    @DBOS.workflow()
    async def noop_workflow() -> int:
        return 1

    # Create all queues in every worker so any worker can enqueue to any queue.
    # Workflows are enqueued to a random queue per call.
    queues = [
        Queue(f"bench-queue-{i}", polling_interval_sec=sys.float_info.min)
        for i in range(num_queues)
    ]

    # Partition listening across workers. num_queues must divide num_workers,
    # so each queue is listened to by exactly num_workers // num_queues workers.
    assert num_workers % num_queues == 0, (
        f"num_queues ({num_queues}) must divide num_workers ({num_workers})"
    )
    listen = [queues[worker_id % num_queues]]

    config: DBOSConfig = {
        "name": "dbos-queue-bench",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
        "executor_id": str(uuid.uuid7()),
    }
    DBOS(config=config)
    DBOS.listen_queues(listen)
    DBOS.launch()

    # Wait until every worker is fully launched before any starts enqueueing.
    startup_barrier.wait()

    async def enqueue_batch() -> int:
        # Fire all enqueues in this batch concurrently. Each workflow goes to a
        # randomly chosen queue. Discard handles.
        await asyncio.gather(
            *(
                random.choice(queues).enqueue_async(noop_workflow)
                for _ in range(enqueue_batch_size)
            )
        )
        return enqueue_batch_size

    async def run() -> dict:
        batches_per_second = target_rps / enqueue_batch_size
        interval = 1.0 / batches_per_second

        enqueued = 0
        enqueue_failures = 0

        # Enqueue at target rate until the fixed window ends.
        window_start = time.monotonic()
        window_end = window_start + duration_s
        i = 0
        while True:
            target_time = window_start + i * interval
            now = time.monotonic()
            if now >= window_end:
                break
            if target_time > now:
                sleep_for = min(target_time - now, window_end - now)
                await asyncio.sleep(sleep_for)
                if time.monotonic() >= window_end:
                    break
            try:
                enqueued += await enqueue_batch()
            except Exception:
                enqueue_failures += 1
            i += 1
        enqueue_end = time.monotonic()
        print(
            f"[pid {os.getpid()}] enqueue done: "
            f"{enqueued} workflows in {enqueue_end - window_start:.2f}s",
            flush=True,
        )

        # Worker 0 immediately counts SUCCESS workflows at the window end.
        completed = 0
        if worker_id == 0:
            conn = await asyncpg.connect(os.environ["BENCHMARK_DATABASE_URL"])
            try:
                completed = await conn.fetchval(
                    "SELECT count(*) FROM dbos.workflow_status WHERE status = 'SUCCESS'"
                )
            finally:
                await conn.close()
            print(
                f"[pid {os.getpid()}] completed at window end: {completed}",
                flush=True,
            )

        return {
            "enqueued": enqueued,
            "enqueue_failures": enqueue_failures,
            "completed": completed,
        }

    try:
        result = asyncio.run(run())
        result_queue.put(result)
        # Stay alive (executor still running) until every worker has finished,
        # so other workers' workflows can still be picked up by this executor.
        done_barrier.wait()
    finally:
        DBOS.destroy()


def run_multiprocess(
    total_rps: int,
    duration_s: float,
    enqueue_batch_size: int,
    pool_size: int,
    executor_threads: int,
    processes: int,
    num_queues: int,
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
    startup_barrier = ctx.Barrier(processes)
    done_barrier = ctx.Barrier(processes)
    workers = []
    for worker_id in range(processes):
        p = ctx.Process(
            target=worker_entry,
            args=(
                worker_id,
                processes,
                per_proc_rps,
                duration_s,
                enqueue_batch_size,
                pool_size,
                executor_threads,
                num_queues,
                startup_barrier,
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
    enqueue_rps = enqueued / duration_s
    completion_rps = completed / duration_s

    print(f"Processes:        {processes}")
    print(f"Queues:           {num_queues}")
    print(f"Target RPS:       {total_rps}  ({per_proc_rps}/proc)")
    print(f"Enqueue batch:    {enqueue_batch_size}")
    print(f"Pool size/proc:   {pool_size}")
    print(f"Exec threads/proc:{executor_threads}")
    print(f"Window:           {duration_s:.2f}s")
    print(f"Enqueued:         {enqueued}")
    print(f"Completed:        {completed}   (status=SUCCESS at window end)")
    print(f"Enqueue failures: {enqueue_failures}")
    print(f"Enqueue RPS:      {enqueue_rps:.0f}")
    print(f"Completion RPS:   {completion_rps:.0f}   (completed / window)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rps", type=int, required=True, help="Total target enqueue rate (workflows/sec)"
    )
    parser.add_argument(
        "--duration", type=float, default=30.0, help="Enqueue phase duration in seconds"
    )
    parser.add_argument(
        "--enqueue-batch",
        type=int,
        default=100,
        help="Concurrent enqueues per batch (Phase 1)",
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
    parser.add_argument(
        "--queues",
        type=int,
        default=1,
        help="Number of queue shards (workers are assigned round-robin)",
    )
    args = parser.parse_args()
    run_multiprocess(
        args.rps,
        args.duration,
        args.enqueue_batch,
        args.pool_size,
        args.executor_threads,
        args.processes,
        args.queues,
    )


if __name__ == "__main__":
    main()
