"""Benchmark DBOS start_workflow_async throughput at a target rate."""

import argparse
import asyncio
import multiprocessing as mp
import os
import time
import uuid
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


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(pct / 100 * (len(s) - 1)))))
    return s[k]


def worker_entry(
    target_rps: int,
    duration_s: float,
    batch_size: int,
    pool_size: int,
    executor_threads: int,
    result_queue: mp.Queue,
) -> None:
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig

    @DBOS.workflow()
    async def noop_workflow() -> None:
        pass

    config: DBOSConfig = {
        "name": "dbos-bench",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
        "executor_id": str(uuid.uuid7()),
    }
    DBOS(config=config)
    DBOS.launch()

    async def start_batch(latencies: list[float]) -> None:
        t0 = time.monotonic()
        handles = await asyncio.gather(
            *(DBOS.start_workflow_async(noop_workflow) for _ in range(batch_size))
        )
        await asyncio.gather(*(h.get_result() for h in handles))
        latencies.append(time.monotonic() - t0)

    async def run() -> dict:
        batches_per_second = target_rps / batch_size
        interval = 1.0 / batches_per_second
        total_batches = int(batches_per_second * duration_s)

        completed = 0
        failed = 0
        latencies: list[float] = []
        tasks: set[asyncio.Task] = set()

        def on_done(task: asyncio.Task) -> None:
            nonlocal completed, failed
            tasks.discard(task)
            if task.exception() is not None:
                failed += 1
            else:
                completed += 1

        start = time.monotonic()
        for i in range(total_batches):
            target_time = start + i * interval
            now = time.monotonic()
            if target_time > now:
                await asyncio.sleep(target_time - now)
            task = asyncio.create_task(start_batch(latencies))
            task.add_done_callback(on_done)
            tasks.add(task)
        schedule_done = time.monotonic()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.monotonic() - start

        return {
            "completed": completed,
            "failed": failed,
            "schedule_time": schedule_done - start,
            "elapsed": elapsed,
            "latencies": latencies,
        }

    try:
        result = asyncio.run(run())
    finally:
        DBOS.destroy()
    result_queue.put(result)


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
    result_queue: mp.Queue = ctx.Queue()
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
                result_queue,
            ),
        )
        p.start()
        workers.append(p)

    results = [result_queue.get() for _ in workers]
    for p in workers:
        p.join()

    completed = sum(r["completed"] for r in results)
    failed = sum(r["failed"] for r in results)
    schedule_time = max(r["schedule_time"] for r in results)
    elapsed = max(r["elapsed"] for r in results)
    drain_time = elapsed - schedule_time
    total_starts = completed * batch_size
    actual_rps = total_starts / elapsed

    all_latencies: list[float] = []
    for r in results:
        all_latencies.extend(r["latencies"])

    print(f"Processes:        {processes}")
    print(f"Target RPS:       {total_rps}  ({per_proc_rps}/proc)")
    print(f"Batch size:       {batch_size}")
    print(f"Pool size/proc:   {pool_size}")
    print(f"Exec threads/proc:{executor_threads}")
    print(f"Schedule time:    {schedule_time:.2f}s")
    print(f"Drain time:       {drain_time:.2f}s   (>0 means DBOS couldn't keep up)")
    print(f"Total elapsed:    {elapsed:.2f}s")
    print(f"Batches OK:       {completed}")
    print(f"Batches FAIL:     {failed}")
    print(f"Total starts:     {total_starts}")
    print(f"Actual RPS:       {actual_rps:.0f}")
    if all_latencies:
        print(
            "Batch latency:    "
            f"p50={percentile(all_latencies, 50)*1000:.1f}ms "
            f"p95={percentile(all_latencies, 95)*1000:.1f}ms "
            f"p99={percentile(all_latencies, 99)*1000:.1f}ms "
            f"max={max(all_latencies)*1000:.1f}ms"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rps", type=int, required=True, help="Total target workflow starts per second"
    )
    parser.add_argument(
        "--duration", type=float, default=30.0, help="Run duration in seconds"
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Workflow starts per batch"
    )
    parser.add_argument(
        "--pool-size", type=int, default=64, help="DBOS system DB pool size per process"
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
