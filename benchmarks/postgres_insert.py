"""Benchmark raw Postgres insert throughput at a target rate."""

import argparse
import asyncio
import os
import time

import asyncpg

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS bench_inserts (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

INSERT_SQL = "INSERT INTO bench_inserts (payload) VALUES ($1)"


async def setup(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(TABLE_DDL)
        await conn.execute("TRUNCATE bench_inserts")


async def insert_batch(pool: asyncpg.Pool, batch_size: int) -> None:
    rows = [(f"payload-{i}",) for i in range(batch_size)]
    async with pool.acquire() as conn:
        await conn.executemany(INSERT_SQL, rows)


async def run(target_rps: int, duration_s: float, batch_size: int, pool_size: int) -> None:
    db_url = os.environ["BENCHMARK_DATABASE_URL"]
    pool = await asyncpg.create_pool(db_url, min_size=pool_size, max_size=pool_size)
    try:
        await setup(pool)

        batches_per_second = target_rps / batch_size
        interval = 1.0 / batches_per_second
        total_batches = int(batches_per_second * duration_s)

        completed = 0
        failed = 0
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
            task = asyncio.create_task(insert_batch(pool, batch_size))
            task.add_done_callback(on_done)
            tasks.add(task)

        # Wait for in-flight batches to complete.
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.monotonic() - start

        total_inserts = completed * batch_size
        actual_rps = total_inserts / elapsed
        print(f"Target RPS:    {target_rps}")
        print(f"Batch size:    {batch_size}")
        print(f"Duration:      {elapsed:.2f}s")
        print(f"Batches OK:    {completed}")
        print(f"Batches FAIL:  {failed}")
        print(f"Total inserts: {total_inserts}")
        print(f"Actual RPS:    {actual_rps:.0f}")
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rps", type=int, required=True, help="Target inserts per second")
    parser.add_argument("--duration", type=float, default=30.0, help="Run duration in seconds")
    parser.add_argument("--batch-size", type=int, default=10, help="Inserts per batch")
    parser.add_argument("--pool-size", type=int, default=32, help="asyncpg pool size")
    args = parser.parse_args()
    asyncio.run(run(args.rps, args.duration, args.batch_size, args.pool_size))


if __name__ == "__main__":
    main()
