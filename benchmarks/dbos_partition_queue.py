"""Benchmark DBOS partitioned queue enqueue + completion throughput at a target rate.

Same two-phase shape as dbos_queue.py: enqueue all workflows at the target rate,
then drain all completions. Reports both enqueue and end-to-end completion
throughput.

The difference is that every queue is created with ``partition_queue=True``, so
concurrency limits apply *per partition* rather than to the queue as a whole.
Each enqueue picks a partition key uniformly at random from ``--partitions``
keys.

Throughput notes:
  * With ``--concurrency 1`` (the default) DBOS takes the batched dequeue path:
    one transaction per poll claims the head-of-line workflow of every
    partition. The rough ceiling is therefore
    ``queues * partitions * concurrency / polling_interval`` workflows/sec, so
    a run needs enough partitions to absorb ``--rps`` or the drain phase will
    dominate. The printed "Dequeue ceiling" line reports this estimate.
  * With ``--concurrency > 1`` DBOS falls back to sweeping one partition per
    round trip, which is much slower for large partition counts.
"""

import argparse
import asyncio
import multiprocessing as mp
import os
import random
import time
import uuid
from urllib.parse import urlparse

import asyncpg


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(pct / 100 * (len(s) - 1)))))
    return s[k]


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
        "name": "dbos-partition-queue-bench-bootstrap",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": 3,
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
    num_partitions: int,
    concurrency: int,
    polling_interval: float,
    drain_timeout: float,
    sample_rate: float,
    ready_barrier,
    enqueue_done_barrier,
    done_barrier,
    result_queue: mp.Queue,
) -> None:
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig, Queue, SetEnqueueOptions

    @DBOS.workflow()
    async def noop_workflow() -> int:
        return 1

    # Create all queues in every worker so any worker can enqueue to any queue.
    # Workflows are enqueued to a random queue per call. On a partitioned queue
    # `concurrency` is the global (cluster-wide) limit *per partition*, so
    # worker_concurrency is left unset: it may not exceed concurrency, and
    # setting it would also disable the batched dequeue path.
    queues = [
        Queue(
            f"bench-partition-queue-{i}",
            concurrency=concurrency,
            partition_queue=True,
            polling_interval_sec=polling_interval,
        )
        for i in range(num_queues)
    ]

    # Partition keys are scoped per queue, so the same key list serves every
    # queue. Pre-build it once to keep the enqueue path free of formatting.
    partition_keys = [f"p{i:08d}" for i in range(num_partitions)]

    # Partition listening across workers. num_queues must divide num_workers,
    # so each queue is listened to by exactly num_workers // num_queues workers.
    assert (
        num_workers % num_queues == 0
    ), f"num_queues ({num_queues}) must divide num_workers ({num_workers})"
    listen = [queues[worker_id % num_queues]]

    config: DBOSConfig = {
        "name": "dbos-partition-queue-bench",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
        "executor_id": str(uuid.uuid7()),
    }
    DBOS(config=config)
    DBOS.listen_queues(listen)
    DBOS.launch()

    # Wait until all processes are started before beginning the benchmark.
    ready_barrier.wait()

    # samples: list of (workflow_id, start_wallclock_seconds) for latency lookup.
    samples: list[tuple[str, float]] = []

    async def enqueue_one() -> None:
        sampled = random.random() < sample_rate
        start = time.time() if sampled else 0.0
        # asyncio.gather wraps each coroutine in a Task, and each Task copies
        # the current contextvars, so the enqueue options set here are local to
        # this enqueue even though the batch runs concurrently.
        with SetEnqueueOptions(queue_partition_key=random.choice(partition_keys)):
            handle = await random.choice(queues).enqueue_async(noop_workflow)
        if sampled:
            samples.append((handle.workflow_id, start))

    async def enqueue_batch() -> int:
        # Fire all enqueues in this batch concurrently.
        await asyncio.gather(*(enqueue_one() for _ in range(enqueue_batch_size)))
        return enqueue_batch_size

    async def run() -> dict:
        DBOS.logger.info(f"Starting enqueue for worker {worker_id}")
        batches_per_second = target_rps / enqueue_batch_size
        interval = 1.0 / batches_per_second
        total_batches = int(batches_per_second * duration_s)

        enqueued = 0
        enqueue_failures = 0

        # --- Phase 1: enqueue at target rate ---
        # Record wall-clock start/end so the parent can compute elapsed across
        # all workers as (last end - first start).
        enqueue_start_wall = time.time()
        loop_start = time.monotonic()
        for i in range(total_batches):
            target_time = loop_start + i * interval
            now = time.monotonic()
            if target_time > now:
                await asyncio.sleep(target_time - now)
            try:
                enqueued += await enqueue_batch()
            except Exception:
                enqueue_failures += 1
        enqueue_end_wall = time.time()
        DBOS.logger.info(
            f"[pid {os.getpid()}] enqueue done: "
            f"{enqueued} workflows in {enqueue_end_wall - enqueue_start_wall:.2f}s",
        )

        # Wait for every worker to finish enqueueing before measuring drain.
        # Run the synchronous barrier in a thread so in-flight workflows on
        # this worker's event loop can continue making progress.
        await asyncio.to_thread(enqueue_done_barrier.wait)

        # --- Phase 2: drain (worker 0 polls list_workflows) ---
        # Worker 0 polls until no ENQUEUED or PENDING workflows remain. Other
        # workers just wait. This avoids per-handle get_result polling pressure
        # on the system DB.
        drain_start_wall = time.time()
        drain_end_wall = drain_start_wall
        drain_timed_out = False
        unfinished_remaining = 0
        if worker_id == 0:
            polls = 0
            while True:
                unfinished = await DBOS.list_workflows_async(
                    status=["PENDING", "ENQUEUED"], limit=1
                )
                if not unfinished:
                    break
                # Per-partition concurrency caps the drain rate, so a run can
                # spend far longer draining than enqueueing. Log progress, and
                # bail out if the (optional) timeout is exceeded.
                if drain_timeout > 0 and time.time() - drain_start_wall > drain_timeout:
                    drain_timed_out = True
                    break
                polls += 1
                if polls % 100 == 0:
                    DBOS.logger.info(
                        f"[pid {os.getpid()}] still draining after "
                        f"{time.time() - drain_start_wall:.0f}s",
                    )
                await asyncio.sleep(0.1)
            drain_end_wall = time.time()
            if drain_timed_out:
                # Report how much actually finished so throughput isn't
                # credited for workflows that never ran.
                conn = await asyncpg.connect(os.environ["BENCHMARK_DATABASE_URL"])
                try:
                    unfinished_remaining = await conn.fetchval(
                        "SELECT count(*) FROM dbos.workflow_status "
                        "WHERE status IN ('PENDING', 'ENQUEUED')"
                    )
                finally:
                    await conn.close()
                DBOS.logger.warning(
                    f"[pid {os.getpid()}] drain timed out after "
                    f"{drain_end_wall - drain_start_wall:.2f}s with "
                    f"{unfinished_remaining} workflows unfinished",
                )
            else:
                DBOS.logger.info(
                    f"[pid {os.getpid()}] drain done in {drain_end_wall - drain_start_wall:.2f}s",
                )

        # Sync so all workers wait until drain finishes before looking up samples.
        await asyncio.to_thread(done_barrier.wait)

        # --- Latency lookup: for each sampled workflow, fetch updated_at ---
        latencies: list[float] = []
        if samples:
            conn = await asyncpg.connect(os.environ["BENCHMARK_DATABASE_URL"])
            try:
                ids = [s[0] for s in samples]
                rows = await conn.fetch(
                    "SELECT workflow_uuid, updated_at FROM dbos.workflow_status "
                    "WHERE workflow_uuid = ANY($1::text[])",
                    ids,
                )
            finally:
                await conn.close()
            updated_at_by_id = {r["workflow_uuid"]: r["updated_at"] for r in rows}
            for wf_id, start in samples:
                ua = updated_at_by_id.get(wf_id)
                if ua is None:
                    continue
                # updated_at is epoch milliseconds
                completed_at_s = float(ua) / 1000.0
                latencies.append(completed_at_s - start)

        return {
            "enqueued": enqueued,
            "enqueue_failures": enqueue_failures,
            "enqueue_start_wall": enqueue_start_wall,
            "enqueue_end_wall": enqueue_end_wall,
            "drain_start_wall": drain_start_wall,
            "drain_end_wall": drain_end_wall,
            "drain_timed_out": drain_timed_out,
            "unfinished_remaining": unfinished_remaining,
            "latencies": latencies,
        }

    try:
        result = asyncio.run(run())
        result_queue.put(result)
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
    num_partitions: int,
    concurrency: int,
    polling_interval: float,
    drain_timeout: float,
    sample_rate: float,
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
    ready_barrier = ctx.Barrier(processes)
    enqueue_done_barrier = ctx.Barrier(processes)
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
                num_partitions,
                concurrency,
                polling_interval,
                drain_timeout,
                sample_rate,
                ready_barrier,
                enqueue_done_barrier,
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
    enqueue_failures = sum(r["enqueue_failures"] for r in results)
    first_start = min(r["enqueue_start_wall"] for r in results)
    last_enqueue_end = max(r["enqueue_end_wall"] for r in results)
    last_drain_end = max(r["drain_end_wall"] for r in results)
    enqueue_time = last_enqueue_end - first_start
    drain_time = last_drain_end - last_enqueue_end
    total_time = last_drain_end - first_start
    drain_timed_out = any(r["drain_timed_out"] for r in results)
    unfinished = sum(r["unfinished_remaining"] for r in results)
    completed = enqueued - unfinished
    enqueue_rps = enqueued / enqueue_time if enqueue_time > 0 else 0
    completion_rps = completed / total_time if total_time > 0 else 0
    # Batched partitioned dequeue claims each partition's head once per poll.
    ceiling = num_queues * num_partitions * concurrency / polling_interval
    all_latencies: list[float] = []
    for r in results:
        all_latencies.extend(r["latencies"])

    print(f"Processes:        {processes}")
    print(f"Queues:           {num_queues}  (partitioned)")
    print(f"Partitions/queue: {num_partitions}")
    print(f"Concurrency:      {concurrency}  (global, per partition)")
    print(f"Polling interval: {polling_interval}s")
    print(f"Dequeue ceiling:  ~{ceiling:.0f} workflows/sec")
    print(f"Target RPS:       {total_rps}  ({per_proc_rps}/proc)")
    print(f"Enqueue batch:    {enqueue_batch_size}")
    print(f"Pool size/proc:   {pool_size}")
    print(f"Exec threads/proc:{executor_threads}")
    print(f"Enqueue time:     {enqueue_time:.2f}s")
    print(f"Drain time:       {drain_time:.2f}s")
    print(f"Total time:       {total_time:.2f}s")
    print(f"Enqueued:         {enqueued}")
    print(f"Enqueue failures: {enqueue_failures}")
    print(f"Enqueue RPS:      {enqueue_rps:.0f}")
    print(f"Completion RPS:   {completion_rps:.0f}   (end-to-end)")
    if drain_timed_out:
        print(
            f"WARNING: drain timed out with {unfinished} workflows unfinished; "
            f"completion RPS covers only the {completed} that finished."
        )
    if all_latencies:
        print(
            f"Latency samples:  {len(all_latencies)}   "
            f"p50={percentile(all_latencies, 50)*1000:.1f}ms "
            f"p95={percentile(all_latencies, 95)*1000:.1f}ms "
            f"p99={percentile(all_latencies, 99)*1000:.1f}ms "
            f"max={max(all_latencies)*1000:.1f}ms"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rps",
        type=int,
        required=True,
        help="Total target enqueue rate (workflows/sec)",
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
        "--pool-size", type=int, default=3, help="DBOS system DB pool size per process"
    )
    parser.add_argument(
        "--executor-threads",
        type=int,
        default=512,
        help="DBOS max_executor_threads per process",
    )
    parser.add_argument(
        "--processes", type=int, default=128, help="Number of worker processes"
    )
    parser.add_argument(
        "--queues",
        type=int,
        default=1,
        help="Number of queue shards (workers are assigned round-robin)",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1000,
        help="Partitions per queue; each workflow picks one uniformly at random",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Global concurrency limit per partition (default 1)",
    )
    parser.add_argument(
        "--polling-interval",
        type=float,
        default=1.0,
        help="Queue polling interval in seconds (dequeue sweep cadence)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=0.0,
        help="Give up on the drain phase after this many seconds (0 = never)",
    )
    parser.add_argument(
        "--sample-rate",
        type=float,
        default=0.01,
        help="Fraction of workflows sampled for latency measurement (default 0.01)",
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
        args.partitions,
        args.concurrency,
        args.polling_interval,
        args.drain_timeout,
        args.sample_rate,
    )


if __name__ == "__main__":
    main()
