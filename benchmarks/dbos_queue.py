"""Benchmark DBOS queue enqueue + completion throughput at a target rate.

Two-phase: enqueue all workflows at the target rate, then drain all completions.
Reports both enqueue and end-to-end completion throughput.

A single database-backed queue, registered once via ``DBOS.register_queue``, so
every worker picks it up from the system database rather than declaring it
locally.

Enqueueing and execution are split across separate process pools, scaled
independently:
  * ``--enqueuers`` processes enqueue through a ``DBOSClient``. They never
    launch the DBOS runtime, so they hold no queue threads and execute no
    workflows -- they only write ENQUEUED rows to the system database.
  * ``--workers`` processes launch the runtime and do nothing but dequeue and
    execute. They enqueue nothing.

Splitting the pools keeps the two sides from competing for the same process's
event loop and connection pool, so a bottleneck on one side is visible rather
than masked by the other.
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

# The queue is database-backed, so the bootstrap process registers it and every
# worker picks it up from the system database. Workers and enqueuers must share
# the bootstrap app name: queues and workflows are owned by the application
# that created them, and a worker only dequeues what its own application owns.
APP_NAME = "dbos-queue-bench"
QUEUE_NAME = "bench-queue"
# Enqueuers hold no workflow code, so they name the target workflow by string.
# Workers register under this exact name to match.
WORKFLOW_NAME = "noop_workflow"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(pct / 100 * (len(s) - 1)))))
    return s[k]


async def recreate_database() -> None:
    """Drop and recreate the benchmark database via POSTGRES_DATABASE_URL.

    pg_stat_statements is preloaded cluster-wide but the extension itself is
    per-database, so dropping the database drops it too. Re-create it here or
    every run would start with no query statistics.
    """
    admin_url = os.environ["POSTGRES_DATABASE_URL"]
    bench_url = os.environ["BENCHMARK_DATABASE_URL"]
    bench_db = urlparse(bench_url).path.lstrip("/")
    conn = await asyncpg.connect(admin_url)
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{bench_db}" WITH (FORCE)')
        await conn.execute(f'CREATE DATABASE "{bench_db}"')
    finally:
        await conn.close()
    conn = await asyncpg.connect(bench_url)
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    except asyncpg.PostgresError as e:
        # Non-fatal: the benchmark still runs, just without query stats. Hit on
        # a server where the library is not preloaded (e.g. a local Postgres).
        print(f"warning: could not enable pg_stat_statements: {e}", flush=True)
    finally:
        await conn.close()


def bootstrap_entry(worker_concurrency: int) -> None:
    """Pre-create the DBOS system schema and register the queue, one-shot.

    Runs in its own spawned process so the parent never imports DBOS.
    Pre-running migrations eliminates the per-worker advisory-lock serialization
    when many workers launch in parallel, and registering the queue here keeps
    the workers from all racing to upsert the same row at startup. The client
    never runs migrations, so enqueuers depend on this having run.
    """
    from dbos import DBOS, DBOSConfig

    config: DBOSConfig = {
        "name": APP_NAME,
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": 3,
    }
    DBOS(config=config)
    DBOS.launch()
    # worker_concurrency is the per-executor cap, so the cluster-wide ceiling
    # scales with --workers. No global limit: the point is to find where the
    # database saturates, not to throttle before it does.
    DBOS.register_queue(
        QUEUE_NAME,
        worker_concurrency=worker_concurrency,
        on_conflict="always_update",
    )
    DBOS.destroy()


def worker_entry(
    pool_size: int,
    executor_threads: int,
    ready_barrier,
    shutdown_event,
) -> None:
    """Launch the DBOS runtime and execute dequeued workflows. Enqueues nothing."""
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig

    # The explicit name is the contract with the enqueuers, which have no
    # handle on this function and pass its name as a string.
    @DBOS.workflow(name=WORKFLOW_NAME)
    async def noop_workflow() -> int:
        return 1

    config: DBOSConfig = {
        "name": APP_NAME,
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
        "executor_id": str(uuid.uuid7()),
    }
    DBOS(config=config)
    # No listen_queues: without a filter, every worker polls every queue this
    # application owns, which is exactly the one the bootstrap registered.
    DBOS.launch()

    try:
        # Wait until all processes are started before beginning the benchmark.
        ready_barrier.wait()
        # Nothing else to do here: the queue threads run in the background.
        # Stay up until the parent reports the drain finished.
        shutdown_event.wait()
    finally:
        DBOS.destroy()


def enqueuer_entry(
    enqueuer_id: int,
    num_enqueuers: int,
    my_enqueues: int,
    duration_s: float,
    max_inflight: int,
    pool_size: int,
    drain_timeout: float,
    sample_rate: float,
    ready_barrier,
    enqueue_done_barrier,
    done_barrier,
    result_queue: mp.Queue,
) -> None:
    """Enqueue through a DBOSClient. Never launches the DBOS runtime."""
    from dbos import DBOSClient, EnqueueOptions

    # application_name makes the enqueued rows owned by the same application
    # the workers run as; without it they would never be dequeued.
    client = DBOSClient(
        system_database_url=os.environ["BENCHMARK_DATABASE_URL"],
        system_database_pool_size=pool_size,
        application_name=APP_NAME,
    )

    # Wait until all processes are started before beginning the benchmark.
    ready_barrier.wait()

    # samples: list of (workflow_id, start_wallclock_seconds) for latency lookup.
    samples: list[tuple[str, float]] = []

    async def run() -> dict:
        print(f"Starting enqueue for enqueuer {enqueuer_id}", flush=True)

        enqueued = 0
        enqueue_failures = 0

        # Arrivals sit on a uniform grid: one enqueue every `gap` seconds,
        # rather than a burst of N fired at once every N*gap. Each enqueuer's
        # grid is additionally offset by a fraction of one gap, because every
        # enqueuer leaves the ready barrier at the same instant -- without the
        # offset the whole pool fires in lockstep and the queue sees a burst
        # train (all arrivals in a fraction of each cycle, silence between)
        # instead of the steady rate the run is supposed to apply.
        gap = duration_s / my_enqueues if my_enqueues else 0.0
        phase = gap * (enqueuer_id / num_enqueuers)

        # Cap in-flight enqueues rather than batching them. Blocking on acquire
        # means this process cannot sustain its share of the target rate; that
        # surfaces as a shortfall in the reported Enqueue RPS instead of
        # silently reshaping the arrival pattern.
        sem = asyncio.Semaphore(max_inflight)
        inflight: set[asyncio.Task] = set()

        async def enqueue_one() -> None:
            # Counted per enqueue: a failure loses exactly one arrival, not the
            # whole batch it happened to be dispatched with.
            nonlocal enqueued, enqueue_failures
            try:
                sampled = random.random() < sample_rate
                start = time.time() if sampled else 0.0
                options: EnqueueOptions = {
                    "queue_name": QUEUE_NAME,
                    "workflow_name": WORKFLOW_NAME,
                }
                handle = await client.enqueue_async(options)
                enqueued += 1
                if sampled:
                    samples.append((handle.get_workflow_id(), start))
            except Exception:
                enqueue_failures += 1
            finally:
                sem.release()

        # --- Phase 1: enqueue at target rate ---
        # Record wall-clock start/end so the parent can compute elapsed across
        # all enqueuers as (last end - first start).
        enqueue_start_wall = time.time()
        loop_start = time.monotonic()
        for i in range(my_enqueues):
            target_time = loop_start + phase + i * gap
            now = time.monotonic()
            if target_time > now:
                await asyncio.sleep(target_time - now)
            await sem.acquire()
            task = asyncio.create_task(enqueue_one())
            inflight.add(task)
            task.add_done_callback(inflight.discard)
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        enqueue_end_wall = time.time()
        print(
            f"[pid {os.getpid()}] enqueue done: "
            f"{enqueued} workflows in {enqueue_end_wall - enqueue_start_wall:.2f}s",
            flush=True,
        )

        # Wait for every enqueuer to finish before measuring drain.
        # Run the synchronous barrier in a thread so the event loop stays
        # responsive.
        await asyncio.to_thread(enqueue_done_barrier.wait)

        # --- Phase 2: drain (enqueuer 0 polls list_workflows) ---
        # Enqueuer 0 polls until no ENQUEUED or PENDING workflows remain. The
        # others just wait. This avoids per-handle get_result polling pressure
        # on the system DB.
        drain_start_wall = time.time()
        drain_end_wall = drain_start_wall
        drain_timed_out = False
        unfinished_remaining = 0
        if enqueuer_id == 0:
            polls = 0
            while True:
                unfinished = await client.list_workflows_async(
                    status=["PENDING", "ENQUEUED"], limit=1
                )
                if not unfinished:
                    break
                # A run that enqueues faster than the workers drain can spend
                # far longer draining than enqueueing. Log progress, and bail
                # out if the (optional) timeout is exceeded.
                if drain_timeout > 0 and time.time() - drain_start_wall > drain_timeout:
                    drain_timed_out = True
                    break
                polls += 1
                if polls % 100 == 0:
                    print(
                        f"[pid {os.getpid()}] still draining after "
                        f"{time.time() - drain_start_wall:.0f}s",
                        flush=True,
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
                print(
                    f"[pid {os.getpid()}] drain timed out after "
                    f"{drain_end_wall - drain_start_wall:.2f}s with "
                    f"{unfinished_remaining} workflows unfinished",
                    flush=True,
                )
            else:
                print(
                    f"[pid {os.getpid()}] drain done in "
                    f"{drain_end_wall - drain_start_wall:.2f}s",
                    flush=True,
                )

        # Sync so all enqueuers wait until drain finishes before looking up samples.
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
        client.destroy()


def run_multiprocess(
    total_rps: int,
    duration_s: float,
    max_inflight: int,
    pool_size: int,
    executor_threads: int,
    num_workers: int,
    num_enqueuers: int,
    worker_concurrency: int,
    drain_timeout: float,
    sample_rate: float,
) -> None:
    asyncio.run(recreate_database())

    # Exact total with the remainder spread one-per-enqueuer. The old form
    # truncated twice (integer rps-per-process, then int() on the batch count),
    # silently under-delivering by up to a third at low rates.
    total_enqueues = round(total_rps * duration_s)
    base, extra = divmod(total_enqueues, num_enqueuers)
    enqueue_counts = [base + (1 if i < extra else 0) for i in range(num_enqueuers)]
    per_proc_rps = total_rps / num_enqueuers

    ctx = mp.get_context("spawn")

    # Pre-create the DBOS schema and register the queue in a single child so
    # workers don't serialize on the migration advisory lock.
    bootstrap = ctx.Process(target=bootstrap_entry, args=(worker_concurrency,))
    bootstrap.start()
    bootstrap.join()
    # Workers silently fall back to running migrations themselves if bootstrap
    # dies, which hides the failure behind slow, serialized worker startup. A
    # dead bootstrap also means no queue, so the run would fail anyway.
    if bootstrap.exitcode != 0:
        raise RuntimeError(f"schema bootstrap failed (exit {bootstrap.exitcode})")

    result_queue: mp.Queue = ctx.Queue()
    # Both pools sync on ready; the phase barriers are enqueuer-only.
    ready_barrier = ctx.Barrier(num_workers + num_enqueuers)
    enqueue_done_barrier = ctx.Barrier(num_enqueuers)
    done_barrier = ctx.Barrier(num_enqueuers)
    shutdown_event = ctx.Event()

    workers = []
    for _ in range(num_workers):
        p = ctx.Process(
            target=worker_entry,
            args=(pool_size, executor_threads, ready_barrier, shutdown_event),
        )
        p.start()
        workers.append(p)

    enqueuers = []
    for enqueuer_id in range(num_enqueuers):
        p = ctx.Process(
            target=enqueuer_entry,
            args=(
                enqueuer_id,
                num_enqueuers,
                enqueue_counts[enqueuer_id],
                duration_s,
                max_inflight,
                pool_size,
                drain_timeout,
                sample_rate,
                ready_barrier,
                enqueue_done_barrier,
                done_barrier,
                result_queue,
            ),
        )
        p.start()
        enqueuers.append(p)

    results = [result_queue.get() for _ in enqueuers]
    for p in enqueuers:
        p.join()
    # The drain is over, so the workers have nothing left to execute.
    shutdown_event.set()
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
    all_latencies: list[float] = []
    for r in results:
        all_latencies.extend(r["latencies"])

    print(f"Workers:          {num_workers}   (runtime, execute only)")
    print(f"Enqueuers:        {num_enqueuers}   (client, enqueue only)")
    print(f"Queue:            {QUEUE_NAME}  (database-backed)")
    print(f"Worker concurr.:  {worker_concurrency}  (per worker process)")
    print(f"Target RPS:       {total_rps}  ({per_proc_rps:.1f}/enqueuer)")
    print(f"Planned enqueues: {total_enqueues}")
    print(f"Max in-flight:    {max_inflight}  (per enqueuer)")
    print(f"Pool size/proc:   {pool_size}")
    print(f"Exec threads/wkr: {executor_threads}")
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
        "--max-inflight",
        type=int,
        default=100,
        help="Max concurrent in-flight enqueues per enqueuer (Phase 1)",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=5,
        help="System DB pool size per process (workers and enqueuers alike)",
    )
    parser.add_argument(
        "--executor-threads",
        type=int,
        default=512,
        help="DBOS max_executor_threads per worker process",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of worker processes (launch the DBOS runtime, execute workflows)",
    )
    parser.add_argument(
        "--enqueuers",
        type=int,
        default=64,
        help="Number of enqueuer processes (DBOSClient only, no DBOS runtime)",
    )
    parser.add_argument(
        "--worker-concurrency",
        type=int,
        default=1000,
        help="Max concurrent workflows from the queue per worker process",
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
        args.max_inflight,
        args.pool_size,
        args.executor_threads,
        args.workers,
        args.enqueuers,
        args.worker_concurrency,
        args.drain_timeout,
        args.sample_rate,
    )


if __name__ == "__main__":
    main()
