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

``--gc N`` adds a third, single process that sweeps completed workflows older
than N minutes out of the system database, for the whole run. It runs every
``--gc-interval`` minutes, which defaults to N: at the default each round faces
exactly one retention window's worth of finished workflows, and the round times
say whether the collector keeps up. Setting the interval apart from the
retention separates the two questions -- how much backlog a sweep clears, and
how often it is asked to. Enqueue and completion throughput are then measured
while it deletes underneath them.

``--progress-interval N`` prints a throughput line every N minutes (5 by
default), covering the interval just ended: workflows enqueued, workflows
completed, and the backlog between the two. Both counts are kept in process --
one shared-memory slot per enqueuer and per worker -- rather than queried,
because a filtered count over ``workflow_status`` would scan the table the
benchmark is stressing and, under ``--gc``, would miss whatever the collector
had already swept.
"""

import argparse
import asyncio
import ctypes
import multiprocessing as mp
import os
import random
import threading
import time
import uuid
from typing import Optional, Union
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
    worker_id: int,
    pool_size: int,
    executor_threads: int,
    num_steps: int,
    output_payload: Optional[bytes],
    completed_totals,
    ready_barrier,
    shutdown_event,
) -> None:
    """Launch the DBOS runtime and execute dequeued workflows. Enqueues nothing."""
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig

    # Progress reporting publishes into this worker's own slot, so the write
    # never contends with another process. The lock is process-local and only
    # orders this worker's executor threads against one another; a workflow
    # already costs several system-database round trips, so an uncontended
    # acquire is not measurable beside it.
    count_lock = threading.Lock()

    # Returns the same buffer on every call, never a fresh one: what is being
    # measured is serializing and writing the bytes, not producing them.
    @DBOS.step()
    async def noop_step() -> Union[int, bytes]:
        return 0 if output_payload is None else output_payload

    # The explicit name is the contract with the enqueuers, which have no
    # handle on this function and pass its name as a string.
    @DBOS.workflow(name=WORKFLOW_NAME)
    async def noop_workflow() -> Union[int, bytes]:
        # Sequential, not gathered: each step is a separate checkpoint write,
        # and running them one after another is what makes --steps a
        # multiplier on the system-database writes per workflow.
        for _ in range(num_steps):
            await noop_step()
        # Counted where the body finishes rather than where DBOS commits the
        # SUCCESS checkpoint a moment later: the rate is the same either way,
        # and only the reported backlog is affected, understated by however
        # many final checkpoint writes are in flight.
        if completed_totals is not None:
            with count_lock:
                completed_totals[worker_id] += 1
        return 1 if output_payload is None else output_payload

    config: DBOSConfig = {
        "name": APP_NAME,
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
        "executor_id": str(uuid.uuid7()),
        "conductor_key": os.environ.get("DBOS_CONDUCTOR_KEY"),
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


def gc_entry(
    gc_minutes: float,
    gc_interval: float,
    pool_size: int,
    ready_barrier,
    shutdown_event,
    result_queue: mp.Queue,
) -> None:
    """Every gc_interval minutes, sweep workflows older than gc_minutes.

    Drives a SystemDatabase built here rather than a launched runtime: garbage
    collection is all this process does, so it registers no workflow, runs no
    queue thread and dequeues nothing -- the sweep never waits behind a queue
    thread for a connection. One process per run, not a pool: concurrent sweeps
    would contend on the same rows and the round times would measure that
    contention rather than the collector.

    Every round is independent and failure is per-round: a sweep that raises is
    counted, logged and retried at the next grid slot on a fresh handle, so a
    collector that hits a dead connection, a lock timeout or a restarted
    database keeps running instead of leaving the rest of the benchmark with no
    GC at all.
    """
    import sqlalchemy as sa
    from dbos._serialization import DefaultSerializer
    from dbos._sys_db import SystemDatabase
    from dbos._workflow_commands import DEFAULT_GC_BATCH_SIZE

    # app_name is the same contract the workers and enqueuers share: GC filters
    # on it, so a name that doesn't match sweeps nothing and every round reports
    # 0 deleted while the table grows.
    def connect():
        db = SystemDatabase.create(
            system_database_url=os.environ["BENCHMARK_DATABASE_URL"],
            engine_kwargs={
                "connect_args": {"application_name": "dbos_queue_bench_gc"},
                "pool_timeout": 30,
                "max_overflow": 0,
                "pool_size": pool_size,
                "pool_pre_ping": True,
            },
            engine=None,
            schema="dbos",
            serializer=DefaultSerializer(),
            executor_id=None,
            # Nothing here waits on a notification, so a listener would only
            # hold an idle connection open for the length of the run.
            use_listen_notify=False,
            app_name=APP_NAME,
        )
        db.check_connection()
        return db

    def close(db) -> None:
        try:
            db.destroy()
        except Exception:
            # Already-dead connections raise on teardown; the handle is being
            # thrown away either way.
            pass

    # How much a round removed, read from the statistics collector: count(*)
    # over workflow_status is a sequential scan that would cost more than the
    # sweep it is measuring. The counter is published on a delay the reader
    # cannot force -- pg_stat_force_next_flush only flushes the calling backend,
    # and the deletes commit on another pooled connection -- so a sweep that
    # finishes fast can read its own work as not yet done and hand the whole
    # tally to the next round. Per-round counts are therefore attribution, not
    # measurement; the run total is exact and reconciled at the end.
    deleted_sql = sa.text(
        "SELECT n_tup_del FROM pg_stat_user_tables "
        "WHERE schemaname = 'dbos' AND relname = 'workflow_status'"
    )

    def deleted_so_far(db) -> int:
        with db.engine.connect() as c:
            return c.execute(deleted_sql).scalar() or 0

    # Two independent clocks: retention_s decides what a sweep may delete,
    # period_s decides how often one runs. Equal by default, and only then does
    # a round face exactly one window's backlog -- a shorter interval splits
    # that window across several rounds, a longer one hands each round more
    # than a window's worth.
    retention_s = gc_minutes * 60.0
    period_s = gc_interval * 60.0
    rounds: list[float] = []
    deleted_per_round: list[int] = []
    failures = 0
    last_error = ""
    sys_db = None
    prev_deleted = None
    try:
        # Wait until all processes are started before beginning the benchmark.
        # Before the database, deliberately: this process is one of the
        # barrier's parties, so failing ahead of it would leave every worker and
        # enqueuer waiting on a barrier that can never fill. A collector that
        # cannot connect should cost the run its GC, not hang the run.
        ready_barrier.wait()
        grid_start = time.monotonic()
        # Counts every attempt, not just the ones that swept: a round that
        # raises gives its slot up and waits for the next rather than spinning
        # against a database that is already unhappy.
        slot = 0
        while True:
            slot += 1
            # Rounds sit on a fixed grid one interval apart, anchored at the
            # barrier, with the first one interval in. A grid rather than a
            # sleep after each round: a sweep that overruns its interval is
            # followed immediately by the next, so a collector falling behind
            # shows up as round times above the interval instead of quietly
            # stretching the schedule and shrinking the backlog each round has
            # to clear.
            wait = grid_start + slot * period_s - time.monotonic()
            # Waiting on the event rather than sleeping gives the run back at
            # shutdown instead of holding it for the rest of the window.
            if wait > 0:
                shutdown_event.wait(wait)
            if shutdown_event.is_set():
                break
            attempt_start = time.monotonic()
            try:
                if sys_db is None:
                    # First round, or a previous one left the handle suspect.
                    # Connecting costs far less than a window, so it happens
                    # here rather than up front, where a database that is not
                    # up yet would cost the run its collector outright.
                    sys_db = connect()
                    if prev_deleted is None:
                        # Baselined once, not on every reconnect: n_tup_del is
                        # cumulative on the database side and outlives the
                        # handle, so re-reading it here would silently discard
                        # whatever batches a failed round committed before it
                        # raised.
                        prev_deleted = deleted_so_far(sys_db)
                # Timed from here so the round time stays the sweep, not a
                # reconnect an earlier failure forced.
                start = time.monotonic()
                # Relative to now, not to the grid: a round that started late
                # still collects everything that has aged out by the time it
                # runs, rather than leaving the overrun to the next round.
                cutoff_ms = int((time.time() - retention_s) * 1000)
                sys_db.garbage_collect(
                    cutoff_epoch_timestamp_ms=cutoff_ms,
                    rows_threshold=None,
                    batch_size=DEFAULT_GC_BATCH_SIZE,
                )
                elapsed = time.monotonic() - start
            except Exception as e:
                # A raised sweep costs this round, not the run. Batches that
                # already committed stay collected, and because the cutoff is
                # recomputed every round, the next slot picks up both its own
                # workflows and whatever this one should have taken.
                failures += 1
                last_error = f"{type(e).__name__}: {e}"
                print(
                    f"[gc] round {slot} failed after "
                    f"{time.monotonic() - attempt_start:.2f}s: {last_error}",
                    flush=True,
                )
                # The handle may be pooling dead connections, so drop it and
                # let the next round build a fresh one.
                if sys_db is not None:
                    close(sys_db)
                    sys_db = None
                continue
            try:
                now_deleted = deleted_so_far(sys_db)
            except Exception:
                # The sweep landed; only the tally is missing. Carrying the
                # old value defers those deletes to whichever round next reads
                # the counter instead of losing them.
                now_deleted = prev_deleted
            rounds.append(elapsed)
            # Clamped: a database that restarted mid-run resets the counter,
            # and a negative delta would silently eat an earlier round's total.
            deleted_per_round.append(max(0, now_deleted - prev_deleted))
            prev_deleted = now_deleted
            print(
                f"[gc] round {slot}: {elapsed:.2f}s, "
                f"{deleted_per_round[-1]} workflows deleted",
                flush=True,
            )
        # Deletes the collector had not published when the final round ended
        # are credited here, so the run total is right even if that round's
        # own tally is short.
        if deleted_per_round and sys_db is not None:
            try:
                deleted_per_round[-1] += max(0, deleted_so_far(sys_db) - prev_deleted)
            except Exception:
                pass
    finally:
        # In the finally so the parent's blocking get() is always satisfied:
        # without it, a collector that died on the way in would hang the run at
        # the point where it collects results.
        result_queue.put(
            {
                "rounds": rounds,
                "deleted": sum(deleted_per_round),
                "failures": failures,
                "last_error": last_error,
            }
        )
        if sys_db is not None:
            close(sys_db)


def enqueuer_entry(
    enqueuer_id: int,
    num_enqueuers: int,
    my_enqueues: int,
    duration_s: float,
    max_inflight: int,
    pool_size: int,
    drain_timeout: float,
    sample_rate: float,
    enqueued_totals,
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
                if enqueued_totals is not None:
                    # A store of the local total, not a read-modify-write on
                    # shared memory: this coroutine is the slot's only writer
                    # and the event loop runs it single-threaded, so the store
                    # needs no lock.
                    enqueued_totals[enqueuer_id] = enqueued
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
        samples_missing = 0
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
                    # Every enqueued workflow has a row, so a missing one was
                    # collected between its completion and this lookup. Counted
                    # rather than skipped: silently dropping them would shrink
                    # the percentiles to whatever GC's retention window left
                    # behind, with nothing in the output to say so.
                    samples_missing += 1
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
            "samples_missing": samples_missing,
        }

    try:
        result = asyncio.run(run())
        result_queue.put(result)
    finally:
        client.destroy()


def progress_monitor(
    interval_s: float,
    enqueued_totals,
    completed_totals,
    ready_barrier,
    shutdown_event,
) -> None:
    """Print one throughput line per interval, covering the interval just ended.

    Runs as a thread in the parent, which imports no DBOS code and holds no
    database connection, so reporting costs the run nothing beyond summing two
    small arrays.

    Throughput is counted at the source rather than queried: enqueuers publish
    how many workflows they have written, workers how many they have executed,
    each into its own shared-memory slot. Counting in the database instead would
    mean a filtered count over ``workflow_status`` -- a scan of the very table
    the benchmark is stressing, growing more expensive the longer the run goes --
    and under ``--gc`` it would silently miss every finished workflow the
    collector had already swept.

    Both rates are reported, plus the backlog that separates them, because
    either rate alone is ambiguous. A backlog flat near zero means completions
    are only tracking the offered rate, so the number describes the load
    generator; a growing backlog means the completion rate is the system's
    capacity.
    """
    # Joins the barrier as one more party, so the grid is anchored at the
    # instant the run starts rather than at process spawn.
    ready_barrier.wait()
    grid_start = time.monotonic()
    prev_time = grid_start
    prev_enqueued = 0
    prev_completed = 0
    slot = 0
    while True:
        slot += 1
        # A fixed grid anchored at the barrier, like the GC loop: a report that
        # wakes late does not push the ones after it back.
        wait = grid_start + slot * interval_s - time.monotonic()
        # Waiting on the event rather than sleeping ends the reports when the
        # drain does, instead of up to one interval later.
        if wait > 0:
            shutdown_event.wait(wait)
        if shutdown_event.is_set():
            break
        now = time.monotonic()
        # Wall clock read beside the monotonic one, so the stamp names the end
        # of the window this line reports -- the point to line a dip up against
        # in the Postgres log or a metrics dashboard. Durations stay on the
        # monotonic clock, which no time adjustment can step.
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # Completions first: neither array is read as an atomic snapshot, and
        # this order puts the skew where it cannot invert the subtraction --
        # completed is read no later than enqueued, so backlog stays >= 0.
        completed = sum(completed_totals)
        enqueued = sum(enqueued_totals)
        window = now - prev_time
        elapsed = now - grid_start
        window_enqueued = enqueued - prev_enqueued
        window_completed = completed - prev_completed
        # Divided by the interval actually measured rather than the nominal one,
        # so a late wake reports the rate it saw instead of an inflated one.
        enqueue_rps = window_enqueued / window if window > 0 else 0.0
        completion_rps = window_completed / window if window > 0 else 0.0
        cum_enqueue_rps = enqueued / elapsed if elapsed > 0 else 0.0
        cum_completion_rps = completed / elapsed if elapsed > 0 else 0.0
        minutes, seconds = divmod(int(round(elapsed)), 60)
        print(
            f"[progress] {stamp}  t={minutes:02d}:{seconds:02d}  "
            f"window={window:.1f}s  "
            f"enq {window_enqueued} ({enqueue_rps:.0f}/s)  "
            f"done {window_completed} ({completion_rps:.0f}/s)  "
            f"backlog {enqueued - completed}  "
            f"cum: enq {enqueued} ({cum_enqueue_rps:.0f}/s) "
            f"done {completed} ({cum_completion_rps:.0f}/s)",
            flush=True,
        )
        prev_time = now
        prev_enqueued = enqueued
        prev_completed = completed


def run_multiprocess(
    total_rps: int,
    duration_s: float,
    max_inflight: int,
    pool_size: int,
    executor_threads: int,
    num_workers: int,
    num_enqueuers: int,
    worker_concurrency: int,
    num_steps: int,
    output_kb: int,
    drain_timeout: float,
    sample_rate: float,
    gc_minutes: float,
    gc_interval: float,
    progress_interval: float,
) -> None:
    asyncio.run(recreate_database())

    # Exact total with the remainder spread one-per-enqueuer. The old form
    # truncated twice (integer rps-per-process, then int() on the batch count),
    # silently under-delivering by up to a third at low rates.
    total_enqueues = round(total_rps * duration_s)
    base, extra = divmod(total_enqueues, num_enqueuers)
    enqueue_counts = [base + (1 if i < extra else 0) for i in range(num_enqueuers)]
    per_proc_rps = total_rps / num_enqueuers

    # One buffer for the whole run, generated here and inherited by every
    # worker, so no call ever pays to produce its own return value. urandom
    # rather than a repeated byte because the outputs land in a TOASTable
    # column: compressible filler would store smaller than the size asked for.
    output_payload = os.urandom(output_kb * 1024) if output_kb > 0 else None

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
    gc_result_queue: mp.Queue = ctx.Queue()
    gc_enabled = gc_minutes > 0
    progress_enabled = progress_interval > 0
    # One slot per process, so publishing a count never contends across
    # processes and the parent's read is a plain sum. lock=False because no slot
    # has two writers: an enqueuer's event loop is single-threaded, and a
    # worker's executor threads are ordered by a lock inside that process.
    enqueued_totals = (
        ctx.Array(ctypes.c_int64, num_enqueuers, lock=False)
        if progress_enabled
        else None
    )
    completed_totals = (
        ctx.Array(ctypes.c_int64, num_workers, lock=False) if progress_enabled else None
    )
    # Both pools, the collector and the progress monitor sync on ready; the
    # phase barriers are enqueuer-only.
    ready_barrier = ctx.Barrier(
        num_workers
        + num_enqueuers
        + (1 if gc_enabled else 0)
        + (1 if progress_enabled else 0)
    )
    enqueue_done_barrier = ctx.Barrier(num_enqueuers)
    done_barrier = ctx.Barrier(num_enqueuers)
    shutdown_event = ctx.Event()

    if progress_enabled:
        # A thread, not a process: the counters are already shared memory and
        # the parent has nothing else to do while it waits on results. Daemon,
        # because it is a party on the ready barrier -- a run that dies before
        # every process reaches that barrier would otherwise leave it blocked
        # there holding up the parent's exit.
        threading.Thread(
            target=progress_monitor,
            args=(
                progress_interval * 60.0,
                enqueued_totals,
                completed_totals,
                ready_barrier,
                shutdown_event,
            ),
            daemon=True,
        ).start()

    gc_proc = None
    if gc_enabled:
        gc_proc = ctx.Process(
            target=gc_entry,
            args=(
                gc_minutes,
                gc_interval,
                pool_size,
                ready_barrier,
                shutdown_event,
                gc_result_queue,
            ),
        )
        gc_proc.start()

    workers = []
    for worker_id in range(num_workers):
        p = ctx.Process(
            target=worker_entry,
            args=(
                worker_id,
                pool_size,
                executor_threads,
                num_steps,
                output_payload,
                completed_totals,
                ready_barrier,
                shutdown_event,
            ),
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
                enqueued_totals,
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
    # The drain is over, so the workers have nothing left to execute and the
    # collector nothing left to collect.
    shutdown_event.set()
    for p in workers:
        p.join()
    gc_result = None
    if gc_proc is not None:
        # Blocks out whichever round was in flight when shutdown was signalled.
        gc_result = gc_result_queue.get()
        gc_proc.join()

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
    samples_missing = sum(r["samples_missing"] for r in results)
    all_latencies: list[float] = []
    for r in results:
        all_latencies.extend(r["latencies"])

    print(f"Workers:          {num_workers}   (runtime, execute only)")
    print(f"Enqueuers:        {num_enqueuers}   (client, enqueue only)")
    print(f"Queue:            {QUEUE_NAME}  (database-backed)")
    print(f"Worker concurr.:  {worker_concurrency}  (per worker process)")
    print(f"Steps/workflow:   {num_steps}")
    if output_kb > 0:
        print(
            f"Output size:      {output_kb} KB   "
            f"(workflow and every step, {(num_steps + 1) * output_kb} KB/workflow)"
        )
    else:
        print("Output size:      0   (int returns)")
    print(f"Target RPS:       {total_rps}  ({per_proc_rps:.1f}/enqueuer)")
    print(f"Planned enqueues: {total_enqueues}")
    print(f"Max in-flight:    {max_inflight}  (per enqueuer)")
    print(f"Pool size/proc:   {pool_size}")
    print(f"Exec threads/wkr: {executor_threads}")
    if gc_enabled:
        print(
            f"GC:               every {gc_interval:g} min, "
            f"older than {gc_minutes:g} min  (1 process)"
        )
    else:
        print("GC:               off")
    if progress_enabled:
        print(f"Progress:         every {progress_interval:g} min")
    else:
        print("Progress:         off")
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
    if samples_missing:
        print(
            f"Samples GC'd:     {samples_missing}   (sampled workflows collected "
            f"before the lookup; percentiles cover the survivors only)"
        )
    if gc_result is not None:
        gc_rounds = gc_result["rounds"]
        if gc_rounds:
            print(f"GC rounds:        {len(gc_rounds)}")
            print(
                f"GC round time:    p50={percentile(gc_rounds, 50):.2f}s "
                f"p95={percentile(gc_rounds, 95):.2f}s "
                f"max={max(gc_rounds):.2f}s "
                f"mean={sum(gc_rounds)/len(gc_rounds):.2f}s"
            )
            print(
                f"GC deleted:       {gc_result['deleted']} workflows   "
                f"({gc_result['deleted']/len(gc_rounds):.0f}/round)"
            )
        elif gc_result["failures"]:
            print("GC rounds:        0   (every attempt failed)")
        else:
            print(
                f"GC rounds:        0   (the run ended before the first round, "
                f"{gc_interval:g} min in)"
            )
        if gc_result["failures"]:
            print(
                f"GC failures:      {gc_result['failures']}   (rounds that "
                f"raised and were retried at the next slot; "
                f"last: {gc_result['last_error']})"
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
        "--steps",
        type=int,
        default=0,
        dest="num_steps",
        help="Number of steps each workflow runs (default 0). A step does no "
        "work of its own, so this scales the checkpoint rows written per "
        "workflow; --output-size sets how large each of those rows is",
    )
    parser.add_argument(
        "--output-size",
        type=int,
        default=0,
        dest="output_kb",
        help="Kilobytes of random data returned by the workflow and by each of "
        "its steps (default 0 = return an int, as before). The buffer is "
        "generated once for the whole run and shared, so this costs writes, not "
        "CPU. Combines with --steps: N steps store (N+1) x this per workflow",
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
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=5.0,
        help="Minutes between progress reports, each covering the interval just "
        "ended: workflows enqueued, workflows completed and the backlog between "
        "them (0 = no reports). Reports start with the enqueue phase and "
        "continue through the drain, where the enqueue rate falls to zero and "
        "the completion rate shows how fast the backlog clears",
    )
    parser.add_argument(
        "--gc",
        type=float,
        default=0.0,
        dest="gc_minutes",
        help="Garbage-collect workflows older than this many minutes, from one "
        "dedicated process for the whole run (0 = no GC). Only finished "
        "workflows are eligible, so nothing the run still has to drain is ever "
        "collected. Keep it well under --duration, or nothing is ever old "
        "enough to sweep",
    )
    parser.add_argument(
        "--gc-interval",
        type=float,
        default=None,
        help="Minutes between GC rounds (default: the --gc retention, so each "
        "round clears exactly one window). Shorter splits a window across "
        "several rounds, longer hands each round more than a window. No effect "
        "without --gc",
    )
    args = parser.parse_args()
    # Defaulting here rather than in argparse keeps the default tied to --gc
    # whatever it is set to, instead of freezing a number in the help text.
    gc_interval = args.gc_interval if args.gc_interval is not None else args.gc_minutes
    if args.gc_minutes > 0 and gc_interval <= 0:
        # A non-positive period puts every grid slot in the past, which is a
        # spin loop against the database rather than a fast collector.
        parser.error("--gc-interval must be greater than 0")
    run_multiprocess(
        args.rps,
        args.duration,
        args.max_inflight,
        args.pool_size,
        args.executor_threads,
        args.workers,
        args.enqueuers,
        args.worker_concurrency,
        args.num_steps,
        args.output_kb,
        args.drain_timeout,
        args.sample_rate,
        args.gc_minutes,
        gc_interval,
        args.progress_interval,
    )


if __name__ == "__main__":
    main()
