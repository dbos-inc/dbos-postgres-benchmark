"""Per-host load generator for the sharded DBOS benchmark.

Runs on one client host against the PgDog sidecar on that host, and is launched by
orchestrate.py on every active client at once. Spawns --processes DBOS processes;
each launches DBOS on the sharded system database and waits for the shared
--start-at wall-clock time. Together they then offer --rps workflows per second,
open loop, for --duration seconds.

There is one queue (--queue) and one shard key per shard (--keys), each routed by PgDog
to its shard (admin.py bootstrap finds them); each workflow lives under one of the keys.
Every process dequeues from the queue: each poll finds the shard keys with enqueued
work, with one read of every shard, then claims under each key, on that key's shard. A host's
enqueues are split evenly over the keys, each key's enqueued by one of its processes
(key i by process i mod --processes): one enqueuer per key per host batches far more
rows per statement than every process enqueuing its share.

Enqueues are batched SQL inserts, as an application enqueuing in bulk writes them: every
--enqueue-interval, the enqueuer writes the workflows now due as ENQUEUED rows, inputs
and status, in one SQL statement through PgDog, the same rows a DBOSClient enqueue
writes. DBOSClient.enqueue writes one workflow per transaction. PgDog sends a multi-row INSERT to every shard, so
the statement carries PgDog's routing comment for the queue's shard key. Batches go out
on up to --enqueue-connections connections at once: one batch in flight at a time caps
an enqueuer at what one connection inserts, about the offered rate of a busy shard, so
once a slow batch put it behind schedule it never caught up.

Open loop means a workflow is enqueued at its scheduled time whether or not earlier
ones have finished, and latency is measured from that scheduled time, so it includes
the wait for the next batch. A saturated system therefore shows up as rising latency
and as completions falling behind the offered rate, not as a quietly lower offered
rate (coordinated omission).

A queue's workflows are dequeued by all the processes assigned to it, so each is
usually run by a different process than the one that enqueued it. Its completion
is recorded at the end of its body by whichever process ran it, so the
benchmark adds no work of its own such as waiting on a handle. That is before DBOS
writes the workflow's outcome: latency leaves out that one write, and admin.py
counts outcome writes on the shards so a backlog of them still shows.

Completions are counted per wall-clock second since --start-at, so the orchestrator
can sum seconds across hosts and measure the steady-state window alone. Latencies
go into a log-bucketed histogram that merges exactly across processes and hosts.

The last stdout line is ``RESULT {json}``: this host's merged results.
"""

import argparse
import asyncio
import json
import math
import multiprocessing as mp
import os
import queue as queue_mod
import random
import socket
import subprocess
import sys
import threading
import time
import traceback
import uuid
from collections import Counter
from typing import Any, Optional

WORKFLOW_NAME = "sharded_bench_workflow"
# DBOS's application_name for its system database connections.
APPLICATION_NAME = "dbos_transact"
# Most workflows one enqueue statement writes, so one statement's work stays bounded.
MAX_BATCH = 5000
# Seconds past the end of the run a process may still issue starts that were due.
ISSUE_GRACE_S = 1.0
# Seconds past the drain the parent waits for workers to report.
REPORT_GRACE_S = 120

# Latency histogram: bucket b holds latencies in [1.02^b, 1.02^(b+1)) microseconds.
HIST_BASE = 1.02
LOG_BASE = math.log(HIST_BASE)


def hist_bucket(seconds: float) -> int:
    return int(math.log(max(seconds * 1e6, 1.0)) / LOG_BASE)


def hist_percentile(hist: dict[int, int], pct: float) -> Optional[float]:
    """The upper edge, in seconds, of the bucket holding the pct-th percentile."""
    total = sum(hist.values())
    if total == 0:
        return None
    rank = math.ceil(pct / 100 * total)
    seen = 0
    for bucket in sorted(hist):
        seen += hist[bucket]
        if seen >= rank:
            return HIST_BASE ** (bucket + 1) / 1e6
    return HIST_BASE ** (max(hist) + 1) / 1e6


def merge_counts(into: dict, other: dict) -> None:
    for k, v in other.items():
        into[k] = into.get(k, 0) + v


def worker_entry(
    proc_id: int, args: argparse.Namespace, result_queue: mp.Queue
) -> None:
    # Exit if the parent dies: an orphaned worker would keep running its workflows
    # into whatever database the router points at next.
    parent = os.getppid()

    def watchdog() -> None:
        while True:
            time.sleep(1)
            if os.getppid() != parent:
                os._exit(1)

    threading.Thread(target=watchdog, daemon=True).start()

    # All DBOS code lives in the worker process, so the parent stays light.
    from dbos import DBOS, DBOSConfig

    start_at = args.start_at
    window_start = start_at + args.warmup
    window_end = start_at + args.duration

    started: Counter[int] = Counter()
    completed: Counter[int] = Counter()
    hist: Counter[int] = Counter()
    counts = {
        "start_failures": 0,
        "not_issued": 0,
    }
    # Workflow bodies record completions from DBOS's executor threads.
    lock = threading.Lock()

    def record_completion(t_sched: float) -> None:
        now = time.time()
        with lock:
            completed[int(now - start_at)] += 1
            if window_start <= t_sched < window_end:
                hist[hist_bucket(now - t_sched)] += 1

    @DBOS.step()
    async def noop_step() -> None:
        return None

    # Named explicitly: enqueue rows refer to the workflow by this name.
    @DBOS.workflow(name=WORKFLOW_NAME)
    async def bench_workflow(t_sched: float) -> float:
        for _ in range(args.steps):
            await noop_step()
        record_completion(t_sched)
        return t_sched

    import sqlalchemy as sa

    config: DBOSConfig = {
        "name": "dbos-bench-sharded",
        "system_database_url": args.url,
        # DBOS's own engine settings, but without pool_pre_ping: a health-check round
        # trip on every connection checkout cost ~16% of the client CPU per workflow,
        # and DBOS already retries a statement that hits a dead connection.
        "system_database_engine": sa.create_engine(
            sa.make_url(args.url).set(drivername="postgresql+psycopg"),
            pool_size=args.pool_size,
            max_overflow=0,
            pool_timeout=30,
            pool_pre_ping=False,
            connect_args={"application_name": APPLICATION_NAME, "connect_timeout": 10},
        ),
        "sharding": True,
        "run_admin_server": False,
        "enable_otlp": False,
        # Fewer threads than DBOS's 64: every database round trip a thread makes
        # contends for the GIL with the rest, and fewer contenders cost less CPU. Not
        # too few: a thread waiting on a slow shard is one fewer for every other shard.
        "max_executor_threads": args.executor_threads,
        "executor_id": str(uuid.uuid7()),
        # Sharding is an enterprise feature: DBOS checks the key's entitlement at
        # launch. Not conductor_key, which would also connect every process to Conductor.
        "transact_enterprise_key": os.environ["DBOS_TRANSACT_ENTERPRISE_KEY"],
    }
    queue_name = args.queue
    # The keys this process enqueues for the host, each at an equal share of its rate.
    my_keys = [k for i, k in enumerate(args.keys) if i % args.processes == proc_id]
    enqueue_rate = args.rps / len(args.keys)
    DBOS(config=config)
    DBOS.listen_queues([queue_name])
    DBOS.launch()
    # Registered by admin.py bootstrap.
    assert DBOS.retrieve_queue(queue_name) is not None, f"no queue {queue_name}"
    launched_late = time.time() > start_at

    import psycopg

    # A batch's inputs and ENQUEUED status rows, as one statement: a single statement
    # is atomic, so no workflow is visible to a dequeue before its inputs. The batch
    # travels as two arrays, so the text never changes: psycopg converts a query's
    # placeholders afresh unless it is short and repeated, and a VALUES list of a
    # batch's length is neither. A batch's workflows share one shard key, so they all
    # live on that key's shard.
    def enqueue_statement(uuid_function: str, key: str) -> str:
        # Postgres generates the batch's rows from a start time, step and count, so
        # the enqueuer does no per-row work: each workflow's input is its scheduled
        # time, in DBOS's portable JSON, the input format meant for writers outside
        # Python.
        return (
            # Quoted: PgDog routed some unquoted keys (with spaces, colons, underscores)
            # to a shard other than their rows'.
            f'/* pgdog_sharding_key: "{key}" */ '
            f"WITH batch AS (SELECT {uuid_function}::text AS workflow_uuid, "
            '\'{"namedArgs":{},"positionalArgs":[\' '
            "|| (%(base)s::float8 + k * %(step)s::float8)::text || ']}' AS inputs "
            "FROM generate_series(0, %(n)s - 1) AS k), "
            "stored AS (INSERT INTO dbos.workflow_input (workflow_uuid, inputs, shard_key) "
            "SELECT workflow_uuid, inputs, %(key)s FROM batch) "
            "INSERT INTO dbos.workflow_status (workflow_uuid, status, name, queue_name, "
            "application_name, serialization, shard_key) "
            "SELECT workflow_uuid, 'ENQUEUED', %(name)s, %(queue)s, %(app)s, "
            "'portable_json', %(key)s FROM batch"
        )

    async def enqueue_all(key: str) -> None:
        # DBOS's application_name: PgDog shares server connections among its clients
        # and SETs and RESETs any parameter on which they differ, on every handoff.
        async def connect() -> Any:
            return await psycopg.AsyncConnection.connect(
                args.url, autocommit=True, application_name=APPLICATION_NAME
            )

        conns = [await connect() for _ in range(args.enqueue_connections)]
        # Time-ordered IDs, like DBOS's own, keep the primary key's inserts at the end
        # of its index; uuidv7() is Postgres 18's, so older servers get random ones.
        try:
            await conns[0].execute("SELECT uuidv7()")
            enqueue_sql = enqueue_statement("uuidv7()", key)
        except psycopg.errors.UndefinedFunction:
            enqueue_sql = enqueue_statement("gen_random_uuid()", key)
        idle: asyncio.Queue = asyncio.Queue()
        for c in conns:
            idle.put_nowait(c)
        in_flight: set[asyncio.Task] = set()
        total = int(enqueue_rate * args.duration)
        end = start_at + args.duration
        # Each enqueuer's schedule is offset by a fraction of an interval, so the
        # enqueuers on a host do not all fire on the same instant.
        phase = random.random() / enqueue_rate
        issued = 0

        def due_now() -> int:
            return min(total, int((time.time() - start_at - phase) * enqueue_rate) + 1)

        async def send(conn: Any, first: int, n: int) -> None:
            params = {
                "base": start_at + phase + first / enqueue_rate,
                "step": 1 / enqueue_rate,
                "n": n,
                "queue": queue_name,
                "key": key,
                "name": WORKFLOW_NAME,
                "app": config["name"],
            }
            try:
                await conn.execute(enqueue_sql, params)
                started[int(time.time() - start_at)] += n
            except Exception as e:
                if not counts["start_failures"]:
                    print(f"enqueue failed: {e}", flush=True)
                counts["start_failures"] += n
                # PgDog closes a client whose statement waits past its checkout
                # timeout; kept, the closed connection would fail every later batch.
                if conn.closed:
                    try:
                        conn = await connect()
                    except Exception:
                        pass
            finally:
                idle.put_nowait(conn)

        await asyncio.sleep(max(0.0, start_at - time.time()))
        # Starts stop at the end of the run: past it, a slipped schedule would keep
        # the process issuing, and reporting, long after every other one. The grace
        # lets a loop that wakes a little late still issue the starts already due.
        stop = end + ISSUE_GRACE_S
        while issued < total and time.time() < stop:
            if due_now() > issued:
                # Waits while every connection has a batch in flight; whatever came
                # due meanwhile goes in this batch.
                conn = await idle.get()
                n = min(due_now() - issued, MAX_BATCH)
                task = asyncio.create_task(send(conn, issued, n))
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)
                issued += n
            # Behind schedule, the next batch goes out at once, on another connection.
            if due_now() <= issued:
                await asyncio.sleep(args.enqueue_interval)
        await asyncio.gather(*in_flight)
        counts["not_issued"] += total - issued

    async def run() -> None:
        await asyncio.gather(*(enqueue_all(key) for key in my_keys))
        # Workflows may still be queued; keep executing them until the drain ends.
        await asyncio.sleep(
            max(0.0, start_at + args.duration + args.drain - time.time())
        )

    ok = False
    try:
        asyncio.new_event_loop().run_until_complete(run())
        ok = True
    except BaseException:
        traceback.print_exc()
    finally:
        with lock:
            result = {
                "started": dict(started),
                "completed": dict(completed),
                "hist": dict(hist),
                "launched_late": launched_late,
                **counts,
            }
        result_queue.put(result)
        # Flush the queue's feeder thread, then exit without tearing anything down.
        # Workflows still in flight would otherwise keep retrying against the database
        # instead of letting the process exit, and a process that outlives its run
        # writes into the next point's freshly recreated database.
        result_queue.close()
        result_queue.join_thread()
        os._exit(0 if ok else 1)


def cpu_ticks() -> tuple[int, int]:
    """(busy, total) jiffies across every CPU on the host."""
    with open("/proc/stat") as f:
        fields = [int(x) for x in f.readline().split()[1:]]
    idle = fields[3] + fields[4]
    return sum(fields) - idle, sum(fields)


def pgdog_ticks() -> Optional[int]:
    """User + system jiffies of the PgDog process, if it is visible from this host."""
    try:
        pid = subprocess.run(
            ["pgrep", "-xo", "pgdog"], capture_output=True, text=True, check=True
        ).stdout.split()[0]
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        return int(fields[11]) + int(fields[12])
    except Exception:
        return None


def query_cache(admin_url: Optional[str]) -> Optional[dict[str, tuple[int, int]]]:
    """PgDog's routing counts: each statement's (direct, multi), meaning how many of its
    executions went to one shard and how many to several."""
    if not admin_url:
        return None
    try:
        import psycopg

        with psycopg.connect(admin_url, autocommit=True, connect_timeout=5) as c:
            cur = c.execute("SHOW QUERY_CACHE")
            cols = [d.name for d in cur.description or []]
            q, d, m = cols.index("query"), cols.index("direct"), cols.index("multi")
            return {
                " ".join(str(r[q]).split()): (int(r[d] or 0), int(r[m] or 0))
                for r in cur.fetchall()
            }
    except Exception as e:
        print(f"warning: could not read PgDog query cache: {e}", flush=True)
        return None


def routing_diff(
    a: Optional[dict], b: Optional[dict], elapsed: float, top: int = 10
) -> Optional[dict[str, Any]]:
    if a is None or b is None or elapsed <= 0:
        return None
    rows = []
    for query, (direct, multi) in b.items():
        d0, m0 = a.get(query, (0, 0))
        rows.append((query, (direct - d0) / elapsed, (multi - m0) / elapsed))
    rows.sort(key=lambda r: -r[2])
    return {
        "direct_per_s": sum(r[1] for r in rows),
        "multi_per_s": sum(r[2] for r in rows),
        "top_multi": [
            {"query": r[0][:300], "multi_per_s": r[2]} for r in rows[:top] if r[2] > 0
        ],
    }


def sample_window(args: argparse.Namespace) -> dict[str, Any]:
    """Host CPU, PgDog cores used, and PgDog routing, over the measurement window."""
    time.sleep(max(0.0, args.start_at + args.warmup - time.time()))
    busy0, total0 = cpu_ticks()
    pg0 = pgdog_ticks()
    qc0 = query_cache(args.router_admin_url)
    t0 = time.time()
    time.sleep(max(0.0, args.start_at + args.duration - time.time()))
    busy1, total1 = cpu_ticks()
    pg1 = pgdog_ticks()
    qc1 = query_cache(args.router_admin_url)
    elapsed = time.time() - t0
    hz = os.sysconf("SC_CLK_TCK")
    return {
        "host_cpu_pct": 100 * (busy1 - busy0) / max(1, total1 - total0),
        "pgdog_cores": (
            (pg1 - pg0) / hz / elapsed
            if pg0 is not None and pg1 is not None and elapsed > 0
            else None
        ),
        "routing": routing_diff(qc0, qc1, elapsed),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="System database URL (PgDog)")
    parser.add_argument("--router-admin-url", help="PgDog admin database, for routing")
    parser.add_argument("--rps", type=float, required=True, help="This host's rate")
    parser.add_argument("--start-at", type=float, required=True, help="Epoch seconds")
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument(
        "--warmup", type=float, default=60.0, help="Seconds excluded from latency"
    )
    parser.add_argument(
        "--drain", type=float, default=30.0, help="Seconds to wait after the run"
    )
    parser.add_argument("--steps", type=int, default=0, help="Steps per workflow")
    parser.add_argument("--queue", required=True, help="The queue every process uses")
    parser.add_argument(
        "--keys",
        type=lambda s: s.split(","),
        required=True,
        help="Comma-separated shard keys, one per shard",
    )
    parser.add_argument("--processes", type=int, default=os.cpu_count())
    parser.add_argument("--pool-size", type=int, default=16)
    parser.add_argument("--executor-threads", type=int, default=8)
    parser.add_argument(
        "--enqueue-interval",
        type=float,
        default=0.01,
        help="Seconds between a process's enqueue batches",
    )
    parser.add_argument(
        "--enqueue-connections",
        type=int,
        default=4,
        help="Enqueue batches each enqueuer may have in flight at once",
    )
    args = parser.parse_args()

    ctx = mp.get_context("spawn")
    result_queue: mp.Queue = ctx.Queue()
    procs = [
        ctx.Process(target=worker_entry, args=(i, args, result_queue))
        for i in range(args.processes)
    ]
    for p in procs:
        p.start()
    window = sample_window(args)
    results = []
    # Workers report right after the drain; past this, a missing one is stuck.
    deadline = args.start_at + args.duration + args.drain + REPORT_GRACE_S
    while len(results) < len(procs) and time.time() < deadline:
        try:
            results.append(result_queue.get(timeout=5))
        except queue_mod.Empty:
            # Every worker has exited, so whatever they reported is already queued.
            if not any(p.is_alive() for p in procs):
                while True:
                    try:
                        results.append(result_queue.get_nowait())
                    except queue_mod.Empty:
                        break
                break
    for p in procs:
        p.join(timeout=5)
        if p.is_alive():
            p.kill()
    missing = len(procs) - len(results)
    if missing == len(procs):
        sys.exit("no worker process reported; see their output above")
    if missing:
        print(f"warning: {missing} worker process(es) never reported", flush=True)

    merged: dict[str, Any] = {
        "host": socket.gethostname(),
        "processes": args.processes,
        "rps": args.rps,
        "started": {},
        "completed": {},
        "hist": {},
        "start_failures": 0,
        "launched_late": 0,
        "not_issued": 0,
        "workers_missing": missing,
        **window,
    }
    for r in results:
        for k in ("started", "completed", "hist"):
            merge_counts(merged[k], r[k])
        for k in (
            "start_failures",
            "not_issued",
        ):
            merged[k] += r[k]
        merged["launched_late"] += int(r["launched_late"])
    print("RESULT " + json.dumps(merged), flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
