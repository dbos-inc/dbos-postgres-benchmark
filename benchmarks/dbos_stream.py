"""Benchmark DBOS stream-write throughput from steps.

Topology: one stream per workflow, many workflows per worker process. Each
workflow runs a single long-lived step that writes to its own stream via
DBOS.write_stream_async (the from-step, at-least-once path) as fast as it can
for the measurement window. Aggregate throughput = total values written / sec.

Writes to a single stream serialize on the offset (INSERT ... SELECT MAX(offset)+1
under a (workflow_uuid, key, offset) primary key), so aggregate throughput comes
from running many streams concurrently. Sweep --streams-per-worker / --processes
upward until writes/sec plateaus and per-write p99 latency rises: that knee is the
maximum supported throughput.

Optionally pace each producer to a target aggregate rate with --rps (default 0 =
flat out).
"""

import argparse
import asyncio
import ipaddress
import multiprocessing as mp
import os
import random
import socket
import time
import uuid
from urllib.parse import urlparse

import asyncpg


def pin_db_hosts_to_ip() -> None:
    """Resolve the DB hostnames once and rewrite the URLs to their IPs.

    Many worker processes each open a connection pool plus DBOS background
    threads (notification listener, scheduler, queue poller), all calling
    getaddrinfo() on the same RDS hostname at once. That overwhelms the local
    stub resolver, which returns intermittent EAI_AGAIN ("Temporary failure in
    name resolution"). Resolving once here and connecting by IP eliminates all
    per-connection DNS lookups; spawned workers inherit the rewritten os.environ.

    No-op if the host is already an IP or cannot be resolved. RDS default
    sslmode does not verify the hostname, so connecting by IP is safe.
    """
    for var in ("POSTGRES_DATABASE_URL", "BENCHMARK_DATABASE_URL"):
        url = os.environ.get(var)
        if not url:
            continue
        parsed = urlparse(url)
        host = parsed.hostname
        if host is None:
            continue
        try:
            ipaddress.ip_address(host)
            continue  # already a literal IP
        except ValueError:
            pass
        try:
            ip = socket.gethostbyname(host)
        except OSError as e:
            print(f"warning: could not resolve {host} for {var}: {e}", flush=True)
            continue
        # Swap only the host token in the netloc, preserving credentials and
        # port exactly (raw, so no risk of re-encoding the password).
        netloc = parsed.netloc
        at = netloc.rfind("@")
        creds = netloc[: at + 1]  # "user:pass@" or "" if no credentials
        hostport = netloc[at + 1 :]
        port = hostport.rsplit(":", 1)[1] if ":" in hostport else None
        new_netloc = creds + (f"{ip}:{port}" if port else ip)
        os.environ[var] = parsed._replace(netloc=new_netloc).geturl()
        print(f"Pinned {host} -> {ip} for {var}", flush=True)


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


def bootstrap_schema_entry(use_listen_notify: bool) -> None:
    """Pre-create the DBOS system schema (incl. streams table + trigger) in a
    one-shot subprocess.

    Runs in its own spawned process so the parent never imports DBOS.
    Pre-running migrations eliminates the per-worker advisory-lock serialization
    when many workers launch in parallel.

    use_listen_notify is decided here: with it False the migrations skip the
    streams AFTER INSERT trigger, so writes avoid a per-row pg_notify. Workers
    must use the same value (it cannot change after the schema is created).
    """
    from dbos import DBOS, DBOSConfig

    config: DBOSConfig = {
        "name": "dbos-stream-bench-bootstrap",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": 2,
        "use_listen_notify": use_listen_notify,
    }
    DBOS(config=config)
    DBOS.launch()
    DBOS.destroy()


async def count_stream_rows() -> int:
    """Total rows in dbos.streams (one per committed write, no readers/sentinels)."""
    conn = await asyncpg.connect(os.environ["BENCHMARK_DATABASE_URL"])
    try:
        return await conn.fetchval("SELECT count(*) FROM dbos.streams")
    finally:
        await conn.close()


def worker_entry(
    worker_id: int,
    streams_per_worker: int,
    start_epoch_value,
    duration_s: float,
    per_producer_rps: float,
    payload_size: int,
    pool_size: int,
    executor_threads: int,
    sample_rate: float,
    max_samples: int,
    use_listen_notify: bool,
    ready_barrier,
    launched_barrier,
    result_queue: mp.Queue,
) -> None:
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig

    payload = "x" * payload_size

    @DBOS.step()
    async def stream_writer(key: str, start_epoch: float) -> dict:
        # Wait for the synchronized start so every producer's measurement window
        # is the same wall-clock interval [start_epoch, start_epoch + duration].
        now = time.time()
        if start_epoch > now:
            await asyncio.sleep(start_epoch - now)
        deadline = start_epoch + duration_s

        count = 0
        failures = 0
        first_write = 0.0
        last_write = 0.0
        latencies: list[float] = []

        async def one_write() -> None:
            nonlocal count, failures, first_write, last_write
            sampled = len(latencies) < max_samples and random.random() < sample_rate
            t0 = time.monotonic() if sampled else 0.0
            try:
                await DBOS.write_stream_async(key, payload)
            except Exception:
                failures += 1
                return
            count += 1
            wall = time.time()
            if first_write == 0.0:
                first_write = wall
            last_write = wall
            if sampled:
                latencies.append(time.monotonic() - t0)

        if per_producer_rps > 0:
            # Paced: schedule writes at a fixed interval within the window.
            interval = 1.0 / per_producer_rps
            i = 0
            while True:
                target = start_epoch + i * interval
                if target >= deadline:
                    break
                now = time.time()
                if target > now:
                    await asyncio.sleep(target - now)
                await one_write()
                i += 1
        else:
            # Flat out: write as fast as the DB accepts until the deadline.
            while time.time() < deadline:
                await one_write()

        return {
            "count": count,
            "failures": failures,
            "first_write": first_write,
            "last_write": last_write,
            "latencies": latencies,
        }

    @DBOS.workflow()
    async def producer(key: str, start_epoch: float) -> dict:
        return await stream_writer(key, start_epoch)

    config: DBOSConfig = {
        "name": "dbos-stream-bench",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
        "executor_id": str(uuid.uuid7()),
        "use_listen_notify": use_listen_notify,
    }
    DBOS(config=config)
    DBOS.launch()

    async def run() -> dict:
        # Wait until every worker's DBOS is launched before choosing the start
        # time, so the synchronized window doesn't include process startup.
        await asyncio.to_thread(ready_barrier.wait)

        # Worker 0 picks the shared start time (now + grace) so producers on all
        # workers begin writing at the same wall-clock instant.
        start_epoch = start_epoch_value.value
        await asyncio.to_thread(launched_barrier.wait)
        start_epoch = start_epoch_value.value

        # Launch one producer workflow per stream. start_workflow_async returns
        # quickly (a row insert); each producer then sleeps until start_epoch.
        keys = [f"w{worker_id}-s{i}" for i in range(streams_per_worker)]
        handles = [
            await DBOS.start_workflow_async(producer, key, start_epoch) for key in keys
        ]
        results = await asyncio.gather(*(h.get_result() for h in handles))

        count = sum(r["count"] for r in results)
        failures = sum(r["failures"] for r in results)
        firsts = [r["first_write"] for r in results if r["first_write"] > 0]
        lasts = [r["last_write"] for r in results if r["last_write"] > 0]
        latencies: list[float] = []
        for r in results:
            latencies.extend(r["latencies"])

        return {
            "count": count,
            "failures": failures,
            "first_write": min(firsts) if firsts else 0.0,
            "last_write": max(lasts) if lasts else 0.0,
            "latencies": latencies,
        }

    try:
        result = asyncio.run(run())
        result_queue.put(result)
    finally:
        DBOS.destroy()


def set_start_epoch_entry(start_epoch_value, ready_barrier, launched_barrier, grace):
    """Tiny coordinator process: after all workers launch DBOS, set the shared
    start time, then release them to launch producers.

    Kept as a separate participant so the value is set exactly once, between the
    two barriers, regardless of which worker gets there first.
    """
    ready_barrier.wait()
    start_epoch_value.value = time.time() + grace
    launched_barrier.wait()


def run_multiprocess(
    processes: int,
    streams_per_worker: int,
    duration_s: float,
    total_rps: int,
    payload_size: int,
    pool_size: int,
    executor_threads: int,
    sample_rate: float,
    max_samples: int,
    start_grace: float,
    use_listen_notify: bool,
) -> None:
    # Pin DB hostnames to IPs before any connection is opened, so the many
    # worker processes don't storm the local DNS resolver (see the function).
    pin_db_hosts_to_ip()

    asyncio.run(recreate_database())

    ctx = mp.get_context("spawn")

    # Pre-create the DBOS schema in a single child so workers don't serialize
    # on the migration advisory lock. This also fixes whether the streams
    # pg_notify trigger exists, so workers must match use_listen_notify.
    bootstrap = ctx.Process(target=bootstrap_schema_entry, args=(use_listen_notify,))
    bootstrap.start()
    bootstrap.join()

    total_streams = processes * streams_per_worker
    per_producer_rps = (total_rps / total_streams) if total_rps > 0 else 0.0

    result_queue: mp.Queue = ctx.Queue()
    start_epoch_value = ctx.Value("d", 0.0)
    # +1 participant: the coordinator that sets start_epoch between the barriers.
    ready_barrier = ctx.Barrier(processes + 1)
    launched_barrier = ctx.Barrier(processes + 1)

    coordinator = ctx.Process(
        target=set_start_epoch_entry,
        args=(start_epoch_value, ready_barrier, launched_barrier, start_grace),
    )
    coordinator.start()

    workers = []
    for worker_id in range(processes):
        p = ctx.Process(
            target=worker_entry,
            args=(
                worker_id,
                streams_per_worker,
                start_epoch_value,
                duration_s,
                per_producer_rps,
                payload_size,
                pool_size,
                executor_threads,
                sample_rate,
                max_samples,
                use_listen_notify,
                ready_barrier,
                launched_barrier,
                result_queue,
            ),
        )
        p.start()
        workers.append(p)

    results = [result_queue.get() for _ in workers]
    for p in workers:
        p.join()
    coordinator.join()

    start_epoch = start_epoch_value.value
    written = sum(r["count"] for r in results)
    failures = sum(r["failures"] for r in results)
    firsts = [r["first_write"] for r in results if r["first_write"] > 0]
    lasts = [r["last_write"] for r in results if r["last_write"] > 0]
    first_write = min(firsts) if firsts else start_epoch
    last_write = max(lasts) if lasts else start_epoch
    measured_span = last_write - first_write

    # The synchronized window is the denominator: every producer wrote within
    # [start_epoch, start_epoch + duration]. measured_span is a cross-check.
    writes_per_sec = written / duration_s if duration_s > 0 else 0.0
    late_start = first_write - start_epoch  # >0 means launch overran start-grace

    all_latencies: list[float] = []
    for r in results:
        all_latencies.extend(r["latencies"])

    row_count = asyncio.run(count_stream_rows())

    print(f"Processes:         {processes}")
    print(f"Streams/worker:    {streams_per_worker}")
    print(f"Total streams:     {total_streams}")
    print(f"Payload size:      {payload_size} bytes")
    print(f"Mode:              {'paced' if total_rps > 0 else 'flat-out'}")
    if total_rps > 0:
        print(f"Target RPS:        {total_rps}  ({per_producer_rps:.1f}/stream)")
    print(
        f"Pool size/proc:    {pool_size}   (total DB conns ~= {processes * pool_size})"
    )
    print(f"Exec threads/proc: {executor_threads}")
    print(
        f"LISTEN/NOTIFY:     {'on' if use_listen_notify else 'off (no write trigger)'}"
    )
    print(f"Window:            {duration_s:.2f}s")
    print(f"Measured span:     {measured_span:.2f}s   (should be ~= window)")
    print(f"Late start:        {late_start:.2f}s   (>~0.5s: raise --start-grace)")
    print(f"Writes:            {written}")
    print(f"Write failures:    {failures}")
    print(f"streams rows:      {row_count}   (== writes: {row_count == written})")
    print(f"Writes/sec:        {writes_per_sec:.0f}   (aggregate, from steps)")
    if all_latencies:
        print(
            f"Write latency:     samples={len(all_latencies)}   "
            f"p50={percentile(all_latencies, 50)*1000:.2f}ms "
            f"p95={percentile(all_latencies, 95)*1000:.2f}ms "
            f"p99={percentile(all_latencies, 99)*1000:.2f}ms "
            f"max={max(all_latencies)*1000:.2f}ms"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--processes", type=int, default=16, help="Number of worker processes"
    )
    parser.add_argument(
        "--streams-per-worker",
        type=int,
        default=16,
        help="Concurrent streams (= producer workflows) per worker process",
    )
    parser.add_argument(
        "--duration", type=float, default=30.0, help="Measurement window in seconds"
    )
    parser.add_argument(
        "--rps",
        type=int,
        default=0,
        help="Target aggregate writes/sec (0 = flat out, the default)",
    )
    parser.add_argument(
        "--payload-size",
        type=int,
        default=64,
        help="Bytes per stream value (default 64, ~LLM-token sized)",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=0,
        help="DBOS system DB pool size per process "
        "(0 = auto: streams-per-worker + 4)",
    )
    parser.add_argument(
        "--executor-threads",
        type=int,
        default=0,
        help="DBOS max_executor_threads per process "
        "(0 = auto: max(64, streams-per-worker * 2))",
    )
    parser.add_argument(
        "--sample-rate",
        type=float,
        default=0.01,
        help="Fraction of writes sampled for latency (default 0.01)",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=1000,
        help="Max latency samples retained per producer (default 1000)",
    )
    parser.add_argument(
        "--start-grace",
        type=float,
        default=5.0,
        help="Seconds between launch and synchronized write start (default 5)",
    )
    parser.add_argument(
        "--listen-notify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use LISTEN/NOTIFY (default on). --no-listen-notify drops the "
        "streams pg_notify trigger to isolate raw insert throughput",
    )
    args = parser.parse_args()

    pool_size = args.pool_size if args.pool_size > 0 else args.streams_per_worker + 4
    executor_threads = (
        args.executor_threads
        if args.executor_threads > 0
        else max(64, args.streams_per_worker * 2)
    )

    run_multiprocess(
        args.processes,
        args.streams_per_worker,
        args.duration,
        args.rps,
        args.payload_size,
        pool_size,
        executor_threads,
        args.sample_rate,
        args.max_samples,
        args.start_grace,
        args.listen_notify,
    )


if __name__ == "__main__":
    main()
