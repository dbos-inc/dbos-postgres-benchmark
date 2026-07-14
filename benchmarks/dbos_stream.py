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


def next_minute_start(min_lead_s: float) -> float:
    """The next wall-clock minute boundary at least min_lead_s away.

    Split writer/reader hosts each compute this independently and land on the
    same instant without exchanging timestamps -- just launch both within the
    same window. Boundaries closer than min_lead_s are skipped so there is time
    for processes to spawn and DBOS to launch before the window opens.
    """
    now = time.time()
    boundary = (now // 60.0 + 1.0) * 60.0
    while boundary - now < min_lead_s:
        boundary += 60.0
    return boundary


def stream_identity(worker_id: int, stream_index: int) -> tuple[str, str]:
    """(workflow_id, stream key) for a producer stream.

    Deterministic so reader processes can address any stream by computing its id
    instead of discovering the producer's auto-generated workflow id.
    """
    return f"prod-{worker_id}-{stream_index}", "data"


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


async def wait_for_schema(timeout_s: float = 180.0) -> None:
    """Reader role: block until the writer host has created the DB and schema.

    The reader host must never recreate the database (that would drop the
    writers' data), so it waits for dbos.streams to appear instead. This makes
    host launch order forgiving: start either side first.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            conn = await asyncpg.connect(os.environ["BENCHMARK_DATABASE_URL"])
            try:
                if await conn.fetchval(
                    "SELECT to_regclass('dbos.streams') IS NOT NULL"
                ):
                    return
            finally:
                await conn.close()
        except Exception:
            pass  # DB may not exist yet, or is mid-recreate on the writer host
        await asyncio.sleep(0.5)
    raise RuntimeError(
        "timed out waiting for dbos.streams — start the --role writer host "
        "(it creates the database and schema)"
    )


def worker_entry(
    worker_id: int,
    streams_per_worker: int,
    start_epoch: float,
    duration_s: float,
    per_producer_rps: float,
    jitter: float,
    payload_size: int,
    pool_size: int,
    executor_threads: int,
    sample_rate: float,
    use_listen_notify: bool,
    embed_timestamp: bool,
    result_queue: mp.Queue,
) -> None:
    # All DBOS code lives inside the worker process.
    from dbos import DBOS, DBOSConfig, SetWorkflowID

    static_payload = "x" * payload_size

    def make_value() -> str:
        # When readers are present, prefix each value with the write wall-clock
        # time so a reader can measure end-to-end (write->read) latency. Padded
        # back to ~payload_size. Otherwise use the precomputed static payload.
        if not embed_timestamp:
            return static_payload
        prefix = f"{time.time():.6f}|"
        return prefix + "x" * max(0, payload_size - len(prefix))

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
            sampled = random.random() < sample_rate
            t0 = time.monotonic() if sampled else 0.0
            try:
                await DBOS.write_stream_async(key, make_value())
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
            # Paced: schedule writes at a fixed interval. Offset this producer's
            # schedule by a random phase within `jitter` fraction of the interval
            # so producers don't all fire in lockstep -- lockstep bursts the DB
            # every interval and inflates latency even at low average rates.
            # jitter=0 reproduces the old synchronized ticks.
            interval = 1.0 / per_producer_rps
            phase = random.random() * interval * jitter
            i = 0
            while True:
                target = start_epoch + phase + i * interval
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
        # start_epoch is an absolute wall-clock instant chosen by the parent, so
        # every producer -- on this host or another -- opens the same window.
        # Launch one producer workflow per stream with a deterministic id so
        # reader processes can find it. start_workflow_async returns quickly (a
        # row insert); each producer then sleeps until start_epoch.
        handles = []
        for i in range(streams_per_worker):
            wfid, key = stream_identity(worker_id, i)
            with SetWorkflowID(wfid):
                handles.append(
                    await DBOS.start_workflow_async(producer, key, start_epoch)
                )
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


def reader_entry(
    reader_id: int,
    reader_processes: int,
    processes: int,
    streams_per_worker: int,
    fanout: int,
    start_epoch: float,
    duration_s: float,
    drain_timeout: float,
    pool_size: int,
    executor_threads: int,
    sample_rate: float,
    use_listen_notify: bool,
    read_result_queue: mp.Queue,
) -> None:
    # All DBOS code lives inside the reader process.
    from dbos import DBOS, DBOSConfig

    # Build this process's slice of the consumer list. Every stream has `fanout`
    # consumers; consumer global index c is handled by reader c % reader_processes.
    consumers: list[tuple[str, str]] = []
    gidx = 0
    for w in range(processes):
        for i in range(streams_per_worker):
            wfid, key = stream_identity(w, i)
            for _ in range(fanout):
                if gidx % reader_processes == reader_id:
                    consumers.append((wfid, key))
                gidx += 1

    config: DBOSConfig = {
        "name": "dbos-stream-bench-reader",
        "system_database_url": os.environ["BENCHMARK_DATABASE_URL"],
        "run_admin_server": False,
        "sys_db_pool_size": pool_size,
        "max_executor_threads": executor_threads,
        "executor_id": str(uuid.uuid7()),
        "use_listen_notify": use_listen_notify,
    }
    DBOS(config=config)
    DBOS.launch()

    async def consume(wfid: str, key: str, start_epoch: float, hard_deadline: float):
        now = time.time()
        if start_epoch > now:
            await asyncio.sleep(start_epoch - now)

        # Wait for the producer workflow to exist before streaming: read_stream's
        # first empty read calls get_status(), which raises for a missing workflow
        # (readers and producers start together, so this races the producer launch).
        while time.time() < hard_deadline:
            try:
                await (await DBOS.retrieve_workflow_async(wfid)).get_status()
                break
            except Exception:
                await asyncio.sleep(0.05)

        count = 0
        failed = 0
        first_read = 0.0
        last_read = 0.0
        latencies: list[float] = []
        try:
            async for value in DBOS.read_stream_async(wfid, key, offset=0):
                recv = time.time()
                count += 1
                if first_read == 0.0:
                    first_read = recv
                last_read = recv
                if random.random() < sample_rate:
                    try:
                        latencies.append(recv - float(value.split("|", 1)[0]))
                    except ValueError, IndexError:
                        pass
                if recv > hard_deadline:
                    break
        except Exception:
            failed = 1
        return {
            "count": count,
            "failed": failed,
            "first_read": first_read,
            "last_read": last_read,
            "latencies": latencies,
        }

    async def run() -> dict:
        hard_deadline = start_epoch + duration_s + drain_timeout

        results = await asyncio.gather(
            *(consume(wfid, key, start_epoch, hard_deadline) for wfid, key in consumers)
        )

        latencies: list[float] = []
        for r in results:
            latencies.extend(r["latencies"])
        firsts = [r["first_read"] for r in results if r["first_read"] > 0]
        lasts = [r["last_read"] for r in results if r["last_read"] > 0]
        return {
            "count": sum(r["count"] for r in results),
            "failed": sum(r["failed"] for r in results),
            "first_read": min(firsts) if firsts else 0.0,
            "last_read": max(lasts) if lasts else 0.0,
            "latencies": latencies,
        }

    try:
        read_result_queue.put(asyncio.run(run()))
    finally:
        DBOS.destroy()


def run_multiprocess(
    processes: int,
    streams_per_worker: int,
    duration_s: float,
    total_rps: int,
    jitter: float,
    payload_size: int,
    pool_size: int,
    executor_threads: int,
    sample_rate: float,
    start_grace: float,
    use_listen_notify: bool,
    reader_processes: int,
    fanout: int,
    reader_pool_size: int,
    reader_executor_threads: int,
    read_drain_timeout: float,
    role: str,
) -> None:
    # Pin DB hostnames to IPs before any connection is opened, so the many
    # worker processes don't storm the local DNS resolver (see the function).
    pin_db_hosts_to_ip()

    # Pick the window up front, before any setup, so split writer/reader hosts
    # compute it at ~the same moment and agree. Split roles snap to a wall-clock
    # minute boundary (no timestamps to pass around); a single host just waits
    # out the grace. Either way the grace must cover process spawn + DBOS launch.
    if role == "all":
        start_epoch = time.time() + start_grace
    else:
        start_epoch = next_minute_start(start_grace)
    lead = start_epoch - time.time()
    print(
        f"Sync start:        {time.strftime('%H:%M:%S', time.localtime(start_epoch))}"
        f"   (in {lead:.1f}s)"
        + ("   <- both hosts must print this time" if role != "all" else ""),
        flush=True,
    )

    ctx = mp.get_context("spawn")

    # The writer host owns the data: it recreates the database and pre-creates
    # the schema. A reader host must never do this (it would drop the writers'
    # data mid-run), so it waits for the schema to appear instead.
    if role == "reader":
        asyncio.run(wait_for_schema())
    else:
        asyncio.run(recreate_database())
        # Pre-create the DBOS schema in a single child so workers don't serialize
        # on the migration advisory lock. This also fixes whether the streams
        # pg_notify trigger exists, so workers must match use_listen_notify.
        bootstrap = ctx.Process(
            target=bootstrap_schema_entry, args=(use_listen_notify,)
        )
        bootstrap.start()
        bootstrap.join()

    total_streams = processes * streams_per_worker
    per_producer_rps = (total_rps / total_streams) if total_rps > 0 else 0.0
    readers_on = reader_processes > 0 and fanout > 0
    # Producers embed a write timestamp in each value only when readers will
    # consume it (to measure end-to-end latency) — it costs work on the hot path.
    # This keys off the args, not the role, so a writer-only host still embeds
    # timestamps when readers are running on another host.
    embed_timestamp = readers_on

    # role decides what runs *here*; the topology args must match on both hosts
    # so the reader host computes the same stream identities as the writers.
    run_producers = role in ("all", "writer")
    run_readers = readers_on and role in ("all", "reader")

    result_queue: mp.Queue = ctx.Queue()
    read_result_queue: mp.Queue = ctx.Queue()
    n_producers = processes if run_producers else 0
    n_readers = reader_processes if run_readers else 0

    workers = []
    for worker_id in range(n_producers):
        p = ctx.Process(
            target=worker_entry,
            args=(
                worker_id,
                streams_per_worker,
                start_epoch,
                duration_s,
                per_producer_rps,
                jitter,
                payload_size,
                pool_size,
                executor_threads,
                sample_rate,
                use_listen_notify,
                embed_timestamp,
                result_queue,
            ),
        )
        p.start()
        workers.append(p)

    readers = []
    for reader_id in range(n_readers):
        p = ctx.Process(
            target=reader_entry,
            args=(
                reader_id,
                n_readers,
                processes,
                streams_per_worker,
                fanout,
                start_epoch,
                duration_s,
                read_drain_timeout,
                reader_pool_size,
                reader_executor_threads,
                sample_rate,
                use_listen_notify,
                read_result_queue,
            ),
        )
        p.start()
        readers.append(p)

    results = [result_queue.get() for _ in workers]
    read_results = [read_result_queue.get() for _ in readers]
    for p in workers:
        p.join()
    for p in readers:
        p.join()

    written = sum(r["count"] for r in results)
    failures = sum(r["failures"] for r in results)
    firsts = [r["first_write"] for r in results if r["first_write"] > 0]
    lasts = [r["last_write"] for r in results if r["last_write"] > 0]
    first_write = min(firsts) if firsts else start_epoch
    last_write = max(lasts) if lasts else start_epoch
    write_span = last_write - first_write

    # Divide by the actual write span (first write -> last write), not the
    # nominal window: in paced mode the loop stops on count, so writers that
    # can't hold the target spill past the window and dividing by the window
    # would overstate the sustained rate. In flat-out the span ~= window.
    writes_per_sec = written / write_span if write_span > 0 else 0.0
    late_start = first_write - start_epoch  # >0 means launch overran start-grace

    all_latencies: list[float] = []
    for r in results:
        all_latencies.extend(r["latencies"])

    row_count = asyncio.run(count_stream_rows())

    print(
        f"Role:              {role}   (producers here: {run_producers}, readers here: {run_readers})"
    )
    print(f"Processes:         {processes}")
    print(f"Streams/worker:    {streams_per_worker}")
    print(f"Total streams:     {total_streams}")
    print(f"Payload size:      {payload_size} bytes")
    print(f"Mode:              {'paced' if total_rps > 0 else 'flat-out'}")
    if total_rps > 0:
        print(f"Target RPS:        {total_rps}  ({per_producer_rps:.1f}/stream)")
        print(f"Jitter:            {jitter:.2f}   (fraction of interval, 0 = lockstep)")
    print(
        f"LISTEN/NOTIFY:     {'on' if use_listen_notify else 'off (no write trigger)'}"
    )
    print(f"Window:            {duration_s:.2f}s")

    if run_producers:
        print(
            f"Pool size/proc:    {pool_size}   (producer DB conns ~= {processes * pool_size})"
        )
        print(f"Exec threads/proc: {executor_threads}")
        span_note = (
            "   <- >window: writers behind target"
            if write_span > duration_s * 1.05
            else ""
        )
        print(f"Write span:        {write_span:.2f}s{span_note}")
        print(f"Late start:        {late_start:.2f}s   (>~0.5s: raise --start-grace)")
        print(f"Writes:            {written}")
        print(f"Write failures:    {failures}")
        print(f"streams rows:      {row_count}   (== writes: {row_count == written})")
        print(f"Writes/sec:        {writes_per_sec:.0f}   (aggregate, over write span)")
    else:
        # Reader-only host: writers ran elsewhere, so the streams table is the
        # only view of what was written.
        print(f"streams rows:      {row_count}   (written by the --role writer host)")
    if all_latencies:
        print(
            f"Write latency:     samples={len(all_latencies)}   "
            f"p50={percentile(all_latencies, 50)*1000:.2f}ms "
            f"p95={percentile(all_latencies, 95)*1000:.2f}ms "
            f"p99={percentile(all_latencies, 99)*1000:.2f}ms "
            f"max={max(all_latencies)*1000:.2f}ms"
        )

    if run_readers:
        read = sum(r["count"] for r in read_results)
        read_failures = sum(r["failed"] for r in read_results)
        r_firsts = [r["first_read"] for r in read_results if r["first_read"] > 0]
        r_lasts = [r["last_read"] for r in read_results if r["last_read"] > 0]
        read_span = (max(r_lasts) - min(r_firsts)) if r_firsts and r_lasts else 0.0
        reads_per_sec = read / read_span if read_span > 0 else 0.0
        # Use the streams table as the write count so a reader-only host (whose
        # writers live on another host) can still compute coverage.
        expected = row_count * fanout  # each written value read once per fanout reader
        coverage = read / expected if expected > 0 else 0.0
        e2e: list[float] = []
        for r in read_results:
            e2e.extend(r["latencies"])
        reader_conns = reader_processes * reader_pool_size

        print(f"Reader procs:      {reader_processes}")
        print(f"Fanout:            {fanout}   (readers/stream)")
        print(
            f"Reader pool/proc:  {reader_pool_size}   (reader DB conns ~= {reader_conns})"
        )
        print(f"Reads:             {read}")
        print(f"Read failures:     {read_failures}")
        print(
            f"Coverage:          {coverage:.3f}   (reads / writes*fanout; 1.0 = all delivered)"
        )
        print(f"Read span:         {read_span:.2f}s")
        print(f"Reads/sec:         {reads_per_sec:.0f}   (aggregate, over read span)")
        if e2e:
            print(
                f"E2E latency:       samples={len(e2e)}   "
                f"p50={percentile(e2e, 50)*1000:.2f}ms "
                f"p95={percentile(e2e, 95)*1000:.2f}ms "
                f"p99={percentile(e2e, 99)*1000:.2f}ms "
                f"max={max(e2e)*1000:.2f}ms   (write commit -> read delivery)"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--role",
        choices=("all", "writer", "reader"),
        default="all",
        help="What to run on this host: 'all' (default, writers+readers here), "
        "'writer' (producers only; recreates the DB and schema), or 'reader' "
        "(readers only; waits for the writer host's schema). Split roles start "
        "at the next wall-clock minute boundary, so two hosts sync with no "
        "timestamps -- just launch both in the same window with identical args",
    )
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
        "--jitter",
        type=float,
        default=1.0,
        help="Paced mode: randomize each producer's phase over this fraction of "
        "the write interval so they don't fire in lockstep (default 1.0 = full "
        "interval; 0 = synchronized). No effect in flat-out mode",
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
        help="DBOS system DB pool size per process, applied to producers AND "
        "readers (0 = auto: producers streams-per-worker+4, readers "
        "consumers-per-reader+4)",
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
        help="Fraction of writes/reads sampled for latency (default 0.01). Every "
        "sampled value is kept, so percentiles cover the whole run uniformly; "
        "this is the only control on sample volume. Each producer/consumer "
        "returns its samples through the workflow result, so keep it low on long "
        "high-throughput runs",
    )
    parser.add_argument(
        "--start-grace",
        type=float,
        default=20.0,
        help="Lead time before the window opens; must cover process spawn + "
        "DBOS launch (default 20). For --role all this is the wait outright; "
        "for writer/reader it is the minimum lead, and minute boundaries closer "
        "than this are skipped. Check 'Late start' in the output if too small",
    )
    parser.add_argument(
        "--listen-notify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use LISTEN/NOTIFY (default on). --no-listen-notify drops the "
        "streams pg_notify trigger to isolate raw insert throughput",
    )
    parser.add_argument(
        "--reader-processes",
        type=int,
        default=0,
        help="Number of separate reader processes (0 = writers only, the default)",
    )
    parser.add_argument(
        "--fanout",
        type=int,
        default=1,
        help="Readers per stream (default 1). Read QPS = fanout * write QPS",
    )
    parser.add_argument(
        "--reader-executor-threads",
        type=int,
        default=0,
        help="DBOS max_executor_threads per reader process "
        "(0 = auto: max(64, consumers-per-reader * 2))",
    )
    parser.add_argument(
        "--read-drain-timeout",
        type=float,
        default=60.0,
        help="Extra seconds past the window for readers to drain backlog (default 60)",
    )
    args = parser.parse_args()

    if args.role == "reader" and not (args.reader_processes > 0 and args.fanout > 0):
        parser.error("--role reader needs --reader-processes > 0 and --fanout > 0")

    readers_on = args.reader_processes > 0 and args.fanout > 0
    total_consumers = args.processes * args.streams_per_worker * args.fanout
    consumers_per_reader = (
        -(-total_consumers // args.reader_processes) if readers_on else 0
    )

    # --pool-size applies to both producer and reader processes: when set (>0)
    # both use that value; when auto (0) each role is sized to its own
    # concurrency (producers to streams-per-worker, readers to consumers-per-reader).
    if args.pool_size > 0:
        pool_size = args.pool_size
        reader_pool_size = args.pool_size
    else:
        pool_size = args.streams_per_worker + 4
        reader_pool_size = consumers_per_reader + 4

    executor_threads = (
        args.executor_threads
        if args.executor_threads > 0
        else max(64, args.streams_per_worker * 2)
    )
    reader_executor_threads = (
        args.reader_executor_threads
        if args.reader_executor_threads > 0
        else max(64, consumers_per_reader * 2)
    )

    run_multiprocess(
        args.processes,
        args.streams_per_worker,
        args.duration,
        args.rps,
        args.jitter,
        args.payload_size,
        pool_size,
        executor_threads,
        args.sample_rate,
        args.start_grace,
        args.listen_notify,
        args.reader_processes,
        args.fanout,
        reader_pool_size,
        reader_executor_threads,
        args.read_drain_timeout,
        args.role,
    )


if __name__ == "__main__":
    main()
