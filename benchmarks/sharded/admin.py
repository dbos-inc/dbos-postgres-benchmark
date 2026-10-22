"""Shard administration for the sharded benchmark, run by orchestrate.py on the control host.

Talks to each shard directly, bypassing PgDog, except for bootstrap, which runs DBOS's
migrations through PgDog so they reach every shard the way the application sees them.

  reset      Drop and recreate the benchmark database on every shard. PgDog must be
             stopped first: it would otherwise hold connections to the old database.
  bootstrap  Launch DBOS once against the router, so the workers that launch next do
             not serialize on the migration lock. Find one shard key per shard and
             register the benchmark's queue; prints
             ``RESULT {"queue": name, "keys": [...]}``, keys in shard order.
  window     Sleep until --start, snapshot every shard, sleep until --end, snapshot
             again, and print the per-shard difference as ``RESULT {json}``.
  status     Workflows each shard created and completed over the last --window
             seconds, and how many wait and run now, as ``RESULT [json]``. Indexed
             reads only, so it can run during a measurement.

Shards come from --shards, a JSON list of {"admin_host", "admin_port"}.
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import psycopg

BENCH_DB = "benchmark"
# The one queue every process dequeues from, spanning every shard key.
QUEUE_NAME = "sharded_bench"


def shard_url(shard: dict, user: str, password: str, db: str) -> str:
    return f"postgresql://{user}:{password}@{shard['admin_host']}:{shard['admin_port']}/{db}"


def reset_shard(shard: dict, user: str, password: str) -> None:
    with psycopg.connect(
        shard_url(shard, user, password, "postgres"), autocommit=True
    ) as c:
        c.execute(f'DROP DATABASE IF EXISTS "{BENCH_DB}" WITH (FORCE)')
        c.execute(f'CREATE DATABASE "{BENCH_DB}"')
    # pg_stat_statements is preloaded server-wide but the extension is per database.
    with psycopg.connect(
        shard_url(shard, user, password, BENCH_DB), autocommit=True
    ) as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")


PROBE_TABLE = "public.dbos_bench_shard_probe"


def shard_keys(router_url: str, shards: list, user: str, password: str) -> list[str]:
    """One shard key per shard, in shard order, that PgDog routes to that shard. Rather
    than reimplement PgDog's hash, route candidates through it into a scratch table
    and read back where each landed."""
    names: list[Optional[str]] = [None] * len(shards)
    tried = 0
    with psycopg.connect(router_url, autocommit=True) as router:
        # DDL through the router reaches every shard.
        router.execute(f"DROP TABLE IF EXISTS {PROBE_TABLE}")
        router.execute(f"CREATE TABLE {PROBE_TABLE} (shard_key varchar PRIMARY KEY)")
        while None in names:
            if tried >= 1000 * len(shards):
                raise RuntimeError("could not find a shard key for every shard")
            for k in range(tried, tried + 4 * len(shards)):
                router.execute(
                    f"INSERT INTO {PROBE_TABLE} (shard_key) VALUES (%s)", (f"k{k}",)
                )
            tried += 4 * len(shards)
            for i, shard in enumerate(shards):
                if names[i] is not None:
                    continue
                url = shard_url(shard, user, password, BENCH_DB)
                with psycopg.connect(url, autocommit=True) as c:
                    landed = [
                        r[0] for r in c.execute(f"SELECT shard_key FROM {PROBE_TABLE}")
                    ]
                if landed:
                    # The earliest candidate, so a shard gets the same name every run.
                    names[i] = min(landed, key=lambda name: int(name[1:]))
        router.execute(f"DROP TABLE {PROBE_TABLE}")
    return [name for name in names if name is not None]


def bootstrap(
    router_url: str, worker_concurrency: int, polling_interval: float
) -> None:
    from dbos import DBOS, DBOSConfig

    config: DBOSConfig = {
        "name": "dbos-bench-sharded",
        "system_database_url": router_url,
        "sharding": True,
        "run_admin_server": False,
        "enable_otlp": False,
        "sys_db_pool_size": 3,
        "transact_enterprise_key": os.environ["DBOS_TRANSACT_ENTERPRISE_KEY"],
    }
    DBOS(config=config)
    DBOS.launch()
    # A queue's configuration lives in the system database, so one registration serves
    # every process.
    DBOS.register_queue(
        QUEUE_NAME,
        worker_concurrency=worker_concurrency,
        polling_interval_sec=polling_interval,
    )
    DBOS.destroy()


def snapshot(shard: dict, user: str, password: str) -> dict[str, Any]:
    """Cumulative counters on one shard; window() diffs two of them."""
    with psycopg.connect(
        shard_url(shard, user, password, BENCH_DB), autocommit=True
    ) as c:
        db = c.execute(
            "SELECT xact_commit, xact_rollback, tup_inserted, tup_updated, tup_fetched, "
            "blks_hit, blks_read FROM pg_stat_database WHERE datname = current_database()"
        ).fetchone()
        tables = {
            r[0]: {"ins": r[1], "upd": r[2], "live": r[3], "dead": r[4]}
            for r in c.execute(
                "SELECT relname, n_tup_ins, n_tup_upd, n_live_tup, n_dead_tup "
                "FROM pg_stat_user_tables WHERE schemaname = 'dbos'"
            )
        }
        statements = {
            str(r[0]): {"query": r[1], "calls": r[2], "ms": r[3]}
            for r in c.execute(
                "SELECT queryid, left(regexp_replace(query, '\\s+', ' ', 'g'), 200), "
                "calls, total_exec_time FROM pg_stat_statements "
                "WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())"
            )
        }
        connections = c.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
        ).fetchone()[0]
    keys = ("commits", "rollbacks", "tup_inserted", "tup_updated", "tup_fetched")
    return {
        **dict(zip(keys, db[:5])),
        "blks_hit": db[5],
        "blks_read": db[6],
        "tables": tables,
        "statements": statements,
        "connections": connections,
        "t": time.time(),
    }


def diff(a: dict[str, Any], b: dict[str, Any], top: int) -> dict[str, Any]:
    elapsed = b["t"] - a["t"]
    out: dict[str, Any] = {"elapsed": elapsed, "connections": b["connections"]}
    for k in ("commits", "rollbacks", "tup_inserted", "tup_updated", "tup_fetched"):
        out[f"{k}_per_s"] = (b[k] - a[k]) / elapsed
    hits = b["blks_hit"] - a["blks_hit"]
    reads = b["blks_read"] - a["blks_read"]
    out["cache_hit_pct"] = 100 * hits / max(1, hits + reads)
    ws_a = a["tables"].get("workflow_status", {"ins": 0})
    ws_b = b["tables"].get("workflow_status", {"ins": 0, "live": 0, "dead": 0})
    out["workflows_per_s"] = (ws_b["ins"] - ws_a["ins"]) / elapsed
    out["workflow_status_live"] = ws_b.get("live", 0)
    out["workflow_status_dead"] = ws_b.get("dead", 0)
    stmts = []
    for qid, s in b["statements"].items():
        prev = a["statements"].get(qid, {"calls": 0, "ms": 0.0})
        calls = s["calls"] - prev["calls"]
        if calls > 0:
            stmts.append(
                {
                    "queryid": qid,
                    "query": s["query"],
                    "calls_per_s": calls / elapsed,
                    "mean_ms": (s["ms"] - prev["ms"]) / calls,
                    "exec_ms_per_s": (s["ms"] - prev["ms"]) / elapsed,
                }
            )
    stmts.sort(key=lambda s: -s["exec_ms_per_s"])
    out["statements_per_s"] = sum(s["calls_per_s"] for s in stmts)
    out["exec_ms_per_s"] = sum(s["exec_ms_per_s"] for s in stmts)
    out["top_statements"] = stmts[:top]
    return out


def window(
    shards: list, user: str, password: str, start: float, end: float, top: int
) -> list:
    with ThreadPoolExecutor(max_workers=max(1, len(shards))) as pool:

        def all_snapshots() -> list:
            return list(pool.map(lambda s: snapshot(s, user, password), shards))

        time.sleep(max(0.0, start - time.time()))
        before = all_snapshots()
        time.sleep(max(0.0, end - time.time()))
        after = all_snapshots()
        outcomes = list(
            pool.map(lambda s: count_outcomes(s, user, password, start, end), shards)
        )
    results = [diff(a, b, top) for a, b in zip(before, after)]
    for result, n in zip(results, outcomes):
        result["outcomes_per_s"] = n / (end - start)
    return results


def count_outcomes(
    shard: dict, user: str, password: str, start: float, end: float
) -> int:
    """Workflows whose outcome the shard recorded within [start, end): completed_at is
    set by the outcome write itself, so unlike the per-table counters, which backends
    flush up to a second late, this is exact."""
    with psycopg.connect(
        shard_url(shard, user, password, BENCH_DB), autocommit=True
    ) as c:
        return c.execute(
            "SELECT count(*) FROM dbos.workflow_status "
            "WHERE completed_at >= %s AND completed_at < %s",
            (int(start * 1000), int(end * 1000)),
        ).fetchone()[0]


def shard_status(shard: dict, user: str, password: str, window_s: float) -> dict:
    """One shard's recent rates and current backlog. The rate window ends a second ago,
    on the shard's clock, which also stamps created_at and completed_at, so outcome
    writes still in flight at its end are not missed."""
    try:
        with psycopg.connect(
            shard_url(shard, user, password, BENCH_DB),
            autocommit=True,
            connect_timeout=5,
        ) as c:
            hi = c.execute(
                "SELECT (extract(epoch FROM now()) * 1000)::bigint - 1000"
            ).fetchone()[0]
            lo = hi - int(window_s * 1000)
            created, completed, waiting, running = c.execute(
                "SELECT (SELECT count(*) FROM dbos.workflow_status "
                "        WHERE created_at >= %(lo)s AND created_at < %(hi)s), "
                "       (SELECT count(*) FROM dbos.workflow_status "
                "        WHERE completed_at >= %(lo)s AND completed_at < %(hi)s), "
                "       (SELECT count(*) FROM dbos.workflow_status WHERE status = 'ENQUEUED'), "
                "       (SELECT count(*) FROM dbos.workflow_status WHERE status = 'PENDING')",
                {"lo": lo, "hi": hi},
            ).fetchone()
    except psycopg.Error as e:
        # A shard past the running point's count has no benchmark database, or no tables.
        return {"error": str(e).strip().splitlines()[0]}
    return {
        "created_per_s": created / window_s,
        "completed_per_s": completed / window_s,
        "waiting": waiting,
        "running": running,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["reset", "bootstrap", "window", "status"])
    parser.add_argument("--shards", help="JSON list of shards")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD", ""))
    parser.add_argument("--router-url", help="System database URL through PgDog")
    parser.add_argument("--start", type=float)
    parser.add_argument("--end", type=float)
    parser.add_argument("--top", type=int, default=12)
    parser.add_argument(
        "--window", type=float, default=10.0, help="status: seconds the rates cover"
    )
    parser.add_argument(
        "--worker-concurrency",
        type=int,
        default=1000,
        help="Per-process limit of loadgen's queue",
    )
    parser.add_argument(
        "--polling-interval",
        type=float,
        default=0.5,
        help="Seconds between a process's polls of loadgen's queue",
    )
    args = parser.parse_args()

    shards = json.loads(args.shards)
    if args.command == "bootstrap":
        keys = shard_keys(args.router_url, shards, args.user, args.password)
        bootstrap(args.router_url, args.worker_concurrency, args.polling_interval)
        print("RESULT " + json.dumps({"queue": QUEUE_NAME, "keys": keys}), flush=True)
        return
    if args.command == "status":
        with ThreadPoolExecutor(max_workers=len(shards)) as pool:
            result = list(
                pool.map(
                    lambda s: shard_status(s, args.user, args.password, args.window),
                    shards,
                )
            )
        print("RESULT " + json.dumps(result), flush=True)
        return
    if args.command == "reset":
        with ThreadPoolExecutor(max_workers=len(shards)) as pool:
            list(pool.map(lambda s: reset_shard(s, args.user, args.password), shards))
        return
    result = window(shards, args.user, args.password, args.start, args.end, args.top)
    print("RESULT " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
