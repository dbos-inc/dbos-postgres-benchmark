"""Drive the sharded DBOS benchmark: sweep the shard count and measure how throughput scales.

Run from your machine. Hosts and shards come from an inventory: the terraform/sharded
stack's `inventory` output (--terraform-dir), or a JSON file (--inventory) such as
local-inventory.json, which runs everything on this machine against
docker-compose.yml's four shards.

Provision once for the largest shard count; each point of the sweep uses the first N
shards and every client host, with --processes DBOS processes in total spread evenly
over the hosts. For each point:

  1. Stop PgDog on every client, then drop and recreate the benchmark database on
     the N shards, so every point starts from an empty system database.
  2. Write a PgDog config naming those N shards to every client and start
     PgDog there. PgDog runs as a sidecar on every client host: it holds no shared
     state (the shard map is static config), so the routing tier scales out with the
     clients instead of becoming a tier of its own to size.
  3. Launch DBOS once through PgDog, which runs migrations on every shard.
  4. Start loadgen.py on every client at one shared wall-clock time, offering
     N * --rps-per-shard workflows per second in total, each host a share in
     proportion to its processes, while admin.py snapshots
     every shard over the steady-state window.
  5. Write the point's merged results to --out, and print a summary line.

`orchestrate.py status` reports a running benchmark from another terminal: each
shard's created and completed workflows/s over the last few seconds, how many wait and
run, and each client's CPU, PgDog cores, and position in the run.

The window is [--warmup, --duration) seconds after the shared start. Linear scaling
means completions/s per shard and latency stay flat as N grows. Two ways to sweep:

  * Fixed load per shard, below single-shard capacity (say 75% of it): every point
    should keep up with its offered load at the same latency.
  * Load per shard above capacity: completions/s per shard is then each point's
    capacity, and should not fall as N grows.
"""

import argparse
import json
import math
import os
import shlex
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from loadgen import hist_percentile  # noqa: E402

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
# Pinned: a new release can change routing. At least v0.1.61: earlier releases send every
# explicit transaction's BEGIN and COMMIT to every shard, even when its statements reach
# one, so per-shard work grows with the shard count (pgdogdev/pgdog#1623).
PGDOG_IMAGE = (
    "ghcr.io/pgdogdev/pgdog:v0.1.62"
    "@sha256:956980d35a2358ae670b411359f971b02fac60c279527a4e998de91cbce595f7"
)
PGDOG_CONTAINER = "dbos-bench-pgdog"
BENCH_DB = "benchmark"
SSH_OPTS = [
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=30",
    "-o", "BatchMode=yes",
]  # fmt: skip
# Sharding is an enterprise feature, so DBOS needs a license key. Each host keeps it in a
# file, read into the environment of the commands that launch DBOS: on a command line,
# ps and this script's error messages would show it.
KEY_ENV = "DBOS_TRANSACT_ENTERPRISE_KEY"
KEY_FILE = ".dbos_enterprise_key"
WITH_KEY = f'{KEY_ENV}="$(cat {KEY_FILE})"'

T = TypeVar("T")


def clock(t: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(t))


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Host:
    """A client host: this machine, or one reached over SSH."""

    def __init__(self, spec: dict, workdir: str) -> None:
        self.ssh: Optional[str] = spec.get("ssh")
        self.workdir = workdir
        self.name = self.ssh or "local"

    def run(
        self, cmd: str, input: Optional[str] = None, check: bool = True, cd: bool = True
    ) -> str:
        if cd:
            cmd = f"cd {self.workdir} && {cmd}"
        argv = ["ssh", *SSH_OPTS, self.ssh, cmd] if self.ssh else ["bash", "-c", cmd]
        p = subprocess.run(
            argv,
            input=input,
            capture_output=True,
            text=True,
            cwd=None if self.ssh else REPO,
        )
        if check and p.returncode != 0:
            raise RuntimeError(
                f"{self.name}: `{cmd[:200]}` exited {p.returncode}\n"
                f"{p.stdout[-3000:]}\n{p.stderr[-3000:]}"
            )
        return p.stdout

    def write(self, path: str, content: str) -> None:
        self.run(f"mkdir -p $(dirname {path}) && cat > {path}", input=content)

    def push(self, local_paths: list[str], remote_dir: str) -> None:
        assert self.ssh, "push is for remote hosts"
        self.run(f"mkdir -p {remote_dir}")
        subprocess.run(
            ["rsync", "-az", "-e", " ".join(["ssh", *SSH_OPTS]), *local_paths,
             f"{self.ssh}:{self.workdir}/{remote_dir}/"],
            check=True,
        )  # fmt: skip


def parallel(fns: list[Callable[[], T]]) -> list[T]:
    with ThreadPoolExecutor(max_workers=max(1, len(fns))) as pool:
        return list(pool.map(lambda f: f(), fns))


def result_line(output: str) -> Any:
    for line in reversed(output.splitlines()):
        if line.startswith("RESULT "):
            return json.loads(line[len("RESULT ") :])
    raise RuntimeError(f"no RESULT line in output:\n{output[-3000:]}")


# ── Inventory ───────────────────────────────────────────────


def load_inventory(args: argparse.Namespace) -> dict:
    if args.inventory:
        return json.loads(Path(args.inventory).read_text())
    out = subprocess.run(
        ["terraform", f"-chdir={args.terraform_dir}", "output", "-json", "inventory"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return json.loads(out)


# ── PgDog ───────────────────────────────────────────────────


def pgdog_configs(
    inv: dict, shards: list[dict], args: argparse.Namespace, pool_size: int
) -> tuple[str, str]:
    general = f"""[general]
host = "0.0.0.0"
port = 6432
workers = {args.pgdog_workers}
default_pool_size = {pool_size}
min_pool_size = 1
pooler_mode = "transaction"
prepared_statements = "extended"
# "auto" skips parsing with one shard, which would make the one-shard baseline cheaper
# than every other point; parse always, so each point pays the same routing cost.
query_parser = "on"
openmetrics_port = 9090
# SHOW QUERY_CACHE is how the benchmark measures routing; the default keeps only 1000.
query_cache_limit = 1000000
log_connections = false
log_disconnections = false
"""
    databases = "".join(f"""
[[databases]]
name = "{BENCH_DB}"
host = "{s['host']}"
port = {s['port']}
database_name = "{BENCH_DB}"
shard = {i}
""" for i, s in enumerate(shards))
    sharded = f"""
# Every DBOS table carries shard_key, so one column-wide entry routes them all.
[[sharded_tables]]
database = "{BENCH_DB}"
column = "shard_key"
data_type = "varchar"

[admin]
password = "{inv['password']}"
"""
    users = f"""[[users]]
name = "{inv['user']}"
database = "{BENCH_DB}"
password = "{inv['password']}"
"""
    return general + databases + sharded, users


def kill_strays(host: Host) -> None:
    """Kill benchmark processes left from an earlier run, as an interrupted SSH session
    leaves its remote command running. Their workers exit within a second once their
    parent is gone (loadgen's watchdog). The pattern needs the interpreter, so it spares
    editors naming the scripts, and its brackets keep pkill off its own shell."""
    host.run(
        "pkill -9 -f '[p]ython[0-9.]* benchmarks/sharded/(loadgen|admin)[.]py' "
        "&& sleep 2; true"
    )


def reset_hosts(hosts: list[Host]) -> None:
    parallel([lambda h=h: (kill_strays(h), stop_pgdog(h)) for h in hosts])


def stop_pgdog(host: Host) -> None:
    host.run(f"docker rm -f {PGDOG_CONTAINER} >/dev/null 2>&1 || true")


def start_pgdog(host: Host, inv: dict, toml: str, users: str) -> None:
    cfg = "$PWD/.pgdog"
    host.write(".pgdog/pgdog.toml", toml)
    host.write(".pgdog/users.toml", users)
    host.run(
        f"docker run -d --name {PGDOG_CONTAINER} --ulimit nofile=1048576:1048576 "
        f"{inv['pgdog_docker_args']} "
        f"-v {cfg}/pgdog.toml:/pgdog/pgdog.toml:ro -v {cfg}/users.toml:/pgdog/users.toml:ro "
        f"{inv.get('pgdog_image', PGDOG_IMAGE)} >/dev/null"
    )
    port = inv["router_port"]
    host.run(
        f"for i in $(seq 60); do PGPASSWORD={shlex.quote(inv['password'])} psql -qtA "
        f"-h 127.0.0.1 -p {port} -U {inv['user']} -d {BENCH_DB} -c 'SELECT 1' "
        f">/dev/null 2>&1 && exit 0; sleep 1; done; "
        f"docker logs --tail 50 {PGDOG_CONTAINER}; exit 1"
    )


def router_url(inv: dict) -> str:
    return (
        f"postgresql://{inv['user']}:{inv['password']}@127.0.0.1:"
        f"{inv['router_port']}/{BENCH_DB}"
    )


def router_admin_url(inv: dict) -> str:
    return f"postgresql://admin:{inv['password']}@127.0.0.1:{inv['router_port']}/admin"


# ── Sync ────────────────────────────────────────────────────


def sync(hosts: list[Host], inv: dict, args: argparse.Namespace) -> dict[str, str]:
    """Install core and enterprise, with the benchmark scripts, on every remote host:
    each built into a wheel from its local checkout if there is one, else its latest
    release. Returns the versions installed."""
    wheels = Path(tempfile.mkdtemp(prefix="dbos-wheels-"))
    releases: list[str] = []
    for repo, package in (
        (args.core_dir, "dbos"),
        (args.enterprise_dir, "dbos-enterprise"),
    ):
        if Path(repo).is_dir():
            log(f"building wheel from {repo}")
            subprocess.run(
                ["uv", "build", "--wheel", "--out-dir", str(wheels), repo],
                check=True,
                capture_output=True,
            )
        else:
            log(f"no checkout at {repo}; installing the latest {package} release")
            releases.append(package)
    built = sorted(wheels.glob("*.whl"))
    versions = {w.name.split("-")[0]: w.name.split("-")[1] for w in built}
    scripts = [str(HERE / "loadgen.py"), str(HERE / "admin.py")]
    image = inv.get("pgdog_image", PGDOG_IMAGE)
    install = [f"wheels/{w.name}" for w in built] + releases + ["psycopg[binary]"]

    def one(h: Host) -> None:
        # A fresh host may still be installing uv and Docker.
        h.run(f"cloud-init status --wait >/dev/null; mkdir -p {h.workdir}", cd=False)
        h.run("rm -rf wheels")
        if built:
            h.push([str(w) for w in built], "wheels")
        h.push(scripts, "benchmarks/sharded")
        h.run(
            "uv venv --allow-existing --python 3.14 venv >/dev/null 2>&1 && "
            "uv pip install --python venv/bin/python --reinstall -q "
            + shlex.join(install)
        )
        h.run(f"docker pull -q {image} >/dev/null")

    remote = [h for h in hosts if h.ssh]
    log(f"syncing {len(remote)} hosts")
    parallel([lambda h=h: one(h) for h in remote])
    if releases:
        # Named as the wheels are, so a result says the same whichever was installed.
        report = (
            "import importlib.metadata as m, sys; "
            "print(*(m.version(p) for p in sys.argv[1:]))"
        )
        found = remote[0].run(
            f"venv/bin/python -c {shlex.quote(report)} {shlex.join(releases)}"
        )
        versions.update(zip((p.replace("-", "_") for p in releases), found.split()))
    return versions


def git_head(repo: str) -> str:
    try:
        sha = subprocess.run(
            ["git", "-C", repo, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()  # fmt: skip
        dirty = subprocess.run(
            ["git", "-C", repo, "status", "--porcelain"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()  # fmt: skip
        return sha + ("-dirty" if dirty else "")
    except Exception:
        return "unknown"


# ── One point of the sweep ──────────────────────────────────


def run_point(
    inv: dict,
    hosts: list[Host],
    n: int,
    args: argparse.Namespace,
    rids: dict[str, str],
) -> dict[str, Any]:
    if n > len(inv["shards"]):
        raise SystemExit(
            f"{n} shards requested; the inventory has {len(inv['shards'])}"
        )
    shards = inv["shards"][:n]
    clients = hosts
    control = clients[0]
    total_rps = n * args.rps_per_shard
    procs = spread(args.processes, len(clients))
    py = inv["python"]
    password = shlex.quote(inv["password"])
    shards_arg = shlex.quote(json.dumps(shards))

    log(
        f"--- {n} shards, {len(clients)} clients, {args.processes} processes, "
        f"{total_rps:.0f} workflows/s offered"
    )
    reset_hosts(hosts)
    control.run(
        f"PGPASSWORD={password} {py} benchmarks/sharded/admin.py reset "
        f"--user {inv['user']} --shards {shards_arg}"
    )
    # A shard's connections come from every client's PgDog, so its budget is split
    # among them: a per-PgDog size would grow past max_connections with the fleet.
    pool_size = math.ceil(args.shard_connections / len(clients))
    toml, users = pgdog_configs(inv, shards, args, pool_size)
    # A PgDog started before the migrations treats DBOS's tables as unsharded and
    # refuses every write carrying a routing comment ("cannot write to an omnisharded
    # table with a shard directive"), so the enqueues through it all fail; the PgDog
    # that ran the migrations does not. So migrate through the control host's, then
    # restart every PgDog on the finished schema.
    start_pgdog(control, inv, toml, users)
    url = router_url(inv) if not args.direct else direct_url(inv)
    boot = result_line(
        control.run(
            f"PGPASSWORD={password} {WITH_KEY} {py} benchmarks/sharded/admin.py bootstrap "
            f"--router-url {shlex.quote(url)} --user {inv['user']} --shards {shards_arg} "
            f"--worker-concurrency {args.worker_concurrency} "
            f"--polling-interval {args.polling_interval}"
        )
    )
    parallel(
        [lambda h=h: (stop_pgdog(h), start_pgdog(h, inv, toml, users)) for h in clients]
    )
    log(f"queue {boot['queue']}; shard keys, one per shard: {', '.join(boot['keys'])}")

    start_at = time.time() + args.lead
    loadgen_args = [
        "--url", url,
        "--start-at", f"{start_at:.3f}",
        "--duration", str(args.duration),
        "--warmup", str(args.warmup),
        "--drain", str(args.drain),
        "--steps", str(args.steps),
        "--queue", boot["queue"],
        "--keys", ",".join(boot["keys"]),
        "--pool-size", str(args.pool_size),
        "--executor-threads", str(args.executor_threads),
        "--enqueue-interval", str(args.enqueue_interval),
        "--enqueue-connections", str(args.enqueue_connections),
    ]  # fmt: skip
    if not args.direct:
        loadgen_args += ["--router-admin-url", router_admin_url(inv)]

    def loadgen_cmd(host_procs: int) -> str:
        # Each host's rate is its share of the processes, so every process runs alike.
        host_args = loadgen_args + [
            "--processes", str(host_procs),
            "--rps", str(total_rps * host_procs / args.processes),
        ]  # fmt: skip
        return f"{WITH_KEY} {py} benchmarks/sharded/loadgen.py " + shlex.join(host_args)

    window_cmd = (
        f"PGPASSWORD={password} {py} benchmarks/sharded/admin.py window "
        f"--user {inv['user']} --shards {shards_arg} "
        f"--start {start_at + args.warmup:.3f} --end {start_at + args.duration:.3f}"
    )
    log(
        f"load runs {clock(start_at)}-{clock(start_at + args.duration)}, "
        f"then up to {args.drain:.0f}s drain; results by ~"
        f"{clock(start_at + args.duration + args.drain + 15)} (nothing prints until then)"
    )
    outputs = parallel(
        [
            lambda h=h, k=k: result_line(h.run(loadgen_cmd(k)))
            for h, k in zip(clients, procs)
        ]
        + [lambda: result_line(control.run(window_cmd))]
    )
    host_results, shard_results = outputs[:-1], outputs[-1]
    missing = sum(h.get("workers_missing", 0) for h in host_results)
    failures = sum(h["start_failures"] for h in host_results)
    not_issued = sum(h.get("not_issued", 0) for h in host_results)
    outcomes = sum(s.get("outcomes_per_s", 0) for s in shard_results)
    completions = point_completions(host_results, args)
    if completions and outcomes < 0.95 * completions:
        log(
            f"warning: workflows finished {completions:.0f}/s but the shards recorded "
            f"only {outcomes:.0f} outcomes/s: outcome writes are falling behind"
        )
    if failures:
        log(f"warning: {failures} enqueues failed; see the load generator's output")
    if missing or not_issued:
        log(
            f"warning: {missing} worker processes never reported, {not_issued} "
            "scheduled enqueues were never issued; this point was overloaded"
        )
    point = summarize(n, len(clients), total_rps, args, host_results, shard_results)
    point["start_at"] = start_at
    point["pgdog_pool_size"] = pool_size
    point["shards"] = shard_results
    point["hosts"] = host_results
    point["db_identifiers"] = [s.get("identifier") for s in shards]
    if rids:
        window = (start_at + args.warmup, start_at + args.duration)
        stats = parallel(
            [
                lambda s=s: db_stats(rids[s["identifier"]], *window, args.region)
                for s in shards
            ]
        )
        point["shard_db_cpu_pct"] = [st["cpu_pct"] for st in stats]
        # Average active sessions: past the shard's 16 vCPUs, queries queue for CPU.
        point["shard_db_load"] = [st["load"] for st in stats]
        cpus = [c for c in point["shard_db_cpu_pct"] if c is not None]
        point["db_cpu_pct_max"] = max(cpus) if cpus else None
    return point


def point_completions(hosts: list[dict], args: argparse.Namespace) -> float:
    """Completions per second over the window, summed across hosts."""
    lo, hi = int(args.warmup), int(args.duration)
    done = sum(v for h in hosts for k, v in h["completed"].items() if lo <= int(k) < hi)
    return done / (hi - lo)


def spread(total: int, hosts: int) -> list[int]:
    """total processes over hosts as evenly as they divide: the first total % hosts get one more."""
    base, extra = divmod(total, hosts)
    return [base + (i < extra) for i in range(hosts)]


def direct_url(inv: dict) -> str:
    """Shard 0 without PgDog: the router's overhead, measured at one shard."""
    s = inv["shards"][0]
    return (
        f"postgresql://{inv['user']}:{inv['password']}@{s['admin_host']}:"
        f"{s['admin_port']}/{BENCH_DB}"
    )


def summarize(
    n: int,
    n_clients: int,
    total_rps: float,
    args: argparse.Namespace,
    hosts: list[dict],
    shards: list[dict],
) -> dict[str, Any]:
    completed: Counter[int] = Counter()
    started: Counter[int] = Counter()
    hist: Counter[int] = Counter()
    for h in hosts:
        completed.update({int(k): v for k, v in h["completed"].items()})
        started.update({int(k): v for k, v in h["started"].items()})
        hist.update({int(k): v for k, v in h["hist"].items()})
    lo, hi = int(args.warmup), int(args.duration)
    secs = hi - lo
    per_sec = [completed.get(s, 0) for s in range(lo, hi)]
    completion_rps = sum(per_sec) / secs
    pct = {
        f"p{p}_ms": (v * 1000 if (v := hist_percentile(hist, p)) is not None else None)
        for p in (50, 90, 99, 99.9)
    }
    wf_per_shard = [s["workflows_per_s"] for s in shards]
    routing = [h["routing"] for h in hosts if h.get("routing")]
    multi_by_query: dict[str, float] = defaultdict(float)
    for r in routing:
        for q in r["top_multi"]:
            multi_by_query[q["query"]] += q["multi_per_s"]
    pgdog = [h["pgdog_cores"] for h in hosts if h.get("pgdog_cores") is not None]
    return {
        "shards_n": n,
        "clients_n": n_clients,
        "offered_rps": total_rps,
        "start_rps": sum(started.get(s, 0) for s in range(lo, hi)) / secs,
        "completion_rps": completion_rps,
        "completion_rps_per_shard": completion_rps / n,
        # Completions per second over the window, worst second against best.
        "completion_rps_min_s": min(per_sec, default=0),
        "completion_rps_max_s": max(per_sec, default=0),
        **pct,
        "start_failures": sum(h["start_failures"] for h in hosts),
        "workers_missing": sum(h.get("workers_missing", 0) for h in hosts),
        # Scheduled enqueues never issued: enqueue batches fell behind the schedule.
        "not_issued": sum(h.get("not_issued", 0) for h in hosts),
        "processes_launched_late": sum(h["launched_late"] for h in hosts),
        "client_cpu_pct_max": max(h["host_cpu_pct"] for h in hosts),
        "pgdog_cores_mean": sum(pgdog) / len(pgdog) if pgdog else None,
        "router_direct_per_s": (
            sum(r["direct_per_s"] for r in routing) if routing else None
        ),
        "router_multi_per_s": (
            sum(r["multi_per_s"] for r in routing) if routing else None
        ),
        "top_multi_shard": sorted(
            ({"query": q, "per_s": v} for q, v in multi_by_query.items()),
            key=lambda x: -x["per_s"],
        )[:8],
        # Load on each shard, measured on the shard itself.
        "shard_workflows_per_s": wf_per_shard,
        "shard_skew": max(wf_per_shard) / max(1e-9, min(wf_per_shard)),
        # Outcome writes the shards recorded; completions are counted in the workflow
        # body, before that write, so a gap between the two is a backlog of them.
        "db_outcomes_per_s": sum(s.get("outcomes_per_s", 0) for s in shards),
        "shard_statements_per_s_mean": sum(s["statements_per_s"] for s in shards) / n,
        # The shard holding the global key (queues, schedules, versions) gets every
        # process's polls of them, so it runs hotter than the rest.
        "shard_statements_per_s_max": max(s["statements_per_s"] for s in shards),
        # Backend milliseconds spent executing per second: ~1000 per busy core.
        "shard_exec_ms_per_s_mean": sum(s["exec_ms_per_s"] for s in shards) / n,
        "shard_commits_per_s_mean": sum(s["commits_per_s"] for s in shards) / n,
        "shard_connections_max": max(s["connections"] for s in shards),
    }


def print_summary(points: list[dict]) -> None:
    base = points[0]["completion_rps_per_shard"] if points else 0
    header = (
        f"{'shards':>6} {'clients':>7} {'offered/s':>10} {'done/s':>10} {'done/s/shard':>12} "
        f"{'eff':>5} {'p50ms':>7} {'p99ms':>8} {'skew':>5} {'fanout/s':>9} "
        f"{'stmts/s/shard':>13} {'hot':>5} {'client cpu':>10} {'pgdog':>6} {'db cpu':>7}"
    )
    print(header)
    for p in points:
        eff = p["completion_rps_per_shard"] / base if base else 0
        multi = p["router_multi_per_s"]
        print(
            f"{p['shards_n']:>6} {p['clients_n']:>7} {p['offered_rps']:>10.0f} "
            f"{p['completion_rps']:>10.0f} {p['completion_rps_per_shard']:>12.0f} "
            f"{eff:>5.2f} {fmt(p['p50_ms'], 7, 1)} {fmt(p['p99_ms'], 8, 1)} "
            f"{p['shard_skew']:>5.2f} "
            f"{fmt(multi, 9, 0)} "
            f"{p['shard_statements_per_s_mean']:>13.0f} "
            f"{p['shard_statements_per_s_max'] / max(1e-9, p['shard_statements_per_s_mean']):>5.2f} "
            f"{p['client_cpu_pct_max']:>9.0f}% "
            f"{fmt(p['pgdog_cores_mean'], 6, 1)} "
            f"{fmt(p.get('db_cpu_pct_max'), 6, 0)}{'%' if p.get('db_cpu_pct_max') is not None else ' '}"
        )


def fmt(v: Optional[float], width: int, digits: int) -> str:
    return f"{v:>{width}.{digits}f}" if v is not None else f"{'-':>{width}}"


def resource_ids(identifiers: list[str], region: str) -> dict[str, str]:
    """Each RDS instance's DbiResourceId, which Performance Insights names it by."""

    def one(ident: str) -> str:
        return subprocess.run(
            ["aws", "rds", "describe-db-instances", "--region", region,
             "--db-instance-identifier", ident,
             "--query", "DBInstances[0].DbiResourceId", "--output", "text"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()  # fmt: skip

    return dict(zip(identifiers, parallel([lambda i=i: one(i) for i in identifiers])))


def db_stats(resource_id: str, start: float, end: float, region: str) -> dict:
    """One shard's OS CPU and database load over the window, per second, from
    Performance Insights: its data lags by about a second, where CloudWatch's lags
    minutes. CPU needs Enhanced Monitoring, which the terraform stack turns on."""
    out = subprocess.run(
        ["aws", "pi", "get-resource-metrics", "--region", region,
         "--service-type", "RDS", "--identifier", resource_id,
         "--metric-queries",
         '[{"Metric":"os.cpuUtilization.total.avg"},{"Metric":"db.load.avg"}]',
         "--start-time", str(int(start)), "--end-time", str(int(end)),
         "--period-in-seconds", "1", "--output", "json"],
        capture_output=True, text=True,
    )  # fmt: skip
    stats: dict[str, Optional[float]] = {"cpu_pct": None, "load": None}
    if out.returncode != 0:
        log(
            f"warning: no Performance Insights data for {resource_id}: {out.stderr[-300:]}"
        )
        return stats
    for m in json.loads(out.stdout)["MetricList"]:
        values = [d["Value"] for d in m["DataPoints"] if "Value" in d]
        key = "cpu_pct" if m["Key"]["Metric"].startswith("os.") else "load"
        stats[key] = sum(values) / len(values) if values else None
    return stats


# ── Status ──────────────────────────────────────────────────

# Two CPU samples a couple of seconds apart, PgDog's CPU, the host's clock, and the load
# generator's command line, which carries the run's start time and length.
HOST_PROBE = f"""
c1=$(head -1 /proc/stat)
docker stats --no-stream --format '{{{{.CPUPerc}}}}' {PGDOG_CONTAINER} 2>/dev/null \
  > /tmp/dbos-bench-pgdog-cpu &
sleep 2
c2=$(head -1 /proc/stat)
wait
echo "cpu $c1"
echo "cpu $c2"
echo "pgdog $(cat /tmp/dbos-bench-pgdog-cpu)"
echo "now $(date +%s.%N)"
pgrep -af '[p]ython[0-9.]* benchmarks/sharded/loadgen[.]py' | sed 's/^/loadgen /'
true
"""


def probe_host(host: Host) -> dict[str, Any]:
    out: dict[str, Any] = {"host": host.name, "cpu_pct": None, "pgdog_cores": None}
    cpu = []
    for line in host.run(HOST_PROBE, cd=False).splitlines():
        kind, _, rest = line.partition(" ")
        if kind == "cpu":
            cpu.append([int(x) for x in rest.split()[1:9]])
        elif kind == "pgdog" and rest.strip().endswith("%"):
            out["pgdog_cores"] = float(rest.strip()[:-1]) / 100
        elif kind == "now":
            out["now"] = float(rest)
        elif kind == "loadgen":
            argv = shlex.split(rest.split(" ", 1)[1])
            opts = dict(zip(argv, argv[1:]))
            out["run"] = {
                k: float(opts[f"--{k.replace('_', '-')}"])
                for k in ("start_at", "duration", "drain", "rps")
                if f"--{k.replace('_', '-')}" in opts
            }
    if len(cpu) == 2:
        total = sum(cpu[1]) - sum(cpu[0])
        idle = (cpu[1][3] + cpu[1][4]) - (cpu[0][3] + cpu[0][4])
        out["cpu_pct"] = 100 * (1 - idle / max(1, total))
    return out


def run_phase(host: dict[str, Any]) -> str:
    run = host.get("run")
    if not run or "start_at" not in run:
        return "no load generator running"
    t = host["now"] - run["start_at"]
    duration = run.get("duration", 0)
    if t < 0:
        return f"launching, load starts in {-t:.0f}s"
    if t < duration:
        return f"load +{t:.0f}s of {duration:.0f}s"
    return f"draining, +{t - duration:.0f}s after the load"


def print_status(
    inv: dict, hosts: list[Host], shards: list[dict], window: float
) -> None:
    script = (HERE / "admin.py").read_text()
    password = shlex.quote(inv["password"])
    # The script goes on stdin, so this works whatever admin.py the hosts last synced.
    shard_cmd = (
        f"PGPASSWORD={password} {inv['python']} - status --user {inv['user']} "
        f"--shards {shlex.quote(json.dumps(shards))} --window {window}"
    )
    outputs = parallel(
        [lambda h=h: probe_host(h) for h in hosts]
        + [lambda: result_line(hosts[0].run(shard_cmd, input=script))]
    )
    host_stats, shard_stats = outputs[:-1], outputs[-1]
    offered = sum(h["run"].get("rps", 0) for h in host_stats if h.get("run"))
    log(
        f"{run_phase(host_stats[0])}; {offered:.0f}/s offered; "
        f"rates over the last {window:.0f}s"
    )
    print(
        f"{'shard':>6} {'created/s':>10} {'done/s':>10} {'waiting':>10} {'running':>8}"
    )
    total = Counter[str]()
    for i, st in enumerate(shard_stats):
        if "error" in st:
            print(f"{i:>6}  {st['error'][:70]}")
            continue
        total.update(st)
        print(
            f"{i:>6} {st['created_per_s']:>10.0f} {st['completed_per_s']:>10.0f} "
            f"{st['waiting']:>10} {st['running']:>8}"
        )
    print(
        f"{'total':>6} {total['created_per_s']:>10.0f} {total['completed_per_s']:>10.0f} "
        f"{total['waiting']:>10} {total['running']:>8}"
    )
    for h in host_stats:
        print(
            f"  {h['host']}: cpu {fmt(h['cpu_pct'], 3, 0)}%, "
            f"pgdog {fmt(h['pgdog_cores'], 4, 1)} cores"
        )


def status_main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="orchestrate.py status",
        description="Report a running benchmark: shard rates and backlog, client CPU.",
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--inventory", help="Inventory JSON file")
    src.add_argument("--terraform-dir", help="Read the inventory from terraform output")
    parser.add_argument(
        "--shards", type=int, help="Report the first N shards (default: all)"
    )
    parser.add_argument(
        "--window", type=float, default=10.0, help="Seconds the rates cover"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Report every --window seconds until Ctrl-C",
    )
    parser.add_argument("--ssh-key", help="Private key for the clients")
    args = parser.parse_args(argv)
    if args.ssh_key:
        SSH_OPTS.extend(["-i", os.path.expanduser(args.ssh_key)])
    SSH_OPTS.extend(["-o", "ConnectTimeout=10"])
    inv = load_inventory(args)
    hosts = [Host(spec, inv.get("workdir", "bench")) for spec in inv["clients"]]
    shards = inv["shards"][: args.shards]
    while True:
        started = time.time()
        print_status(inv, hosts, shards, args.window)
        if not args.watch:
            return
        time.sleep(max(0.0, args.window - (time.time() - started)))


# ── Main ────────────────────────────────────────────────────


def main() -> None:
    if sys.argv[1:2] == ["status"]:
        status_main(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--inventory", help="Inventory JSON file")
    src.add_argument("--terraform-dir", help="Read the inventory from terraform output")
    parser.add_argument(
        "--shards", default="1,2,4,8", help="Comma-separated shard counts to sweep"
    )
    parser.add_argument("--rps-per-shard", type=float, required=True)
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument("--warmup", type=float, default=60.0)
    parser.add_argument("--drain", type=float, default=30.0)
    parser.add_argument(
        "--lead",
        type=float,
        default=60.0,
        help="Seconds to launch DBOS before the start",
    )
    parser.add_argument("--steps", type=int, default=0)
    parser.add_argument(
        "--processes",
        type=int,
        required=True,
        help="DBOS processes in total, spread evenly over every client host",
    )
    # Each process talks to every shard through one pool and one executor, so a slow
    # shard's statements tie them up for all the others: at 5 connections and 4
    # threads, one shard's bad minute held every shard of four to its rate. More is not
    # better: at 48 and 32, a slow shard drew every process's threads, so up to all of
    # PgDog's connections to it at once, and its writes convoyed on the table's
    # extension lock while every other shard sat idle.
    parser.add_argument("--pool-size", type=int, default=16)
    parser.add_argument("--executor-threads", type=int, default=8)
    parser.add_argument(
        "--worker-concurrency",
        type=int,
        default=1000,
        # A process claims at most this many per poll, so it caps the queue at the
        # processes times this per poll: 200 once capped 64 polling every second at
        # ~12,800/s.
        help="Most workflows of the queue each process runs at once",
    )
    parser.add_argument(
        "--polling-interval",
        type=float,
        default=0.5,
        # With worker concurrency, it sets how fast each process can claim: at most the
        # concurrency per poll. Shorter polls with smaller claims keep each claim's scan
        # short; a long one holds locks on rows other processes already claimed and ran.
        help="Seconds between a process's polls of the queue",
    )
    parser.add_argument(
        "--enqueue-interval",
        type=float,
        default=0.01,
        # Small batches: big ones convoyed on the tables' extension lock at startup.
        help="Seconds between each process's enqueue batches",
    )
    parser.add_argument(
        "--enqueue-connections",
        type=int,
        default=4,
        help="Enqueue batches each shard key's enqueuer may have in flight at once",
    )
    parser.add_argument("--pgdog-workers", type=int, default=8)
    parser.add_argument(
        "--shard-connections",
        type=int,
        default=256,
        help="Server connections each shard accepts from all PgDogs together; "
        "each transaction holds one for ~4 network round trips, so 20K workflows/s "
        "keeps ~100 busy",
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Bypass PgDog and connect to shard 0 directly; one shard only",
    )
    parser.add_argument("--skip-sync", action="store_true")
    parser.add_argument(
        "--ssh-key", help="Private key for the clients (the key pair terraform used)"
    )
    parser.add_argument(
        "--core-dir",
        default=str(Path.home() / "dbos-transact-py"),
        help="dbos checkout to build and install; without one, the latest release",
    )
    parser.add_argument(
        "--enterprise-dir",
        default=str(Path.home() / "dbos-enterprise-py"),
        help="dbos-enterprise checkout to build and install; without one, the "
        "latest release",
    )
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--out", default=str(REPO / "results" / "sharded"))
    args = parser.parse_args()
    if args.ssh_key:
        SSH_OPTS.extend(["-i", os.path.expanduser(args.ssh_key)])

    sweep = [int(x) for x in args.shards.split(",")]
    if args.direct and sweep != [1]:
        raise SystemExit(
            "--direct bypasses the router, so it runs one shard: --shards 1"
        )
    if args.warmup >= args.duration:
        raise SystemExit("--warmup must be shorter than --duration")
    inv = load_inventory(args)
    hosts = [Host(spec, inv.get("workdir", "bench")) for spec in inv["clients"]]
    if args.processes < len(hosts):
        raise SystemExit(
            f"--processes {args.processes} is fewer than the {len(hosts)} client hosts"
        )

    key = os.environ.get(KEY_ENV)
    if not key:
        raise SystemExit(f"Set {KEY_ENV} to a DBOS license key: sharding needs one")

    versions: dict[str, str] = {}
    if not args.skip_sync and any(h.ssh for h in hosts):
        versions = sync(hosts, inv, args)
    parallel(
        [lambda h=h: h.run(f"umask 077 && cat > {KEY_FILE}", input=key) for h in hosts]
    )
    meta = {
        "args": vars(args),
        "versions": versions,
        "core_git": git_head(args.core_dir),
        "enterprise_git": git_head(args.enterprise_dir),
        "benchmark_git": git_head(str(REPO)),
        "pgdog_image": inv.get("pgdog_image", PGDOG_IMAGE),
    }
    out = Path(args.out) / time.strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True, exist_ok=True)
    log(f"results go to {out}")

    identifiers = [s["identifier"] for s in inv["shards"] if s.get("identifier")]
    rids = resource_ids(identifiers, args.region) if identifiers else {}
    points = []
    try:
        for n in sweep:
            point = run_point(inv, hosts, n, args, rids)
            point["window"] = [args.warmup, args.duration]
            points.append(point)
            (out / f"shards-{n}.json").write_text(
                json.dumps({**meta, **point}, indent=1)
            )
            print_summary(points)
    finally:
        reset_hosts(hosts)
    summary = [
        {k: v for k, v in p.items() if k not in ("hosts", "shards")} for p in points
    ]
    (out / "summary.json").write_text(json.dumps({**meta, "points": summary}, indent=1))
    log(f"wrote {out}")


if __name__ == "__main__":
    main()
