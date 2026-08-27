#!/usr/bin/env bash
# Watch autovacuum and dead-tuple accumulation during a benchmark run.
#
#   ./scripts/monitor_vacuum.sh              # one snapshot
#   watch -n2 ./scripts/monitor_vacuum.sh    # live
#
# Reads BENCHMARK_DATABASE_URL. This workload updates every workflow row at
# least twice (ENQUEUED -> PENDING -> SUCCESS), so dead tuples build quickly and
# autovacuum on dbos.workflow_status is usually the thing to watch.
set -euo pipefail

DB="${1:-${BENCHMARK_DATABASE_URL:?set BENCHMARK_DATABASE_URL or pass a URL}}"
PSQL=(psql "$DB" -X -q)

# pg_stat_progress_vacuum renamed columns in PG17: num_dead_tuples ->
# num_dead_item_ids, max_dead_tuples -> max_dead_tuple_bytes (+ dead_tuple_bytes).
VER=$("${PSQL[@]}" -tAc "SHOW server_version_num")
if [ "$VER" -ge 170000 ]; then
  DEADCOLS="p.num_dead_item_ids AS dead_ids, pg_size_pretty(p.dead_tuple_bytes) AS dead_bytes"
else
  DEADCOLS="p.num_dead_tuples AS dead_ids, p.max_dead_tuples::text AS dead_bytes"
fi

"${PSQL[@]}" <<SQL
\pset border 2

\echo == dead tuples by table (dbos schema, incl. TOAST) ==
SELECT COALESCE(par.relname || ' (toast)', s.relname)               AS relname,
       s.n_live_tup                                                 AS live,
       s.n_dead_tup                                                 AS dead,
       round(100.0*s.n_dead_tup/NULLIF(s.n_live_tup+s.n_dead_tup,0),1) AS pct_dead,
       -- Row versions per 8KB page. Says at a glance whether the payload is
       -- stored inline (low, so vacuum reads the payload on every pass) or
       -- pushed out to TOAST (high). A ~1KB output is the worst case: too big
       -- to pack, too small to cross the 2032-byte TOAST threshold.
       round((s.n_live_tup+s.n_dead_tup)
             / NULLIF(pg_relation_size(s.relid)/8192.0,0),1)        AS ver_per_pg,
       s.n_tup_upd                                                  AS updates,
       round(100.0*s.n_tup_hot_upd/NULLIF(s.n_tup_upd,0),1)         AS hot_pct,
       s.autovacuum_count                                           AS av_runs,
       to_char(s.last_autovacuum,'HH24:MI:SS')                      AS last_av,
       -- Autovacuum fires when dead > threshold + scale_factor * reltuples.
       (current_setting('autovacuum_vacuum_threshold')::int
        + current_setting('autovacuum_vacuum_scale_factor')::float
          * GREATEST(c.reltuples,0))::bigint                        AS fires_at
-- all_tables, not user_tables: a TOAST table lives in pg_toast, which
-- pg_stat_user_tables filters out. It is reached from its parent's
-- reltoastrelid, which is also the only way to learn whose TOAST it is.
FROM pg_stat_all_tables s
JOIN pg_class c ON c.oid = s.relid
LEFT JOIN pg_class par ON par.reltoastrelid = s.relid
LEFT JOIN pg_namespace pn ON pn.oid = par.relnamespace
-- Every dbos table, but only TOAST tables that hold something: each parent
-- has a TOAST relation whether or not anything was ever pushed out to it, and
-- a dozen empty ones would push the interesting rows off the bottom.
WHERE s.schemaname = 'dbos'
   OR (pn.nspname = 'dbos' AND s.n_live_tup + s.n_dead_tup > 0)
ORDER BY s.n_dead_tup DESC, s.n_live_tup DESC
LIMIT 14;

\echo
\echo == vacuums running right now ==
SELECT p.pid,
       -- relid::regclass on a TOAST table prints pg_toast.pg_toast_NNNNN,
       -- which says nothing about which table is being vacuumed.
       COALESCE(par.relname || ' (toast)', p.relid::regclass::text) AS tbl,
       p.phase,
       p.heap_blks_scanned || '/' || p.heap_blks_total              AS blks,
       round(100.0*p.heap_blks_scanned/NULLIF(p.heap_blks_total,0),1) AS pct,
       p.index_vacuum_count                                         AS idx_passes,
       $DEADCOLS,
       date_trunc('second', now()-a.xact_start)                     AS running_for
FROM pg_stat_progress_vacuum p
JOIN pg_stat_activity a USING (pid)
LEFT JOIN pg_class par ON par.reltoastrelid = p.relid;

\echo
\echo == autovacuum worker backends ==
SELECT pid,
       date_trunc('second', now()-xact_start) AS running_for,
       state,
       query
FROM pg_stat_activity
WHERE query LIKE 'autovacuum:%'
ORDER BY xact_start;

\echo
\echo == table size: heap vs indexes vs TOAST ==
SELECT s.relname,
       pg_size_pretty(pg_total_relation_size(s.relid))              AS total,
       pg_size_pretty(pg_relation_size(s.relid))                    AS heap,
       pg_size_pretty(pg_indexes_size(s.relid))                     AS indexes,
       -- Broken out rather than lumped in with the indexes: when the payload
       -- is TOASTed this is where nearly all the bytes are, and it is the one
       -- part nothing on the hot path ever scans.
       pg_size_pretty(COALESCE(pg_total_relation_size(c.reltoastrelid),0)) AS toast
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
WHERE s.schemaname = 'dbos'
ORDER BY pg_total_relation_size(s.relid) DESC
LIMIT 8;

\echo
\echo == effective autovacuum settings ==
SELECT name, setting, unit
FROM pg_settings
WHERE name IN ('autovacuum','autovacuum_max_workers','autovacuum_naptime',
               'autovacuum_vacuum_threshold','autovacuum_vacuum_scale_factor',
               'autovacuum_vacuum_insert_threshold','autovacuum_vacuum_cost_delay',
               'autovacuum_vacuum_cost_limit','autovacuum_work_mem')
ORDER BY name;
SQL
