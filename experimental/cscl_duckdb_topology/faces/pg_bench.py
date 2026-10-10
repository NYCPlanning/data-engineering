"""Time the PG (PostGIS) versions of the same steps, without touching the build schema.

    BUILD_ENGINE_SERVER=postgresql://... python pg_bench.py [--models ...] [--reps 3]

Each model's dbt-compiled SQL (pg_compiled/, from `dbt compile` against the schema holding
the finished tables) is run as `CREATE TEMP TABLE ... AS <compiled sql>` in one session.
Each model reads its upstream refs from the persisted tables in that schema (identical
content to what the chain would produce), so persisted inputs keep their indexes and
parallel scans; output goes to a session temp table. dbt's post-build index creation is
not timed (DuckDB has none either).
"""

import argparse
import json
import os
import statistics
import time
from pathlib import Path

import psycopg2

HERE = Path(__file__).parent
COMPILED = HERE / "pg_compiled"

MODELS = [
    "int__boundary__cb2010_edges",
    "int__boundary__cb2010_raw",
    "int__boundary__cb2010",
    "qa__boundary__cb2010_validity",
    "int__boundary__nta2020_edges",
    "int__boundary__nta2020_raw",
    "int__boundary__nta2020",
    "qa__boundary__nta2020_validity",
    "int__topology__ap_police",
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--models", nargs="+", default=MODELS)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--db", default="db-cscl")
    p.add_argument(
        "--explain", action="store_true", help="EXPLAIN ANALYZE each model once instead"
    )
    p.add_argument("--out", type=Path)
    args = p.parse_args()

    conn = psycopg2.connect(f"{os.environ['BUILD_ENGINE_SERVER']}/{args.db}")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '15min'")

    info = {}
    for setting in [
        "server_version",
        "max_parallel_workers_per_gather",
        "max_parallel_workers",
        "max_worker_processes",
        "work_mem",
        "shared_buffers",
        "effective_cache_size",
        "jit",
    ]:
        cur.execute(f"SHOW {setting}")
        info[setting] = cur.fetchone()[0]
    cur.execute("SELECT postgis_lib_version(), postgis_geos_version()")
    info["postgis"], info["geos"] = cur.fetchone()
    print(json.dumps(info))

    results = {}
    for model in args.models:
        sql = (COMPILED / f"{model}.sql").read_text().strip().rstrip(";")
        if args.explain:
            cur.execute(
                f"EXPLAIN (ANALYZE, BUFFERS) CREATE TEMP TABLE t_{model} AS {sql}"
            )
            print(f"=== {model}")
            print("\n".join(r[0] for r in cur.fetchall()))
            cur.execute(f"DROP TABLE IF EXISTS pg_temp.t_{model}")
            continue
        times = []
        for _ in range(args.reps):
            cur.execute(f"DROP TABLE IF EXISTS pg_temp.t_{model}")
            t0 = time.perf_counter()
            cur.execute(f"CREATE TEMP TABLE t_{model} AS {sql}")
            times.append(time.perf_counter() - t0)
        cur.execute(f"SELECT count(*) FROM pg_temp.t_{model}")
        n = cur.fetchone()[0]
        cur.execute(f"DROP TABLE IF EXISTS pg_temp.t_{model}")
        results[model] = {"times": times, "rows": n}
        print(
            f"{model:35s} cold {times[0]:8.2f}  median {statistics.median(times):8.2f}  "
            f"all {' '.join(f'{t:.2f}' for t in times)}  rows {n}",
            flush=True,
        )
    conn.close()
    if args.out:
        args.out.write_text(json.dumps({"server": info, "results": results}, indent=1))


if __name__ == "__main__":
    main()
