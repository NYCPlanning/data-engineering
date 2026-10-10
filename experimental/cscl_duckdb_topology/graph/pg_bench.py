"""Time the same steps in Postgres without touching any build schema: each dbt-compiled
model runs as CREATE TEMP TABLE ... AS <compiled sql> in one session, reading the
previous steps' temp tables and the existing int__topology__vertices from --source-schema.
The dbt-configured indexes are built on each temp table and it's ANALYZEd (temp tables
are never auto-analyzed), timed separately from the CTAS.

    cd products/cscl && dbt compile --select int__topology__exact_points+ ...  # see RESULTS.md
    python pg_bench.py --source-schema ar_cscl_districts_gdb_db_qa_oct4 --runs 3

Needs BUILD_ENGINE_SERVER and BUILD_ENGINE_SCHEMA (load the cscl direnv first).
"""

import argparse
import json
import os
import re
import statistics
import time
from pathlib import Path

import psycopg2

HERE = Path(__file__).parent
REPO = HERE.parents[2]
COMPILED = (
    REPO
    / "products/cscl/target/compiled/cscl/models/intermediate/atomicpolygon_topology"
)

# model -> dbt-configured indexes (models' config blocks)
STEPS = {
    "int__topology__exact_points": ["USING gist (geom)"],
    "int__topology__grid_cells": ["(x_cell, y_cell)", "USING gist (geom)"],
    "int__topology__point_to_node": [],
    "int__topology__nodes": ["(node_id)", "USING gist (geom)"],
    "int__topology__edges_unnoded": [
        "(node_lo, node_hi, is_arc)",
        "(atomicid)",
        "USING gist (geom)",
    ],
    "int__topology__edge_crossings": [],
    "int__topology__edges": ["(node_lo, node_hi, is_arc)", "(atomicid)"],
}


def compiled_sql(model: str, compiled_schema: str, source_schema: str) -> str:
    sql = (COMPILED / f"{model}.sql").read_text()
    sql = sql.replace(
        f'"{compiled_schema}"."int__topology__vertices"',
        f'"{source_schema}"."int__topology__vertices"',
    )
    sql = re.sub(
        rf'"db-cscl"\."{compiled_schema}"\."(int__topology__\w+)"', r"pg_temp.\1", sql
    )
    return sql.replace(f'"db-cscl"."{source_schema}"', f'"{source_schema}"')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    compiled_schema = os.environ["BUILD_ENGINE_SCHEMA"]

    conn = psycopg2.connect(f"{os.environ['BUILD_ENGINE_SERVER']}/db-cscl")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '15min'")
    info = {}
    for setting in [
        "server_version",
        "max_parallel_workers_per_gather",
        "max_parallel_workers",
        "work_mem",
        "shared_buffers",
        "jit",
    ]:
        cur.execute(f"SHOW {setting}")
        info[setting] = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
    info["active_sessions_at_start"] = cur.fetchone()[0]
    print(info)

    timings: dict[str, dict[str, list[float]]] = {
        m: {"ctas": [], "index_analyze": []} for m in STEPS
    }
    rows = {}
    for run in range(args.runs):
        for model in STEPS:
            cur.execute(f"DROP TABLE IF EXISTS pg_temp.{model}")
        for model, indexes in STEPS.items():
            sql = compiled_sql(model, compiled_schema, args.source_schema)
            t = time.perf_counter()
            cur.execute(f"CREATE TEMP TABLE {model} AS {sql}")
            ctas = time.perf_counter() - t
            t = time.perf_counter()
            for idx in indexes:
                cur.execute(f"CREATE INDEX ON pg_temp.{model} {idx}")
            cur.execute(f"ANALYZE pg_temp.{model}")
            idx_time = time.perf_counter() - t
            timings[model]["ctas"].append(ctas)
            timings[model]["index_analyze"].append(idx_time)
            cur.execute(f"SELECT count(*) FROM pg_temp.{model}")
            rows[model] = cur.fetchone()[0]
            print(
                f"run {run + 1} {model}: ctas {ctas:.2f}s, idx+analyze {idx_time:.2f}s",
                flush=True,
            )

    print(
        f"\n{'model':32s} {'ctas cold':>9s} {'ctas med':>9s} {'idx med':>8s} {'rows':>9s}"
    )
    summary = {"_settings": info}
    for model, t in timings.items():
        summary[model] = {
            "ctas_runs": t["ctas"],
            "ctas_median": statistics.median(t["ctas"]),
            "index_analyze_median": statistics.median(t["index_analyze"]),
            "rows": rows[model],
        }
        print(
            f"{model:32s} {t['ctas'][0]:9.2f} {statistics.median(t['ctas']):9.2f} "
            f"{statistics.median(t['index_analyze']):8.2f} {rows[model]:9,d}"
        )
    (HERE / "data" / "pg_timings.json").write_text(json.dumps(summary, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
