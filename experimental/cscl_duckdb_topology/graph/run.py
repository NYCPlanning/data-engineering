"""Build the AP topology node/edge graph in DuckDB from the exported PG vertices, timing
each step.

    python run.py                 # 3 runs of every step, in-memory
    python run.py --runs 1

Each sql/NN_*.sql file is a SELECT, materialized as CREATE OR REPLACE TABLE <name>. Run 1
is the cold run (fresh process, nothing cached); the reported median is over all runs.
Final tables are saved to data/graph.duckdb for compare.py.
"""

import argparse
import json
import statistics
import time
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
SQL = HERE / "sql"
DATA = HERE / "data"

# dbt_project.yml vars
VARS = {
    "tol": "0.0003662109375",  # ap_topology_node_merge_tolerance_ft
    "split_tol": "0.001",  # ap_topology_edge_split_tolerance_ft
    "max_hops": "6",  # ap_topology_edge_split_max_hops
}

# (table, sql file). point_to_node_using_key is an alternative to point_to_node, timed
# alongside it; downstream steps read point_to_node.
STEPS = [
    ("exact_points", "01_exact_points.sql"),
    ("grid_cells", "02_grid_cells.sql"),
    ("point_to_node", "03a_point_to_node.sql"),
    ("point_to_node_using_key", "03b_point_to_node_using_key.sql"),
    ("nodes", "04_nodes.sql"),
    ("raw_edges", "05a_raw_edges.sql"),
    ("edges_unnoded", "05b_edges_unnoded.sql"),
    ("edge_crossings", "06_edge_crossings.sql"),
    ("edges", "07_edges.sql"),
]


def render(path: Path) -> str:
    sql = path.read_text()
    for k, v in VARS.items():
        sql = sql.replace("${" + k + "}", v)
    return sql


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--threads", type=int)
    args = parser.parse_args()

    con = duckdb.connect()
    con.sql("LOAD spatial")
    if args.threads:
        con.sql(f"SET threads = {args.threads}")
    settings = con.sql(
        "SELECT version(), current_setting('threads'), current_setting('memory_limit')"
    ).fetchone()
    print(f"duckdb {settings[0]}, threads={settings[1]}, memory_limit={settings[2]}")

    t0 = time.perf_counter()
    con.sql(f"CREATE TABLE vertices AS FROM '{DATA / 'vertices.parquet'}'")
    con.sql(f"CREATE TABLE arc_lines AS FROM '{DATA / 'arc_lines.parquet'}'")
    print(f"load parquet: {time.perf_counter() - t0:.2f}s")

    timings: dict[str, list[float]] = {name: [] for name, _ in STEPS}
    for run in range(args.runs):
        for name, file in STEPS:
            sql = render(SQL / file)
            t = time.perf_counter()
            con.sql(f"CREATE OR REPLACE TABLE {name} AS {sql}")
            timings[name].append(time.perf_counter() - t)
            print(f"  {name}: {timings[name][-1]:.2f}s", flush=True)
        print(
            f"run {run + 1}: "
            + ", ".join(f"{n}={ts[-1]:.2f}" for n, ts in timings.items())
        )

    print(f"\n{'step':26s} {'cold':>7s} {'median':>7s} {'rows':>9s}")
    summary = {}
    for name, ts in timings.items():
        rows = con.sql(f"SELECT count(*) FROM {name}").fetchone()[0]
        summary[name] = {"runs": ts, "median": statistics.median(ts), "rows": rows}
        print(f"{name:26s} {ts[0]:7.2f} {statistics.median(ts):7.2f} {rows:9,d}")
    summary["_settings"] = {
        "version": settings[0],
        "threads": settings[1],
        "memory_limit": settings[2],
    }
    suffix = f"_t{args.threads}" if args.threads else ""
    (DATA / f"duckdb_timings{suffix}.json").write_text(json.dumps(summary, indent=2))

    out = DATA / "graph.duckdb"
    out.unlink(missing_ok=True)
    con.sql(f"ATTACH '{out}' AS out")
    for name, _ in STEPS:
        con.sql(f"CREATE TABLE out.{name} AS FROM {name}")


if __name__ == "__main__":
    main()
