"""Run and time the DuckDB face build (and optional police overlay) on the exported Parquet.

    python run.py                          # cb2010, 3 reps, default threads
    python run.py --layers cb2010 nta2020 police --reps 3 --compare
    python run.py --threads 1 --union-agg ST_CoverageUnion_Agg

Each rep runs in a fresh subprocess with a fresh in-memory database, so rep 1 is the cold
run (first Parquet read, extension load) and reps 2..n are warm only in the OS-cache sense.
Every statement in sql/ is timed on its own; the label is <sql file>:<table created>.
"""

import argparse
import json
import os
import re
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
SQL = HERE / "sql"
DATA = HERE / "data"

LAYERS = {
    "cb2010": {"entity_column": "bctcb2010"},
    "nta2020": {"entity_column": "nta2020"},
}
POLICE_LAYERS = [
    {"layer": "precinct", "layer_table": "nypdprecinct", "layer_id": "precinct"},
    {
        "layer": "patrol_borough",
        "layer_table": "nypdpatrolborough",
        "layer_id": "patrol_borough",
    },
    {"layer": "sector", "layer_table": "nypdbeat", "layer_id": "sector"},
]
BOUNDARY_STEPS = [
    "10_classify_edges.sql",
    "20_build_raw.sql",
    "30_strip_noise_rings.sql",
    "40_validity.sql",
]


def one_rep(args) -> dict:
    import duckdb

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads = {args.threads}")
    timings: list[tuple[str, float]] = []

    def run(file: str, **params):
        text = (
            (SQL / file)
            .read_text()
            .format(data=DATA, union_agg=args.union_agg, **params)
        )
        for stmt in con.extract_statements(text):
            m = re.search(r"CREATE OR REPLACE TABLE (\w+)", stmt.query)
            t0 = time.perf_counter()
            con.execute(stmt)
            timings.append(
                (
                    f"{file.removesuffix('.sql')}:{m.group(1) if m else '?'}",
                    time.perf_counter() - t0,
                )
            )

    t0 = time.perf_counter()
    con.execute("LOAD spatial")
    timings.append(("load_spatial", time.perf_counter() - t0))
    run("00_load.sql")
    for name in args.layers:
        if name == "police":
            run("police_00_load.sql")
            run("police_10_aps.sql")
            for layer in POLICE_LAYERS:
                run("police_20_layer.sql", **layer)
            run("police_30_assemble.sql")
            continue
        for step in BOUNDARY_STEPS:
            run(step, name=name, **LAYERS[name])

    compare = {}
    if args.compare:
        for name in args.layers:
            if name == "police":
                run("police_90_compare.sql")
                compare["police"] = con.sql("FROM police_cmp").df().to_dict("records")
            else:
                run("90_compare.sql", name=name, **LAYERS[name])
                compare[name] = {
                    t: con.sql(f"FROM {name}_cmp_{t}").df().to_dict("records")
                    for t in ["edges", "validity", "geom"]
                }
        timings = [t for t in timings if "compare" not in t[0]]

    rows = {}
    for (table,) in con.sql("SELECT table_name FROM duckdb_tables()").fetchall():
        rows[table] = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]

    settings = dict(
        con.sql(
            "SELECT name, value FROM duckdb_settings() WHERE name IN ('threads', 'memory_limit')"
        ).fetchall()
    )
    return {
        "timings": timings,
        "rows": rows,
        "compare": compare,
        "duckdb_version": duckdb.__version__,
        "spatial_version": con.sql(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'spatial'"
        ).fetchone()[0],
        "settings": settings,
        # macOS reports ru_maxrss in bytes
        "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20,
        # other work on the machine skews sub-second timings; record it with each rep
        "loadavg_1m": os.getloadavg()[0],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--layers", nargs="+", default=["cb2010"], choices=[*LAYERS, "police"]
    )
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--threads", type=int)
    p.add_argument(
        "--union-agg",
        default="ST_Union_Agg",
        choices=["ST_Union_Agg", "ST_CoverageUnion_Agg"],
    )
    p.add_argument(
        "--compare", action="store_true", help="compare to PG on the last rep"
    )
    p.add_argument("--out", type=Path, help="write all reps as JSON here")
    p.add_argument("--one-rep", action="store_true", help=argparse.SUPPRESS)
    args = p.parse_args()

    if args.one_rep:
        print(json.dumps(one_rep(args), default=str))
        return

    reps = []
    for i in range(args.reps):
        cmd = [
            sys.executable,
            __file__,
            "--one-rep",
            "--layers",
            *args.layers,
            "--union-agg",
            args.union_agg,
        ]
        if args.threads:
            cmd += ["--threads", str(args.threads)]
        if args.compare and i == args.reps - 1:
            cmd.append("--compare")
        out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
        reps.append(json.loads(out))

    last = reps[-1]
    print(
        f"duckdb {last['duckdb_version']} spatial {last['spatial_version']} "
        f"threads={last['settings']['threads']} memory_limit={last['settings']['memory_limit']} "
        f"union={args.union_agg} peak_rss_mb={max(r['peak_rss_mb'] for r in reps):.0f} "
        f"loadavg_1m={','.join(str(round(r['loadavg_1m'])) for r in reps)}"
    )
    print(f"{'step':55s} {'cold':>7s} {'median':>7s}  all")
    labels = [t[0] for t in reps[0]["timings"]]
    for idx, label in enumerate(labels):
        ts = [r["timings"][idx][1] for r in reps]
        print(
            f"{label:55s} {ts[0]:7.3f} {statistics.median(ts):7.3f}  {' '.join(f'{t:.3f}' for t in ts)}"
        )
    totals = [sum(t for _, t in r["timings"]) for r in reps]
    print(f"{'TOTAL':55s} {totals[0]:7.3f} {statistics.median(totals):7.3f}")
    if last["compare"]:
        print(json.dumps(last["compare"], indent=1, default=str))
    if args.out:
        args.out.write_text(json.dumps(reps, indent=1, default=str))


if __name__ == "__main__":
    main()
