"""Export the PG inputs (and PG's finished outputs, for comparison) to Parquet.

Inputs for the DuckDB build:
  vertices.parquet  - int__topology__vertices, point geometry as exact x/y doubles
  arc_lines.parquet - one canonical linearized arc per arc edge, keyed by its endpoint
                      coordinates (DuckDB spatial has no curve types; see RESULTS.md)

PG reference outputs (pg_*.parquet) are only read by compare.py.

    python export_from_pg.py --schema ar_cscl_districts_gdb_db_qa_oct4

Needs BUILD_ENGINE_SERVER (load the cscl direnv first).
"""

import argparse
import os
import time
from pathlib import Path

import duckdb

DATA = Path(__file__).parent / "data"

EXPORTS = {
    "vertices": """
        SELECT atomicid, water_flag, part, ring, seq, is_arc_mid,
               st_x(geom) AS x, st_y(geom) AS y
        FROM {s}.int__topology__vertices
    """,
    "arc_lines": """
        SELECT DISTINCT ON (node_lo, node_hi)
            st_x(st_startpoint(geom)) AS lo_x, st_y(st_startpoint(geom)) AS lo_y,
            st_x(st_endpoint(geom)) AS hi_x, st_y(st_endpoint(geom)) AS hi_y,
            st_asbinary(geom) AS wkb
        FROM {s}.int__topology__edges_unnoded
        WHERE is_arc
        ORDER BY node_lo, node_hi
    """,
    "pg_exact_points": """
        SELECT point_id, st_x(geom) AS x, st_y(geom) AS y, vertex_count
        FROM {s}.int__topology__exact_points
    """,
    "pg_grid_cells": """
        SELECT x_cell, y_cell, st_x(geom) AS x, st_y(geom) AS y, exact_point_ids
        FROM {s}.int__topology__grid_cells
    """,
    "pg_point_to_node": "SELECT exact_point_id, node_id FROM {s}.int__topology__point_to_node",
    "pg_nodes": """
        SELECT node_id, st_x(geom) AS x, st_y(geom) AS y FROM {s}.int__topology__nodes
    """,
    "pg_edges_unnoded": """
        SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_asbinary(geom) AS wkb
        FROM {s}.int__topology__edges_unnoded
    """,
    "pg_edge_crossings": """
        SELECT node_lo, node_hi, node_id, st_x(geom) AS x, st_y(geom) AS y
        FROM {s}.int__topology__edge_crossings
    """,
    "pg_edges": """
        SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_asbinary(geom) AS wkb
        FROM {s}.int__topology__edges
    """,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=True)
    parser.add_argument("--db", default="db-cscl")
    args = parser.parse_args()

    DATA.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.sql("LOAD postgres")
    dsn = f"{os.environ['BUILD_ENGINE_SERVER']}/{args.db}"
    con.sql(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_ONLY)")
    for name, sql in EXPORTS.items():
        t0 = time.perf_counter()
        out = DATA / f"{name}.parquet"
        query = sql.format(s=args.schema).replace("'", "''")
        con.sql(
            f"COPY (SELECT * FROM postgres_query('pg', '{query}')) TO '{out}' (FORMAT parquet)"
        )
        n = con.sql(f"SELECT count(*) FROM '{out}'").fetchone()[0]
        print(f"{name:20s} {n:>9,d} rows  {time.perf_counter() - t0:6.1f}s")


if __name__ == "__main__":
    main()
