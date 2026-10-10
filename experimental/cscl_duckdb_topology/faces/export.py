"""Export the PG inputs (and PG reference outputs) for the DuckDB face build to Parquet.

    BUILD_ENGINE_SERVER=postgresql://... python export.py [--schema SCHEMA]

Geometry travels as WKB (ST_AsBinary) and is rebuilt with ST_GeomFromWKB. Edges and
stg__atomicpolygons.geom are already linear in PG (edges are LineStrings with arcs
densified upstream; stg__atomicpolygons.geom = st_makevalid(linearize(raw))), and the
NYPD layers are plain MultiPolygons, so no further linearization is needed - the export
asserts that.
"""

import argparse
import os
import time
from pathlib import Path

import duckdb

DATA = Path(__file__).parent / "data"

# name -> (select list, from); geometry column is always aliased `wkb`
TABLES = {
    # inputs
    "edges": (
        "node_lo, node_hi, is_arc, atomicid, water_flag, st_asbinary(geom) AS wkb",
        "int__topology__edges",
    ),
    "ap_entities": (
        "atomicid, water_flag, bctcb2010, nta2020",
        "int__topology__ap_entities",
    ),
    "atomicpolygons": (
        "atomicid, st_asbinary(geom) AS wkb",
        "stg__atomicpolygons",
    ),
    "nypdprecinct": (
        "globalid, precinct, st_asbinary(geom) AS wkb",
        "stg__nypdprecinct",
    ),
    "nypdpatrolborough": (
        "globalid, patrol_borough, st_asbinary(geom) AS wkb",
        "stg__nypdpatrolborough",
    ),
    "nypdbeat": ("globalid, sector, st_asbinary(geom) AS wkb", "stg__nypdbeat"),
    # PG reference outputs, for comparison
    "pg_cb2010_edges": (
        "entity_id, node_lo, node_hi, is_arc, edge_type",
        "int__boundary__cb2010_edges",
    ),
    "pg_cb2010_raw": (
        "entity_id, st_asbinary(geom) AS wkb",
        "int__boundary__cb2010_raw",
    ),
    "pg_cb2010": ("entity_id, st_asbinary(geom) AS wkb", "int__boundary__cb2010"),
    "pg_cb2010_validity": ("*", "qa__boundary__cb2010_validity"),
    "pg_nta2020_edges": (
        "entity_id, node_lo, node_hi, is_arc, edge_type",
        "int__boundary__nta2020_edges",
    ),
    "pg_nta2020_raw": (
        "entity_id, st_asbinary(geom) AS wkb",
        "int__boundary__nta2020_raw",
    ),
    "pg_nta2020": ("entity_id, st_asbinary(geom) AS wkb", "int__boundary__nta2020"),
    "pg_nta2020_validity": ("*", "qa__boundary__nta2020_validity"),
    "pg_ap_police": ("*", "int__topology__ap_police"),
}

CURVE_CHECK = """
SELECT count(*) FROM {schema}.{table} WHERE st_hasarc(geom)
"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schema", default=os.environ.get("BUILD_ENGINE_SCHEMA"))
    p.add_argument("--db", default="db-cscl")
    p.add_argument("--only", nargs="*", help="export just these names")
    args = p.parse_args()

    DATA.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.sql("LOAD spatial; LOAD postgres;")
    dsn = f"{os.environ['BUILD_ENGINE_SERVER']}/{args.db}"
    escaped = dsn.replace("'", "''")
    con.execute(f"ATTACH '{escaped}' AS pg (TYPE postgres, READ_ONLY)")

    for table in ["int__topology__edges", "stg__atomicpolygons"]:
        sql = CURVE_CHECK.format(schema=args.schema, table=table)
        n = con.execute("SELECT * FROM postgres_query('pg', ?)", [sql]).fetchone()[0]
        assert n == 0, f"{table} has {n} curved geometries"

    for name, (cols, table) in TABLES.items():
        if args.only and name not in args.only:
            continue
        t0 = time.perf_counter()
        pg_sql = f"SELECT {cols} FROM {args.schema}.{table}"
        con.execute(
            "CREATE OR REPLACE TEMP TABLE t AS SELECT * FROM postgres_query('pg', ?)",
            [pg_sql],
        )
        has_wkb = "wkb" in [r[0] for r in con.sql("DESCRIBE t").fetchall()]
        sel = "* EXCLUDE (wkb), ST_GeomFromWKB(wkb) AS geom" if has_wkb else "*"
        out = DATA / f"{name}.parquet"
        con.execute(f"COPY (SELECT {sel} FROM t) TO '{out}' (FORMAT parquet)")
        n = con.sql("SELECT count(*) FROM t").fetchone()[0]
        print(f"{name:20s} {n:>8d} rows  {time.perf_counter() - t0:6.1f}s")


if __name__ == "__main__":
    main()
