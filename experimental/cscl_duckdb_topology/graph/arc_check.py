"""Compare arcs rebuilt in DuckDB from their control points (sql/arc_reconstruct.sql)
with the linearized arcs carried over from PG (data/arc_lines.parquet).

    python arc_check.py   # after run.py
"""

import time
from pathlib import Path

import duckdb

HERE = Path(__file__).parent


def main() -> None:
    con = duckdb.connect()
    con.sql("LOAD spatial")
    con.sql(f"ATTACH '{HERE / 'data' / 'graph.duckdb'}' AS d (READ_ONLY)")
    con.sql("USE d")
    t = time.perf_counter()
    con.sql(
        "CREATE TEMP TABLE rebuilt AS "
        + (HERE / "sql" / "arc_reconstruct.sql").read_text()
    )
    print(
        f"rebuild {con.sql('SELECT count(*) FROM rebuilt').fetchone()[0]:,d} arcs: "
        f"{time.perf_counter() - t:.2f}s"
    )
    print(
        con.sql("""
        WITH carried AS (
            SELECT DISTINCT node_lo, node_hi, geom FROM d.edges_unnoded WHERE is_arc
        ),
        j AS (
            SELECT r.collinear, r.geom AS g_duck, c.geom AS g_pg
            FROM rebuilt AS r JOIN carried AS c USING (node_lo, node_hi)
        ),
        dev AS (
            SELECT
                *,
                st_npoints(g_duck) = st_npoints(g_pg) AS same_npoints,
                st_aswkb(g_duck) = st_aswkb(g_pg) AS identical,
                greatest(
                    list_max([st_distance(p.geom, g_pg) FOR p IN st_dump(st_points(g_duck))]),
                    list_max([st_distance(p.geom, g_duck) FOR p IN st_dump(st_points(g_pg))])
                ) AS max_vertex_dev
            FROM j
        )
        SELECT
            count(*) AS arcs,
            count(*) FILTER (WHERE collinear) AS collinear,
            count(*) FILTER (WHERE identical) AS bit_identical,
            count(*) FILTER (WHERE same_npoints) AS same_npoints,
            count(*) FILTER (WHERE max_vertex_dev > 1e-9) AS dev_gt_1e9,
            count(*) FILTER (WHERE max_vertex_dev > 1e-6) AS dev_gt_1e6,
            max(max_vertex_dev) AS max_dev_ft
        FROM dev
    """)
    )


if __name__ == "__main__":
    main()
