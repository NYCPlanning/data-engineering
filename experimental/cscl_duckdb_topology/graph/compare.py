"""Compare the DuckDB graph (data/graph.duckdb, from run.py) with PG's tables
(data/pg_*.parquet, from export_from_pg.py).

Two checks per table:
  by id     - exact equality on the PG keys (node ids are assigned deterministically
              from coordinate order in both engines, so they should agree)
  by coords - order-independent: nodes as coordinate pairs, edges as their (unordered)
              endpoint coordinate pair + is_arc + atomicid, so a different id assignment
              would not hide or cause a difference
Geometry equality is exact: both sides are normalized through DuckDB's ST_AsWKB.
"""

from pathlib import Path

import duckdb

DATA = Path(__file__).parent / "data"

# (name, sql returning a single mismatch count). `d` = DuckDB schema, PG parquet via pg_*.
CHECKS = {
    "exact_points by id": """
        SELECT count(*) FROM (
            (SELECT point_id, x, y, vertex_count FROM d.exact_points
             EXCEPT ALL SELECT point_id, x, y, vertex_count FROM pg_exact_points)
            UNION ALL
            (SELECT point_id, x, y, vertex_count FROM pg_exact_points
             EXCEPT ALL SELECT point_id, x, y, vertex_count FROM d.exact_points))
    """,
    "grid_cells by cell": """
        SELECT count(*) FROM (
            (SELECT x_cell, y_cell, st_x(geom) x, st_y(geom) y, exact_point_ids FROM d.grid_cells
             EXCEPT ALL SELECT x_cell, y_cell, x, y, list_sort(exact_point_ids) FROM pg_grid_cells)
            UNION ALL
            (SELECT x_cell, y_cell, x, y, list_sort(exact_point_ids) FROM pg_grid_cells
             EXCEPT ALL SELECT x_cell, y_cell, st_x(geom), st_y(geom), exact_point_ids FROM d.grid_cells))
    """,
    "point_to_node by id": """
        SELECT count(*) FROM (
            (FROM d.point_to_node EXCEPT ALL FROM pg_point_to_node)
            UNION ALL (FROM pg_point_to_node EXCEPT ALL FROM d.point_to_node))
    """,
    "point_to_node_using_key vs point_to_node (DuckDB)": """
        SELECT count(*) FROM (
            (FROM d.point_to_node EXCEPT ALL FROM d.point_to_node_using_key)
            UNION ALL (FROM d.point_to_node_using_key EXCEPT ALL FROM d.point_to_node))
    """,
    # clusters as sets of member coordinates, independent of node ids
    "point_to_node by coords": """
        WITH dk AS (
            SELECT list_sort(list([e.x, e.y])) AS members
            FROM d.point_to_node p JOIN d.exact_points e ON p.exact_point_id = e.point_id
            GROUP BY p.node_id),
        pk AS (
            SELECT list_sort(list([e.x, e.y])) AS members
            FROM pg_point_to_node p JOIN pg_exact_points e ON p.exact_point_id = e.point_id
            GROUP BY p.node_id)
        SELECT count(*) FROM ((FROM dk EXCEPT ALL FROM pk) UNION ALL (FROM pk EXCEPT ALL FROM dk))
    """,
    "nodes by id": """
        SELECT count(*) FROM (
            (SELECT node_id, x, y FROM d.nodes EXCEPT ALL FROM pg_nodes)
            UNION ALL (FROM pg_nodes EXCEPT ALL SELECT node_id, x, y FROM d.nodes))
    """,
    "nodes by coords": """
        SELECT count(*) FROM (
            (SELECT x, y FROM d.nodes EXCEPT ALL SELECT x, y FROM pg_nodes)
            UNION ALL (SELECT x, y FROM pg_nodes EXCEPT ALL SELECT x, y FROM d.nodes))
    """,
    "edges_unnoded by id (incl. geometry)": """
        SELECT count(*) FROM (
            (SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(geom) FROM d.edges_unnoded
             EXCEPT ALL
             SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(st_geomfromwkb(wkb)) FROM pg_edges_unnoded)
            UNION ALL
            (SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(st_geomfromwkb(wkb)) FROM pg_edges_unnoded
             EXCEPT ALL
             SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(geom) FROM d.edges_unnoded))
    """,
    "edges_unnoded by coords": "{edges_by_coords:edges_unnoded}",
    "edge_crossings by id": """
        SELECT count(*) FROM (
            (SELECT node_lo, node_hi, node_id, x, y FROM d.edge_crossings EXCEPT ALL FROM pg_edge_crossings)
            UNION ALL (FROM pg_edge_crossings EXCEPT ALL SELECT node_lo, node_hi, node_id, x, y FROM d.edge_crossings))
    """,
    "edges by id (incl. geometry)": """
        SELECT count(*) FROM (
            (SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(geom) FROM d.edges
             EXCEPT ALL
             SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(st_geomfromwkb(wkb)) FROM pg_edges)
            UNION ALL
            (SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(st_geomfromwkb(wkb)) FROM pg_edges
             EXCEPT ALL
             SELECT node_lo, node_hi, is_arc, atomicid, water_flag, st_aswkb(geom) FROM d.edges))
    """,
    "edges by coords": "{edges_by_coords:edges}",
}

EDGES_BY_COORDS = """
    WITH dk AS (
        SELECT
            list_sort([[st_x(st_startpoint(geom)), st_y(st_startpoint(geom))],
                       [st_x(st_endpoint(geom)), st_y(st_endpoint(geom))]]) AS ends,
            is_arc, atomicid
        FROM d.{t}),
    pk AS (
        SELECT
            list_sort([[st_x(st_startpoint(g)), st_y(st_startpoint(g))],
                       [st_x(st_endpoint(g)), st_y(st_endpoint(g))]]) AS ends,
            is_arc, atomicid
        FROM (SELECT *, st_geomfromwkb(wkb) AS g FROM pg_{t}))
    SELECT count(*) FROM ((FROM dk EXCEPT ALL FROM pk) UNION ALL (FROM pk EXCEPT ALL FROM dk))
"""


def main() -> None:
    con = duckdb.connect()
    con.sql("LOAD spatial")
    con.sql(f"ATTACH '{DATA / 'graph.duckdb'}' AS d (READ_ONLY)")
    for f in DATA.glob("pg_*.parquet"):
        con.sql(f"CREATE VIEW {f.stem} AS FROM '{f}'")
    for name, sql in CHECKS.items():
        if sql.startswith("{edges_by_coords:"):
            sql = EDGES_BY_COORDS.format(t=sql.split(":")[1].rstrip("}"))
        n = con.sql(sql).fetchone()[0]
        print(f"{'OK  ' if n == 0 else 'DIFF'} {name}: {n} mismatched rows")


if __name__ == "__main__":
    main()
