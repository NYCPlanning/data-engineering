"""Parity: DuckDB macros (macros/duckdb_functions.sql) vs the plpgsql functions.

Postgres side runs through psql against the build DB (BUILD_ENGINE_SERVER, db-cscl,
schema ar_cscl_gdb_lion_fixes by default). Skipped when psql or the DB is unavailable.

    pytest products/cscl/tests/test_duckdb_function_parity.py
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

SCHEMA = os.environ.get("CSCL_PARITY_SCHEMA", "ar_cscl_gdb_lion_fixes")
MACROS = Path(__file__).parent.parent / "macros" / "duckdb_functions.sql"
TOL = 1e-6
N = 12


def _pg(sql: str):
    server = os.environ.get("BUILD_ENGINE_SERVER")
    if not server or not shutil.which("psql"):
        pytest.skip("no Postgres access")
    r = subprocess.run(
        ["psql", f"{server}/db-cscl", "-Atc", sql],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if r.returncode != 0:
        pytest.skip(f"Postgres unavailable: {r.stderr.strip()[:200]}")
    return json.loads(r.stdout)


@pytest.fixture(scope="module")
def duck():
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
    except duckdb.Error:
        con.execute("INSTALL spatial; LOAD spatial")
    con.execute(MACROS.read_text())
    return con


def _close(a, b):
    if a is None or b is None:
        return a is b
    return abs(a - b) <= TOL


def test_text_functions(duck):
    cases = [
        ("12", 5, "0", False, False),
        ("12", 5, " ", False, True),
        ("1234567", 3, "0", False, False),
        (None, 4, "0", False, False),
        (None, 4, "0", True, False),
        (None, 4, "0", True, True),
        ("", 3, "0", False, False),
    ]
    levels = [(1, "street"), (26, "street"), (27, "street"), (99, "street"), (None, "street"),
              (3, "shoreline"), (3, "nonstreetfeatures"), (None, "nonstreetfeatures"), (0, None)]  # fmt: skip

    def lit(v):
        return "NULL" if v is None else f"'{v}'" if isinstance(v, str) else str(v)

    pg_text = _pg(
        "select json_agg(format_lion_text(v, n, f, b, l)) from (values "
        + ",".join(
            f"({lit(a)}::varchar,{b},{lit(c)}::varchar,{d},{e})"
            for a, b, c, d, e in cases
        )
        + ") t(v,n,f,b,l)"
    )
    duck_text = [
        duck.execute(
            "select format_lion_text(?::varchar, ?, ?, ?, ?)", [a, b, c, d, e]
        ).fetchone()[0]
        for a, b, c, d, e in cases
    ]
    assert pg_text == duck_text

    pg_lvl = _pg(
        "select json_agg(convert_level_code(l, t)) from (values "
        + ",".join(f"({lit(a)}::integer,{lit(b)}::text)" for a, b in levels)
        + ") x(l,t)"
    )
    duck_lvl = [
        duck.execute(
            "select convert_level_code(?::integer, ?::varchar)", [a, b]
        ).fetchone()[0]
        for a, b in levels
    ]
    assert pg_lvl == duck_lvl


def test_find_circle_real_centerlines(duck):
    rows = _pg(f"""
        select json_agg(r) from (
            select ps.segmentid,
                st_astext(ps.start_point, 15) s, st_astext(ps.midpoint, 15) m,
                st_astext(ps.end_point, 15) e,
                st_x(find_circle(ps.start_point, ps.midpoint, ps.end_point)) cx,
                st_y(find_circle(ps.start_point, ps.midpoint, ps.end_point)) cy,
                st_z(find_circle(ps.start_point, ps.midpoint, ps.end_point)) radius
            from {SCHEMA}.int__primary_segments ps
            join {SCHEMA}.stg__centerline c using (segmentid)
            where c.curve <> 'S' order by ps.segmentid limit {N}
        ) r""")
    assert rows
    # plus a collinear case, which must be NULL on both sides
    for r in rows:
        cx, cy, rad = duck.execute(
            "select st_x(c), st_y(c), st_z(c) from (select find_circle("
            "ST_GeomFromText(?), ST_GeomFromText(?), ST_GeomFromText(?)) c)",
            [r["s"], r["m"], r["e"]],
        ).fetchone()
        assert (
            _close(cx, r["cx"]) and _close(cy, r["cy"]) and _close(rad, r["radius"])
        ), r
    assert duck.execute(
        "select find_circle(ST_Point(0,0), ST_Point(1,1), ST_Point(2,2))"
    ).fetchone() == (None,)


def test_offset_points_real_centerlines(duck):
    rows = _pg(f"""
        select json_agg(r) from (
            select st_astext(s.geom, 15) g, st_astext(s.midpoint, 15) m,
                st_x(o.l) lx, st_y(o.l) ly, st_x(o.r) rx, st_y(o.r) ry
            from (
                select geom, midpoint
                from {SCHEMA}.int__primary_segments
                where st_geometrytype(geom) = 'ST_LineString'
                order by segmentid limit {N}
            ) s
            cross join lateral offset_points(s.geom, s.midpoint, 2)
                as o (l geometry, r geometry)
        ) r""")
    assert rows
    for r in rows:
        out = duck.execute(
            "select st_x(o.left_offset_point), st_y(o.left_offset_point),"
            " st_x(o.right_offset_point), st_y(o.right_offset_point)"
            " from (select offset_points(ST_GeomFromText(?), ST_GeomFromText(?), 2) o)",
            [r["g"], r["m"]],
        ).fetchone()
        exp = (r["lx"], r["ly"], r["rx"], r["ry"])
        assert all(_close(a, b) for a, b in zip(out, exp)), (r, out)
    # non-LineString input is NULL on both sides
    assert duck.execute(
        "select offset_points(ST_GeomFromText('MULTILINESTRING((0 0,1 1))'), ST_Point(0.5,0.5))"
    ).fetchone() == (None,)


def test_linearize_linear_inputs(duck):
    wkts = ["LINESTRING(0 0,1 1)", "MULTILINESTRING((0 0,1 1),(1 1,2 0))"]
    pg = _pg(
        "select json_agg(st_astext(linearize(st_geomfromtext(w)), 15)) from (values "
        + ",".join(f"('{w}')" for w in wkts)
        + ") t(w)"
    )
    got = [
        duck.execute("select ST_AsText(linearize(ST_GeomFromText(?)))", [w]).fetchone()[
            0
        ]
        for w in wkts
    ]
    assert [g.replace(" ", "").replace(".0", "") for g in got] == [
        p.replace(" ", "") for p in pg
    ]
