"""The DuckDB macros in macros/duckdb_functions.sql, run against an in-memory DuckDB.

Expected values come from each macro's documented behaviour, plus a few curved centerlines
from the 26c production snapshot (fgdb_lion) for find_circle.
"""

import math
from pathlib import Path

import duckdb
import pytest

MACROS = Path(__file__).parent.parent / "macros" / "duckdb_functions.sql"


@pytest.fixture(scope="module")
def duck():
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
    except duckdb.Error:
        con.execute("INSTALL spatial; LOAD spatial")
    # the file is a dbt macro (run at run start); drop the jinja wrapper lines
    sql = "\n".join(
        line for line in MACROS.read_text().splitlines() if not line.startswith("{%")
    )
    con.execute(sql)
    return con


def _one(duck, sql: str, params=None):
    return duck.execute(sql, params or []).fetchone()[0]


@pytest.mark.parametrize(
    "value, n, kwargs, expected",
    [
        ("12", 5, "", "00012"),
        ("12", 5, ", fill := ' ', left_ := TRUE", "12   "),
        ("12", 5, ", left_ := TRUE", "12000"),
        ("1234567", 3, "", "123"),  # truncates on the right
        ("1234567", 3, ", left_ := TRUE", "123"),
        ("", 3, "", "000"),
        (None, 4, "", "0000"),
        (None, 4, ", blank_if_none := TRUE", "    "),
        (None, 4, ", blank_if_none := TRUE, left_ := TRUE", "    "),
        ("7", 3, ", blank_if_none := TRUE", "007"),  # only NULL is blanked
    ],
)
def test_format_lion_text(duck, value, n, kwargs, expected):
    assert _one(
        duck, f"SELECT format_lion_text(?::VARCHAR, ?{kwargs})", [value, n]
    ) == (expected)


@pytest.mark.parametrize(
    "level_code, feature_type, expected",
    [
        (1, "centerline", "A"),
        (13, "centerline", "M"),
        (26, "centerline", "Z"),
        (27, "centerline", None),
        (0, "centerline", None),
        (None, "centerline", None),
        (99, "centerline", "*"),
        (3, "nonstreetfeatures", "*"),
        (None, "nonstreetfeatures", "*"),
        (3, "shoreline", "$"),
        (99, "shoreline", "$"),  # shoreline wins over 99
        (5, None, "E"),
    ],
)
def test_convert_level_code(duck, level_code, feature_type, expected):
    assert (
        _one(
            duck,
            "SELECT convert_level_code(?::INTEGER, ?::VARCHAR)",
            [level_code, feature_type],
        )
        == expected
    )


def _circle(duck, p1, p2, p3):
    return duck.execute(
        "SELECT st_x(c), st_y(c), st_z(c) FROM (SELECT find_circle("
        "st_point(?, ?), st_point(?, ?), st_point(?, ?)) AS c)",
        [*p1, *p2, *p3],
    ).fetchone()


@pytest.mark.parametrize(
    "p1, p2, p3, center, radius",
    [
        ((0, 0), (1, 1), (2, 0), (1, 0), 1),
        ((2, 0), (1, 1), (0, 0), (1, 0), 1),  # point order doesn't matter
        ((15, 10), (10, 15), (5, 10), (10, 10), 5),
        ((10, 0), (0, 10), (-10, 0), (0, 0), 10),
    ],
)
def test_find_circle(duck, p1, p2, p3, center, radius):
    cx, cy, r = _circle(duck, p1, p2, p3)
    assert cx == pytest.approx(center[0], abs=1e-9)
    assert cy == pytest.approx(center[1], abs=1e-9)
    assert r == pytest.approx(radius, abs=1e-9)


def test_find_circle_collinear_is_null(duck):
    assert (
        _one(duck, "SELECT find_circle(st_point(0, 0), st_point(1, 1), st_point(2, 2))")
        is None
    )


# 26c fgdb_lion curved centerlines: start, midpoint (st_lineinterpolatepoint 0.5) and end
# of the shape, as int__primary_segments takes them, with prod's arccenterx/y and radius.
# Prod's radius is the distance from the rounded center to the start point (see
# int__centerline_curve).
PROD_CURVES = [
    (
        "0000136",
        (914964.0404189974, 126559.30422922969),
        (914895.0283825896, 126522.66301642745),
        (914817.8844120651, 126510.2514269054),
        (914819, 126749, 239),
    ),
    (
        "0000282",
        (916798.1982061267, 125363.1147724092),
        (916640.8416866324, 125259.51206574094),
        (916472.6409906596, 125174.64076346159),
        (915819, 126679, 1640),
    ),
    (
        "0000061",
        (914209.9387831837, 122562.90403941274),
        (914273.6861566544, 122462.478597859),
        (914353.6773900092, 122374.44223046303),
        (914824, 122882, 692),
    ),
]


@pytest.mark.parametrize(
    "start, mid, end, prod",
    [c[1:] for c in PROD_CURVES],
    ids=[c[0] for c in PROD_CURVES],
)
def test_find_circle_matches_prod(duck, start, mid, end, prod):
    cx, cy, r = _circle(duck, start, mid, end)
    rounded = (round(cx), round(cy))
    assert rounded == prod[:2]
    assert round(math.dist(rounded, start)) == prod[2]
    # the fitted radius reaches all three points
    for p in (start, mid, end):
        assert math.dist((cx, cy), p) == pytest.approx(r, abs=1e-6)


def _offsets(duck, line_wkt, offset_length=None):
    arg = "" if offset_length is None else f", offset_length := {offset_length}"
    return duck.execute(
        "SELECT st_x(o.left_offset_point), st_y(o.left_offset_point), "
        "st_x(o.right_offset_point), st_y(o.right_offset_point) "
        f"FROM (SELECT offset_points(st_geomfromtext(?){arg}) AS o)",
        [line_wkt],
    ).fetchone()


L_SHAPE = "LINESTRING (0 0, 10 0, 10 10)"


@pytest.mark.parametrize(
    "line, offset_length, expected",
    [
        # eastbound: left is north
        ("LINESTRING (0 0, 10 0)", 2, (5, 2, 5, -2)),
        # westbound: left is south
        ("LINESTRING (10 0, 0 0)", 2, (5, -2, 5, 2)),
        # default offset_length is 1
        ("LINESTRING (0 0, 10 0)", None, (5, 1, 5, -1)),
        # half point on the second (northbound) piece: left is west
        ("LINESTRING (0 0, 10 0, 10 30)", 2, (8, 10, 12, 10)),
        # half point on the shared vertex: the piece ending there
        (L_SHAPE, 2, (10, 2, 10, -2)),
        # doubled back (east, west, east, north): half of 35 is on the westbound leg,
        # though the eastbound legs pass through the same point
        ("LINESTRING (0 0, 10 0, 0 0, 10 0, 10 5)", 2, (2.5, -2, 2.5, 2)),
        # same shape, longer last leg: half of 50 is on the third (eastbound) leg
        ("LINESTRING (0 0, 10 0, 0 0, 10 0, 10 20)", 2, (5, 2, 5, -2)),
        # half point a rounding error past a vertex: still the piece ending there, not
        # the direction of the 1e-10 ft sliver after it
        ("LINESTRING (0 0, 10 0, 10 10.0000000002)", 2, (10, 2, 10, -2)),
        # a repeated vertex at the half point: the last distinct vertex before it
        ("LINESTRING (0 0, 10 0, 10 0, 10 10)", 2, (10, 2, 10, -2)),
        # diagonal: offsets along the unit normal
        ("LINESTRING (0 0, 3 4)", 5, (-2.5, 5, 5.5, -1)),
    ],
)
def test_offset_points(duck, line, offset_length, expected):
    assert _offsets(duck, line, offset_length) == pytest.approx(expected, abs=1e-9)


def test_offset_points_not_a_linestring_is_null(duck):
    assert (
        _one(
            duck,
            "SELECT offset_points(st_geomfromtext('MULTILINESTRING ((0 0, 1 0))'))",
        )
        is None
    )


def test_offset_points_on_table_columns(duck):
    # the macro is called per row on table columns (int__segment_offsets)
    rows = duck.execute(
        """
        SELECT id, st_astext(o.left_offset_point), st_astext(o.right_offset_point)
        FROM (
            SELECT id, offset_points(st_geomfromtext(wkt), offset_length := 2) AS o
            FROM (VALUES
                (1, 'LINESTRING (0 0, 10 0)'),
                (2, 'LINESTRING (0 0, 10 0, 10 30)')
            ) AS t (id, wkt)
        )
        ORDER BY id
        """
    ).fetchall()
    assert rows == [
        (1, "POINT (5 2)", "POINT (5 -2)"),
        (2, "POINT (8 10)", "POINT (12 10)"),
    ]


@pytest.mark.parametrize(
    "wkt, expected",
    [
        # contiguous parts merge into one LineString
        ("MULTILINESTRING ((0 0, 1 0), (1 0, 2 1))", "LINESTRING (0 0, 1 0, 2 1)"),
        # disjoint parts stay a MultiLineString
        (
            "MULTILINESTRING ((0 0, 1 0), (5 5, 6 5))",
            "MULTILINESTRING ((0 0, 1 0), (5 5, 6 5))",
        ),
        # anything else passes through unchanged
        ("LINESTRING (0 0, 1 0, 2 1)", "LINESTRING (0 0, 1 0, 2 1)"),
        ("POLYGON ((0 0, 1 0, 1 1, 0 0))", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
        ("POINT (1 2)", "POINT (1 2)"),
    ],
)
def test_linearize(duck, wkt, expected):
    assert _one(duck, "SELECT st_astext(linearize(st_geomfromtext(?)))", [wkt]) == (
        expected
    )
