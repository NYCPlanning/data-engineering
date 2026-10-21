import math
import random
import struct

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb

from dcpy.geospatial.curves import (
    ARC_TYPE,
    CurveStats,
    linearize_arc,
    linearize_batch,
    linearize_circle,
    split_curve_wkb,
)
from dcpy.geospatial.curves.write import write_geoparquet

TOL = 0.00025


# --- WKB builders -----------------------------------------------------------
def _h(code: int) -> bytes:
    return struct.pack("<BI", 1, code)


def _pts(pts, dims=2) -> bytes:
    return struct.pack("<I", len(pts)) + b"".join(
        struct.pack(f"<{dims}d", *p) for p in pts
    )


def line(pts):
    return _h(2) + _pts(pts)


def circ(pts):
    return _h(8) + _pts(pts)


def compound(*parts):
    return _h(9) + struct.pack("<I", len(parts)) + b"".join(parts)


def linear_ring(pts):
    return _pts(pts)


def polygon(*rings):
    return _h(3) + struct.pack("<I", len(rings)) + b"".join(map(linear_ring, rings))


def curvepoly(*rings):
    return _h(10) + struct.pack("<I", len(rings)) + b"".join(rings)


def multi(code, *kids):
    return _h(code) + struct.pack("<I", len(kids)) + b"".join(kids)


def _arc_on_circle(cx, cy, r, a0, a1):
    am = (a0 + a1) / 2
    return [(cx + r * math.cos(a), cy + r * math.sin(a)) for a in (a0, am, a1)]


# --- linearizer -------------------------------------------------------------
def test_arc_exact_endpoints_and_both_directions():
    a, m, b = (0.0, 0.0), (1.0, 1.0), (2.0, 0.0)  # clockwise a->b
    cw = linearize_arc(a, m, b)
    ccw = linearize_arc(a, (1.0, -1.0), b)
    assert cw[0] == a and cw[-1] == b
    assert all(y > 0 for _, y in cw[1:-1])
    assert all(y < 0 for _, y in ccw[1:-1])
    assert len(cw) > 50


def test_arc_deviation_within_tolerance():
    rng = random.Random(1)
    for _ in range(50):
        r = 10 ** rng.uniform(-2, 4)
        a0 = rng.uniform(0, 6)
        pts = _arc_on_circle(5.0, 7.0, r, a0, a0 + rng.uniform(0.01, 3.0))
        out = linearize_arc(*pts)
        assert out[0] == pts[0] and out[-1] == pts[2]
        # vertices are on the circle; chord midpoints are the worst case
        for p, q in zip(out, out[1:]):
            mid = ((p[0] + q[0]) / 2, (p[1] + q[1]) / 2)
            assert abs(math.hypot(mid[0] - 5, mid[1] - 7) - r) <= TOL * 1.001 + 1e-9 * r


def test_symmetry_random_arcs():
    rng = random.Random(2)
    for _ in range(200):
        r = 10 ** rng.uniform(-1, 4)
        a0 = rng.uniform(-3, 3)
        a, m, b = _arc_on_circle(
            rng.uniform(-1e5, 1e5),
            rng.uniform(-1e5, 1e5),
            r,
            a0,
            a0 + rng.uniform(0.1, 5),
        )
        assert linearize_arc(b, m, a) == linearize_arc(a, m, b)[::-1]


def test_segment_count_tiny_and_huge_radius():
    tiny = linearize_arc(*_arc_on_circle(0, 0, 0.0001, 0, 1.0))
    assert tiny == [tiny[0], tiny[-1]]  # sagitta < tol: straight
    huge = linearize_arc(*_arc_on_circle(0, 0, 1e9, 0, 1e-6))
    assert len(huge) < 200
    flat = linearize_arc(*_arc_on_circle(0, 0, 1e9, 0, 1e-9))
    assert flat == [flat[0], flat[-1]]
    mid = linearize_arc(*_arc_on_circle(0, 0, 1000.0, 0, 1.0))
    assert 20 < len(mid) < 2000


def test_collinear_and_zero_length():
    assert linearize_arc((0, 0), (1, 1), (2, 2)) == [(0, 0), (2, 2)]
    assert linearize_arc((0, 0), (0, 0), (0, 0)) == [(0, 0)]
    assert linearize_arc((0, 0), (2, 2), (1, 1)) == [(0, 0), (1, 1)]


def test_full_circle_keeps_stored_start_and_reverses():
    start, opposite = (3.0, 4.0), (1.0, 4.0)  # circle centered (2, 4), r=1
    ccw = linearize_circle(start, opposite)
    cw = linearize_circle(start, opposite, ccw=False)
    assert ccw[0] == ccw[-1] == start == cw[0] == cw[-1]
    assert cw == ccw[::-1]
    assert any(math.hypot(x - 1, y - 4) < 1e-9 for x, y in ccw)
    assert linearize_arc(start, opposite, start) == ccw


# --- WKB splitting ----------------------------------------------------------
def test_circularstring_to_linestring_with_arc():
    wkb = circ([(0, 0), (1, 1), (2, 0)])
    out, arcs = split_curve_wkb(wkb)
    g = from_wkb(out)
    assert (
        g.geom_type == "LineString" and g.coords[0] == (0, 0) and g.coords[-1] == (2, 0)
    )
    assert len(arcs) == 1
    assert arcs[0][:3] == (0, -1, 0) and arcs[0][3] == 0
    assert arcs[0][4] == len(g.coords) - 1 and arcs[0][5:] == (0, 0, 1, 1, 2, 0)


def test_compound_curve_mixes_lines_and_arcs():
    wkb = compound(
        line([(0, 0), (1, 0)]), circ([(1, 0), (2, 1), (3, 0)]), line([(3, 0), (4, 0)])
    )
    out, arcs = split_curve_wkb(wkb)
    g = from_wkb(out)
    assert g.geom_type == "LineString"
    assert g.coords[0] == (0, 0) and g.coords[-1] == (4, 0)
    assert len(arcs) == 1 and (arcs[0].x0, arcs[0].x2) == (1, 3)
    cs = list(g.coords)
    assert cs[arcs[0].v0] == (1, 0) and cs[arcs[0].v1] == (3, 0) and arcs[0].v0 == 1
    assert len(set(g.coords)) == len(g.coords)  # joints not duplicated


def test_zero_length_arc_misread_as_circle_is_dropped():
    # Centerline SegmentID 358043 as GDAL returns it: its last FGDB arc has no
    # length, but its (start angle, central angle) = (0, 2pi) come back as the
    # opposite point of a full circle (~2.98M ft once linearized).
    a = (939246.346435547, 144568.58782959)
    b = (939190.180236816, 144622.737609863)
    c = (939189.24621582, 144622.686401367)
    real_arc = circ([b, (939189.713294443, 144622.710763048), c])
    misread = circ(
        [c, (541902.82471594, 541909.107901247), (0.0, 2 * math.pi),
         (397286.42149988, -397280.138314573), c]
    )  # fmt: skip
    wkb = multi(
        11,
        compound(line([a, (939224.034423828, 144601.90423584), b]), real_arc, misread),
    )
    stats = CurveStats()
    linear, arcs = split_curve_wkb(wkb, TOL, stats=stats)
    g = from_wkb(linear)
    assert stats.point_arcs == 1
    assert len(arcs) == 1 and (arcs[0].x2, arcs[0].y2) == c
    assert g.geoms[0].coords[-1] == c
    assert abs(g.length - 80.7838368475719) < 1e-3  # source Shape_Length

    # a genuine closed circle near the origin is kept
    ring = circ([(3, 4), (2, 5), (1, 4), (2, 3), (3, 4)])
    _, arcs = split_curve_wkb(ring, TOL, stats=stats)
    assert len(arcs) == 2 and stats.point_arcs == 1


def test_curvepolygon_with_arc_in_hole():
    shell = line([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    hole = compound(circ([(4, 5), (5, 6), (6, 5)]), line([(6, 5), (4, 5)]))
    out, arcs = split_curve_wkb(curvepoly(shell, hole))
    g = from_wkb(out)
    assert g.geom_type == "Polygon" and len(g.interiors) == 1
    assert [(a.part, a.ring, a.seq) for a in arcs] == [(0, 1, 0)]
    assert g.interiors[0].coords[0] == (4, 5) and g.interiors[0].coords[-1] == (4, 5)
    assert g.is_valid


def test_multisurface_parts_and_multiness():
    p0 = curvepoly(compound(circ([(0, 0), (1, 1), (2, 0)]), line([(2, 0), (0, 0)])))
    p1 = polygon([(5, 5), (6, 5), (6, 6), (5, 5)])
    p2 = curvepoly(
        compound(circ([(10, 0), (11, 1), (12, 0)]), circ([(12, 0), (11, -1), (10, 0)]))
    )
    out, arcs = split_curve_wkb(multi(12, p0, p1, p2))
    g = from_wkb(out)
    assert g.geom_type == "MultiPolygon" and len(g.geoms) == 3
    assert [(a.part, a.ring, a.seq) for a in arcs] == [(0, 0, 0), (2, 0, 0), (2, 0, 1)]
    one, _ = split_curve_wkb(multi(12, p1))
    assert from_wkb(one).geom_type == "MultiPolygon"


def test_multicurve_and_plain_passthrough():
    out, arcs = split_curve_wkb(
        multi(11, circ([(0, 0), (1, 1), (2, 0)]), line([(5, 5), (6, 6)]))
    )
    assert from_wkb(out).geom_type == "MultiLineString" and len(arcs) == 1
    for wkb in (
        _h(1) + struct.pack("<2d", 1, 2),
        line([(0, 0), (1, 1)]),
        polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
        multi(6, polygon([(0, 0), (1, 0), (1, 1), (0, 0)])),
    ):
        o, a = split_curve_wkb(wkb)
        assert o == wkb and a == []


def test_shared_arc_gives_identical_vertices_on_both_owners():
    a, m, b = (0.0, 0.0), (1.0, 1.0), (2.0, 0.0)
    top = from_wkb(
        split_curve_wkb(curvepoly(compound(circ([a, m, b]), line([b, a]))))[0]
    )
    bot = from_wkb(
        split_curve_wkb(curvepoly(compound(circ([b, m, a]), line([a, b]))))[0]
    )
    n = len(linearize_arc(a, m, b))
    top_arc = list(top.exterior.coords)[:n]  # a -> b
    bot_arc = list(bot.exterior.coords)[:n]  # b -> a
    assert top_arc == bot_arc[::-1]


def test_zm_is_stripped_and_counted():
    iso_z = struct.pack("<BI", 1, 1002) + _pts([(0, 0, 9), (1, 1, 9)], dims=3)
    iso_zm_circ = struct.pack("<BI", 1, 3008) + _pts(
        [(0, 0, 1, 2), (1, 1, 1, 2), (2, 0, 1, 2)], dims=4
    )
    stats = CurveStats()
    o1, _ = split_curve_wkb(iso_z, stats=stats)
    o2, arcs = split_curve_wkb(iso_zm_circ, stats=stats)
    assert list(from_wkb(o1).coords) == [(0, 0), (1, 1)]
    assert not from_wkb(o2).has_z and len(arcs) == 1
    assert stats.zm_stripped == 2 and stats.geometries == 2 and stats.arcs == 1


# --- arrow / parquet --------------------------------------------------------
def _batch():
    wkbs = [
        circ([(0, 0), (1, 1), (2, 0)]),
        None,
        multi(
            12,
            curvepoly(compound(circ([(0, 0), (1, 1), (2, 0)]), line([(2, 0), (0, 0)]))),
        ),
    ]
    return pa.record_batch(
        [pa.array([1, 2, 3]), pa.array(wkbs, type=pa.binary())],
        names=["id", "wkb_geometry"],
    )


def test_linearize_batch_columns():
    out = linearize_batch(_batch())
    assert out.schema.names == ["id", "wkb_geometry", "arcs"]
    assert out.schema.field("arcs").type == pa.list_(ARC_TYPE)
    arcs = out.column("arcs").to_pylist()
    assert [len(a) for a in arcs] == [1, 0, 1]
    assert arcs[0][0]["x1"] == 1.0


def test_geoparquet_roundtrip(tmp_path):
    import json

    path = tmp_path / "o.parquet"
    n = write_geoparquet(
        [linearize_batch(_batch()), linearize_batch(_batch())], path, crs="EPSG:2263"
    )
    assert n == 6
    t = pq.read_table(path)
    assert t.num_rows == 6
    geo = json.loads(pq.read_metadata(path).metadata[b"geo"])
    col = geo["columns"]["wkb_geometry"]
    assert geo["version"] == "1.1.0" and col["encoding"] == "WKB"
    assert col["geometry_types"] == ["LineString", "MultiPolygon"]
    assert col["crs"]["id"]["code"] == 2263


def test_unclosed_ring_is_closed_and_counted():
    stats = CurveStats()
    out, _ = split_curve_wkb(
        curvepoly(
            compound(line([(0, 0), (4, 0), (4, 4)]), circ([(4, 4), (2, 5), (0, 4)]))
        ),
        stats=stats,
    )
    g = from_wkb(out)
    assert g.exterior.is_closed and stats.closed_rings == 1


def test_layer_to_parquet_adds_ogc_fid_in_source_order(tmp_path):
    import geopandas as gpd
    from shapely.geometry import LineString

    from dcpy.geospatial.curves.layer import layer_to_parquet

    src = tmp_path / "src.gpkg"
    gpd.GeoDataFrame(
        {"name": ["a", "b", "c"]},
        geometry=[LineString([(0, 0), (i + 1, 1)]) for i in range(3)],
        crs="EPSG:2263",
    ).to_file(src, layer="lyr", driver="GPKG")
    n, _ = layer_to_parquet(src, "lyr", tmp_path / "l.parquet", tmp_path / "a.parquet")
    t = pq.read_table(tmp_path / "l.parquet")
    assert n == 3
    assert t.column_names[0] == "ogc_fid"
    assert t.column("ogc_fid").to_pylist() == [1, 2, 3]
    assert t.column("name").to_pylist() == ["a", "b", "c"]
