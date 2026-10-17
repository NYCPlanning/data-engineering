import os
import struct
from pathlib import Path

import pyarrow as pa
import pytest
from pyogrio.raw import read_arrow

from dcpy.geospatial.curves import (
    ARC_TYPE,
    Arc,
    curve_wkb,
    linearize_batch,
    parquet_to_gdb,
    recurve_batch,
    split_curve_wkb,
)
from dcpy.geospatial.curves.layer import layer_to_parquet
from dcpy.geospatial.curves.read import gdb_xy_grid
from dcpy.geospatial.curves.wkb import CurveStats, _Reader

from .test_curves import circ, compound, curvepoly, line, multi, polygon

RES = 1e-4  # FGDB xy resolution of the CSCL source, in feet
GRID = {"XORIGIN": "-10000", "YORIGIN": "-10000", "XYSCALE": "10000"}


# --- comparison: segments at the GDB's resolution ---------------------------
def _q(x: float, y: float) -> tuple[int, int]:
    return round(x / RES), round(y / RES)


def _segments(pts, arcs: list[Arc]):
    """Ring/line as ([(kind, start, end)], [arc interior point or None])."""
    keys, mids, prev = [], [], 0
    for a in arcs:
        for i in range(prev, a.v0):
            keys.append(("L", _q(*pts[i]), _q(*pts[i + 1])))
            mids.append(None)
        keys.append(("C", _q(a.x0, a.y0), _q(a.x2, a.y2)))
        mids.append((a.x1, a.y1))
        prev = a.v1
    for i in range(prev, len(pts) - 1):
        keys.append(("L", _q(*pts[i]), _q(*pts[i + 1])))
        mids.append(None)
    return keys, mids


def _canon_ring(keys, mids):
    """Orientation- and start-insensitive form of a closed ring."""
    rev_keys = [(k, b, a) for k, a, b in reversed(keys)]
    best, best_rank = ((), []), None
    for ks, ms in ((keys, mids), (rev_keys, mids[::-1])):
        for i in range(len(ks)):
            cand = (tuple(ks[i:] + ks[:i]), ms[i:] + ms[:i])
            # midpoints break ties, e.g. a circle stored as two semicircles
            rank = (cand[0], [_q(*m) if m else (0, 0) for m in cand[1]])
            if best_rank is None or rank < best_rank:
                best, best_rank = cand, rank
    return best


def canon(wkb: bytes):
    """("area", sorted rings of all parts) or ("line", parts in order).

    Polygon rings are pooled across parts so hole nesting and ring order don't
    matter.
    """
    linear, arcs = split_curve_wkb(wkb)
    node = _Reader(linear, CurveStats(), 1.0, 2).geometry()
    by: dict[tuple[int, int], list[Arc]] = {}
    for a in sorted(arcs, key=lambda a: (a.part, a.ring, a.seq)):
        by.setdefault((a.part, a.ring), []).append(a)
    kind, body = node
    parts = list(enumerate(body)) if kind.startswith("Multi") else [(0, node)]
    if kind in ("Polygon", "MultiPolygon"):
        rings = [
            _canon_ring(*_segments(r, by.get((p, i), [])))
            for p, (_, poly) in parts
            for i, r in enumerate(poly)
        ]
        return "area", sorted(rings, key=lambda r: r[0])
    return "line", [_segments(ln, by.get((p, -1), [])) for p, (_, ln) in parts]


def assert_same_curves(a: bytes, b: bytes) -> bool:
    """Same segments within one grid unit; True if any vertex needed that slack.

    Off-grid vertices (GDAL's linear stand-in for beziers) snap on write.
    """
    (ka, ra), (kb, rb) = canon(a), canon(b)
    assert ka == kb and len(ra) == len(rb)
    snapped = False
    for (keys_a, mids_a), (keys_b, mids_b) in zip(ra, rb):
        assert len(keys_a) == len(keys_b)
        for (kind_a, *pts_a), (kind_b, *pts_b) in zip(keys_a, keys_b):
            assert kind_a == kind_b
            d = max(abs(u - v) for p, q in zip(pts_a, pts_b) for u, v in zip(p, q))
            assert d <= 1
            snapped |= d > 0
        for ma, mb in zip(mids_a, mids_b):
            if ma is not None:
                assert abs(ma[0] - mb[0]) <= RES and abs(ma[1] - mb[1]) <= RES
    return snapped


# --- synthetic --------------------------------------------------------------
ARC = [(0, 0), (1, 1), (2, 0)]
CASES = {
    "compound_line": compound(
        line([(-3, 0), (0, 0)]), circ(ARC), line([(2, 0), (5, 0)])
    ),
    "circularstring_two_arcs": circ([(0, 0), (1, 1), (2, 0), (3, -1), (4, 0)]),
    "curvepolygon_arc_in_hole": curvepoly(
        line([(-10, -10), (10, -10), (10, 10), (-10, 10), (-10, -10)]),
        compound(circ([(0, 0), (1, 1), (2, 0)]), line([(2, 0), (0, 0)])),
    ),
    "full_circle_ring": curvepoly(circ([(5, 0), (-5, 0), (5, 0)])),
    "collinear_arc": compound(circ([(0, 0), (1, 0), (2, 0)]), line([(2, 0), (2, 3)])),
    "multisurface": multi(
        12,
        polygon([(20, 20), (21, 20), (21, 21), (20, 20)]),
        curvepoly(compound(circ(ARC), line([(2, 0), (0, 0)]))),
    ),
    "multicurve": multi(11, line([(9, 9), (8, 8)]), compound(circ(ARC))),
}


@pytest.mark.parametrize("name", CASES)
def test_split_then_curve_round_trips(name):
    src = CASES[name]
    linear, arcs = split_curve_wkb(src)
    out = curve_wkb(linear, arcs)
    assert_same_curves(src, out)
    # and the re-curved geometry splits to the identical linear geometry
    assert split_curve_wkb(out) == (linear, arcs)


def test_output_types_and_linear_passthrough():
    types = {n: curve_wkb(*split_curve_wkb(w))[1] for n, w in CASES.items()}
    assert types["compound_line"] == 9
    assert types["full_circle_ring"] == 10
    assert types["multisurface"] == 12
    assert types["multicurve"] == 11
    plain = polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
    assert curve_wkb(plain, []) == plain


def test_full_circle_keeps_start_point():
    out = curve_wkb(*split_curve_wkb(CASES["full_circle_ring"]))
    # CurvePolygon > 1 ring > CompoundCurve > 1 member > CircularString of 3 points
    assert struct.unpack_from("<3d", out, 5 + 4 + 5 + 4 + 5 + 4) == (5.0, 0.0, -5.0)


def test_comparison_ignores_ring_order_and_orientation():
    shell = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
    hole = [(2, 2), (3, 2), (3, 3), (2, 2)]
    rotated_reversed = [(10, 10), (10, 0), (0, 0), (0, 10), (10, 10)]
    assert_same_curves(polygon(shell, hole), polygon(hole, rotated_reversed))
    with pytest.raises(AssertionError):
        assert_same_curves(polygon(shell), polygon(hole))


def test_recurve_batch_inverts_linearize_batch():
    src = list(CASES.values()) + [None]
    batch = pa.RecordBatch.from_pydict(
        {"id": list(range(len(src))), "wkb_geometry": src}
    )
    out = recurve_batch(linearize_batch(batch))
    assert out.schema.names == ["id", "wkb_geometry"]
    for a, b in zip(src, out.column("wkb_geometry").to_pylist()):
        if a is None:
            assert b is None
        else:
            assert_same_curves(a, b)
    assert ARC_TYPE.num_fields == 11


def test_parquet_to_gdb_keeps_curves(tmp_path):
    from pyogrio.raw import write_arrow

    polys = [
        CASES["multisurface"],
        CASES["full_circle_ring"],
        CASES["curvepolygon_arc_in_hole"],
    ]
    src_gdb = tmp_path / "src.gdb"
    table = pa.table({"name": ["a", "b", "c"], "geom": polys})
    write_arrow(
        table,
        str(src_gdb),
        layer="p",
        driver="OpenFileGDB",
        layer_options=GRID,
        geometry_name="geom",
        geometry_type="MultiPolygon",
        crs="EPSG:2263",
    )
    _, original = read_arrow(str(src_gdb), layer="p")
    _roundtrip_layer(src_gdb, "p", tmp_path, original)


# --- real GDB (set CURVES_SOURCE_GDB to the CSCL ETL Working GDB) -----------
def _roundtrip_layer(gdb: Path, layer: str, tmp: Path, original: pa.Table):
    geom = original.column(original.num_columns - 1).to_pylist()
    lin, arcs = tmp / f"{layer}.parquet", tmp / f"{layer}_arcs.parquet"
    layer_to_parquet(gdb, layer, lin, arcs)
    out = tmp / f"{layer}_out.gdb"
    grid = gdb_xy_grid(gdb, layer)
    stats = parquet_to_gdb(lin, arcs, out, layer, layer_options=grid)
    _, back = read_arrow(str(out), layer=layer)
    back_geom = back.column(back.num_columns - 1).to_pylist()
    assert len(back_geom) == len(geom)
    snapped = 0
    for a, b in zip(geom, back_geom):
        if a is None:
            assert b is None
        else:
            snapped += assert_same_curves(a, b)
    # features with only beziers come back linear, so compare arcs, not types
    assert stats.arcs == sum(len(split_curve_wkb(w)[1]) for w in back_geom if w)
    print(f"{layer}: {stats.arcs} arcs; {snapped} features snapped (bezier stand-ins)")
    return stats


SOURCE_GDB = os.environ.get("CURVES_SOURCE_GDB")


@pytest.mark.skipif(not SOURCE_GDB, reason="CURVES_SOURCE_GDB not set")
@pytest.mark.parametrize(
    "layer",
    os.environ.get("CURVES_LAYERS", "AtomicPolygon,Centerline,CommunityDistrict").split(
        ","
    ),
)
def test_source_layer_round_trip(layer, tmp_path):
    gdb = Path(SOURCE_GDB or "")
    _, original = read_arrow(str(gdb), layer=layer)
    for w in original.column(original.num_columns - 1).to_pylist():
        if w is not None:
            assert_same_curves(w, curve_wkb(*split_curve_wkb(w)))
    _roundtrip_layer(gdb, layer, tmp_path, original)
