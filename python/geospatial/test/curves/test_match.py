import math

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb

from dcpy.geospatial.curves import linearize_arc, split_curve_wkb
from dcpy.geospatial.curves.match import ArcIndex, recurve_matched_parquet
from dcpy.geospatial.curves.write import write_geoparquet

from .test_curves import line, multi, polygon

# quarter circle, radius 100 around (1000, 1000), counter-clockwise from (1100, 1000)
P0, PM, P1 = (
    (1100.0, 1000.0),
    (1000 + 100 / math.sqrt(2), 1000 + 100 / math.sqrt(2)),
    (1000.0, 1100.0),
)
ARC = (*P0, *PM, *P1)


def _ring():
    """A polygon bounded by the arc and two straight edges through the center."""
    return linearize_arc(P0, PM, P1) + [(1000.0, 1000.0), P0]


def _arcs(wkb):
    _, arcs = split_curve_wkb(wkb)
    return [((a.x0, a.y0), (a.x1, a.y1), (a.x2, a.y2)) for a in arcs]


def test_whole_arc_keeps_its_control_points():
    curved = ArcIndex([ARC]).curve_wkb(polygon(_ring()))
    assert _arcs(curved) == [(P0, PM, P1)]
    assert split_curve_wkb(curved)[0] == polygon(_ring())


def test_reversed_ring_and_lines():
    index = ArcIndex([ARC])
    assert _arcs(index.curve_wkb(polygon(_ring()[::-1]))) == [(P1, PM, P0)]
    pts = [(1200.0, 900.0)] + linearize_arc(P0, PM, P1) + [(900.0, 1200.0)]
    assert _arcs(index.curve_wkb(multi(5, line(pts)))) == [(P0, PM, P1)]


def test_ring_starting_inside_the_arc_is_rotated():
    ring = _ring()[:-1]
    k = len(ring) // 3
    rotated = ring[k:] + ring[:k] + [ring[k]]
    curved = ArcIndex([ARC]).curve_wkb(polygon(rotated))
    assert _arcs(curved) == [(P0, PM, P1)]
    relinearized = from_wkb(split_curve_wkb(curved)[0])
    assert relinearized.equals(from_wkb(polygon(rotated)))


def test_piece_of_an_arc_gets_its_own_midpoint():
    """E.g. an arc split by a node: the piece is re-curved on the same circle."""
    pts = linearize_arc(P0, PM, P1)
    piece = pts[: len(pts) // 2 + 1]
    ((a, m, b),) = _arcs(ArcIndex([ARC]).curve_wkb(line(piece)))
    assert a == piece[0] and b == piece[-1]
    assert math.isclose(math.hypot(m[0] - 1000, m[1] - 1000), 100)
    angle = lambda p: math.atan2(p[1] - 1000, p[0] - 1000)  # noqa: E731
    assert math.isclose(angle(m), (angle(a) + angle(b)) / 2)


def test_extra_vertex_on_the_arc_still_matches():
    """A vertex an overlay inserted between two linearized vertices."""
    pts = linearize_arc(P0, PM, P1)
    mid = ((pts[5][0] + pts[6][0]) / 2, (pts[5][1] + pts[6][1]) / 2)
    pts.insert(6, mid)
    assert _arcs(ArcIndex([ARC]).curve_wkb(line(pts))) == [(P0, PM, P1)]


def test_not_curved():
    index = ArcIndex([ARC])
    # a chord between the arc's endpoints, and a segment shorter than the tolerance
    # at one endpoint
    assert index.curve_wkb(line([P0, P1])) == line([P0, P1])
    tiny = line([P1, (P1[0] + 0.0001, P1[1])])
    assert index.curve_wkb(tiny) == tiny
    # vertices on the circle but outside the arc's sweep
    other = linearize_arc((900.0, 1000.0), (1000.0, 900.0), (1100.0, 1000.0))
    assert index.curve_wkb(line(other)) == line(other)
    # straight geometry elsewhere
    square = polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)])
    assert index.curve_wkb(square) == square


def test_full_circle_is_two_half_arcs():
    start, opposite = (1100.0, 1000.0), (900.0, 1000.0)
    ring = linearize_arc(start, opposite, start)
    arcs = _arcs(ArcIndex([(*start, *opposite, *start)]).curve_wkb(polygon(ring)))
    assert len(arcs) == 2
    assert arcs[0][0] == arcs[1][2] == start and arcs[0][2] == arcs[1][0]


def test_recurve_matched_parquet(tmp_path):
    rings = [_ring(), [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array([polygon(r) for r in rings], pa.binary())],
        names=["id", "geometry"],
    )
    write_geoparquet(
        [batch], tmp_path / "linear.parquet", crs="EPSG:2263", geom_col="geometry"
    )
    pq.write_table(
        pa.table({c: [v] for c, v in zip(["x0", "y0", "x1", "y1", "x2", "y2"], ARC)}),
        tmp_path / "arcs.parquet",
    )
    stats = recurve_matched_parquet(
        tmp_path / "linear.parquet", tmp_path / "arcs.parquet", tmp_path / "out.parquet"
    )
    assert (stats.geometries, stats.curved, stats.arcs) == (2, 1, 1)
    out = pq.read_table(tmp_path / "out.parquet")
    assert out.column("id").to_pylist() == [1, 2]
    geoms = out.column("geometry").to_pylist()
    assert _arcs(geoms[0]) == [(P0, PM, P1)] and geoms[1] == polygon(rings[1])
    assert b'"curved": true' in out.schema.metadata[b"geo"]


def test_whole_arcs_win_over_a_piece_of_another():
    """Two consecutive short arcs on the quarter circle's circle, and the whole quarter
    arc (from another feature) passing through: the short arcs are this line's own."""
    pts = linearize_arc(P0, PM, P1)[10:-10]
    k = len(pts) // 2
    a = (*pts[0], *pts[k // 2], *pts[k])
    b = (*pts[k], *pts[(k + len(pts)) // 2], *pts[-1])
    arcs = _arcs(ArcIndex([ARC, a, b]).curve_wkb(line(pts)))
    assert [(x[0], x[2]) for x in arcs] == [(pts[0], pts[k]), (pts[k], pts[-1])]
