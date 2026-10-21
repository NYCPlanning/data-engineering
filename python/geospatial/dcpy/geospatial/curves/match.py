"""Re-curve linear geometry by matching its vertex runs against known arcs.

Geometry that went through SQL (make-valid, polygonize, union, clipping) no longer lines up
with arc vertex indices, but its vertices still lie on the arcs' circles. A run of
consecutive vertices on one arc's circle, inside the arc's sweep, with every segment within
the linearization tolerance of the circle, is that arc or a piece of it (e.g. an arc split
at a node), and becomes a circular arc again. A run spanning the whole arc keeps the arc's
own middle control point; a piece gets the point halfway along its own sweep.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

from dcpy.geospatial.curves.linearize import (
    DEFAULT_MIN_SEGMENTS,
    DEFAULT_TOLERANCE,
    Point,
    _circumcircle,
)
from dcpy.geospatial.curves.recurve import _emit_curved, curved_geo_schema
from dcpy.geospatial.curves.wkb import Arc, CurveStats, _Reader

ARC_COLUMNS = ("x0", "y0", "x1", "y1", "x2", "y2")
_TWO_PI = 2 * math.pi


class _Circle(NamedTuple):
    cx: float
    cy: float
    r: float
    a0: float  # angle of the start point
    sweep: float  # 2*pi for a full circle
    sign: float  # +1 counter-clockwise, -1 clockwise, 0 full circle (either way)
    p0: Point
    pm: Point
    p1: Point


def _circle(p0: Point, pm: Point, p1: Point) -> _Circle | None:
    if p0 == p1:
        if pm == p0:
            return None
        cx, cy = (p0[0] + pm[0]) / 2, (p0[1] + pm[1]) / 2
        r = math.hypot(p0[0] - pm[0], p0[1] - pm[1]) / 2
        return _Circle(
            cx, cy, r, math.atan2(p0[1] - cy, p0[0] - cx), _TWO_PI, 0.0, p0, pm, p1
        )
    circle = _circumcircle(p0, pm, p1)
    if circle is None:
        return None
    cx, cy, r = circle
    a0 = math.atan2(p0[1] - cy, p0[0] - cx)
    a1 = math.atan2(p1[1] - cy, p1[0] - cx)
    cross = (p1[0] - p0[0]) * (pm[1] - p0[1]) - (p1[1] - p0[1]) * (pm[0] - p0[0])
    if cross > 0:  # pm left of the chord: clockwise
        return _Circle(cx, cy, r, a0, (a0 - a1) % _TWO_PI, -1.0, p0, pm, p1)
    return _Circle(cx, cy, r, a0, (a1 - a0) % _TWO_PI, 1.0, p0, pm, p1)


def _angle_delta(a: float, b: float) -> float:
    """b - a wrapped to (-pi, pi]."""
    d = (b - a) % _TWO_PI
    return d - _TWO_PI if d > math.pi else d


class ArcIndex:
    """Arcs (control points p0, pm, p1) indexed by grid cell, for matching vertices.

    ``tolerance`` is the linearization tolerance the geometry was built with: a vertex
    within it of an arc's circle (inside the sweep) can belong to the arc, and a segment
    can when its sagitta on that circle is within it.
    """

    def __init__(
        self,
        arcs: Iterable[tuple[float, float, float, float, float, float]] = (),
        tolerance: float = DEFAULT_TOLERANCE,
        cell: float = 16.0,
    ):
        self.tol = tolerance * 1.2
        self.cell = cell
        self.circles: list[_Circle] = []
        self._cells: defaultdict[tuple[int, int], list[int]] = defaultdict(list)
        seen: set[tuple[Point, Point, Point]] = set()
        for x0, y0, x1, y1, x2, y2 in arcs:
            key = ((x0, y0), (x1, y1), (x2, y2))
            if key in seen or (key[2], key[1], key[0]) in seen:
                continue
            seen.add(key)
            c = _circle(*key)
            if c is not None:
                self._add(c)

    def __len__(self) -> int:
        return len(self.circles)

    def _add(self, c: _Circle) -> None:
        i = len(self.circles)
        self.circles.append(c)
        # cells of points every half cell along the arc, plus their neighbours, so every
        # vertex near the arc finds it in its own cell
        n = max(1, math.ceil(c.r * c.sweep / (self.cell / 2)))
        sign = c.sign or 1.0
        cells: set[tuple[int, int]] = set()
        for k in range(n + 1):
            a = c.a0 + sign * c.sweep * k / n
            gx = math.floor((c.cx + c.r * math.cos(a)) / self.cell)
            gy = math.floor((c.cy + c.r * math.sin(a)) / self.cell)
            cells.update((gx + dx, gy + dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1))
        for key in cells:
            self._cells[key].append(i)

    def on_arc(self, i: int, x: float, y: float) -> bool:
        c = self.circles[i]
        if abs(math.hypot(x - c.cx, y - c.cy) - c.r) > self.tol:
            return False
        if not c.sign:
            return True
        t = ((math.atan2(y - c.cy, x - c.cx) - c.a0) * c.sign) % _TWO_PI
        slack = self.tol / c.r
        return t <= c.sweep + slack or t >= _TWO_PI - slack

    def candidates(self, x: float, y: float) -> tuple[int, ...]:
        ids = self._cells.get((math.floor(x / self.cell), math.floor(y / self.cell)))
        return tuple(i for i in ids if self.on_arc(i, x, y)) if ids else ()

    def _segment_ok(self, c: _Circle, p: Point, q: Point) -> bool:
        half = math.hypot(q[0] - p[0], q[1] - p[1]) / 2
        if half == 0 or half > c.r:
            return False
        return c.r - math.sqrt(c.r * c.r - half * half) <= self.tol

    def _same(self, p: Point, q: Point) -> bool:
        return abs(p[0] - q[0]) <= self.tol and abs(p[1] - q[1]) <= self.tol

    def _run(
        self, pts: list[Point], cands: list[tuple[int, ...]], i: int, ci: int
    ) -> int:
        """Last vertex of the longest run of arc ``ci`` starting at vertex ``i``."""
        c = self.circles[ci]
        j, direction = i, 0.0
        a_prev = math.atan2(pts[i][1] - c.cy, pts[i][0] - c.cx)
        while j + 1 < len(pts) and ci in cands[j + 1]:
            a = math.atan2(pts[j + 1][1] - c.cy, pts[j + 1][0] - c.cx)
            d = _angle_delta(a_prev, a)
            if (
                d == 0
                or d * direction < 0
                or not self._segment_ok(c, pts[j], pts[j + 1])
            ):
                break
            direction, a_prev, j = d, a, j + 1
        return j

    def _deviation(self, pts: list[Point], i: int, j: int, ci: int) -> float:
        c = self.circles[ci]
        return max(abs(math.hypot(x - c.cx, y - c.cy) - c.r) for x, y in pts[i : j + 1])

    def _control(self, pts: list[Point], i: int, j: int, ci: int) -> list[Point]:
        """Control points for vertices i..j on arc ``ci``: the arc's own when the run
        spans it, else the run's endpoints and the point halfway along its sweep."""
        c = self.circles[ci]
        if c.sign and self._spans(pts[i], pts[j], ci):
            return [pts[i], c.pm, pts[j]]
        angles = [math.atan2(y - c.cy, x - c.cx) for x, y in pts[i : j + 1]]
        sweep = sum(_angle_delta(a, b) for a, b in zip(angles, angles[1:]))
        am = angles[0] + sweep / 2
        return [pts[i], (c.cx + c.r * math.cos(am), c.cy + c.r * math.sin(am)), pts[j]]

    def match(
        self, pts: list[Point], closed: bool
    ) -> tuple[list[Point], list[tuple[int, int, list[Point]]]]:
        """Arcs among a line's or ring's vertices, as (v0, v1, control points).

        A closed ring whose first vertex sits inside an arc is rotated to start at a
        vertex that doesn't, so no arc wraps around the closing vertex; the (possibly
        rotated) vertices are returned with the arcs.
        """
        cands = [self.candidates(x, y) for x, y in pts]
        if closed and len(pts) > 3 and set(cands[0]) & set(cands[1]) & set(cands[-2]):
            k = next((k for k in range(1, len(pts) - 1) if not cands[k]), None)
            if k is None:  # every vertex is on an arc: start at an arc's endpoint
                k = next(
                    (
                        k
                        for k in range(1, len(pts) - 1)
                        for ci in cands[k]
                        if self._same(pts[k], self.circles[ci].p0)
                    ),
                    None,
                )
            if k is not None:
                pts = pts[k:-1] + pts[: k + 1]
                cands = cands[k:-1] + cands[: k + 1]
        found: list[tuple[int, int, list[Point]]] = []
        i = 0
        while i < len(pts) - 1:
            runs = [(self._run(pts, cands, i, ci), ci) for ci in cands[i]]
            # a run that is a whole arc wins over a piece of another one (e.g. of a
            # larger arc on nearly the same circle, from another feature)
            whole = [
                (j, ci) for j, ci in runs if j > i and self._spans(pts[i], pts[j], ci)
            ]
            runs = whole or runs
            best_j = max((j for j, _ in runs), default=i)
            longest = [ci for j, ci in runs if j == best_j > i]
            if len(longest) > 1:
                # e.g. two owners' copies of a shared arc with different interior
                # points: the circle the vertices lie on most closely
                longest.sort(key=lambda ci: self._deviation(pts, i, best_j, ci))
            best_ci = longest[0] if longest else -1
            if best_ci < 0 or not self._accept(pts, i, best_j, best_ci):
                i += 1
                continue
            if pts[i] == pts[best_j]:  # a full circle: two half arcs
                half = (i + best_j) // 2
                found.append((i, half, self._control(pts, i, half, best_ci)))
                found.append((half, best_j, self._control(pts, half, best_j, best_ci)))
            else:
                found.append((i, best_j, self._control(pts, i, best_j, best_ci)))
            i = best_j
        return pts, found

    def _spans(self, p: Point, q: Point, ci: int) -> bool:
        """p and q are the arc's two endpoints (either way round)."""
        c = self.circles[ci]
        return (self._same(p, c.p0) and self._same(q, c.p1)) or (
            self._same(p, c.p1) and self._same(q, c.p0)
        )

    def _accept(self, pts: list[Point], i: int, j: int, ci: int) -> bool:
        """At least two segments, or one that is the whole (flat) arc."""
        if j - i >= 2:
            return True
        return self._spans(pts[i], pts[j], ci)

    def curve_wkb(self, linear_wkb: bytes, stats: CurveStats | None = None) -> bytes:
        """ISO curve WKB for ``linear_wkb`` with every matched arc re-curved; the input
        as is when nothing matches. Ring starts may move (see ``match``)."""
        node = _Reader(
            linear_wkb, CurveStats(), DEFAULT_TOLERANCE, DEFAULT_MIN_SEGMENTS
        ).geometry()
        arcs: dict[tuple[int, int], list[Arc]] = {}

        def visit(node, part: int) -> None:
            kind, body = node
            if kind == "LineString":
                _, found = self.match(body, closed=False)
                _collect(found, part, -1)
            elif kind == "Polygon":
                for ri, ring in enumerate(body):
                    body[ri], found = self.match(ring, closed=True)
                    _collect(found, part, ri)
            elif kind in ("MultiLineString", "MultiPolygon"):
                for k, kid in enumerate(body):
                    visit(kid, k)

        def _collect(found, part: int, ring: int) -> None:
            if found:
                arcs[(part, ring)] = [
                    Arc(part, ring, seq, v0, v1, *ctrl[0], *ctrl[1], *ctrl[2])
                    for seq, (v0, v1, ctrl) in enumerate(found)
                ]

        visit(node, 0)
        if stats is not None:
            stats.geometries += 1
            stats.curved += bool(arcs)
            stats.arcs += sum(len(a) for a in arcs.values())
        if not arcs:
            return linear_wkb
        return _emit_curved(node, arcs)


def read_arcs(path: str | Path, batch_size: int = 100_000) -> Iterator[tuple]:
    """Control points (x0, y0, x1, y1, x2, y2) from an arcs parquet file."""
    for b in pq.ParquetFile(str(path)).iter_batches(
        batch_size, columns=list(ARC_COLUMNS)
    ):
        cols = [b.column(c).to_pylist() for c in ARC_COLUMNS]
        yield from zip(*cols)


def recurve_matched_parquet(
    linear_parquet: str | Path,
    arcs_parquet: str | Path,
    out_parquet: str | Path,
    tolerance: float = DEFAULT_TOLERANCE,
    batch_size: int = 10_000,
) -> CurveStats:
    """A linear GeoParquet layer to the same layer with its geometry re-curved (ISO curve
    WKB) wherever its vertices match an arc of ``arcs_parquet`` (columns x0, y0, x1, y1,
    x2, y2: start, interior and end control points). Every other column is kept.

    Streams the layer; the arcs are held in memory. GeoParquet metadata is kept with
    ``geometry_types`` emptied and ``curved: true`` (see ``recurve.recurve_parquet``).
    """
    index = ArcIndex(read_arcs(arcs_parquet), tolerance)
    linear = pq.ParquetFile(str(linear_parquet))
    schema, geom_col = curved_geo_schema(linear)
    stats = CurveStats()
    with pq.ParquetWriter(str(out_parquet), schema) as writer:
        for batch in linear.iter_batches(batch_size):
            geoms = [
                None if wkb is None else index.curve_wkb(wkb, stats)
                for wkb in batch.column(geom_col).to_pylist()
            ]
            cols = [
                pa.array(geoms, pa.binary())
                if f.name == geom_col
                else batch.column(f.name)
                for f in schema
            ]
            writer.write_batch(pa.RecordBatch.from_arrays(cols, schema=schema))
    return stats
