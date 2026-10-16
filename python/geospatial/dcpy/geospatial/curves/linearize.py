"""Deterministic, symmetric linearization of circular arcs (plain ``math``)."""

from __future__ import annotations

import math

Point = tuple[float, float]

DEFAULT_TOLERANCE = 0.00025  # max deviation from the true arc, in CRS units
DEFAULT_MIN_SEGMENTS = 2
MAX_SEGMENTS = 100_000
_MAX_RADIUS = 1e12


def _circumcircle(p0: Point, pm: Point, p1: Point) -> tuple[float, float, float] | None:
    """Center and radius of the circle through three points; None if collinear."""
    bx, by = pm[0] - p0[0], pm[1] - p0[1]
    cx, cy = p1[0] - p0[0], p1[1] - p0[1]
    d = 2.0 * (bx * cy - by * cx)
    if d == 0.0:
        return None
    b2 = bx * bx + by * by
    c2 = cx * cx + cy * cy
    ux = (cy * b2 - by * c2) / d
    uy = (bx * c2 - cx * b2) / d
    r = math.hypot(ux, uy)
    if not math.isfinite(r) or r > _MAX_RADIUS:
        return None
    return p0[0] + ux, p0[1] + uy, r


def segment_count(
    sweep: float,
    radius: float,
    tolerance: float = DEFAULT_TOLERANCE,
    min_segments: int = DEFAULT_MIN_SEGMENTS,
) -> int:
    """N = max(min_segments, ceil(sweep / (2*acos(1 - tol/R)))), so the sagitta <= tol."""
    if radius <= tolerance / 2:
        return min_segments
    # 2*acos(1 - t/R) == 4*asin(sqrt(t/(2R))), the stable form
    step = 4.0 * math.asin(math.sqrt(tolerance / (2.0 * radius)))
    n = math.ceil(sweep / step) if step > 0 else MAX_SEGMENTS
    return max(min_segments, min(MAX_SEGMENTS, n))


def _points_on_circle(
    cx: float, cy: float, r: float, a0: float, signed_sweep: float, n: int
) -> list[Point]:
    step = signed_sweep / n
    return [
        (cx + r * math.cos(a0 + step * k), cy + r * math.sin(a0 + step * k))
        for k in range(n + 1)
    ]


def linearize_circle(
    start: Point,
    opposite: Point,
    ccw: bool = True,
    tolerance: float = DEFAULT_TOLERANCE,
    min_segments: int = DEFAULT_MIN_SEGMENTS,
) -> list[Point]:
    """Full circle as a closed vertex list beginning and ending at ``start``.

    The stored start is kept (ring closure depends on it). A full circle's
    direction isn't recoverable from its control points, so ``ccw`` defaults
    to counter-clockwise; ``ccw=False`` is exactly the reversed list.
    """
    if not ccw:
        return linearize_circle(start, opposite, True, tolerance, min_segments)[::-1]
    cx, cy = (start[0] + opposite[0]) / 2.0, (start[1] + opposite[1]) / 2.0
    r = math.hypot(start[0] - opposite[0], start[1] - opposite[1]) / 2.0
    if r == 0.0:
        return [start]
    n = max(segment_count(2 * math.pi, r, tolerance, min_segments), 4)
    n += n % 2  # even, so the opposite point is a vertex
    a0 = math.atan2(start[1] - cy, start[0] - cx)
    pts = _points_on_circle(cx, cy, r, a0, 2 * math.pi, n)
    pts[0] = pts[-1] = start
    return pts


def linearize_arc(
    p0: Point,
    pm: Point,
    p1: Point,
    tolerance: float = DEFAULT_TOLERANCE,
    min_segments: int = DEFAULT_MIN_SEGMENTS,
) -> list[Point]:
    """Vertices of the arc p0 -> pm -> p1, endpoints included and exact.

    Symmetric: ``linearize_arc(b, m, a) == linearize_arc(a, m, b)[::-1]``
    exactly, because vertices are always generated from the lexicographically
    smaller endpoint and then reversed if needed.
    """
    if p0 == p1:
        return (
            [p0]
            if pm == p0
            else linearize_circle(p0, pm, True, tolerance, min_segments)
        )
    if p1 < p0:
        return linearize_arc(p1, pm, p0, tolerance, min_segments)[::-1]
    circle = _circumcircle(p0, pm, p1)
    if circle is None:
        return [p0, p1]
    cx, cy, r = circle
    a0 = math.atan2(p0[1] - cy, p0[0] - cx)
    a1 = math.atan2(p1[1] - cy, p1[0] - cx)
    # pm to the left of chord p0->p1 means the arc runs clockwise
    cross = (p1[0] - p0[0]) * (pm[1] - p0[1]) - (p1[1] - p0[1]) * (pm[0] - p0[0])
    if cross > 0:
        sweep, sign = (a0 - a1) % (2 * math.pi), -1.0
    else:
        sweep, sign = (a1 - a0) % (2 * math.pi), 1.0
    if sweep < math.pi and 2.0 * r * math.sin(sweep / 4.0) ** 2 < tolerance:
        return [p0, p1]  # sagitta below tolerance
    n = segment_count(sweep, r, tolerance, min_segments)
    pts = _points_on_circle(cx, cy, r, a0, sign * sweep, n)
    pts[0], pts[-1] = p0, p1
    return pts
