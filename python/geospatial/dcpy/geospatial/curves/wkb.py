"""ISO WKB (with curve types) to linear WKB plus a list of the original arcs."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import NamedTuple

from dcpy.geospatial.curves.linearize import (
    DEFAULT_MIN_SEGMENTS,
    DEFAULT_TOLERANCE,
    Point,
    linearize_arc,
)

POINT, LINESTRING, POLYGON = 1, 2, 3
MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, COLLECTION = 4, 5, 6, 7
CIRCULARSTRING, COMPOUNDCURVE, CURVEPOLYGON, MULTICURVE, MULTISURFACE = 8, 9, 10, 11, 12

_EWKB_Z, _EWKB_M, _EWKB_SRID = 0x80000000, 0x40000000, 0x20000000


class Arc(NamedTuple):
    """One circular arc, control points exactly as stored."""

    part: int  # member index within a multi-geometry (0 otherwise)
    ring: int  # ring index within a polygon (0 = shell); -1 for lines
    seq: int  # arc order within its (part, ring)
    x0: float
    y0: float
    x1: float  # interior control point
    y1: float
    x2: float
    y2: float


@dataclass
class CurveStats:
    """Counters accumulated across calls, e.g. a whole layer."""

    geometries: int = 0
    arcs: int = 0
    zm_stripped: int = 0  # geometries whose Z and/or M values were dropped


# linear geometry tree: ("Point", pt) | ("LineString", pts) | ("Polygon", [pts])
# | ("MultiX"/"GeometryCollection", [children])
_Node = tuple


class _Reader:
    def __init__(self, data: bytes, stats: CurveStats, tol: float, min_seg: int):
        self.data, self.pos = data, 0
        self.stats, self.tol, self.min_seg = stats, tol, min_seg
        self.arcs: list[Arc] = []
        self._seq: dict[tuple[int, int], int] = {}

    def header(self) -> tuple[str, int, int]:
        """Read byte order and type; returns (struct endian char, base type, extra dims)."""
        endian = "<" if self.data[self.pos] == 1 else ">"
        (code,) = struct.unpack_from(endian + "I", self.data, self.pos + 1)
        self.pos += 5
        extra = 0
        if code & (_EWKB_Z | _EWKB_M | _EWKB_SRID):
            extra = bool(code & _EWKB_Z) + bool(code & _EWKB_M)
            if code & _EWKB_SRID:
                self.pos += 4
            code &= 0x0FFFFFFF
        else:
            extra = {0: 0, 1: 1, 2: 1, 3: 2}[code // 1000]
            code %= 1000
        if extra:
            self.stats.zm_stripped += 1
        return endian, code, extra

    def count(self, endian: str) -> int:
        (n,) = struct.unpack_from(endian + "I", self.data, self.pos)
        self.pos += 4
        return n

    def points(self, endian: str, extra: int, n: int) -> list[Point]:
        width = 2 + extra
        flat = struct.unpack_from(f"{endian}{n * width}d", self.data, self.pos)
        self.pos += 8 * n * width
        return [(flat[i], flat[i + 1]) for i in range(0, n * width, width)]

    def arc_points(self, pts: list[Point], part: int, ring: int) -> list[Point]:
        out = [pts[0]]
        for i in range(0, len(pts) - 2, 2):
            p0, pm, p1 = pts[i], pts[i + 1], pts[i + 2]
            seq = self._seq.get((part, ring), 0)
            self._seq[(part, ring)] = seq + 1
            self.arcs.append(Arc(part, ring, seq, *p0, *pm, *p1))
            out.extend(linearize_arc(p0, pm, p1, self.tol, self.min_seg)[1:])
        return out

    def curve(
        self, part: int, ring: int, hdr: tuple[str, int, int] | None = None
    ) -> list[Point]:
        """A LineString, CircularString or CompoundCurve as linear points."""
        endian, code, extra = hdr or self.header()
        if code == LINESTRING:
            return self.points(endian, extra, self.count(endian))
        if code == CIRCULARSTRING:
            pts = self.points(endian, extra, self.count(endian))
            return self.arc_points(pts, part, ring) if len(pts) >= 3 else pts
        if code == COMPOUNDCURVE:
            out: list[Point] = []
            for _ in range(self.count(endian)):
                seg = self.curve(part, ring)
                out.extend(seg if not out else seg[1:])
            return out
        raise ValueError(f"unsupported curve type {code}")

    def geometry(self, part: int = 0) -> _Node:
        hdr = self.header()
        endian, code, extra = hdr
        if code == POINT:
            return ("Point", self.points(endian, extra, 1)[0])
        if code in (LINESTRING, CIRCULARSTRING, COMPOUNDCURVE):
            return ("LineString", self.curve(part, -1, hdr))
        if code in (POLYGON, CURVEPOLYGON):
            rings = []
            for ri in range(self.count(endian)):
                if code == POLYGON:
                    rings.append(self.points(endian, extra, self.count(endian)))
                else:
                    rings.append(self.curve(part, ri))
            return ("Polygon", rings)
        names = {
            MULTIPOINT: "MultiPoint",
            MULTILINESTRING: "MultiLineString",
            MULTIPOLYGON: "MultiPolygon",
            MULTICURVE: "MultiLineString",
            MULTISURFACE: "MultiPolygon",
            COLLECTION: "GeometryCollection",
        }
        if code in names:
            kids = [self.geometry(i) for i in range(self.count(endian))]
            return (names[code], kids)
        raise ValueError(f"unsupported WKB geometry type {code}")


_CODES = {
    "Point": POINT,
    "LineString": LINESTRING,
    "Polygon": POLYGON,
    "MultiPoint": MULTIPOINT,
    "MultiLineString": MULTILINESTRING,
    "MultiPolygon": MULTIPOLYGON,
    "GeometryCollection": COLLECTION,
}


def _coords(pts: list[Point]) -> bytes:
    return struct.pack(f"<I{2 * len(pts)}d", len(pts), *[c for p in pts for c in p])


def _emit(node: _Node) -> bytes:
    kind, body = node
    head = struct.pack("<BI", 1, _CODES[kind])
    if kind == "Point":
        return head + struct.pack("<2d", *body)
    if kind == "LineString":
        return head + _coords(body)
    if kind == "Polygon":
        return head + struct.pack("<I", len(body)) + b"".join(_coords(r) for r in body)
    return head + struct.pack("<I", len(body)) + b"".join(_emit(k) for k in body)


def split_curve_wkb(
    wkb: bytes,
    tolerance: float = DEFAULT_TOLERANCE,
    min_segments: int = DEFAULT_MIN_SEGMENTS,
    stats: CurveStats | None = None,
) -> tuple[bytes, list[Arc]]:
    """Linear 2D WKB (multi-ness preserved) and the arcs the input contained.

    Z/M values are dropped and counted in ``stats.zm_stripped``.
    """
    stats = stats if stats is not None else CurveStats()
    r = _Reader(wkb, stats, tolerance, min_segments)
    node = r.geometry()
    stats.geometries += 1
    stats.arcs += len(r.arcs)
    return _emit(node), r.arcs
