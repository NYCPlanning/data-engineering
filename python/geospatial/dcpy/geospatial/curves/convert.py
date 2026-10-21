"""Arrow batch glue: curved WKB column in, linear WKB column plus ``arcs`` out."""

from __future__ import annotations

import logging

import pyarrow as pa

from dcpy.geospatial.curves.linearize import DEFAULT_MIN_SEGMENTS, DEFAULT_TOLERANCE
from dcpy.geospatial.curves.wkb import CurveStats, split_curve_wkb

logger = logging.getLogger(__name__)

ARC_TYPE = pa.struct(
    [
        ("part", pa.int32()),
        ("ring", pa.int32()),
        ("seq", pa.int32()),
        ("v0", pa.int32()),
        ("v1", pa.int32()),
        ("x0", pa.float64()),
        ("y0", pa.float64()),
        ("x1", pa.float64()),
        ("y1", pa.float64()),
        ("x2", pa.float64()),
        ("y2", pa.float64()),
    ]
)
ARCS_COLUMN = "arcs"


def linearize_batch(
    batch: pa.RecordBatch,
    geom_col: str = "wkb_geometry",
    tolerance: float = DEFAULT_TOLERANCE,
    min_segments: int = DEFAULT_MIN_SEGMENTS,
    stats: CurveStats | None = None,
) -> pa.RecordBatch:
    """Replace ``geom_col`` with linear WKB and append an ``arcs`` column."""
    stats = stats if stats is not None else CurveStats()
    zm_before, point_arcs_before = stats.zm_stripped, stats.point_arcs
    geoms: list[bytes | None] = []
    arcs: list[list[dict]] = []
    for wkb in batch.column(geom_col).to_pylist():
        if wkb is None:
            geoms.append(None)
            arcs.append([])
            continue
        linear, found = split_curve_wkb(wkb, tolerance, min_segments, stats)
        geoms.append(linear)
        arcs.append([a._asdict() for a in found])
    if stats.zm_stripped > zm_before:
        logger.warning(
            "stripped Z/M from %d geometries in batch", stats.zm_stripped - zm_before
        )
    if stats.point_arcs > point_arcs_before:
        logger.warning(
            "dropped %d zero-length arcs misread as full circles in batch",
            stats.point_arcs - point_arcs_before,
        )
    idx = batch.schema.get_field_index(geom_col)
    out = batch.set_column(
        idx,
        pa.field(geom_col, pa.binary(), metadata=batch.schema.field(idx).metadata),
        pa.array(geoms, type=pa.binary()),
    )
    return out.append_column(ARCS_COLUMN, pa.array(arcs, type=pa.list_(ARC_TYPE)))
