"""Pure-Python handling of curved (circular arc) geometry.

Curved ISO WKB is split into a linear geometry plus a list of the original arcs.
Only ``read`` touches GDAL.
"""

from dcpy.geospatial.curves.convert import ARC_TYPE, linearize_batch
from dcpy.geospatial.curves.linearize import (
    DEFAULT_TOLERANCE,
    linearize_arc,
    linearize_circle,
)
from dcpy.geospatial.curves.wkb import Arc, CurveStats, split_curve_wkb

__all__ = [
    "ARC_TYPE",
    "Arc",
    "CurveStats",
    "DEFAULT_TOLERANCE",
    "linearize_arc",
    "linearize_batch",
    "linearize_circle",
    "split_curve_wkb",
]
