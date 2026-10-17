"""Pure-Python handling of curved (circular arc) geometry.

Curved ISO WKB is split into a linear geometry plus a list of the original arcs,
and ``curve_wkb`` rebuilds it. Only ``read`` and the ``recurve`` writer touch GDAL.
"""

from dcpy.geospatial.curves.convert import ARC_TYPE, linearize_batch
from dcpy.geospatial.curves.linearize import (
    DEFAULT_TOLERANCE,
    linearize_arc,
    linearize_circle,
)
from dcpy.geospatial.curves.recurve import curve_wkb, parquet_to_gdb, recurve_batch
from dcpy.geospatial.curves.wkb import Arc, CurveStats, split_curve_wkb

__all__ = [
    "ARC_TYPE",
    "Arc",
    "CurveStats",
    "DEFAULT_TOLERANCE",
    "curve_wkb",
    "linearize_arc",
    "linearize_batch",
    "linearize_circle",
    "parquet_to_gdb",
    "recurve_batch",
    "split_curve_wkb",
]
