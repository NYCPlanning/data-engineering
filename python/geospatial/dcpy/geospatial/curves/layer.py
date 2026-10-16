"""Convert one curved GDB layer to a linear GeoParquet plus a flat arcs parquet."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from dcpy.geospatial.curves.convert import ARC_TYPE, ARCS_COLUMN, linearize_batch
from dcpy.geospatial.curves.linearize import DEFAULT_MIN_SEGMENTS, DEFAULT_TOLERANCE
from dcpy.geospatial.curves.read import read_curved_batches
from dcpy.geospatial.curves.wkb import CurveStats
from dcpy.geospatial.curves.write import write_geoparquet

ARCS_SCHEMA = pa.schema(
    [pa.field("ogc_fid", pa.int32())]
    + [ARC_TYPE.field(i) for i in range(ARC_TYPE.num_fields)]
)


def _decode_dictionaries(batch: pa.RecordBatch) -> pa.RecordBatch:
    """GDAL returns coded-value-domain fields as dictionaries whose indices are the
    stored codes (what ogr2ogr writes); keep the codes, not the descriptions."""
    for i, field in enumerate(batch.schema):
        if pa.types.is_dictionary(field.type):
            batch = batch.set_column(
                i,
                pa.field(field.name, field.type.index_type),
                cast(pa.DictionaryArray, batch.column(i)).indices,
            )
    return batch


def layer_to_parquet(
    source: str | Path,
    layer: str,
    linear_out: str | Path,
    arcs_out: str | Path,
    tolerance: float = DEFAULT_TOLERANCE,
    min_segments: int = DEFAULT_MIN_SEGMENTS,
    batch_size: int = 10_000,
) -> tuple[int, CurveStats]:
    """Stream ``layer`` into a linear GeoParquet and a flat arcs parquet.

    Rows keep source (FID) order. The arcs file carries ``ogc_fid`` = 1-based row
    number, the id a GDAL/ogr2ogr load assigns, so arcs join back to the loaded table.
    Returns (row count, stats).
    """
    stats = CurveStats()
    n_rows = 0
    with (
        read_curved_batches(source, layer, batch_size) as (meta, it),
        pq.ParquetWriter(str(arcs_out), ARCS_SCHEMA) as arcs_writer,
    ):
        geom_col = meta["geometry_name"]

        def linear_batches():
            nonlocal n_rows
            for batch in it:
                out = linearize_batch(batch, geom_col, tolerance, min_segments, stats)
                out = _decode_dictionaries(out)
                arcs_col = out.column(ARCS_COLUMN)
                fids = [
                    n_rows + i + 1
                    for i, n in enumerate(arcs_col.value_lengths().to_pylist())
                    for _ in range(n or 0)
                ]
                if fids:
                    arcs_writer.write_table(
                        pa.Table.from_arrays(
                            [pa.array(fids, pa.int32()), *arcs_col.flatten().flatten()],
                            schema=ARCS_SCHEMA,
                        )
                    )
                n_rows += batch.num_rows
                yield out.drop_columns([ARCS_COLUMN])

        write_geoparquet(
            linear_batches(), linear_out, crs=meta["crs"], geom_col=geom_col
        )
    return n_rows, stats
