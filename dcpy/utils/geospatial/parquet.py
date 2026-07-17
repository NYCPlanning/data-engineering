import json
from collections.abc import Iterator
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pyarrow import parquet

from dcpy.utils.geospatial import parquet_models as geoparquet


def _is_geoparquet(m: parquet.FileMetaData):
    return m.metadata is not None and geoparquet.GEOPARQUET_METADATA_KEY in m.metadata


def is_geoparquet(filepath: Path) -> bool:
    """Return True if the parquet file carries GeoParquet geometry metadata."""
    return _is_geoparquet(parquet.read_metadata(filepath))


def read_metadata(filepath: Path) -> geoparquet.MetaData:
    """
    Given filepath to GeoParquet file, returns both standard pyarrow parquet FileMetaData
    And geospatial metadata as defined in GeoParquet spec
    """
    parquet_metadata = parquet.read_metadata(filepath)
    if not _is_geoparquet(parquet_metadata):
        raise TypeError(f"{filepath} is not a geoparquet file.")
    assert parquet_metadata.metadata  # this is determined within helper
    geo = parquet_metadata.metadata[geoparquet.GEOPARQUET_METADATA_KEY]
    geo_parquet = geoparquet.GeoParquet(**json.loads(geo))
    return geoparquet.MetaData(file_metadata=parquet_metadata, geo_parquet=geo_parquet)


def read_df(filepath: Path) -> pd.DataFrame:
    """
    Read parquet file either as pd.DataFrame or gpd.GeoDataFrame
    For now, type signature is just pd.DataFrame
    If GeoDataFrame is needed with resulting DataFrame, should check type of object at runtime
    """
    parquet_metadata = parquet.read_metadata(filepath)
    if _is_geoparquet(parquet_metadata):
        return gpd.read_parquet(filepath)
    else:
        return pd.read_parquet(filepath)


def iter_batches_df(filepath: Path, batch_size: int) -> Iterator[pd.DataFrame]:
    """Yield a parquet file as pd.DataFrames of at most batch_size rows each.

    Lets a caller stream a large file without materializing it all at once.
    For non-geospatial parquet only: geometry columns are not reconstructed as a
    GeoDataFrame (use read_df for geoparquet). An empty file still yields one
    empty frame so a downstream table gets created with the right columns.
    """
    pq_file = parquet.ParquetFile(filepath)
    yielded = False
    for batch in pq_file.iter_batches(batch_size=batch_size):
        yielded = True
        yield batch.to_pandas()
    if not yielded:
        yield pq_file.schema_arrow.empty_table().to_pandas()
