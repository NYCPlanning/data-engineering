import geopandas as gpd
import json
import pandas as pd
from pathlib import Path
from pyarrow import parquet

from dcpy.models.geospatial import parquet as geoparquet


def read_metadata(filepath: Path) -> geoparquet.MetaData:
    """
    Given filepath to GeoParquet file, returns both standard pyarrow parquet FileMetaData
    And geospatial metadata as defined in GeoParquet spec
    """
    parquet_metadata = parquet.read_metadata(filepath)
    if geoparquet.GEOPARQUET_METADATA_KEY not in parquet_metadata.metadata:
        raise TypeError(f"{filepath} is not a geoparquet file.")
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
    if geoparquet.GEOPARQUET_METADATA_KEY in parquet_metadata.metadata:
        return gpd.read_parquet(filepath)
    else:
        return pd.read_parquet(filepath)
