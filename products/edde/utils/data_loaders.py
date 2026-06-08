"""Data loading utilities for EDDE.

This module provides functions to load:
- Datasets from build metadata
- Excel files
"""

import config
import geopandas as gp
import pandas as pd

from dcpy.lifecycle.builds import load
from dcpy.utils.geospatial import parquet
from dcpy.utils.logging import logger


def load_data(
    name: str, version: str = "", cols: list = [], is_geospatial: bool = False
) -> pd.DataFrame | gp.GeoDataFrame:
    """Load a dataset from build metadata.

    Args:
        name: Dataset name
        version: Dataset version (optional)
        cols: Columns to select (optional, defaults to all)
        is_geospatial: Whether to load as GeoDataFrame

    Returns:
        DataFrame or GeoDataFrame with the requested data
    """
    # TODO: is_geospatial flag is hacky. This should be inferred elsewhere

    build_metadata = load.get_build_metadata(config.PRODUCT_PATH)
    logger.info(f"loading dataset={name}, version={version}")
    assert build_metadata.load_result, "You must load data before reading data."

    if is_geospatial:
        df = parquet.read_df(
            load.get_imported_filepath(build_metadata.load_result, name, version)
        )
    else:
        df = load.get_imported_df(
            build_metadata.load_result, ds_id=name, version=version
        )
    return df.filter(items=cols or df.columns.to_list())


def read_from_excel(
    file_path, category: str, sheet_name: str = "", columns: list = [], **kwargs
) -> pd.DataFrame:
    """Read data from an Excel file.

    Args:
        file_path: Path to Excel file
        category: Category (unused, for backwards compatibility)
        sheet_name: Sheet name to read
        columns: Columns to read (optional)
        **kwargs: Additional arguments passed to pd.read_excel

    Returns:
        DataFrame with the Excel data
    """
    kwargs = {} if not columns else {"usecols": columns}
    return pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)  # type: ignore


# NOTE: load_PUMS was removed because EDDE now uses pre-aggregated ACS data
# from resources/ACS_PUMS/ Excel files instead of fetching raw data from Census API.
# See resources.py and aggregate/load_aggregated.py:load_acs() for current approach.
