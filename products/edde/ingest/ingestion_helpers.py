from dcpy.lifecycle.builds import load
from dcpy.utils.geospatial import parquet
import pandas as pd
import geopandas as gp

import config


def load_data(
    name: str, version: str = "", cols: list = [], is_geospatial: bool = False
) -> pd.DataFrame | gp.GeoDataFrame:
    # TODO: is_geospatial flag is hacky. This should be inferred elsewhere

    build_metadata = load.get_build_metadata(config.PRODUCT_PATH)
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
    kwargs = {} if not columns else {"usecols": columns}
    return pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)  # type: ignore
