import pandas as pd
import geopandas as gpd

from pathlib import Path

from dcpy.models import file
from dcpy.models.lifecycle.ingest import (
    Config,
)


def read_data_to_df(
    config: Config, local_data_path: Path
) -> gpd.GeoDataFrame | pd.DataFrame:
    """
    Reads data from a specified path and returns a pandas or geopandas dataframe depending
    whether the data is geosatial (specified in the config parameter).

    Parameters:
        config(recipes.ExtractConfig): Object containing metadata about the data, including its format
                         and whether it is geospatial.
        local_data_path(Path): Local path where the data is stored.

    Returns: pd.DataFrame or gpd.GeoDataFrame.
    """

    data_load_config = config.file_format

    match data_load_config:
        case file.Shapefile() as shapefile:
            gdf = gpd.read_file(
                local_data_path,
                crs=shapefile.crs,
                encoding=shapefile.encoding,
            )
        case file.Geodatabase() as geodatabase:
            gdf = gpd.read_file(
                local_data_path,
                crs=geodatabase.crs,
                encoding=geodatabase.encoding,
                layer=geodatabase.layer,
            )
        case file.Csv() as csv:
            df = pd.read_csv(
                local_data_path,
                index_col=False,
                encoding=data_load_config.encoding,
                delimiter=data_load_config.delimiter,
            )

            if not csv.geometry:
                gdf = df

            else:
                # case when geometry is in one column (i.e. polygon or point object type)
                if isinstance(csv.geometry.geom_column, str):
                    geom_column = csv.geometry.geom_column
                    assert (
                        geom_column in df.columns
                    ), f"❌ Geometry column specified in recipe template does not exist in {config.raw_filename}"

                    # replace NaN values with None. Otherwise gpd throws an error
                    if df[geom_column].isnull().any():
                        df[geom_column] = df[geom_column].astype(object)
                        df[geom_column] = df[geom_column].where(
                            df[geom_column].notnull(), None
                        )

                    df[geom_column] = gpd.GeoSeries.from_wkt(df[geom_column])

                    gdf = gpd.GeoDataFrame(
                        df,
                        geometry=geom_column,
                        crs=csv.geometry.crs,
                    )
    return gdf
