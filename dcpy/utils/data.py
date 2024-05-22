import json
import pandas as pd
import geopandas as gpd

from pathlib import Path

from dcpy.utils.logging import logger
from dcpy.models import file

import zipfile

EXTRACTED_FILES_DIR = Path("tmp/extracted_files")


def read_data_to_df(
    data_format: file.Format, local_data_path: Path
) -> gpd.GeoDataFrame | pd.DataFrame:
    """
    Reads data from a specified path and returns a pandas or geopandas dataframe depending
    whether the data is geospatial (specified in the data_format parameter).

    Parameters:
        data_format(file.Foramt): Object containing metadata about the data, including its format
                         and whether it is geospatial.
        local_data_path(Path): Local path where the data is stored.

    Returns:
        pd.DataFrame or gpd.GeoDataFrame: The loaded data as a DataFrame.

    Raises:
        AssertionError: If a file with `unzipped_filename` exists in local filesystem before unzipping (for zipped files).
        AssertionError: If the expected unzipped filename is not present in the unzipped files (for zipped files).
        AssertionError: If a specified geometry column does not exist in the DataFrame (for csv data format case).

    Notes:
        - This function will unzip files if the unzipped filename is specified in the config.
        - The function will load the data into a GeoDataFrame if it is geospatial, otherwise into a DataFrame.
    """

    # case when an input file is a zip
    if data_format.unzipped_filename is not None:
        logger.info(f"Unzipping files from {local_data_path}...")

        unzipped_filename = data_format.unzipped_filename
        unzipped_file_path = EXTRACTED_FILES_DIR / unzipped_filename

        unzipped_files = unzip_file(
            zipped_filename=local_data_path, output_dir=EXTRACTED_FILES_DIR
        )

        # remove leading and trailing slashes
        unzipped_filename = unzipped_filename.strip("/")
        unzipped_files = [filename.strip("/") for filename in unzipped_files]

        assert (
            unzipped_filename in unzipped_files
        ), f"❌ {unzipped_filename} is not present in unzipped files after extraction. Aborting..."

        local_data_path = unzipped_file_path

    match data_format:
        case file.Shapefile():
            gdf = gpd.read_file(
                local_data_path,
                crs=data_format.crs,
                encoding=data_format.encoding,
            )
        case file.Geodatabase():
            gdf = gpd.read_file(
                local_data_path,
                crs=data_format.crs,
                encoding=data_format.encoding,
                layer=data_format.layer,
            )
        case file.GeoJson():
            gdf = gpd.read_file(local_data_path, encoding=data_format.encoding)
        case file.Csv():
            df = pd.read_csv(
                local_data_path,
                index_col=False,
                encoding=data_format.encoding,
                delimiter=data_format.delimiter,
                names=data_format.column_names,
                dtype=data_format.dtype,
            )
            gdf = (
                df if not data_format.geometry else df_to_gdf(df, data_format.geometry)
            )
        case file.Xlsx():
            df = pd.read_excel(
                local_data_path,
                sheet_name=data_format.sheet_name,
            )
            gdf = (
                df if not data_format.geometry else df_to_gdf(df, data_format.geometry)
            )

        case file.Json():
            if data_format.json_read_fn == "read_json":
                df = (
                    pd.read_json(local_data_path, **data_format.json_read_kwargs)
                    if data_format.json_read_kwargs
                    else pd.read_json(local_data_path)
                )
            else:
                with open(local_data_path) as f:
                    json_str = json.load(f)
                df = (
                    pd.json_normalize(json_str, **data_format.json_read_kwargs)
                    if data_format.json_read_kwargs
                    else pd.json_normalize(json_str)
                )
            gdf = (
                df if not data_format.geometry else df_to_gdf(df, data_format.geometry)
            )

    return gdf


def unzip_file(zipped_filename: Path, output_dir: Path) -> list[str]:
    """
    Extracts file(s) from a specified zipped file into an output directory.
    Output directory doesn't need to exist.

    Parameters:
        zipped_filename (Path): Path to the zip archive to be extracted.
        output_dir (Path): Path to the directory where files will be extracted.

    Returns:
        list[str]: A list of filenames of the extracted files.

    Raises:
        AssertionError: If the zip archive does not exist.
    """

    assert (
        zipped_filename.exists()
    ), f"❌ Provided path {zipped_filename} to zipped file wasn't found. Try again"

    with zipfile.ZipFile(zipped_filename, "r") as zip_ref:
        zip_ref.extractall(output_dir)
        extracted_files = zip_ref.namelist()

    logger.info(
        f"✅ Successfully extracted the following file(s) from {zipped_filename}: {extracted_files}"
    )

    return extracted_files


def df_to_gdf(df: pd.DataFrame, geometry: file.Geometry) -> gpd.GeoDataFrame:
    """
    Convert a pandas DataFrame to a GeoDataFrame based on the provided geometry information.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    geometry (file.Geometry): An object containing geometry information.

    Returns:
    gpd.GeoDataFrame: The resulting GeoDataFrame.
    """

    # case when geometry is in one column (i.e. polygon or point object type)
    if isinstance(geometry.geom_column, str):
        geom_column = geometry.geom_column
        assert (
            geom_column in df.columns
        ), f"❌ Geometry column specified in recipe template does not exist in the input data."

        # replace NaN values with None. Otherwise gpd throws an error
        if df[geom_column].isnull().any():
            df[geom_column] = df[geom_column].astype(object)
            df[geom_column] = df[geom_column].where(df[geom_column].notnull(), None)

        df[geom_column] = gpd.GeoSeries.from_wkt(df[geom_column])

        gdf = gpd.GeoDataFrame(
            df,
            geometry=geom_column,
            crs=geometry.crs,
        )
    # case when geometry is specified as lon and lat columns
    else:
        x_column = geometry.geom_column.x
        y_column = geometry.geom_column.y
        assert (
            x_column in df.columns and y_column in df.columns
        ), f"❌ Longitude or latitude columns specified in the recipe template do not exist in the input data"

        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[x_column], df[y_column]),
            crs=geometry.crs,
        )

    return gdf
