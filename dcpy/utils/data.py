from collections import defaultdict
import ijson
import json
import pandas as pd
import geopandas as gpd
import os
from pathlib import Path
import shutil
from typing import Literal

from dcpy.models import file
from dcpy.utils.logging import logger
from dcpy.utils.geospatial.transform import df_to_gdf

import zipfile


def _get_dtype(dtype: str | dict | None) -> str | dict | defaultdict | None:
    """
    A helper function to have a way of specifying both kwarg and default dtypes for a file to be read
    """
    match dtype:
        case dict():
            if "__default__" in dtype:
                default = dtype.pop("__default__")
                return defaultdict(lambda: default, **dtype)
            else:
                return dtype
        case _:
            return dtype


def _read_geojson_crs(local_data_path: Path) -> str | None:
    file_crs = None
    with open(local_data_path, "rb") as f:
        for item in ijson.items(f, "crs"):
            if isinstance(item, dict):
                file_crs = item.get("properties", {}).get("name")
            else:
                file_crs = str(item)
            break
    return file_crs


def read_data_to_df(
    data_format: file.Format, local_data_path: Path, clean_extracted_zip: bool = True
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

        extracted_files_dir = local_data_path.parent / "extracted_files"

        unzipped_filename = data_format.unzipped_filename
        unzipped_file_path = extracted_files_dir / unzipped_filename

        unzipped_files = unzip_file(
            zipped_filename=local_data_path, output_dir=extracted_files_dir
        )

        assert unzipped_filename in unzipped_files, (
            f"❌ {unzipped_filename} is not present in unzipped files after extraction. Aborting..."
        )

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
            crs = data_format.crs or _read_geojson_crs(local_data_path) or "EPSG:4326"
            gdf = gpd.read_file(local_data_path, encoding=data_format.encoding)
            gdf.set_crs(crs, allow_override=True, inplace=True)
        case file.Csv():
            model_kwargs = data_format.model_dump()
            for key in ["type", "geometry", "unzipped_filename", "dtype"]:
                model_kwargs.pop(key, None)
            kwargs: dict = {
                "index_col": False,
                "low_memory": False,
                "dtype": _get_dtype(data_format.dtype),
            }
            kwargs.update(model_kwargs)
            df = pd.read_csv(local_data_path, **kwargs)
            gdf = (
                df if not data_format.geometry else df_to_gdf(df, data_format.geometry)
            )
        case file.Excel():
            df = pd.read_excel(
                local_data_path,
                sheet_name=data_format.sheet_name,
                engine=data_format.engine,
                dtype=_get_dtype(data_format.dtype),
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
            df = serialize_nested_objects(df)

            gdf = (
                df if not data_format.geometry else df_to_gdf(df, data_format.geometry)
            )
        case file.Html():
            df = pd.read_html(local_data_path, **data_format.kwargs)[data_format.table]
            gdf = (
                df if not data_format.geometry else df_to_gdf(df, data_format.geometry)
            )

    if data_format.unzipped_filename and clean_extracted_zip:
        assert extracted_files_dir
        shutil.rmtree(extracted_files_dir)

    return gdf


def unzip_file(zipped_filename: Path, output_dir: Path) -> set[str]:
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

    assert zipped_filename.exists(), (
        f"❌ Provided path {zipped_filename} to zipped file wasn't found. Try again"
    )

    with zipfile.ZipFile(zipped_filename, "r") as zip_ref:
        zip_ref.extractall(output_dir)
        extracted_files = set()

        for info in zip_ref.infolist():
            extracted_files.add(info.filename.rstrip("/"))
            dir = os.path.dirname(info.filename).rstrip("/")
            if dir:
                extracted_files.add(dir)

    logger.info(
        f"✅ Successfully extracted the following file(s) from {zipped_filename}: {extracted_files}"
    )

    return extracted_files


def serialize_nested_objects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Serialize nested objects (dictionaries and lists) to JSON strings.
    """

    def serialize_value(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    # Apply serialization to each cell in the DataFrame
    return df.map(serialize_value)


def upsert_df_columns(
    df: pd.DataFrame,
    upsert_df: pd.DataFrame,
    key: list[str],
    insert_behavior: Literal["allow", "ignore", "error"] = "allow",
    missing_key_behavior: Literal["null", "coalesce", "error"] = "error",
    allow_duplicate_keys=False,
) -> pd.DataFrame:
    """
    Upserts columns in a dataframe by left joining another dataframe

    Parameters:
        df: dataframe to upsert and return
        upsert_df: a df containing columns to join by and columns to upsert into df
        on: column(s) to join the two datasets by.
        insert_behavior: how to handle columns in the upsert df not present in main df. Either upsert, update (and ignore additional), or try to update and error on additional
        missing_key_behavior: how to handle when keys in the left df are missing in the right (rows that do not join) when updating
            "null": regardless of value for row for updated column, update with "None"/"NaN"
            "coalesce": if row is not joined and row is updated, default to old value
            "error": throw error if any rows are not joined
        allow_duplicate_keys: boolean flag. If true, allows duplicate keys in both dataframes, meaning returned df may have more rows in input
    """
    cols = set(df.columns)
    upsert_cols = set(upsert_df.columns)
    if not set(key).issubset(cols):
        raise ValueError("Cannot upsert: 'by' columns not present in df")
    if not set(key).issubset(upsert_cols):
        raise ValueError("Cannot upsert: 'by' columns not present in upsert_df")

    if (not allow_duplicate_keys) and not len(df) == len(df[key].drop_duplicates()):
        raise ValueError("Cannot upsert: df keys are not unique")
    if (not allow_duplicate_keys) and not len(upsert_df) == len(
        upsert_df[key].drop_duplicates()
    ):
        raise ValueError("Cannot upsert: upsert_df keys are not unique")

    if insert_behavior == "error" and not upsert_cols.issubset(cols):
        raise ValueError(
            f"Unexpected columns found in upsert_df: {upsert_cols.difference(cols)}"
        )
    if insert_behavior == "ignore":
        upsert_df = upsert_df[[c for c in upsert_df.columns if c in cols]]

    upsert_columns = [c for c in upsert_df.columns if c not in key]
    output_columns = list(df.columns) + [c for c in upsert_columns if c not in cols]

    suffix = "__upsert"
    joined = df.merge(
        upsert_df, on=key, suffixes=(None, suffix), indicator=True, how="left"
    )

    if (missing_key_behavior == "error") and any(joined["_merge"] == "left_only"):
        raise ValueError("Not all keys in df found in upsert_df")

    for column in upsert_columns:
        col_type = joined[column].dtype
        upsert_column = column + suffix if column in cols else column
        if missing_key_behavior == "coalesce":
            joined[column] = joined.apply(
                lambda row: (
                    row[column] if row["_merge"] == "left_only" else row[upsert_column]
                ),
                axis=1,
            )
        else:
            joined[column] = joined[upsert_column]
        joined[column] = joined[column].astype(col_type, errors="ignore")

    return joined[output_columns]
