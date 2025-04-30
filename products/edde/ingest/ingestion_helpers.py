"""Miscellaneous ingestion related tasks"""

from dcpy.lifecycle.builds import load
from pandas import DataFrame
import pandas as pd


# import config
import config


def add_leading_zero_PUMA(df: DataFrame) -> DataFrame:
    df["puma"] = "0" + df["puma"].astype(str)
    return df


def load_data(name: str, version: str = "", cols: list = []) -> pd.DataFrame:
    build_metadata = load.get_build_metadata(config.PRODUCT_PATH)
    assert build_metadata.load_result, "You must load data before reading data."

    df = load.get_imported_df(build_metadata.load_result, ds_id=name, version=version)
    return df.filter(items=cols or df.columns.to_list())


def read_from_excel(
    file_path, category: str, sheet_name: str = None, columns: str = None, **kwargs
) -> pd.DataFrame:
    read_excel_args = {
        "io": file_path,
        "sheet_name": sheet_name,
        "usecols": columns,
    } | kwargs
    df = pd.read_excel(**read_excel_args)
    return df
