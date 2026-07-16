import hashlib
import re

import geopandas as gpd
import numpy as np
import pandas as pd


def hash_each_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    e.g. df = hash_each_row(df)
    this function will create a "uid" column with hashed row values
    ----------
    df: input dataframe
    """
    geom_column: str | None = (
        df.geometry.name if isinstance(df, gpd.GeoDataFrame) else None
    )

    def hash_helper(row):
        if geom_column:
            geom_string = row[geom_column].wkt if row[geom_column] else "None"
            # .sum(), not .values.sum(): under pandas' Arrow-backed string dtype
            # (default under future.infer_string, pandas 3.x), .values returns an
            # ArrowStringArray, which has no .sum() - Series.sum() still does the
            # same string concatenation correctly regardless of backing dtype.
            s = geom_string + row.drop(geom_column).astype(str).sum()
        else:
            s = row.astype(str).sum()
        return hashlib.md5(s.encode("utf-8")).hexdigest()

    df["uid"] = df.apply(hash_helper, axis=1)
    cols = list(df.columns)
    cols.remove("uid")
    cols = ["uid"] + cols
    return df[cols]


def format_field_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change field name to lower case
    and replace all spaces with underscore
    """

    def format_func(x):
        return re.sub(r"\W+", "", x.lower().strip().replace("-", "_").replace(" ", "_"))

    df.columns = df.columns.map(format_func)
    return df


def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({np.nan: None})
    df = df.drop_duplicates()
    return df
