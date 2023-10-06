import hashlib
import numpy as np
import re

import pandas as pd


def hash_each_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    e.g. df = hash_each_row(df)
    this function will create a "uid" column with hashed row values
    ----------
    df: input dataframe
    """
    df["temp_column"] = df.astype(str).values.sum(axis=1)

    def hash_helper(x):
        return hashlib.md5(x.encode("utf-8")).hexdigest()

    df["uid"] = df["temp_column"].apply(hash_helper)
    del df["temp_column"]
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
