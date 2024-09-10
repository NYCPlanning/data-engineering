import math
import os

import numpy as np
import pandas as pd

outliers = [
    999999999,
    333333333,
    222222222,
    666666666,
    888888888,
    555555555,
    -999999999,
    -333333333,
    -222222222,
    -666666666,
    -888888888,
    -555555555,
]


def get_c(e, m):
    if e == 0:
        return np.nan
    else:
        return m / 1.645 / e * 100


def get_p(e, agg_e):
    if agg_e == 0:
        return np.nan
    else:
        return e / agg_e * 100


def get_z(e, m, p, agg_e, agg_m):
    if p == 0:
        return np.nan
    elif p == 100:
        return np.nan
    elif agg_e == 0:
        return np.nan
    elif m**2 - (e * agg_m / agg_e) ** 2 < 0:
        return math.sqrt(m**2 + (e * agg_m / agg_e) ** 2) / agg_e * 100
    else:
        return math.sqrt(m**2 - (e * agg_m / agg_e) ** 2) / agg_e * 100


def rounding(df: pd.DataFrame, digits: int) -> pd.DataFrame:
    """
    Round c, e, m, p, z fields based on rounding digits from metadata
    """
    df["c"] = df["c"].round(1)
    df["e"] = df["e"].round(digits)
    df["m"] = df["m"].round(digits)
    df["p"] = df["p"].round(1)
    df["z"] = df["z"].round(1)
    return df


def write_to_cache(df: pd.DataFrame, path: str):
    """
    this function will cache a dataframe to a given path
    """
    if not os.path.isfile(path):
        df.to_pickle(path)
    return None
