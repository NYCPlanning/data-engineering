"""Functions to add margin of error and remove standard error. 
For final output generally want MOE instead of SE.
For debugging want both
"""
import pandas as pd
from scipy import stats
import numpy as np

z_score = stats.norm.ppf(0.95)


def variance_measures(df, add_MOE):
    if add_MOE:
        df = SE_to_MOE(df)
    df = add_CV(df)
    return df


def SE_to_MOE(df):
    df["moe"] = df["se"] * z_score
    return df


def add_CV(df: pd.DataFrame):

    df["cv"] = df["se"] / df["V1"] * 100

    return df
