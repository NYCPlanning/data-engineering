"""Test module for python_script processing step tests"""

import geopandas as gpd
import pandas as pd


def simple_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Simple transformation that adds a column"""
    df = df.copy()
    df["new_column"] = "test_value"
    return df


def transform_with_kwargs(df: pd.DataFrame, multiplier: int = 2) -> pd.DataFrame:
    """Transformation that uses kwargs"""
    df = df.copy()
    if "value" in df.columns:
        df["doubled_value"] = df["value"] * multiplier
    return df


def geo_transform(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transformation that works with GeoDataFrame"""
    df = df.copy()
    df["area"] = df.geometry.area
    return df


def returns_plain_df_from_gdf(df: gpd.GeoDataFrame) -> pd.DataFrame:
    """Returns plain DataFrame from GeoDataFrame (should be converted back)"""
    result = pd.DataFrame(df)
    result["processed"] = True
    return result


def wrong_return_type(df: pd.DataFrame) -> list:
    """Returns wrong type (should raise error)"""
    return list(df.columns)


def not_a_function(df: pd.DataFrame):
    """This is actually a variable, not a function"""
    pass


# This is not callable
NOT_CALLABLE = "not_a_function"


def with_type_hint(df: pd.DataFrame) -> pd.DataFrame:
    """Function with proper type hint"""
    df = df.copy()
    df["type_hinted"] = True
    return df


def wrong_type_hint(df: pd.DataFrame) -> str:
    """Function with wrong return type hint (should error)"""
    df = df.copy()
    df["wrong_hint"] = True
    return df


def returns_wrong_type_at_runtime(df: pd.DataFrame):
    """Function without type hint that returns wrong type at runtime"""
    return list(df.columns)
