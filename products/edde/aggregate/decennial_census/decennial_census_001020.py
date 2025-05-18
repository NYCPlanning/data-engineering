from dcpy.utils.logging import logger
from functools import cache
import pandas as pd

from utils.PUMA_helpers import clean_PUMAs
from internal_review.set_internal_review_file import set_internal_review_files

# Map ACS year (or in general, input year for many functions) to decennial census year
year_map = {"2000": "00", "0812": "10", "1519": "20", "1721": "20", "1923": "20"}


@cache
def load_decennial_census_001020() -> pd.DataFrame:
    """Load in the xlsx file, fill the missing values with the values from geogtype, rename the columns
    following conventions, drop the duplicate column"""

    df = pd.read_excel(
        "./resources/decennial_census_data/EDDE_Census00-10-20_MUTU.xlsx",
        skiprows=2,
        dtype={"GeogType": str, "GeoID": str},
    )

    df.rename(
        columns={
            "GeogType": "geo_type",
            "GeoID": "geo_id",
            "Pop20": "pop_20_count",
            "Pop20P": "pop_20_pct",
            "Hsp20": "pop_20_hsp_count",
            "Hsp20P": "pop_20_hsp_pct",
            "WNH20": "pop_20_wnh_count",
            "WNH20P": "pop_20_wnh_pct",
            "BNH20": "pop_20_bnh_count",
            "BNH20P": "pop_20_bnh_pct",
            "ANH20": "pop_20_anh_count",
            "ANH20P": "pop_20_anh_pct",
            "OTwoNH20": "pop_20_onh_count",
            "OTwoNH20P": "pop_20_onh_pct",
            "Pop10": "pop_10_count",
            "Pop10P": "pop_10_pct",
            "Hsp10": "pop_10_hsp_count",
            "Hsp10P": "pop_10_hsp_pct",
            "WNH10": "pop_10_wnh_count",
            "WNH10P": "pop_10_wnh_pct",
            "BNH10": "pop_10_bnh_count",
            "BNH10P": "pop_10_bnh_pct",
            "ANH10": "pop_10_anh_count",
            "ANH10P": "pop_10_anh_pct",
            "OTwoNH10": "pop_10_onh_count",
            "OTwoNH10P": "pop_10_onh_pct",
            "Pop00": "pop_00_count",
            "Pop00P": "pop_00_pct",
            "Hsp00": "pop_00_hsp_count",
            "Hsp00P": "pop_00_hsp_pct",
            "WNH00": "pop_00_wnh_count",
            "WNH00P": "pop_00_wnh_pct",
            "BNH00": "pop_00_bnh_count",
            "BNH00P": "pop_00_bnh_pct",
            "ANH00": "pop_00_anh_count",
            "ANH00P": "pop_00_anh_pct",
            "OTwoNH00": "pop_00_onh_count",
            "OTwoNH00P": "pop_00_onh_pct",
        },
        inplace=True,
    )
    df.geo_id = df.geo_id.fillna(df.geo_type)

    df = df.replace(
        {
            "geo_id": {
                "Bronx": "BX",
                "Brooklyn": "BK",
                "Manhattan": "MN",
                "Queens": "QN",
                "Staten Island": "SI",
                "NYC": "citywide",
            }
        }
    )

    puma_rows_mask = df["geo_type"] == "PUMA2020"
    df.loc[puma_rows_mask, "geo_id"] = df.loc[puma_rows_mask, "geo_id"].apply(
        clean_PUMAs
    )
    df.set_index("geo_id", inplace=True)
    return df


def create_citywide_level_df_by_year(df, year):
    """create the dataframes by geography type and year, strip year from columns"""
    df_citywide = (
        df.loc[["citywide"]].reset_index().rename(columns={"geo_id": "citywide"})
    )
    df_citywide.set_index("citywide", inplace=True)

    final = df_citywide.filter(regex=f"citywide|{year}")
    final.columns = final.columns.str.replace(f"_{year}", "")

    return final


def create_borough_level_df_by_year(df, year):
    """create the dataframes by geography type and year, strip year from columns"""
    df_borough = (
        df.loc[["BX", "BK", "MN", "QN", "SI"]]
        .reset_index()
        .rename(columns={"geo_id": "borough"})
    )
    df_borough.set_index("borough", inplace=True)

    final = df_borough.filter(regex=f"borough|{year}")
    final.columns = final.columns.str.replace(f"_{year}", "")

    return final


def create_puma_level_df_by_year(df, year):
    """create the dataframes by geography type and year, strip year from columns"""
    df_puma = (
        df.loc[df["geo_type"] == "PUMA2020"]
        .reset_index()
        .rename(columns={"geo_id": "puma"})
    )
    df_puma.set_index("puma", inplace=True)

    final = df_puma.filter(regex=f"puma|{year}")
    final.columns = final.columns.str.replace(f"_{year}", "")

    return final


def decennial_census_001020(
    geography: str, year: str = "2000", write_to_internal_review=False
) -> pd.DataFrame:
    logger.info(f"Running decennial_census_001020 for {geography}, {year}")
    assert geography in ["citywide", "borough", "puma"]
    assert year in year_map

    df = load_decennial_census_001020()

    if geography == "citywide":
        final = create_citywide_level_df_by_year(df, year_map[year])

    if geography == "borough":
        final = create_borough_level_df_by_year(df, year_map[year])

    if geography == "puma":
        final = create_puma_level_df_by_year(df, year_map[year])

    if write_to_internal_review:
        set_internal_review_files(
            data=[
                (
                    final,
                    f"demographics_{year}_decennial_census.csv",
                    geography,
                )
            ],
            category="demographics",
        )

    return final
