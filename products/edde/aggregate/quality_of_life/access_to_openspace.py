import pandas as pd
from resources import load
from utils.geo_helpers import clean_PUMAs, puma_to_borough


def load_access_to_open_space():
    """ "Drop the percentage served column on reading in the excel file and calculate ourselves"""
    df = load("transportation_park_access")[["PUMA", "Pop_Served", "Total_Pop"]]
    df.rename(
        columns={
            "PUMA": "puma",
            "Pop_Served": "access_openspace_count",
            "Total_Pop": "total_pop",
        },
        inplace=True,
    )

    df["puma"] = df["puma"].apply(func=clean_PUMAs)
    df = assign_geo_cols(df)

    return df


def assign_geo_cols(df):
    """Set up dataset with geographic specific columns for aggregation"""
    df["borough"] = df.apply(axis=1, func=puma_to_borough)

    df["citywide"] = "citywide"

    return df


def calculate_open_space(df, geography):
    aggregated = df.groupby(geography)[["access_openspace_count", "total_pop"]].sum()
    aggregated["access_openspace_pct"] = (
        aggregated["access_openspace_count"] / aggregated["total_pop"]
    ) * 100
    aggregated = aggregated.round(2)
    return aggregated[["access_openspace_count", "access_openspace_pct"]]


def access_to_openspace(geography: str) -> pd.DataFrame:
    """Main accessor for this variable"""
    assert geography in ["citywide", "borough", "puma"]
    df = load_access_to_open_space()
    return calculate_open_space(df, geography)


def set_results_for_internal_review(df, geography):
    """Saves results to .csv so that reviewers can see results during code review"""
