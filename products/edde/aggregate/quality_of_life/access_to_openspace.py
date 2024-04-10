import pandas as pd
from utils.PUMA_helpers import puma_to_borough, clean_PUMAs
from internal_review.set_internal_review_file import set_internal_review_files


def load_access_to_open_space():
    """ "Drop the percentage served column on reading in the excel file and calculate ourselves"""
    df = pd.read_excel(
        "./resources/quality_of_life/Park_Access.xlsx",
        usecols=["PUMA", "Pop_Served", "Total_Pop20"],
        dtype={"PUMA": str},
    )
    df.rename(
        columns={
            "PUMA": "puma",
            "Pop_Served": "access_openspace_count",
            "Total_Pop20": "total_pop_2020",
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
    aggregated = df.groupby(geography)[
        ["access_openspace_count", "total_pop_2020"]
    ].sum()
    aggregated["access_openspace_pct"] = (
        aggregated["access_openspace_count"] / aggregated["total_pop_2020"]
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
    set_internal_review_files(
        data=[
            (df, "access_openspace.csv", geography),
        ],
        category="quality_of_life",
    )
