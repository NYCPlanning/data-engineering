import pandas as pd
from utils.PUMA_helpers import assign_2010_puma_col
from internal_review.set_internal_review_file import set_internal_review_files
from ingest.ingestion_helpers import read_from_S3


""""We need to download the data, separate the data by construction type (New Construction vs. Preservation)
as these will be two separate indicators, unit income level, and the various citywide reporting geography
(citywide, borough, PUMA) statistics. There are roughly 20% of records that are confidential in this
dataset and these are dropped in the PUMA level aggregates per HPD.
 """

unit_income_levels = [
    "extremely_low_income_units",
    "very_low_income_units",
    "low_income_units",
    "moderate_income_units",
    "middle_income_units",
    "other_income_units",
]


def load_housing_ny():
    """load the HPD Housing NY Units by Building dataset"""
    df = read_from_S3(
        "hpd_hny_units_by_building",
        "housing_production",
        cols=get_columns(),
    )

    df = df.replace(
        {
            "borough": {
                "Bronx": "BX",
                "Brooklyn": "BK",
                "Manhattan": "MN",
                "Queens": "QN",
                "Staten Island": "SI",
            }
        }
    )
    # casting to numeric for calculation
    for c in get_numeric_columns():
        df[c] = pd.to_numeric(df[c])

    return df


def get_columns() -> list:
    cols = [
        "project_id",
        "project_name",
        "project_start_date",
        "project_completion_date",
        "number",
        "street",
        "borough",
        "latitude_(internal)",
        "longitude_(internal)",
        "building_completion_date",
        "reporting_construction_type",
        "extremely_low_income_units",
        "very_low_income_units",
        "low_income_units",
        "moderate_income_units",
        "middle_income_units",
        "other_income_units",
    ]
    return cols


def get_numeric_columns() -> list:
    cols = [
        "extremely_low_income_units",
        "very_low_income_units",
        "low_income_units",
        "moderate_income_units",
        "middle_income_units",
        "other_income_units",
        "latitude_(internal)",
        "longitude_(internal)",
    ]
    return cols


def pivot_add_total(df, geography):
    df = pivot_and_flatten_index(df, geography)
    df = add_total(df)
    return df


def pivot_and_flatten_index(df, geography):
    level_mapper = {
        "extremely_low_income_units": "units_eli",
        "very_low_income_units": "units_vli",
        "low_income_units": "units_li",
        "moderate_income_units": "units_mi",
        "middle_income_units": "units_midi",
        "other_income_units": "units_oi",
    }

    df = df.pivot(
        index=geography,
        columns="reporting_construction_type",
        values=unit_income_levels,
    )

    df.columns = ["_".join(a) for a in df.columns.to_flat_index()]

    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    cols = df.columns

    for full, abbr in level_mapper.items():
        cols = [c.replace(full, abbr) for c in cols]

    cols = [c.replace("new_construction", "newconstruction") + "_count" for c in cols]

    df.columns = cols

    df.reset_index(inplace=True)

    return df


def add_total(df):
    for t in ["preservation", "newconstruction"]:
        cols = [
            f"units_{band}_{t}_count"
            for band in ["eli", "vli", "li", "mi", "midi", "oi"]
        ]
        df[f"units_allami_{t}_count"] = df[cols].sum(axis=1)
    return df


def citywide_hny_units_con_type(df):
    """Get total unit counts by construction type (new construction vs preservation)"""

    results = (
        df.groupby("reporting_construction_type")[unit_income_levels]
        .sum()
        .reset_index()
    )

    results["citywide"] = "citywide"

    results = pivot_add_total(results, "citywide")

    return results.set_index("citywide")


def borough_hny_units_con_type(df):
    results = (
        df.groupby(["reporting_construction_type", "borough"])[unit_income_levels]
        .sum()
        .reset_index()
    )

    results = pivot_add_total(results, "borough")

    return results.set_index("borough")


def PUMA_hny_units_con_type(df):
    """This function drops any confidential records (as per HPD) as they shouldn't be recorded at the
    PUMA level geography but kept in at larger geo units. Currently the assign_PUMA_col function can't
    take any lat/long that are null so drop those records, and assign PUMA using geography_helpers.py,
    and aggregate at the PUMA level"""
    filter_df = df[~df["project_name"].isin(["CONFIDENTIAL"])]

    filter_df.dropna(subset=["latitude_(internal)", "longitude_(internal)"])

    puma_df = assign_2010_puma_col(
        filter_df, "latitude_(internal)", "longitude_(internal)"
    )

    results = (
        puma_df.groupby(["reporting_construction_type", "puma"])[unit_income_levels]
        .sum()
        .reset_index()
    )

    results = pivot_add_total(results, "puma")

    return results.set_index("puma")


def affordable_housing(geography: str) -> pd.DataFrame:
    """Main accessor for this variable"""
    assert geography in ["citywide", "borough", "puma"]
    housing_ny = load_housing_ny()
    if geography == "citywide":
        return citywide_hny_units_con_type(housing_ny)
    if geography == "borough":
        return borough_hny_units_con_type(housing_ny)
    if geography == "puma":
        return PUMA_hny_units_con_type(housing_ny)


def affordable_housing_internal_review():
    housing_ny = load_housing_ny()
    citywide = citywide_hny_units_con_type(housing_ny)
    by_borough = borough_hny_units_con_type(housing_ny)
    by_puma = PUMA_hny_units_con_type(housing_ny)
    set_internal_review_files(
        [
            (citywide, "affordable_housing_preservation_construction.csv", "citywide"),
            (by_borough, "affordable_housing_preservation_construction.csv", "borough"),
            (by_puma, "affordable_housing_preservation_construction.csv", "puma"),
        ],
        "housing_production",
    )


if __name__ == "__main__":
    df = load_housing_ny()

    results_citywide = citywide_hny_units_con_type(df)

    results_borough = borough_hny_units_con_type(df)

    results_puma = PUMA_hny_units_con_type(df)

    # output to csv for data checks  - these need to be dumped in DO edm-recipes

    set_internal_review_files(
        [
            (
                results_citywide,
                "affordable_housing_preservation_construction.csv",
                "citywide",
            ),
            (
                results_borough,
                "affordable_housing_preservation_construction.csv",
                "borough",
            ),
            (results_puma, "affordable_housing_preservation_construction.csv", "puma"),
        ],
        "housing_production",
    )
