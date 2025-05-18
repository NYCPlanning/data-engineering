"""This python script takes the Household Economic Security indicators that Erica initially sent over
in an xlsx spreadsheet (Educational attainment data points) cleans them and outputs them so
that they can be collated using the established collate process"""

import pandas as pd

from aggregate.load_aggregated import load_2000_census_pums_all_data
from aggregate.aggregation_helpers import order_aggregated_columns
from utils.PUMA_helpers import clean_PUMAs, dcp_pop_races
from utils.dcp_population_excel_helpers import (
    race_suffix_mapper,
    map_stat_suffix,
)
from internal_review.set_internal_review_file import set_internal_review_files

# from aggregate.aggregation_helpers import order_aggregated_columns, get_category


edu_name_mapper = {
    "p25pl": "age_p25pl",
    "lths": "edu_lths",
    "hsgrd": "edu_hs",
    "sclga": "edu_smcol",
    "bchd": "edu_bchpl",
}


def filter_to_economic(df):
    """filter to educational attainment indicators"""
    df = df.filter(regex="GeoID|P25pl|LTHS|HSGrd|SClgA|BchD")

    return df


def rename_cols(df):
    cols = map(str.lower, df.columns)
    # Replace dcp pop race codes with dcp DE established codes
    for code, race in race_suffix_mapper.items():
        cols = [col.replace(code, race) for col in cols]
    # Replace dcp pop stat suffix code with dcp DE codes
    cols = [map_stat_suffix(col, "count", False) for col in cols]
    # replace data point names
    for code, name in edu_name_mapper.items():
        cols = [col.replace(code, name) for col in cols]

    df.columns = cols
    return df


def pums_2000_economics(geography: str, year="2000", write_to_internal_review=False):
    """Main accessor. I know passing year here is silly, need to write it his way to
    export. Needs refactor"""
    assert geography in ["puma", "borough", "citywide"]
    assert year == "2000"

    df = load_2000_census_pums_all_data()

    df = filter_to_economic(df)

    final = rename_cols(df)

    if geography == "citywide":
        final = df.loc[["citywide"]].reset_index().rename(columns={"GeoID": "citywide"})
    elif geography == "borough":
        final = (
            df.loc[["BX", "BK", "MN", "QN", "SI"]]
            .reset_index()
            .rename(columns={"GeoID": "borough"})
        )
    else:
        final = df.loc["3701":"4114"].reset_index().rename(columns={"GeoID": "puma"})
        final["puma"] = final["puma"].apply(func=clean_PUMAs)

    final.set_index(geography, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, "economic_2000.csv", geography),
            ],
            "household_economic_security",
        )

    final = order_pums_2000_economic(final)

    return final


def order_pums_2000_economic(final: pd.DataFrame):
    """Quick function written up against deadline, can definitely be refactored"""
    indicators_denom: list[tuple] = [
        (
            "edu",
            "age_p25pl",
        )
    ]
    categories = {
        "edu": ["edu_lths", "edu_hs", "edu_smcol", "edu_bchpl", "age_p25pl"],
        "race": dcp_pop_races,
    }
    final = order_aggregated_columns(
        df=final,
        indicators_denom=indicators_denom,
        categories=categories,
        household=False,
        exclude_denom=True,
        demographics_category=False,
    )
    return final
