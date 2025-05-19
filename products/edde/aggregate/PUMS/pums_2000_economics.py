"""This python script takes the Household Economic Security indicators that Erica initially sent over
in an xlsx spreadsheet (Educational attainment data points) cleans them and outputs them so
that they can be collated using the established collate process"""

import pandas as pd

from aggregate.load_aggregated import load_2000_census
from aggregate.aggregation_helpers import order_aggregated_columns
from utils.geo_helpers import dcp_pop_races
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


def _filter_to_economic(df):
    """filter to educational attainment indicators"""
    df = df.filter(regex="GeoID|P25pl|LTHS|HSGrd|SClgA|BchD")

    return df


def _rename_cols(df):
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


def _order_pums_2000_economic(final: pd.DataFrame):
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


def pums_2000_economics(geography: str, year="2000", write_to_internal_review=False):
    assert geography in ["puma", "borough", "citywide"]
    assert year == "2000"

    final = (
        load_2000_census()
        .loc[geography]
        .rename_axis(geography)
        .pipe(_filter_to_economic)
        .pipe(_rename_cols)
        .pipe(_order_pums_2000_economic)
    )

    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, "economic_2000.csv", geography),
            ],
            "household_economic_security",
        )

    return final
