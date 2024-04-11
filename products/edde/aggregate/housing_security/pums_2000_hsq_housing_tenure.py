import pandas as pd
from utils.dcp_population_excel_helpers import (
    race_suffix_mapper,
    map_stat_suffix,
    load_2000_census_pums_all_data,
)
from utils.PUMA_helpers import clean_PUMAs, dcp_pop_races
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.aggregation_helpers import order_aggregated_columns

## Mapper specific to this script
housing_tenure_name_mapper = {
    "occhu": "units_occupied_2000",
    "oocc": "units_occupied_owner_2000",
    "rocc": "units_occupied_renter_2000",
}


def filter_to_hsq_indicators(df):
    """Filter to columns that are only housing tenure related that should be included in the housing
    Security + quality 2000 output"""
    df = df.filter(regex="Occ|OOcc|ROcc")
    return df


def rename_cols(df):
    cols = map(str.lower, df.columns)
    # Replace dcp pop race codes with dcp DE established codes
    for code, race in race_suffix_mapper.items():
        cols = [col.replace(code, race) for col in cols]
    # Replace dcp pop stat suffix code with dcp DE codes
    cols = [map_stat_suffix(col, "count", False) for col in cols]
    # replace data point names
    for code, name in housing_tenure_name_mapper.items():
        cols = [col.replace(code, name) for col in cols]

    df.columns = cols
    return df


def pums_2000_hsq_housing_tenure(geography: str, write_to_internal_review=False):
    """Main accessor for this indicator"""
    assert geography in ["puma", "borough", "citywide"]

    source_data = load_2000_census_pums_all_data()

    df = filter_to_hsq_indicators(source_data)

    final = rename_cols(df)

    if geography == "citywide":
        final = (
            final.loc[["citywide"]].reset_index().rename(columns={"GeoID": "citywide"})
        )
    elif geography == "borough":
        final = (
            final.loc[["BX", "BK", "MN", "QN", "SI"]]
            .reset_index()
            .rename(columns={"GeoID": "borough"})
        )
    else:
        final = final.loc["3701":"4114"].reset_index().rename(columns={"GeoID": "puma"})
        final["puma"] = final["puma"].apply(func=clean_PUMAs)

    final.set_index(geography, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, "security_2000_acs.csv", geography),
            ],
            "housing_security",
        )

    final = order_pums_2000_hsq(final)

    return final


def order_pums_2000_hsq(final: pd.DataFrame):
    """Quick function written up against deadline, can definitely be refactored"""
    indicators_denom: list[tuple] = [
        (
            "units",
            "units_occupied",
        )
    ]
    categories = {
        "units": [
            "units_occupied_owner_2000",
            "units_occupied_renter_2000",
            "units_occupied_2000",
        ],
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
