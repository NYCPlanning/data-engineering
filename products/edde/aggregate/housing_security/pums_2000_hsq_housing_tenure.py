import pandas as pd

from aggregate.load_aggregated import load_2000_census
from aggregate.aggregation_helpers import order_aggregated_columns
from utils.dcp_population_excel_helpers import (
    race_suffix_mapper,
    map_stat_suffix,
)
from utils.PUMA_helpers import clean_PUMAs, dcp_pop_races
from internal_review.set_internal_review_file import set_internal_review_files

_housing_tenure_name_mapper = {
    "occhu": "units_occupied_2000",
    "oocc": "units_occupied_owner_2000",
    "rocc": "units_occupied_renter_2000",
}


def _rename_cols(df):
    cols = map(str.lower, df.columns)
    # Replace dcp pop race codes with dcp DE established codes
    for code, race in race_suffix_mapper.items():
        cols = [col.replace(code, race) for col in cols]
    # Replace dcp pop stat suffix code with dcp DE codes
    cols = [map_stat_suffix(col, "count", False) for col in cols]
    # replace data point names
    for code, name in _housing_tenure_name_mapper.items():
        cols = [col.replace(code, name) for col in cols]

    df.columns = cols
    return df


def _order_pums_2000_hsq(final: pd.DataFrame):
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


def pums_2000_hsq_housing_tenure(geography: str, write_to_internal_review=False):
    assert geography in ["puma", "borough", "citywide"]

    source_data = load_2000_census()

    """Filter to columns that are only housing tenure related that should be included in the housing
    Security + quality 2000 output"""
    df = source_data.filter(regex="Occ|OOcc|ROcc")

    final = _rename_cols(df)

    boroughs = ["BX", "BK", "MN", "QN", "SI"]

    if geography == "citywide":
        final = (
            final.loc[["citywide"]].reset_index().rename(columns={"GeoID": "citywide"})
        )
    elif geography == "borough":
        final = final.loc[boroughs].reset_index().rename(columns={"GeoID": "borough"})
    else:
        non_puma_indices = boroughs + ["citywide"]
        final = (
            final.loc[~final.index.isin(non_puma_indices)]
            .reset_index()
            .rename(columns={"GeoID": "puma"})
        )
        final["puma"] = final["puma"].apply(func=clean_PUMAs)

    final.set_index(geography, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, "security_2000_acs.csv", geography),
            ],
            "housing_security",
        )

    final = _order_pums_2000_hsq(final)

    return final
