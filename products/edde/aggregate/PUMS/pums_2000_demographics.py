import pandas as pd

from aggregate.load_aggregated import load_2000_census_pums_all_data
from aggregate.aggregation_helpers import (
    demographic_indicators_denom,
    order_aggregated_columns,
    get_category,
)
from utils.PUMA_helpers import clean_PUMAs, dcp_pop_races
from internal_review.set_internal_review_file import set_internal_review_files
from utils.dcp_population_excel_helpers import (
    race_suffix_mapper,
    map_stat_suffix,
    remove_duplicate_cols,
)

# Create name mapper for variables - include like for like swap for consistncy
name_mapper = {
    "fb": "fb",
    "lep": "lep",
    "mdage": "age_median",
    "p5pl": "age_p5pl",
    "pu16": "age_popu16",
    "p16t64": "age_p16t64",
    "p65pl": "age_p65pl",
    "pop_": "pop_denom_",
}

median_mapper = {
    "age_median_count": "age_median_median",
    "age_median_anh_count": "age_median_anh_median",
    "age_median_bnh_count": "age_median_bnh_median",
    "age_median_hsp_count": "age_median_hsp_median",
    "age_median_wnh_count": "age_median_wnh_median",
}


def filter_to_demo_indicators(df):
    """Remove columns that don't pretain to demographic indicators (Occupied units,
    renter occupied units, owner occupied units which come 200 ACS PUMS Data but are
    Housing Security and Quality category indicators"""
    df = df.drop(
        df.filter(regex="P25pl|LTHS|HSGrd|SClgA|BchD|Occ|OOcc|ROcc").columns, axis=1
    )
    return df


def rename_cols(df):
    cols = map(str.lower, df.columns)
    for code, race in race_suffix_mapper.items():
        cols = [col.replace(code, race) for col in cols]
    cols = [map_stat_suffix(col, "count", False) for col in cols]
    for code, name in name_mapper.items():
        cols = [col.replace(code, name) for col in cols]
    for code, med in median_mapper.items():
        cols = [col.replace(code, med) for col in cols]

    df.columns = cols
    return df


def pums_2000_demographics(geography: str, year="2000", write_to_internal_review=False):
    """Main accessor. I know passing year here is silly, need to write it his way to
    export. Needs refactor"""
    assert year == "2000"
    source_data = load_2000_census_pums_all_data()

    source_data = filter_to_demo_indicators(source_data)

    source_data = remove_duplicate_cols(source_data)

    source_data = rename_cols(source_data)

    if geography == "citywide":
        final = (
            source_data.loc[["citywide"]]
            .reset_index()
            .rename(columns={"GeoID": "citywide"})
        )
    elif geography == "borough":
        final = (
            source_data.loc[["BX", "BK", "MN", "QN", "SI"]]
            .reset_index()
            .rename(columns={"GeoID": "borough"})
        )
    else:
        final = (
            source_data.loc["3701":"4114"]
            .reset_index()
            .rename(columns={"GeoID": "puma"})
        )
        final["puma"] = final["puma"].apply(func=clean_PUMAs)

    final.set_index(geography, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, "demographics_00_PUMS.csv", geography),
                # (df_borough, "demographics_00_PUMS.csv", "borough"),
                # (df_puma, "demographics_00_PUMS.csv", "puma"),
            ],
            "demographics",
        )

    final = order_pums_2000_demographics(final)
    return final


def order_pums_2000_demographics(final: pd.DataFrame):
    """Quick function written up against deadline, can definitely be refactored"""
    indicators_denom = demographic_indicators_denom
    categories = {
        "LEP": ["lep"],
        "foreign_born": ["fb"],
        "age_bucket": get_category("age_bucket"),
        "total_pop": ["pop_denom"],
        "age_p5pl": ["age_p5pl"],
        "race": dcp_pop_races,
    }
    final = order_aggregated_columns(
        df=final,
        indicators_denom=indicators_denom,
        categories=categories,
        household=False,
        exclude_denom=True,
        demographics_category=True,
    )
    return final
