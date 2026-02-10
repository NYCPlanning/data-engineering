import pandas as pd
from aggregate.aggregation_helpers import (
    demographic_indicators_denom,
    get_category,
    order_aggregated_columns,
)
from aggregate.load_aggregated import load_2000_census
from internal_review.set_internal_review_file import set_internal_review_files
from utils.dcp_population_excel_helpers import (
    map_stat_suffix,
    race_suffix_mapper,
    remove_duplicate_cols,
)
from utils.geo_helpers import dcp_pop_races

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


def _filter_to_demo_indicators(df):
    """Remove columns that don't pretain to demographic indicators (Occupied units,
    renter occupied units, owner occupied units which come 200 ACS PUMS Data but are
    Housing Security and Quality category indicators"""
    df = df.drop(
        df.filter(regex="P25pl|LTHS|HSGrd|SClgA|BchD|Occ|OOcc|ROcc").columns, axis=1
    )
    return df


def _rename_cols(df):
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


def _order_pums_2000_demographics(final: pd.DataFrame):
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


def pums_2000_demographics(geography: str, year="2000", write_to_internal_review=False):
    assert year == "2000"
    final = (
        load_2000_census()
        .loc[geography]
        .rename_axis(geography)
        .pipe(_filter_to_demo_indicators)
        .pipe(remove_duplicate_cols)
        .pipe(_rename_cols)
        .pipe(_order_pums_2000_demographics)
    )

    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, "demographics_00_PUMS.csv", geography),
            ],
            "demographics",
        )

    return final
