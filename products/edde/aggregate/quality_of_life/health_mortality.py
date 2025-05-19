"""This file will have three accessors for three indicators that all rely on the same source data"""

import pandas as pd
import numpy as np
import re

from utils.geo_helpers import clean_PUMAs
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.clean_aggregated import order_PUMS_QOL_multiple_years

# Latest header in "resources/quality_of_life/health_mortality/DOHMH_death rate and overdose.xlsx"
LATEST_YEAR = "1620"

ind_name_mapper = {
    "infant_mortality_per1000": "infantmortality",
    "overdose_mortality_per100000": "overdosemortality",
    "premature_mortality_per100000": "prematuremortality",
}

race_mapper = {"_A": "_anh", "_B": "_bnh", "_H": "_hsp", "_W": "_wnh"}

borough_name_mapper = {
    "Bronx": "BX",
    "Brooklyn": "BK",
    "Manhattan": "MN",
    "Queens": "QN",
    "Staten Island": "SI",
}


def shorten_year_range(col):
    match = re.search("\\_(\\d{2})\\_(\\d{2})", col)
    if match:
        return col.replace(match.group(0), f"_{match.group(1)}{match.group(2)}")
    else:
        return col


def infant_mortality(geography: str, year=LATEST_YEAR, write_to_internal_review=False):
    ind_name = "infant_mortality_per1000"
    clean_data = load_clean_source_data(geography=geography)

    # columns operations
    cols = clean_data.columns
    ind_cols = [
        c for c in cols if ind_name in c
    ]  # only clean columns for the indicator performed
    final = rename_reorder_columns(
        clean_data[ind_cols], ind_name=ind_name, geography=geography, year=year
    )

    final.replace(to_replace="*", value=np.nan, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "health_infant_mortality.csv", geography)],
            category="quality_of_life",
        )
    return final


def overdose_mortality(
    geography: str, year=LATEST_YEAR, write_to_internal_review=False
):
    ind_name = "overdose_mortality_per100000"
    clean_data = load_clean_source_data(geography=geography)

    cols = clean_data.columns
    ind_cols = [c for c in cols if ind_name in c]
    final = rename_reorder_columns(
        clean_data[ind_cols], ind_name=ind_name, geography=geography, year=year
    )

    final.replace(to_replace="*", value=np.nan, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "health_overdose_mortality.csv", geography)],
            category="quality_of_life",
        )

    return final


def premature_mortality(
    geography: str, year=LATEST_YEAR, write_to_internal_review=False
):
    ind_name = "premature_mortality_per100000"
    clean_data = load_clean_source_data(geography=geography)

    cols = clean_data.columns
    ind_cols = [c for c in cols if ind_name in c]
    final = rename_reorder_columns(
        clean_data[ind_cols], ind_name=ind_name, geography=geography, year=year
    )

    final.replace(to_replace="*", value=np.nan, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "health_premature_mortality.csv", geography)],
            category="quality_of_life",
        )
    return final


def rename_reorder_columns(df: pd.DataFrame, ind_name: str, geography: str, year: str):
    if geography == "puma":
        years = ["0004", "1014"] + [year]
    else:
        years = ["2000", "2010"] + ["20" + year[2:]]

    cols = df.columns
    cols = [c.replace(ind_name, ind_name_mapper[ind_name]) for c in cols]
    for letter, race in race_mapper.items():
        cols = [c.replace(letter, race) for c in cols]
    if geography == "puma":
        cols = [shorten_year_range(c) for c in cols]
    df.columns = ["health_" + col + "_rate" for col in cols]
    # reorder items to standard
    col_order = order_PUMS_QOL_multiple_years(
        categories=["health_" + ind_name_mapper[ind_name]],
        measures=["rate"],
        years=years,
    )

    df = df.reindex(columns=col_order)

    return df


def load_clean_source_data(geography: str):
    read_excel_args = {
        "puma": {
            "io": "resources/quality_of_life/health_mortality/DOHMH_death rate and overdose.xlsx",
            "sheet_name": "PUMA",
            "header": 1,
            "nrows": 55,
            "dtype": {"PUMA": str},
        },
        "borough": {
            "io": "resources/quality_of_life/health_mortality/DOHMH_death rate and overdose.xlsx",
            "sheet_name": "Borough",
            "header": 1,
            "nrows": 5,
        },
        "citywide": {
            "io": "resources/quality_of_life/health_mortality/DOHMH_death rate and overdose.xlsx",
            "sheet_name": "City",
            "header": 1,
            "nrows": 1,
        },
    }

    source_data = pd.read_excel(**read_excel_args[geography])

    if geography == "puma":
        source_data.rename(columns={"PUMA": "puma"}, inplace=True)
        source_data["puma"] = source_data["puma"].apply(func=clean_PUMAs)
    elif geography == "citywide":
        source_data["City"] = "citywide"
        source_data.rename(columns={"City": "citywide"}, inplace=True)
    else:
        source_data.rename(columns={"Borough": "borough"}, inplace=True)
        source_data["borough"] = source_data["borough"].map(borough_name_mapper)

    clean_data = source_data.set_index(geography)

    return clean_data
