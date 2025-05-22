from typing import List
import pandas as pd
from utils.dcp_population_excel_helpers import (
    map_stat_suffix,
    race_suffix_mapper,
    race_suffix_mapper_global,
    reorder_year_race,
)
from utils.geo_helpers import acs_years


def sort_columns(df: pd.DataFrame):
    """Put each indicator next to it's standard error"""
    return df.reindex(sorted(df.columns), axis=1)


def order_PUMS_QOL(categories, measures) -> List:
    """Use local races constant instead of importing from PUMS helpers because no onh
    in this data"""
    rv = []
    for c in categories:
        for m in measures:
            rv.append(f"{c}_{m}")
    for c in categories:
        for r in race_suffix_mapper_global.values():
            for m in measures:
                rv.append(f"{c}_{r}_{m}")

    return rv


def order_PUMS_QOL_multiple_years(categories, measures, years):
    rv = []
    for y in years:
        for c in categories:
            for m in measures:
                rv.append(f"{c}_{y}_{m}")
        for c in categories:
            for r in race_suffix_mapper_global.values():
                for m in measures:
                    rv.append(f"{c}_{y}_{r}_{m}")

    return rv


def order_affordable(measures, income) -> List:
    rv = []
    for i in income:
        for m in measures:
            rv.append(f"units_affordable_{i}_{m}")

    return rv


def rename_col_housing_security(
    df: pd.DataFrame,
    name_mapper: dict,
    race_mapper: dict,
    suffix_mode: str,
):
    """Rename the columns to follow conventions laid out in the wiki and issue #59"""
    cols = map(str.lower, df.columns)
    # Recode race id
    for code, race in race_mapper.items():
        cols = [col.replace(code, race) for col in cols]

    # Recode year
    for year in acs_years:
        cols = [col.replace(year[2:], year) for col in cols]

    # Recode standard stat suffix
    cols = [map_stat_suffix(col, suffix_mode, True) for col in cols]

    # Rename data points
    for k, ind_name in name_mapper.items():
        cols = [col.replace(k.lower(), ind_name) for col in cols]

    # Rename the columns to follow wiki conventions
    cols = [reorder_year_race(col) for col in cols]

    df.columns = cols

    return df


def rename_columns_demo(df: pd.DataFrame, end_year: str):
    cols = map(str.lower, df.columns)
    for code, race in race_suffix_mapper.items():
        cols = [col.replace(code, race) for col in cols]

    # cols = [col.replace(f"_{end_year}e", f"_{year}_count") for col in cols]
    # cols = [col.replace(f"_{end_year}m", f"_{year}_count_moe") for col in cols]
    # cols = [col.replace(f"_{end_year}c", f"_{year}_count_cv") for col in cols]
    # cols = [col.replace(f"_{end_year}p", f"_{year}_pct") for col in cols]
    # cols = [col.replace(f"_{end_year}z", f"_{year}_pct_moe") for col in cols]

    cols = [col.replace(f"_{end_year}e", "_count") for col in cols]
    cols = [col.replace(f"_{end_year}m", "_count_moe") for col in cols]
    cols = [col.replace(f"_{end_year}c", "_count_cv") for col in cols]
    cols = [col.replace(f"_{end_year}p", "_pct") for col in cols]
    cols = [col.replace(f"_{end_year}z", "_pct_moe") for col in cols]

    cols = [col.replace("mdage", "age_median") for col in cols]
    cols = [col.replace("pu16", "age_popu16") for col in cols]
    cols = [col.replace("p16t64", "age_p16t64") for col in cols]
    cols = [col.replace("p65pl", "age_p65pl") for col in cols]
    cols = [col.replace("pop5p", "age_p5pl") for col in cols]
    cols = [col.replace("pop1", "pop_denom") for col in cols]

    # for k, v in reorder_year_race_mapper.items():

    # cols = [col.replace(k, v) for col in cols]

    cols = [col.replace("count", "median") if "median" in col else col for col in cols]

    df.columns = cols

    return df
