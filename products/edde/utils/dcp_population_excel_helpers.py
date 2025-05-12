import pandas as pd
import re

# Creat helpful global mappers for dcp

count_suffix_mapper_global = {
    "e": "count",
    "m": "count_moe",
    "c": "count_cv",
    "p": "pct",
    "z": "pct_moe",
}

median_suffix_mapper_global = {
    "e": "median",
    "m": "median_moe",
    "c": "median_cv",
    "p": "median_pct",  # To be removed on further processing
    "z": "median_pct_moe",  # To be removed on further processing
}

measure_suffixes = {
    "e": "",
    "m": "moe",
    "c": "cv",
    "p": "pct",
    "z": "pct_moe",
}

race_suffix_mapper_global = {"a": "anh", "b": "bnh", "h": "hsp", "w": "wnh"}

race_suffix_mapper = {
    "_a": "_anh_",
    "_b": "_bnh_",
    "_h": "_hsp_",
    "_w": "_wnh_",
}


def map_stat_suffix(col, mode, keep_year):
    if mode == "count":
        suffix_mapper = count_suffix_mapper_global
    elif mode == "median":
        suffix_mapper = median_suffix_mapper_global
    else:
        raise Exception(
            "Function map_stat_suffix expects argument 'mode' to be either 'count' or 'median'"
        )
    match = re.search("\\_(\\d{2}|\\d{4})(E|M|C|P|Z|e|m|c|p|z)$", col)
    if match:
        if keep_year:
            new_suffix = f"_{match.group(1)}_{suffix_mapper[match.group(2).lower()]}"
        else:
            new_suffix = f"_{suffix_mapper[match.group(2).lower()]}"
        return col.replace(match.group(0), new_suffix)
    else:
        return col


def reorder_year_race(col):
    match = re.search(
        f"\\_({'|'.join(race_suffix_mapper_global.values())})\\_(\\d{{4}})", col
    )
    if match:
        return col.replace(match.group(0), f"_{match.group(2)}_{match.group(1)}")
    else:
        return col


### Create base load function that reads dcp population xlsx for 2000 census pums
def load_2000_census_pums_all_data() -> pd.DataFrame:
    df = pd.read_excel(
        "./resources/ACS_PUMS/EDDE_Census2000PUMS.xlsx",
        skiprows=1,
        dtype={"GeoID": str},
    )
    df = df.replace(
        {
            "GeoID": {
                "Bronx": "BX",
                "Brooklyn": "BK",
                "Manhattan": "MN",
                "Queens": "QN",
                "Staten Island": "SI",
                "NYC": "citywide",
            }
        }
    )
    df.set_index("GeoID", inplace=True)
    return df


def remove_duplicate_cols(df):
    """Excel spreadsheet has some duplicate columns that Erica used for calculations"""
    df = df.drop(df.filter(regex="E.1$|M.1$|C.1$|P.1$|Z.1$").columns, axis=1)
    return df
