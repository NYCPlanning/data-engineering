import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.clean_aggregated import order_affordable
from aggregate import load_aggregated
from utils.PUMA_helpers import (
    clean_PUMAs,
    borough_name_mapper,
    acs_years,
)
from utils.dcp_population_excel_helpers import (
    count_suffix_mapper_global,
    map_stat_suffix,
)

ind_mapper = {
    "Af": "units_affordable_",
}

income_mapper = {
    "ELI": "eli",
    "VLI": "vli",
    "LI": "li",
    "MI": "mi",
    "Midi": "midi",
    "HI": "hi",
}


ACS_BASE_VARIABLES = [
    "units_affordable_eli",
    "units_affordable_vli",
    "units_affordable_li",
    "units_affordable_mi",
    "units_affordable_midi",
    "units_affordable_hi",
]

INTERNAL_REVIEW_FILENAME = "units_affordable.csv"
CATEGORY = "housing_security"


def units_affordable_new(
    geography: str, year: str = acs_years[-1], write_to_internal_review=False
) -> pd.DataFrame:
    acs_df = load_aggregated.load_acs(year)
    acs_vars_table = load_aggregated.make_acs_parsed_variables_table(acs_df)

    acs_subset_df = load_aggregated.select_acs_cols(
        acs_df, year, ACS_BASE_VARIABLES, acs_vars_table
    )

    final = (
        acs_subset_df.loc[geography]
        .reset_index()
        .rename(columns={"geog": geography})
        .set_index(geography)
    )

    if write_to_internal_review:
        set_internal_review_files(
            [(final, INTERNAL_REVIEW_FILENAME, geography)],
            CATEGORY,
        )
    return final


def units_affordable(
    geography: str, year: str = acs_years[-1], write_to_internal_review=False
) -> pd.DataFrame:
    assert geography in ["citywide", "borough", "puma"]

    clean_df = load_source_clean_data(year)
    if geography == "puma":
        final = clean_df.loc[clean_df.Geog.str[0].isna()].copy()
        final["puma"] = final.Geog.apply(func=clean_PUMAs)
    elif geography == "borough":
        clean_df["borough"] = clean_df.Geog.map(borough_name_mapper)
        final = clean_df.loc[~clean_df.borough.isna()].copy()
    else:
        clean_df.loc[clean_df.Geog == "NYC", "citywide"] = "citywide"
        final = clean_df.loc[~clean_df.citywide.isna()].copy()

    final.drop(columns=["Geog"], inplace=True)
    final.set_index(geography, inplace=True)
    col_order = order_affordable(
        measures=[i for _, i in count_suffix_mapper_global.items()],
        income=[i for _, i in income_mapper.items()],
    )
    final = final.reindex(columns=col_order)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "units_affordable.csv", geography)],
            "housing_security",
        )
    return final


def load_source_clean_data(year) -> pd.DataFrame:
    read_excel_arg = {
        "io": "resources/housing_security/EDDT_UnitsAffordablebyAMI_2017-2021.xlsx",
        "sheet_name": "AffordableAMI",
        "usecols": "A:AJ",
        "header": 0,
        "nrows": 63,
    }

    df = pd.read_excel(**read_excel_arg)

    cols = df.columns
    for code, il in income_mapper.items():
        cols = [col.replace(code, il) for col in cols]
    for code, name in ind_mapper.items():
        cols = [col.replace(code, name) for col in cols]
    cols = [map_stat_suffix(col, "count", False) for col in cols]
    df.columns = cols

    return df
