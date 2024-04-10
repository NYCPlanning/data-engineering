import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.clean_aggregated import order_affordable
from utils.PUMA_helpers import clean_PUMAs, borough_name_mapper, year_range, acs_years
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
        "io": f"resources/housing_security/EDDT_UnitsAffordablebyAMI_{year_range(year)}.xlsx",
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
