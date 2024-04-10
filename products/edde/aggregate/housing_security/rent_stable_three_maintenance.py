import pandas as pd
from utils.PUMA_helpers import clean_PUMAs
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import borough_name_mapper, clean_PUMAs

suffix_mapper = {
    "_N": "_count",
    "Percent MOE\n(95% CI)": "pct_moe",  # don't love this. But the order does matter here. As the MOE is a partial string match
    "MOE\n(95% CI)": "count_moe",
    "CV": "count_cv",
    "Percent": "pct",
}


def rent_stabilized_units(
    geography: str, write_to_internal_review=False
) -> pd.DataFrame:
    """Main accessor"""
    clean_data, denom = load_source_clean_data("units_rentstable")

    indi_final = extract_geography_dataframe(clean_data, geography)
    denom_final = extract_geography_dataframe(denom, geography)
    indi_final.columns = ["units_rentstable_" + c for c in indi_final.columns]
    denom_final.columns = ["units_occurental_" + c for c in denom_final.columns]
    final = pd.concat([indi_final, denom_final], axis=1)
    for code, suffix in suffix_mapper.items():
        final.columns = [c.replace(code, suffix) for c in final.columns]

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "units_rentstable.csv", geography)],
            "housing_security",
        )
    return final


def three_maintenance_units(
    geography: str, write_to_internal_review=False
) -> pd.DataFrame:
    clean_data, denom = load_source_clean_data("units_threemaintenance")

    indi_final = extract_geography_dataframe(clean_data, geography)
    denom_final = extract_geography_dataframe(denom, geography)
    indi_final.columns = ["units_threemaintenance_" + c for c in indi_final.columns]
    denom_final.columns = ["units_occu_" + c for c in denom_final.columns]
    final = pd.concat([indi_final, denom_final], axis=1)
    for code, suffix in suffix_mapper.items():
        final.columns = [c.replace(code, suffix) for c in final.columns]

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "units_threemaintenance.csv", geography)],
            "housing_security",
        )
    return final


def load_source_clean_data(indicator: str) -> pd.DataFrame:
    if indicator == "units_rentstable":
        usecols = [x for x in range(10)]
        sheetname = "2017 HVS Occupied Rental Units"
    elif indicator == "units_threemaintenance":
        usecols = [x for x in range(3)] + [x for x in range(11, 18)]
        sheetname = "2017 HVS Occupied Units"

    read_csv_arg = {
        "filepath_or_buffer": "resources/housing_security/2017_HVS_EDDT.csv",
        "usecols": usecols,
        "header": 1,
        "nrows": 63,
    }
    read_excel_arg = {
        "io": "resources/housing_security/2017 HVS Denominator.xlsx",
        "sheet_name": sheetname,
        "header": 1,
        "nrows": 63,
        "usecols": "A:G",
    }
    data = pd.read_csv(**read_csv_arg)
    data.columns = [c.replace(".1", "") for c in data.columns]
    data.drop(columns=["SE", "Percent SE"], inplace=True)
    data.replace("%", "", regex=True, inplace=True)
    data.replace(",", "", regex=True, inplace=True)
    num_cols = ["N", "MOE\n(95% CI)", "CV", "Percent", "Percent MOE\n(95% CI)"]
    data[num_cols] = data[num_cols].apply(pd.to_numeric, errors="coerce")
    data = data.reindex(
        columns=[
            "SBA",
            "PUMA",
            "CD Name",
            "N",
            "MOE\n(95% CI)",
            "CV",
            "Percent",
            "Percent MOE\n(95% CI)",
        ]
    )

    denom = pd.read_excel(**read_excel_arg)
    denom["PUMA"] = denom["PUMA"].astype(str)
    denom.drop(
        columns=["SE", "Percent", "Percent SE", "Percent MOE\n(95% CI)"],
        errors="ignore",
        inplace=True,
    )
    denom = denom.reindex(
        columns=[
            "SBA",
            "CD Name",
            "PUMA",
            "N",
            "MOE\n(95% CI)",
            "CV",
        ]
    )

    return data, denom


def extract_geography_dataframe(
    clean_data: pd.DataFrame, geography: str
) -> pd.DataFrame:

    if geography == "puma":
        clean_data["puma"] = clean_data["PUMA"].apply(func=clean_PUMAs)
        final = clean_data.loc[~clean_data.puma.isna()].copy()
        remv_label = ["03708 / 3707", "03710 / 3705"]
        final.drop(final.loc[final.puma.isin(remv_label)].index, axis=0, inplace=True)
    elif geography == "borough":
        clean_data["borough"] = clean_data["CD Name"].map(borough_name_mapper)
        final = clean_data.loc[~clean_data.borough.isna()].copy()
    else:
        clean_data.loc[clean_data["CD Name"] == "NYC", "citywide"] = "citywide"
        final = clean_data.loc[~clean_data.citywide.isna()].copy()

    drop_cols = [
        "SBA",
        "PUMA",
        "CD Name",
    ]

    final.drop(columns=drop_cols, inplace=True)
    final.set_index(geography, inplace=True)
    final = final.round(2)

    return final
