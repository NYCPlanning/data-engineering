import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.geo_helpers import clean_PUMAs

_HVS_PATH = "resources/housing_security/nychvs_2023.xlsx"

_DATASETS = {
    "renter_occupied": {
        "tab": "Renter-occupied housing units",
        "prefix": "units_occurental",
    },
    "rent_stabilized": {
        "tab": "Occupied rent stabilized",
        "prefix": "units_rentstable",
    },
    "units_occupied": {"tab": "Occupied housing units", "prefix": "units_occu"},
    "three_plus_probs": {
        "tab": "Occupied housing 3+ problems",
        "prefix": "units_threemaintenance",
    },
}


def _load_nychvs_excel(sheet_name):
    return pd.read_excel(io=_HVS_PATH, sheet_name=sheet_name, dtype={"geo_id": str})


def _load_dataset(dataset_name: str):
    dataset_tab = _DATASETS[dataset_name]["tab"]
    dataset_prefix = _DATASETS[dataset_name]["prefix"]

    def _clean_puma_row(df_row):
        if df_row.geo_type == "puma":
            df_row.geo_id = clean_PUMAs(df_row.geo_id)
        return df_row

    def _percentize_cols(df_series):
        return (
            df_series * 100
            if df_series.name in {"count_cv", "pct", "pct_moe"}
            else df_series
        )

    df = (
        _load_nychvs_excel(dataset_tab)
        .apply(_clean_puma_row, axis=1)
        .set_index(
            ["geo_id", "geo_type"],
        )
        .drop(
            columns=["count_se", "pct_se", "pct_cv"], errors="ignore"
        )  # ignore errors bc we migh not have pct_se col
        .apply(pd.to_numeric, errors="coerce")
        .apply(_percentize_cols, axis=0)
    )
    df.columns = [f"{dataset_prefix}_{ic}" for ic in df.columns]
    return df


def three_maintenance_units(geography: str, write_to_internal_review=False):
    rent_stable = _load_dataset("three_plus_probs")
    renter_occupied = _load_dataset("units_occupied")
    final = pd.concat([rent_stable, renter_occupied], axis=1).xs(
        geography, level="geo_type"
    )
    final.index.names = [geography]
    if write_to_internal_review:
        set_internal_review_files(
            [(final, "units_threemaintenance.csv", geography)],  # type: ignore
            "housing_security",
        )
    return final


def rent_stabilized_units(geography: str, write_to_internal_review=False):
    rent_stable = _load_dataset("rent_stabilized")
    renter_occupied = _load_dataset("renter_occupied")
    final = pd.concat([rent_stable, renter_occupied], axis=1).xs(
        geography, level="geo_type"
    )
    final.index.names = [geography]
    if write_to_internal_review:
        set_internal_review_files(
            [(final, "units_rentstable.csv", geography)],  # type: ignore
            "housing_security",
        )
    return final
