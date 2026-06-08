import pandas as pd
from resources import load
from utils.geo_helpers import clean_PUMAs

_DATASETS = {
    "renter_occupied": {
        "resource": "nychvs_renter_occupied",
        "prefix": "units_occurental",
    },
    "rent_stabilized": {
        "resource": "nychvs_rent_stabilized",
        "prefix": "units_rentstable",
    },
    "units_occupied": {"resource": "nychvs_occupied", "prefix": "units_occu"},
    "three_plus_probs": {
        "resource": "nychvs_three_plus_probs",
        "prefix": "units_threemaintenance",
    },
}


def _load_dataset(dataset_name: str):
    dataset_resource = _DATASETS[dataset_name]["resource"]
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
        load(dataset_resource)
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


def three_maintenance_units(geography: str):
    rent_stable = _load_dataset("three_plus_probs")
    renter_occupied = _load_dataset("units_occupied")
    final = pd.concat([rent_stable, renter_occupied], axis=1).xs(
        geography, level="geo_type"
    )
    final.index.names = [geography]
    return final


def rent_stabilized_units(geography: str):
    rent_stable = _load_dataset("rent_stabilized")
    renter_occupied = _load_dataset("renter_occupied")
    final = pd.concat([rent_stable, renter_occupied], axis=1).xs(
        geography, level="geo_type"
    )
    final.index.names = [geography]
    return final
