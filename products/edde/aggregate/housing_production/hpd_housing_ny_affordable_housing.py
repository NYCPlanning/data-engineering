import geopandas as gpd
import pandas as pd
from ingest.ingestion_helpers import load_data
from utils.geo_helpers import borough_name_mapper, puma_from_point

cols = [
    "project_id",
    "project_name",
    "project_start_date",
    "project_completion_date",
    "number",
    "street",
    "borough",
    "latitude",
    "longitude",
    "building_completion_date",
    "reporting_construction_type",
    "extremely_low_income_units",
    "very_low_income_units",
    "low_income_units",
    "moderate_income_units",
    "middle_income_units",
    "other_income_units",
]

unit_income_levels = [
    "extremely_low_income_units",
    "very_low_income_units",
    "low_income_units",
    "moderate_income_units",
    "middle_income_units",
    "other_income_units",
]


numeric_cols = [
    "extremely_low_income_units",
    "very_low_income_units",
    "low_income_units",
    "moderate_income_units",
    "middle_income_units",
    "other_income_units",
]


def _load_housing_ny():
    df = load_data(
        "hpd_hny_units_by_building",
    ).replace({"borough": borough_name_mapper})

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    ).to_crs("EPSG:2263")

    for c in numeric_cols:
        gdf[c] = pd.to_numeric(gdf[c])
    return gdf


def _pivot_add_total(df, geography):
    df = _pivot_and_flatten_index(df, geography)
    for t in ["preservation", "newconstruction"]:
        cols = [
            f"units_{band}_{t}_count"
            for band in ["eli", "vli", "li", "mi", "midi", "oi"]
        ]
        df[f"units_allami_{t}_count"] = df[cols].sum(axis=1)
    return df


def _pivot_and_flatten_index(df, geography):
    level_mapper = {
        "extremely_low_income_units": "units_eli",
        "very_low_income_units": "units_vli",
        "low_income_units": "units_li",
        "moderate_income_units": "units_mi",
        "middle_income_units": "units_midi",
        "other_income_units": "units_oi",
    }

    df = df.pivot(
        index=geography,
        columns="reporting_construction_type",
        values=unit_income_levels,
    )
    df.columns = ["_".join(a) for a in df.columns.to_flat_index()]
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    cols = df.columns
    for full, abbr in level_mapper.items():
        cols = [c.replace(full, abbr) for c in cols]
    df.columns = [
        c.replace("new_construction", "newconstruction") + "_count" for c in cols
    ]
    return df.reset_index()


def _citywide_hny_units_con_type(df):
    """Get total unit counts by construction type (new construction vs preservation)"""

    results = (
        df.groupby("reporting_construction_type")[unit_income_levels]
        .sum()
        .reset_index()
    )
    results["citywide"] = "citywide"
    results = _pivot_add_total(results, "citywide")
    return results.set_index("citywide")


def _borough_hny_units_con_type(df):
    results = (
        df.groupby(["reporting_construction_type", "borough"])[unit_income_levels]
        .sum()
        .reset_index()
    )
    return _pivot_add_total(results, "borough").set_index("borough")


def _puma_hny_units_con_type(df):
    """This function drops any confidential records (as per HPD) as they shouldn't be recorded at the
    PUMA level geography but kept in at larger geo units. Currently the assign_PUMA_col function can't
    take any lat/long that are null so drop those records, and assign PUMA using geography_helpers.py,
    and aggregate at the PUMA level"""
    filtered_df = df[df.project_name != "CONFIDENTIAL"].dropna(
        subset=["latitude_(internal)", "longitude_(internal)"]
    )
    filtered_df["puma"] = filtered_df.geometry.apply(puma_from_point)
    results = (
        filtered_df.groupby(["reporting_construction_type", "puma"])[unit_income_levels]
        .sum()
        .reset_index()
    )
    results = _pivot_add_total(results, "puma")
    return results.set_index("puma")


def affordable_housing(geography: str) -> pd.DataFrame:
    assert geography in ["citywide", "borough", "puma"]
    housing_ny = _load_housing_ny()
    if geography == "citywide":
        return _citywide_hny_units_con_type(housing_ny)
    elif geography == "borough":
        return _borough_hny_units_con_type(housing_ny)
    elif geography == "puma":
        return _puma_hny_units_con_type(housing_ny)
