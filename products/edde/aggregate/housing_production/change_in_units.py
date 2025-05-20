# from unittest import result
from typing import List
import pandas as pd
import geopandas as gpd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.geo_helpers import get_2020_pumas, borough_num_mapper

from ingest.ingestion_helpers import load_data


job_type_mapper = {
    "All": "",
    "Demolition": "demo",
    "New Building": "new",
    "Alteration_Increase": "alt_increase",
    "Alteration_Decrease": "alt_decrease",
}


def _load_2010_denom():
    df = (
        pd.read_csv(
            "resources/housing_production/2010_census_housing_units_by_2020_NTA.csv",
            dtype={"HUnits": int},
        ).rename(columns={"HUnits": "total_units_2010", "GeoType": "geo_type"})
        # .set_index("Geog")
    )
    df["geo_type"] = df["geo_type"].replace(
        {"NYC2020": "citywide", "Boro2020": "borough", "NTA2020": "puma"}
    )
    df.loc[df["geo_type"] == "borough", "borough"] = df.loc[
        df["geo_type"] == "borough"
    ].Geog
    df.loc[df["geo_type"] == "citywide", "citywide"] = "citywide"

    ntas_to_pumas = (
        load_data("dcp_population_nta_puma_crosswalk_2020")
        .set_index("nta_code")
        .rename(columns={"puma_code": "puma"})
    )
    ntas_to_pumas["puma"] = "0" + ntas_to_pumas["puma"]

    return df.set_index("Geog").join(ntas_to_pumas, how="left").reset_index(drop=True)


def get_columns() -> list:
    cols = [
        "job_number",
        "job_inactive",
        "complete_year",
        "job_status",
        "job_type",
        "boro",
        "classa_net",
        "latitude",
        "longitude",
    ]
    return cols


def pivot_and_flatten_index(df, geography):
    df_pivot = df.pivot(
        index=geography,
        columns="job_type",
        values=["classa_net", "pct"],
    )

    df_pivot.columns = ["_".join(a) for a in df_pivot.columns.to_flat_index()]

    df_pivot.rename(
        columns={
            "classa_net_": "classa_net",
            "pct_": "classa_net_pct",
            "pct_alt_decrease": "classa_net_alt_decrease_pct",
            "pct_alt_increase": "classa_net_alt_increase_pct",
            "pct_demo": "classa_net_demo_pct",
            "pct_new": "classa_net_new_pct",
        },
        inplace=True,
    )

    df_pivot.reset_index().set_index(geography, inplace=True)

    return df_pivot


def _load_housing_data():
    """Load dcp_housing, clean columns, and spatially join in the PUMA"""
    df = load_data("dcp_housing", cols=get_columns())
    df = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    ).to_crs("EPSG:2263")
    for c in [
        "complete_year",
        "classa_net",
        "latitude",
        "longitude",
    ]:
        df[c] = pd.to_numeric(df[c])

    # DROP INACTIVATE JOBS ACCRODING TO SAM
    df = df.loc[df.job_inactive.isnull()]
    df = df.loc[df["complete_year"] >= 2010]

    # drop records where their status is not complete
    df.drop(
        df.loc[df.job_status != "5. Completed Construction"].index, axis=0, inplace=True
    )
    df["citywide"] = "citywide"

    # drop rows where alterations is zero and create two types for alterations
    df.loc[(df.job_type == "Alteration") & (df.classa_net < 0), "job_type"] = (
        "Alteration_Decrease"
    )
    df.loc[(df.job_type == "Alteration") & (df.classa_net > 0), "job_type"] = (
        "Alteration_Increase"
    )
    df.drop(df.loc[df.job_type == "Alteration"].index, axis=0, inplace=True)

    df = df.sjoin(get_2020_pumas(), how="left", predicate="within")
    df.borough = df.boro.astype(str).map(borough_num_mapper)

    return df


def rename_col(cols) -> List:
    new_cols = [col if "pct" in col else col + "_count" for col in cols]

    return new_cols


def change_in_units(geography: str, write_to_internal_review=False):
    assert geography in ["citywide", "borough", "puma"]
    df = _load_housing_data()

    # aggregation begins here
    all_job_type = (
        df.groupby(geography)
        .agg({"classa_net": "sum", "job_type": "max"})
        .reset_index()
    )
    all_job_type.job_type = "All"

    results = (
        df.groupby(["job_type", geography]).agg({"classa_net": "sum"}).reset_index()
    )
    results = pd.concat([results, all_job_type], axis=0)

    # join with 2010 units from census
    census10 = _load_2010_denom()
    census_units = census10.groupby(geography)["total_units_2010"].sum().reset_index()
    results = results.merge(census_units, on=geography, how="left")

    results.job_type = results.job_type.map(job_type_mapper)

    results["pct"] = results["classa_net"] / results["total_units_2010"] * 100.0
    results["pct"] = results["pct"].round(2)
    results = pivot_and_flatten_index(results, geography=geography)

    final = pd.concat([results, census_units.set_index(geography)], axis=1)

    final.columns = rename_col(final.columns)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "change_in_units.csv", geography)],
            "housing_production",
        )

    return final
