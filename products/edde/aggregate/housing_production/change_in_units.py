# from unittest import result
from typing import List
import pandas as pd
import geopandas as gpd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.geo_helpers import clean_PUMAs, PUMAs

from ingest.ingestion_helpers import read_from_S3


job_type_mapper = {
    "All": "",
    "Demolition": "demo",
    "New Building": "new",
    "Alteration_Increase": "alt_increase",
    "Alteration_Decrease": "alt_decrease",
}


def load_housing_data():
    df = read_from_S3("dcp_housing", "housing_production", cols=get_columns())

    for c in [
        "complete_year",
        "classa_net",
        "latitude",
        "longitude",
    ]:
        df[c] = pd.to_numeric(df[c])

    census10 = pd.read_excel(
        "https://www1.nyc.gov/assets/planning/download/office/planning-level/nyc-population/census2010/tothousing_vacant_2010ct.xlsx",
        header=4,
        usecols=[
            "2010 Census FIPS County Code",
            "2010 DCP Borough Code",
            "2010 Census Tract",
            "Total Housing Units",
        ],
        dtype=str,
    )

    puma_cross = pd.read_excel(
        "https://www1.nyc.gov/assets/planning/download/office/data-maps/nyc-population/census2010/nyc2010census_tabulation_equiv.xlsx",
        header=3,
        dtype=str,
        usecols=["2010 Census Bureau FIPS County Code", "2010 Census Tract", "PUMA"],
    )

    census10["nycct"] = (
        census10["2010 Census FIPS County Code"] + census10["2010 Census Tract"]
    )

    puma_cross["nycct"] = (
        puma_cross["2010 Census Bureau FIPS County Code"]
        + puma_cross["2010 Census Tract"]
    )
    puma_cross["PUMA"] = puma_cross["PUMA"].apply(clean_PUMAs)
    puma_cross["PUMA"] = puma_cross["PUMA"].apply(clean_PUMAs)

    census10_ = census10.merge(puma_cross[["nycct", "PUMA"]], how="left", on="nycct")

    census10_["Total Housing Units"] = census10_["Total Housing Units"].apply(
        lambda x: pd.to_numeric(x, errors="coerce")
    )

    census10_.rename(
        columns={
            "Total Housing Units": "total_units_2010",
            "2010 DCP Borough Code": "borough",
            "PUMA": "puma",
        },
        inplace=True,
    )
    census10_.borough = census10_.borough.map(
        {"1": "MN", "2": "BX", "3": "BK", "4": "QN", "5": "SI"}
    )
    census10_["citywide"] = "citywide"
    return df, census10_


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


def clean_jobs(df):
    # DROP INACTIVATE JOBS ACCRODING TO SAM
    df = df.loc[df.job_inactive.isnull()]
    df = df.loc[df["complete_year"] >= 2010]

    # drop records where their status is not complete
    df.drop(
        df.loc[df.job_status != "5. Completed Construction"].index, axis=0, inplace=True
    )

    # drop rows where alterations is zero and create two types for alterations
    df.loc[(df.job_type == "Alteration") & (df.classa_net < 0), "job_type"] = (
        "Alteration_Decrease"
    )

    df.loc[(df.job_type == "Alteration") & (df.classa_net > 0), "job_type"] = (
        "Alteration_Increase"
    )

    df.drop(df.loc[df.job_type == "Alteration"].index, axis=0, inplace=True)

    df["citywide"] = "citywide"
    df.rename(columns={"boro": "borough"}, inplace=True)
    puma = PUMAs
    puma = puma[["puma", "geometry"]]
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    df = gdf.sjoin(puma, how="left", predicate="within")
    df.borough = df.borough.astype(str)
    df.borough = df.borough.map({"1": "MN", "2": "BX", "3": "BK", "4": "QN", "5": "SI"})

    return df


def rename_col(cols) -> List:
    new_cols = [col if "pct" in col else col + "_count" for col in cols]

    return new_cols


def change_in_units(geography: str, write_to_internal_review=False):
    """Main accessor for this function"""
    assert geography in ["citywide", "borough", "puma"]
    df, census10 = load_housing_data()
    df = clean_jobs(df)

    # aggregation begins here
    results = (
        df.groupby(["job_type", geography]).agg({"classa_net": "sum"}).reset_index()
    )
    all_job_type = (
        df.groupby(geography)
        .agg({"classa_net": "sum", "job_type": "max"})
        .reset_index()
    )
    all_job_type.job_type = "All"
    results = pd.concat([results, all_job_type], axis=0)

    # join with 2010 units from census
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
