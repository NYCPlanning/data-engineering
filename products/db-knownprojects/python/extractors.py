# This file processes the raw source data into .sql (`/processed/`) files that are then used to build KPDB.

import sys

import geopandas as gpd
import pandas as pd
import shapely
from shapely import wkb

from . import RAW_DATA_PATH
from .utils import ETL, hash_each_row


@ETL
def dcp_knownprojects() -> pd.DataFrame:
    filename = "kpdb_20211006_shapefiles.zip"
    df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")
    df.rename(columns={"geom": "geometry"}, inplace=True)
    df["geometry"] = df["geometry"].fillna()
    return df


@ETL
def esd_projects() -> pd.DataFrame:
    filename = "2021.2.10 State Developments for Housing Pipeline.xlsx"
    df = pd.read_excel(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    return df


@ETL
def edc_projects() -> pd.DataFrame:
    filename = "2022.11.18 EDC inputs for DCP housing projections.xlsx"
    df = pd.read_excel(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    return df


@ETL
def dcp_n_study() -> pd.DataFrame:
    filename = "nstudy_rezoning_commitments_shapefile_20221017.zip"
    df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")
    return df


@ETL
def dcp_n_study_future() -> pd.DataFrame:
    filename = "future_nstudy_shapefile_20221017.zip"
    df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")
    return df


@ETL
def dcp_n_study_projected() -> gpd.geodataframe.GeoDataFrame:
    filename = "PastNeighborhoodStudies_20221019.zip"
    df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")
    return df


@ETL
def hpd_rfp() -> pd.DataFrame:
    filename = "20221122_HPD_RFPs.xlsx"
    df = pd.read_excel(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    return df


@ETL
def hpd_pc() -> pd.DataFrame:
    filename = "2022_11_23 DCP_SCA Pipeline.xlsx"
    df = pd.read_excel(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    return df


@ETL
def dcp_planneradded():
    filename = "dcp_planneradded_2023_03_21.csv"
    df = pd.read_csv(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    df.rename(columns={"WKT": "geometry"}, inplace=True)
    # print(df)
    return df


@ETL
def edc_dcp_inputs() -> gpd.geodataframe.GeoDataFrame:
    filename = "edc_shapefile_20221118.zip"
    df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")
    # print(df)
    return df


if __name__ == "__main__":
    name = sys.argv[1]
    assert name in list(locals().keys()), f"{name} is invalid"

    print(f"Extracting DCP Housing team source dataset '{name}' ...")
    locals()[name]()
    print("Done!")
