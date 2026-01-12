# This file processes the raw source data into .sql (`/processed/`) files that are then used to build KPDB.

import sys

import geopandas as gpd
import pandas as pd
import zipfile

from . import RAW_DATA_PATH
from .utils import ETL


@ETL
def dcp_knownprojects(filename: str) -> pd.DataFrame:
    df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")
    df.rename(columns={"geom": "geometry"}, inplace=True)
    df["geometry"] = df["geometry"].fillna()
    return df


@ETL
def esd_projects(filename: str) -> pd.DataFrame:
    df = pd.read_excel(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    return df


@ETL
def edc_projects(filename: str) -> pd.DataFrame:
    df = pd.read_excel(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    return df


@ETL
def edc_dcp_inputs(filename: str) -> gpd.GeoDataFrame:
    # latest shapefile fails to load as a zipfile
    # but this might work:
    # filename_prefix = f"{filename}".split(".")[0]
    # df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}!/{filename_prefix}")

    filename_prefix = f"{filename}".split(".")[0]
    with zipfile.ZipFile(f"{RAW_DATA_PATH}/{filename}", "r") as zip_ref:
        zip_ref.extractall(f"{RAW_DATA_PATH}")
    df = gpd.read_file(f"{RAW_DATA_PATH}/{filename_prefix}/{filename_prefix}.shp")
    return df


@ETL
def dcp_n_study(filename: str) -> pd.DataFrame:
    df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")
    return df


@ETL
def dcp_n_study_future(filename: str) -> pd.DataFrame:
    # df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")

    filename_prefix = f"{filename}".split(".")[0]
    with zipfile.ZipFile(f"{RAW_DATA_PATH}/{filename}", "r") as zip_ref:
        zip_ref.extractall(f"{RAW_DATA_PATH}")
    df = gpd.read_file(f"{RAW_DATA_PATH}/{filename_prefix}/{filename_prefix}.shp")
    return df


@ETL
def dcp_n_study_projected(filename: str) -> gpd.GeoDataFrame:
    # df = gpd.read_file(f"zip://{RAW_DATA_PATH}/{filename}")

    filename_prefix = f"{filename}".split(".")[0]
    with zipfile.ZipFile(f"{RAW_DATA_PATH}/{filename}", "r") as zip_ref:
        zip_ref.extractall(f"{RAW_DATA_PATH}")
    df = gpd.read_file(f"{RAW_DATA_PATH}/{filename_prefix}/{filename_prefix}.shp")
    return df


@ETL
def hpd_rfp(filename: str) -> pd.DataFrame:
    df = pd.read_excel(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    return df


@ETL
def dcp_planneradded(filename: str) -> pd.DataFrame:
    df = pd.read_csv(f"{RAW_DATA_PATH}/{filename}", dtype=str)
    df.rename(columns={"WKT": "geometry"}, inplace=True)
    return df


if __name__ == "__main__":
    name = sys.argv[1]
    assert name in list(locals().keys()), f"{name} is invalid"

    print(f"Extracting DCP Housing team source dataset '{name}' ...")
    locals()[name]()
    print("Done!")
