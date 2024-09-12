import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text

SQL_FILE_DIRECTORY = "sql"


def load_data_file(filepath: str) -> pd.DataFrame:
    file_extension = Path(filepath).suffix
    if file_extension == ".csv":
        data = pd.read_csv(filepath)
    elif file_extension == ".json":
        data = pd.read_json(filepath)
    else:
        raise NotImplementedError(f"Unsopported data file extension: {file_extension}")
    return data


def load_shapefile(filepath: str) -> gpd.GeoDataFrame:
    return gpd.read_file(filepath)


def load_geodata_url(url: str) -> gpd.GeoDataFrame:
    return gpd.read_file(url)


def query_sql_records(command: str) -> pd.DataFrame:
    sql_engine = create_engine(os.environ["BUILD_ENGINE"])
    with sql_engine.begin() as connection:
        select_records = pd.read_sql(text(command), connection)

    return select_records


def get_source_data_versions() -> pd.DataFrame:
    sql_engine = create_engine(os.environ["BUILD_ENGINE"])
    with sql_engine.begin() as connection:
        select_records = pd.read_sql_table("source_versions", connection)

    return select_records
