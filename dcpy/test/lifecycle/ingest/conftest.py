import pytest
from typing import Callable
from shapely import wkt
import geopandas as gpd
import pandas as pd
from faker import Faker
import random
from pathlib import Path

random.seed(0)

RESOURCES = Path(__file__).parent / "resources"
TEST_DATA_DIR = RESOURCES / "test_data"
TEST_DATA_NAME = "test"


@pytest.fixture()
def generate_fake_data(gdf: gpd.GeoDataFrame = None):
    """Generates test data in various data formats"""
    if not gdf:
        gdf = generate_gdf()
        gdf2 = generate_gdf(
            columns=[
                "boro_code",
                "block",
                "lot",
                "bbl",
                "text",
                "longitude",
                "latitude",
            ]
        )

    csv_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.csv"
    csv_path_2 = TEST_DATA_DIR / f"{TEST_DATA_NAME}2.csv"
    excel_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.xlsx"
    shp_path = TEST_DATA_DIR / TEST_DATA_NAME
    gdb_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.gdb"
    json_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.json"
    geojson_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.geojson"
    parquet_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.parquet"

    gdf.to_csv(csv_path, index=False)
    gdf2.to_csv(csv_path_2, index=False)  # regular pandas df
    gdf2.to_excel(excel_path, index=False)  # regular pandas df
    gdf2.to_json(json_path, orient="records")  # regular pandas df
    gdf.to_file(shp_path, driver="ESRI Shapefile")
    gdf.to_file(gdb_path, driver="OpenFileGDB")
    gdf.to_file(geojson_path, driver="GeoJSON")
    gdf.to_parquet(parquet_path, index=False)


def generate_gdf(
    columns: list = ["boro_code", "block", "lot", "bbl", "text", "wkt"],
) -> gpd.GeoDataFrame | pd.DataFrame:
    """Generates a geodataframe with specified columns using fake data."""

    def generate_row(columns) -> dict:
        """Generates fake row based on specified columns"""
        data_generators: dict[str, Callable] = {
            "boro_code": DCPFakes.boro_code,
            "block": DCPFakes.block,
            "lot": DCPFakes.lot,
            "bbl": DCPFakes.bbl,
            "text": fake.pystr,
            "latitude": fake.latitude,
            "longitude": fake.longitude,
            "wkt": DCPFakes.wkt,
        }
        row = {}
        lat_lon_value = (
            random.random() > 0.3
        )  # determine if lat and lon should be set or be None

        for col in columns:
            if col in ["latitude", "longitude"]:
                row[col] = data_generators[col]() if lat_lon_value else None
            elif col == "wkt":
                row[col] = data_generators[col]() if random.random() > 0.3 else None
            else:
                row[col] = (
                    data_generators[col]()
                    if random.random() > 0.3
                    else random.choice(["", None])
                )

        return row

    num_rows = 5
    rows = [generate_row(columns=columns) for _ in range(num_rows)]

    if "wkt" in columns:
        gdf = gpd.GeoDataFrame(rows)
        gdf = gdf.set_geometry(col="wkt")
        gdf.crs = "EPSG:4326"
    else:
        gdf = pd.DataFrame(rows)

    return gdf


fake = Faker()


class DCPFakes:
    @staticmethod
    def wkt():
        return wkt.loads(f"Point({fake.latitude()} {fake.longitude()})")

    @staticmethod
    def boro_code():
        return str(random.randrange(1, 6))

    @staticmethod
    def block():
        return str(random.randrange(1, 100 * 1000))

    @staticmethod
    def lot():
        return str(random.randrange(1, 1000))

    @staticmethod
    def bbl():
        boro_code = DCPFakes.boro_code()
        block = DCPFakes.block()
        lot = DCPFakes.lot()
        return f"{boro_code}{block.zfill(5)}{lot.zfill(4)}"
