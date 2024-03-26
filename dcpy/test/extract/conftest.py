import pytest
from shapely import wkt
import geopandas as gpd
from faker import Faker
import random
from pathlib import Path

from dcpy.extract import metadata

random.seed(0)

RESOURCES = Path(__file__).parent / "resources"
TEST_DATA_DIR = RESOURCES / "test_data"
TEST_DATA_NAME = "test"


@pytest.fixture()
def generate_fake_data(gdf: gpd.GeoDataFrame = None):
    """Generates test data in various data formats"""
    if not gdf:
        gdf = generate_gdf()

    csv_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.csv"
    shp_path = TEST_DATA_DIR / TEST_DATA_NAME
    gdb_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.gdb"
    parquet_path = TEST_DATA_DIR / f"{TEST_DATA_NAME}.parquet"

    gdf.to_csv(csv_path, index=False)
    gdf.to_file(shp_path, driver="ESRI Shapefile")
    gdf.to_file(gdb_path, driver="OpenFileGDB")
    gdf.to_parquet(parquet_path, index=False)


def generate_gdf() -> gpd.GeoDataFrame:
    """Generates geodataframe with fake data"""

    def generate_row() -> dict:
        """Generates fake row"""
        fakes = {
            "wkt": DCPFakes.wkt,
            "boro_code": DCPFakes.boro_code,
            "block": DCPFakes.block,
            "lot": DCPFakes.lot,
            "bbl": DCPFakes.bbl,
            "text": fake.pystr,
        }
        row = {}
        for k in fakes.keys():
            if k != "wkt":
                row[k] = (
                    fakes[k]() if random.random() > 0.3 else random.choice(["", None])
                )
            else:
                row[k] = fakes[k]() if random.random() > 0.3 else None
        return row

    num_rows = 5
    rows = [generate_row() for _ in range(num_rows)]
    gdf = gpd.GeoDataFrame(rows)
    gdf = gdf.set_geometry(col="wkt")
    gdf.crs = "EPSG:4326"

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
