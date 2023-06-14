import geopandas as gpd
import pytest

from python.geospatial import (
    NYC_PROJECTION,
    WKT_PROJECTION,
    convert_to_geodata,
    reproject_geometry,
)
from python.utils import load_data_file

TEST_DATA_PATH = "tests/test_data"


def test_convert_to_geodata():
    data = load_data_file(filepath=f"{TEST_DATA_PATH}/census_counties_nyc.csv")
    data = convert_to_geodata(data)
    assert isinstance(data, gpd.GeoDataFrame)
    assert data.crs == WKT_PROJECTION


def test_reporject_geometry():
    data = load_data_file(filepath=f"{TEST_DATA_PATH}/census_counties_nyc.csv")
    data = convert_to_geodata(data)
    data = reproject_geometry(data, WKT_PROJECTION, NYC_PROJECTION)
    assert data.crs == NYC_PROJECTION
