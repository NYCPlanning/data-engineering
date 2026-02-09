import geopandas as gpd
import pandas as pd
import pytest
from python.utils import load_data_file, load_geodata_url, load_shapefile

from . import TEST_DATA_DIR, TOY_SECRET_1PASSWORD, TOY_SECRET_GITHUB


def test_use_of_github_secret():
    assert TOY_SECRET_GITHUB == "toy_secret_value"


def test_use_of_1password_secret():
    assert TOY_SECRET_1PASSWORD == "toy_secret_field"


def test_load_data_file_csv():
    data = load_data_file(filepath=f"{TEST_DATA_DIR}/minimal_data.csv")
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 3
    assert data["column_b"][2] == "b_2"


def test_load_data_file_json():
    data = load_data_file(filepath=f"{TEST_DATA_DIR}/minimal_data.json")
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 2
    assert data.iloc[1]["key_int"] == 300


def test_load_data_file_not_implemented():
    with pytest.raises(NotImplementedError):
        load_data_file(filepath=f"{TEST_DATA_DIR}/data_file.parquet")


def test_load_shapefile():
    data = load_shapefile(filepath=f"{TEST_DATA_DIR}/Open Streets Locations.zip")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 325


def test_load_geodata_url():
    open_streets_url = "https://data.cityofnewyork.us/resource/uiay-nctu.geojson"
    data = load_geodata_url(url=open_streets_url)
    assert isinstance(data, gpd.GeoDataFrame)
    assert "appronstre" in data.columns


@pytest.mark.skip(reason="result is 'Killed' due to large file")
def test_load_shapefile_large():
    geography = load_shapefile("./.data/dev_db", "housing.shp.zip")
    assert len(geography) > 1
