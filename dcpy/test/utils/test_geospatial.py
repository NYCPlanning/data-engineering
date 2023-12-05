import pytest
from unittest import TestCase
from pathlib import Path
import pandas as pd
import geopandas as gpd

from dcpy.utils import geospatial


RESOURCES_DIR = Path(__file__).parent / "resources"


@pytest.fixture(scope="module")
def data_wkb() -> pd.DataFrame:
    return pd.read_csv(
        RESOURCES_DIR / "data_wkb.csv",
    )


def test_convert_to_geodata_wkb(data_wkb):
    geodata = geospatial.convert_to_geodata(
        data=data_wkb, geometry_format="WKB", geometry_column="geom"
    )

    assert isinstance(geodata, gpd.GeoDataFrame)
    assert geodata.columns.to_list() == ["row_id", "geometry", "geometry_error"]
    assert geodata.geom_type.to_list() == ["Point", "MultiPolygon", None, None, None]
    assert str(geodata.crs) == geospatial.PROJECTIONS["WGS_84_DEG"]


def test_reproject_geodata(data_wkb):
    new_crs = geospatial.PROJECTIONS["NY_LONG_ISLAND"]
    geodata_source = geospatial.convert_to_geodata(
        data=data_wkb, geometry_format="WKB", geometry_column="geom"
    )
    geodata = geospatial.reproject_geodata(data=geodata_source, new_crs=new_crs)
    assert geodata.crs == new_crs
    assert round(geodata.area.sum(), 2) == 126924.61  # sqft
