import pytest
from pathlib import Path
import pandas as pd
import geopandas as gpd
import shapely

from dcpy.utils import geospatial


RESOURCES_DIR = Path(__file__).parent / "resources"


@pytest.fixture(scope="module")
def data_wkb() -> pd.DataFrame:
    return pd.read_csv(
        RESOURCES_DIR / "data_wkb.csv",
    )


def test_convert_to_geodata_wkb(data_wkb):
    geodata = geospatial.convert_to_geodata(
        data=data_wkb,
        geometry_format=geospatial.GeometryFormat.wkb,
        geometry_column="geom",
    )

    assert isinstance(geodata, gpd.GeoDataFrame)
    assert geodata.columns.to_list() == ["row_id", "geometry", "geometry_error"]
    assert geodata.geom_type.to_list() == ["Point", "MultiPolygon", None, None, None]
    assert str(geodata.crs) == geospatial.GeometryCRS.wgs_84_deg.value


def test_from_wkb_fails(data_wkb):
    with pytest.raises(
        shapely.errors.GEOSException, match=r"Unexpected EOF parsing WKB"
    ):
        data_wkb["new_geometry_column"] = gpd.GeoSeries.from_wkb(data_wkb["geom"])


def test_projected_crs(data_wkb):
    new_crs = geospatial.GeometryCRS.ny_long_island.value
    geodata_source = geospatial.convert_to_geodata(
        data=data_wkb,
        geometry_format=geospatial.GeometryFormat.wkb,
        geometry_column="geom",
    )
    geodata = geodata_source.to_crs(new_crs)
    assert geodata.crs == new_crs
    assert round(geodata.area.sum(), 2) == 126924.61  # sqft
