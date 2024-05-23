import pytest
from pathlib import Path
import pandas as pd
import geopandas as gpd
import shapely
from tempfile import TemporaryDirectory

from dcpy.models.geospatial import geometry
from dcpy.models.file import Geometry as FileGeometry
from dcpy.utils import geospatial, data


RESOURCES_DIR = Path(__file__).parent / "resources"


@pytest.fixture()
def data_wkb() -> pd.DataFrame:
    return pd.read_csv(
        RESOURCES_DIR / "data_wkb.csv",
    )


@pytest.fixture()
def data_wkt() -> pd.DataFrame:
    return pd.read_csv(
        RESOURCES_DIR / "data_wkt.csv",
    )


def test_df_to_gdf_wkb(data_wkb):
    epsg = "EPSG:4326"
    geom = FileGeometry(
        geom_column="geom",
        format=geometry.StandardGeometryFormat.wkb,
        crs=epsg,
    )
    gdf = geospatial.df_to_gdf(data_wkb, geom)

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.columns.to_list() == [
        "row_id",
        "geom",
    ]
    assert isinstance(gdf["geom"], gpd.GeoSeries)
    assert gdf.geom_type.to_list() == ["Point", "MultiPolygon", None, None, None]
    assert str(gdf.crs) == epsg


def test_df_to_gdf_wkt(data_wkt):
    epsg = "EPSG:4326"
    geom = FileGeometry(
        geom_column="geom",
        format=geometry.StandardGeometryFormat.wkt,
        crs=epsg,
    )
    geodata = geospatial.df_to_gdf(data_wkt, geom)

    assert isinstance(geodata, gpd.GeoDataFrame)
    assert geodata.columns.to_list() == [
        "row_id",
        "geom",
    ]
    assert isinstance(geodata["geom"], gpd.GeoSeries)
    assert geodata.geom_type.to_list() == ["Point", "MultiPolygon", None, None, None]
    assert str(geodata.crs) == epsg


def test_geoseries_constructors_fail(data_wkb, data_wkt):
    """
    This issue can either be handled by using shapely.from_wkb/wkt
    or by converting pandas 'nan's to 'None'
    """
    with pytest.raises(TypeError):
        data_wkb["new_geometry_column"] = gpd.GeoSeries.from_wkb(data_wkb["geom"])

    with pytest.raises(TypeError):
        data_wkb["new_geometry_column"] = gpd.GeoSeries.from_wkt(data_wkt["geom"])


def test_projected_crs(data_wkb):
    new_crs = "EPSG:2263"
    geom = FileGeometry(
        geom_column="geom",
        format=geometry.StandardGeometryFormat.wkb,
        crs="EPSG:4326",
    )
    gdf_source = geospatial.df_to_gdf(data_wkb, geom)
    gdf = gdf_source.to_crs(new_crs)
    assert gdf.crs == new_crs
    assert round(gdf.area.sum(), 2) == 126924.61  # sqft


@pytest.fixture()
def gdf(data_wkt):
    geometry = FileGeometry(geom_column="geom", crs="EPSG:4236")
    return data.df_to_gdf(data_wkt, geometry)


def test_read_geoparquet_metadata(gdf):
    with TemporaryDirectory() as dir:
        filepath = f"{dir}/tmp.parquet"
        gdf.to_parquet(filepath)

        meta = geospatial.read_geoparquet_metadata(filepath)

        assert "geom" in meta.geo_parquet.columns
        assert meta.geo_parquet.columns["geom"].crs.id
        assert meta.geo_parquet.columns["geom"].crs.id.code == 4236


def test_reproject_gdf(gdf):
    assert gdf.crs.srs == "EPSG:4236"
    state_plane = geospatial.reproject_gdf(gdf, target_crs="EPSG:2263")
    assert state_plane.crs.srs == "EPSG:2263"


@pytest.mark.parametrize(
    "xy_str, example",
    [
        ("x, y", "-74.0, 40.7"),
        ("Point(x y)", "Point(-74.0 40.7)"),
        pytest.param("Point(x y)", "-74.0 40.7", marks=pytest.mark.xfail),
        pytest.param("Point(x y y)", "-74.0 40.7", marks=pytest.mark.xfail),
    ],
)
def test_point_xy_str(xy_str, example):
    point_xy = geometry.PointXYStr(point_xy_str=xy_str)
    wkt = point_xy.wkt(example)
    assert shapely.from_wkt(wkt)
