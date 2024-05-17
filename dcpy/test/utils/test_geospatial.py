import pytest
from pathlib import Path
import pandas as pd
import geopandas as gpd
from tempfile import TemporaryDirectory

from dcpy.models.file import Geometry
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


def test_convert_to_geodata_wkb(data_wkb):
    geodata = geospatial.convert_to_geodata(
        data=data_wkb,
        geometry_column="geom",
        geometry_format=geospatial.GeometryFormat.wkb,
        crs=geospatial.GeometryCRS.wgs_84_deg,
    )

    assert isinstance(geodata, gpd.GeoDataFrame)
    assert geodata.columns.to_list() == [
        "row_id",
        "geom",
        "geometry_generated",
        "geometry_error",
    ]
    assert isinstance(geodata["geometry_generated"], gpd.GeoSeries)
    assert geodata.geom_type.to_list() == ["Point", "MultiPolygon", None, None, None]
    assert list(geodata[geodata.geometry.isnull()]["geometry_error"].unique()) == [
        "Expected bytes or string, got float"
    ]
    assert str(geodata.crs) == geospatial.GeometryCRS.wgs_84_deg.value


# @pytest.mark.skip
def test_convert_to_geodata_wkt(data_wkt):
    geodata = geospatial.convert_to_geodata(
        data=data_wkt,
        geometry_column="geom",
        geometry_format=geospatial.GeometryFormat.wkt,
        crs=geospatial.GeometryCRS.wgs_84_deg,
    )

    assert isinstance(geodata, gpd.GeoDataFrame)
    assert geodata.columns.to_list() == [
        "row_id",
        "geom",
        "geometry_generated",
        "geometry_error",
    ]
    assert isinstance(geodata["geometry_generated"], gpd.GeoSeries)
    assert geodata.geom_type.to_list() == ["Point", "MultiPolygon", None, None, None]
    assert str(geodata.crs) == geospatial.GeometryCRS.wgs_84_deg.value


def test_geoseries_constructors_fail(data_wkb, data_wkt):
    with pytest.raises(TypeError):
        data_wkb["new_geometry_column"] = gpd.GeoSeries.from_wkb(data_wkb["geom"])

    with pytest.raises(TypeError):
        data_wkb["new_geometry_column"] = gpd.GeoSeries.from_wkt(data_wkt["geom"])


def test_projected_crs(data_wkb):
    new_crs = geospatial.GeometryCRS.ny_long_island.value
    geodata_source = geospatial.convert_to_geodata(
        data=data_wkb,
        geometry_column="geom",
        geometry_format=geospatial.GeometryFormat.wkb,
        crs=geospatial.GeometryCRS.wgs_84_deg,
    )
    geodata = geodata_source.to_crs(new_crs)
    assert geodata.crs == new_crs
    assert round(geodata.area.sum(), 2) == 126924.61  # sqft


@pytest.fixture()
def gdf(data_wkt):
    geometry = Geometry(geom_column="geom", crs="EPSG:4236")
    return data.df_to_gdf(data_wkt, geometry)


def test_read_parquet_metadata(gdf):
    with TemporaryDirectory() as dir:
        filepath = f"{dir}/tmp.parquet"
        gdf.to_parquet(filepath)

        meta = geospatial.read_parquet_metadata(filepath)

        assert "geom" in meta.geo_parquet.columns
        assert meta.geo_parquet.columns["geom"].crs.id
        assert meta.geo_parquet.columns["geom"].crs.id.code == 4236


def test_reproject_gdf(gdf):
    assert gdf.crs.srs == "EPSG:4236"
    state_plane = geospatial.reproject_gdf(gdf, target_crs="EPSG:2263")
    assert state_plane.crs.srs == "EPSG:2263"
