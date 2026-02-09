from pathlib import Path
from tempfile import TemporaryDirectory

import geopandas as gpd
import pandas as pd
import pytest
import shapely
from shapely import (
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

from dcpy.models.file import Geometry as FileGeometry
from dcpy.models.geospatial import geometry
from dcpy.utils.geospatial import parquet, transform

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


@pytest.fixture()
def gdf(data_wkt):
    geometry = FileGeometry(geom_column="geom", crs="EPSG:4236")
    return transform.df_to_gdf(data_wkt, geometry)


class TestModels:
    """
    This code maybe doesn't quite belong here, but while we don't have
    much testing around models, this custom model functionality seemed to belong here
    """

    @pytest.mark.parametrize(
        "xy_str, example",
        [
            ("x, y", "-74.0, 40.7"),
            ("Point(x y)", "Point(-74.0 40.7)"),
            pytest.param("Point(x y)", "-74.0 40.7", marks=pytest.mark.xfail),
            pytest.param("Point(x y y)", "-74.0 40.7", marks=pytest.mark.xfail),
        ],
    )
    def test_point_xy_str(self, xy_str, example):
        point_xy = geometry.PointXYStr(point_xy_str=xy_str)
        wkt = point_xy.wkt(example)
        assert shapely.from_wkt(wkt)


class TestTransform:
    def test_df_to_gdf_wkb(self, data_wkb):
        epsg = "EPSG:4326"
        geom = FileGeometry(
            geom_column="geom",
            format=geometry.StandardGeometryFormat.wkb,
            crs=epsg,
        )
        gdf = transform.df_to_gdf(data_wkb, geom)

        assert isinstance(gdf, gpd.GeoDataFrame)
        assert gdf.columns.to_list() == [
            "row_id",
            "geom",
        ]
        assert isinstance(gdf["geom"], gpd.GeoSeries)
        assert gdf.geom_type.to_list() == ["Point", "MultiPolygon", None, None, None]
        assert str(gdf.crs) == epsg

    def test_df_to_gdf_wkt(self, data_wkt):
        epsg = "EPSG:4326"
        geom = FileGeometry(
            geom_column="geom",
            format=geometry.StandardGeometryFormat.wkt,
            crs=epsg,
        )
        geodata = transform.df_to_gdf(data_wkt, geom)

        assert isinstance(geodata, gpd.GeoDataFrame)
        assert geodata.columns.to_list() == [
            "row_id",
            "geom",
        ]
        assert isinstance(geodata["geom"], gpd.GeoSeries)
        assert geodata.geom_type.to_list() == [
            "Point",
            "MultiPolygon",
            None,
            None,
            None,
        ]
        assert str(geodata.crs) == epsg

    def test_geoseries_constructors_fail(self, data_wkb, data_wkt):
        """
        This issue can either be handled by using shapely.from_wkb/wkt
        or by converting pandas 'nan's to 'None'
        """
        with pytest.raises(TypeError):
            data_wkb["new_geometry_column"] = gpd.GeoSeries.from_wkb(data_wkb["geom"])

        with pytest.raises(TypeError):
            data_wkb["new_geometry_column"] = gpd.GeoSeries.from_wkt(data_wkt["geom"])

    def test_projected_crs(self, data_wkb):
        new_crs = "EPSG:2263"
        geom = FileGeometry(
            geom_column="geom",
            format=geometry.StandardGeometryFormat.wkb,
            crs="EPSG:4326",
        )
        gdf_source = transform.df_to_gdf(data_wkb, geom)
        gdf = gdf_source.to_crs(new_crs)
        assert gdf.crs == new_crs
        assert round(gdf.area.sum(), 2) == 126924.61  # sqft

    def test_reproject_gdf(self, gdf):
        assert gdf.crs.srs == "EPSG:4236"
        state_plane = transform.reproject_gdf(gdf, target_crs="EPSG:2263")
        assert state_plane.crs.srs == "EPSG:2263"


class TestParquet:
    def test_read_geoparquet_metadata(self, gdf):
        with TemporaryDirectory() as dir:
            filepath = f"{dir}/tmp.parquet"
            gdf.to_parquet(filepath)

            meta = parquet.read_metadata(filepath)

            assert meta.geo_parquet.primary_column.crs_string == "EPSG:4236"

    def test_read_parquet_metadata(self):
        with pytest.raises(TypeError, match="is not a geoparquet file."):
            _meta = parquet.read_metadata(RESOURCES_DIR / "simple.parquet")

    def test_read_parquet(self):
        df = parquet.read_df(RESOURCES_DIR / "simple.parquet")
        assert not isinstance(df, gpd.GeoDataFrame)

        gdf = parquet.read_df(RESOURCES_DIR / "geo.parquet")
        assert isinstance(gdf, gpd.GeoDataFrame)


@pytest.mark.parametrize(
    "input, expected",
    [
        (None, None),
        (Point(0, 1), MultiPoint([(0, 1)])),
        (MultiPoint([(2, 3), (4, 5)]), MultiPoint([(2, 3), (4, 5)])),
        (LineString([(0, 0), (1, 1)]), MultiLineString([[(0, 0), (1, 1)]])),
        (
            MultiLineString([[(0, 0), (-1, 1)], [(0, 0), (1, -1)]]),
            MultiLineString([[(0, 0), (-1, 1)], [(0, 0), (1, -1)]]),
        ),
        (
            Polygon([(0, 0), (0, 1), (1, 0), (0, 0)]),
            MultiPolygon([Polygon([(0, 0), (0, 1), (1, 0), (0, 0)])]),
        ),
        (
            MultiPolygon(
                [
                    Polygon([(0, 0), (0, 1), (1, 0), (0, 0)]),
                    Polygon([(0, 0), (0, -1), (-1, 0), (0, 0)]),
                ]
            ),
            MultiPolygon(
                [
                    Polygon([(0, 0), (0, 1), (1, 0), (0, 0)]),
                    Polygon([(0, 0), (0, -1), (-1, 0), (0, 0)]),
                ]
            ),
        ),
    ],
)
def test_multi(input, expected):
    assert transform.multi(input) == expected
