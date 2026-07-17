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

from dcpy.utils.formats import Geometry as FileGeometry
from dcpy.utils.geospatial import geometry, parquet, transform

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
        geom_types = gdf.geom_type.to_list()
        assert geom_types[:2] == ["Point", "MultiPolygon"]
        # geopandas returns NaN (not None) for missing geometries
        assert all(pd.isna(g) for g in geom_types[2:])
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
        geom_types = geodata.geom_type.to_list()
        assert geom_types[:2] == ["Point", "MultiPolygon"]
        # geopandas returns NaN (not None) for missing geometries
        assert all(pd.isna(g) for g in geom_types[2:])
        assert str(geodata.crs) == epsg

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

    def test_is_geoparquet(self):
        assert parquet.is_geoparquet(RESOURCES_DIR / "geo.parquet")
        assert not parquet.is_geoparquet(RESOURCES_DIR / "simple.parquet")

    def test_iter_batches_df(self):
        # simple.parquet has 5 rows; a batch_size of 2 must split it without loss
        batches = list(parquet.iter_batches_df(RESOURCES_DIR / "simple.parquet", 2))
        assert len(batches) > 1
        assert all(len(b) <= 2 for b in batches)
        combined = pd.concat(batches, ignore_index=True)
        assert combined.equals(parquet.read_df(RESOURCES_DIR / "simple.parquet"))

    def test_iter_batches_df_empty(self):
        with TemporaryDirectory() as dir:
            filepath = f"{dir}/empty.parquet"
            pd.DataFrame({"a": [], "b": []}).to_parquet(filepath)

            batches = list(parquet.iter_batches_df(Path(filepath), 100))

        # an empty file still yields one empty frame carrying the columns
        assert len(batches) == 1
        assert len(batches[0]) == 0
        assert list(batches[0].columns) == ["a", "b"]


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
