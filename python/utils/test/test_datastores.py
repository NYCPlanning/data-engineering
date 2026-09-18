import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import MultiPoint, MultiPolygon, Point, Polygon

from dcpy.utils import datastores


def _point_gdf(points: list[Point]) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"id": range(len(points))}, geometry=points, crs="EPSG:4326"
    )


def test_normalize_to_single_geom_type_promotes_mixed_point_family():
    gdf = gpd.GeoDataFrame(
        {"id": [1, 2]},
        geometry=[Point(0, 0), MultiPoint([(1, 1), (2, 2)])],
        crs="EPSG:4326",
    )
    result = datastores._normalize_to_single_geom_type(gdf, "layer")
    assert set(result.geom_type) == {"MultiPoint"}


def test_normalize_to_single_geom_type_promotes_mixed_polygon_family():
    square = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    multi_square = MultiPolygon([square])
    gdf = gpd.GeoDataFrame(
        {"id": [1, 2]}, geometry=[square, multi_square], crs="EPSG:4326"
    )
    result = datastores._normalize_to_single_geom_type(gdf, "layer")
    assert set(result.geom_type) == {"MultiPolygon"}


def test_normalize_to_single_geom_type_leaves_already_uniform_data_alone():
    gdf = _point_gdf([Point(0, 0), Point(1, 1)])
    result = datastores._normalize_to_single_geom_type(gdf, "layer")
    assert set(result.geom_type) == {"Point"}


def test_normalize_to_single_geom_type_raises_on_mixed_families():
    square = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    gdf = gpd.GeoDataFrame(
        {"id": [1, 2]}, geometry=[Point(0, 0), square], crs="EPSG:4326"
    )
    with pytest.raises(ValueError, match="different families"):
        datastores._normalize_to_single_geom_type(gdf, "mixed_layer")


def test_write_shapefile_zip_raises_on_empty_frame(tmp_path):
    empty = _point_gdf([])
    with pytest.raises(ValueError, match="No features"):
        datastores.write_shapefile_zip(empty, "layer", tmp_path / "out.zip", tmp_path)


def test_write_shapefile_zip_produces_a_readable_zip(tmp_path):
    gdf = _point_gdf([Point(0, 0), Point(1, 1)])
    out_path = tmp_path / "out.shp.zip"
    datastores.write_shapefile_zip(gdf, "points", out_path, tmp_path)

    assert out_path.exists()
    read_back = gpd.read_file(out_path)
    assert len(read_back) == 2


def test_write_gdb_zip_writes_multiple_layers(tmp_path):
    points = _point_gdf([Point(0, 0), Point(1, 1)])
    table = pd.DataFrame({"id": [1, 2], "note": ["a", "b"]})
    out_path = tmp_path / "out.gdb.zip"

    datastores.write_gdb_zip(
        [("points_layer", points), ("plain_table", table)], out_path, tmp_path
    )

    assert out_path.exists()
    read_points = gpd.read_file(out_path, layer="points_layer")
    assert len(read_points) == 2
    read_table = gpd.read_file(out_path, layer="plain_table")
    assert len(read_table) == 2
    assert list(read_table["note"]) == ["a", "b"]


def test_write_gdb_zip_raises_on_empty_layer_by_default(tmp_path):
    empty = _point_gdf([])
    with pytest.raises(ValueError, match="No rows to export"):
        datastores.write_gdb_zip(
            [("empty_layer", empty)], tmp_path / "out.gdb.zip", tmp_path
        )


def test_write_gdb_zip_writes_schema_only_for_allowed_empty_layer(tmp_path):
    empty = _point_gdf([])
    out_path = tmp_path / "out.gdb.zip"

    datastores.write_gdb_zip(
        [("empty_layer", empty)],
        out_path,
        tmp_path,
        allow_empty={"empty_layer"},
    )

    read_back = gpd.read_file(out_path, layer="empty_layer")
    assert len(read_back) == 0
