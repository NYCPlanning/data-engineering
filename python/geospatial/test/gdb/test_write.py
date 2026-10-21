"""Folding parquet layers into a zipped FGDB."""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
from shapely.geometry import MultiPolygon, Polygon

from dcpy.geospatial.gdb.write import (
    ParquetLayer,
    layer_geometry_type,
    write_gdb_from_parquet,
)


def test_write_gdb_from_parquet(tmp_path: Path):
    square = MultiPolygon([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])])
    gpd.GeoDataFrame({"code": ["a"]}, geometry=[square], crs="EPSG:2263").to_parquet(
        tmp_path / "districts.parquet"
    )
    # "blank" is all-null, i.e. Arrow's null type in parquet
    pd.DataFrame({"name": ["x", "y"], "blank": [None, None]}).to_parquet(
        tmp_path / "names.parquet"
    )

    out = write_gdb_from_parquet(
        [
            ParquetLayer("districts", tmp_path / "districts.parquet"),
            ParquetLayer("names", tmp_path / "names.parquet"),
        ],
        tmp_path / "out" / "things.gdb.zip",
    )

    assert out.exists()
    gdb = f"/vsizip/{out}/things.gdb"
    assert [name for name, _ in pyogrio.list_layers(gdb)] == ["districts", "names"]
    districts = pyogrio.read_dataframe(gdb, layer="districts")
    assert districts.crs.to_epsg() == 2263
    # GDAL's default FGDB xy grid snaps coordinates (pass the source grid as
    # layer_options to round-trip exactly, as below)
    assert districts.geometry.iloc[0].hausdorff_distance(square) < 1e-4
    assert districts["code"].tolist() == ["a"]
    assert layer_geometry_type(tmp_path / "districts.parquet") == "MultiPolygon"
    assert layer_geometry_type(tmp_path / "names.parquet") is None
    info = pyogrio.read_info(gdb, layer="names")
    assert info["geometry_type"] is None
    assert info["features"] == 2


def test_layer_options_set_the_xy_grid(tmp_path: Path):
    square = MultiPolygon([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])])
    gpd.GeoDataFrame(geometry=[square], crs="EPSG:2263").to_parquet(
        tmp_path / "d.parquet"
    )
    grid = {
        "XYSCALE": "10000",
        "XORIGIN": "-100",
        "YORIGIN": "-100",
        "XYTOLERANCE": "0.001",
    }
    out = write_gdb_from_parquet(
        [ParquetLayer("d", tmp_path / "d.parquet", layer_options=grid)],
        tmp_path / "d.gdb.zip",
    )
    geom = pyogrio.read_dataframe(f"/vsizip/{out}/d.gdb").geometry.iloc[0]
    assert geom.hausdorff_distance(square) == 0
