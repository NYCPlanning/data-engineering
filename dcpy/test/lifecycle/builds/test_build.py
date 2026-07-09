import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from shapely import LineString, MultiPolygon, Point, Polygon

from dcpy.lifecycle.builds.export import export, export_geodataset_from_postgres
from dcpy.lifecycle.builds.models import ExportFormat

_point_row = {"id": 1, "geometry": Point(0, 0)}
_polygon_row = {
    "id": 2,
    "geometry": MultiPolygon([Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])]),
}
_mixed_gdf = gpd.GeoDataFrame(
    [_point_row, _polygon_row], geometry="geometry", crs="EPSG:4326"
)


def _make_pg_client(gdf: gpd.GeoDataFrame) -> MagicMock:
    client = MagicMock()
    client.read_table_gdf.return_value = gdf
    return client


def _read_shapefile_from_zip(zip_path: Path) -> gpd.GeoDataFrame:
    """Extract zip to a temp dir and read the shapefile inside."""
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp)
        shp_files = list(Path(tmp).rglob("*.shp"))
        assert shp_files, f"No .shp found in {zip_path}"
        return gpd.read_file(shp_files[0])


def test_shapefile_export_no_filter(tmp_path):
    # Shapefiles support only one geometry type per layer; use a homogeneous GDF
    points_gdf = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
    out = tmp_path / "out.zip"
    export_geodataset_from_postgres(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.shapefile,
        pg_client=_make_pg_client(points_gdf),
    )
    result = _read_shapefile_from_zip(out)
    assert len(result) == 1
    assert result.geom_type.iloc[0] == "Point"


def test_shapefile_export_mixed_raises(tmp_path):
    """Shapefile export with point+polygon mix raises a clear error."""
    out = tmp_path / "out.zip"
    with pytest.raises(ValueError, match="different families"):
        export_geodataset_from_postgres(
            table_name="mytable",
            file_path=out,
            format=ExportFormat.shapefile,
            pg_client=_make_pg_client(_mixed_gdf.copy()),
        )


def test_shapefile_export_point_and_multipoint(tmp_path):
    """Point and MultiPoint in the same column are normalized to MultiPoint and exported."""
    from shapely import MultiPoint

    mixed_points_gdf = gpd.GeoDataFrame(
        [
            {"id": 1, "geometry": Point(0, 0)},
            {"id": 2, "geometry": MultiPoint([(1, 1), (2, 2)])},
        ],
        geometry="geometry",
        crs="EPSG:4326",
    )
    out = tmp_path / "out.zip"
    export_geodataset_from_postgres(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.shapefile,
        pg_client=_make_pg_client(mixed_points_gdf),
    )
    result = _read_shapefile_from_zip(out)
    assert len(result) == 2
    assert set(result.geom_type) == {"MultiPoint"}


def test_shapefile_export_points(tmp_path):
    out = tmp_path / "points.zip"
    export_geodataset_from_postgres(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.shapefile,
        pg_client=_make_pg_client(_mixed_gdf.copy()),
        geometry_type="points",
    )
    result = _read_shapefile_from_zip(out)
    assert len(result) == 1
    assert result.geom_type.iloc[0] == "Point"


def test_shapefile_export_polygons(tmp_path):
    out = tmp_path / "polygons.zip"
    export_geodataset_from_postgres(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.shapefile,
        pg_client=_make_pg_client(_mixed_gdf.copy()),
        geometry_type="polygons",
    )
    result = _read_shapefile_from_zip(out)
    assert len(result) == 1
    assert result.geom_type.iloc[0] in ("Polygon", "MultiPolygon")


def test_shapefile_export_filter_returns_empty_raises(tmp_path):
    """Shapefile export raises when geometry_type filter matches no rows."""
    points_only_gdf = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
    out = tmp_path / "out.zip"
    with pytest.raises(ValueError, match="No features to export"):
        export_geodataset_from_postgres(
            table_name="mytable",
            file_path=out,
            format=ExportFormat.shapefile,
            pg_client=_make_pg_client(points_only_gdf),
            geometry_type="polygons",  # filter matches zero rows
        )


def test_gdb_export(tmp_path):
    points_gdf = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
    out = tmp_path / "out.zip"
    export_geodataset_from_postgres(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.gdb,
        pg_client=_make_pg_client(points_gdf),
        geometry_type="points",
        layer="mytable_points",
    )
    assert out.exists()
    # verify the named layer is readable from the zip
    result = gpd.read_file(f"zip://{out}!out.gdb", layer="mytable_points")
    assert len(result) == 1


def test_gdb_export_multi_table(tmp_path):
    """Multiple layers sharing a filename go into one GDB with separate named layers."""
    from dcpy.lifecycle.builds.export import _write_gdb_zip

    points_gdf = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
    polygons_gdf = _mixed_gdf[_mixed_gdf.geom_type != "Point"].copy()

    out = tmp_path / "combined.zip"
    with tempfile.TemporaryDirectory() as tmp_str:
        _write_gdb_zip(
            [("places", points_gdf), ("boundaries", polygons_gdf)],
            out,
            Path(tmp_str),
        )

    assert out.exists()
    places = gpd.read_file(f"zip://{out}!combined.gdb", layer="places")
    boundaries = gpd.read_file(f"zip://{out}!combined.gdb", layer="boundaries")
    assert len(places) == 1
    assert len(boundaries) == 1


def test_gdb_export_non_spatial_table(tmp_path):
    """A plain (non-spatial) DataFrame is written as a geometry-less GDB table,
    alongside spatial layers in the same file."""
    import pandas as pd

    from dcpy.lifecycle.builds.export import _write_gdb_zip

    points_gdf = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
    table_df = pd.DataFrame({"nodeid": [1, 2, 3], "stname": ["A ST", "B AVE", "C PL"]})

    out = tmp_path / "combined.zip"
    with tempfile.TemporaryDirectory() as tmp_str:
        _write_gdb_zip(
            [("node", points_gdf), ("node_stname", table_df)],
            out,
            Path(tmp_str),
        )

    assert out.exists()
    node = gpd.read_file(f"zip://{out}!combined.gdb", layer="node")
    node_stname = gpd.read_file(f"zip://{out}!combined.gdb", layer="node_stname")
    assert len(node) == 1
    assert len(node_stname) == 3
    # non-spatial layer round-trips its columns with no geometry
    assert list(node_stname.columns) == ["nodeid", "stname"]


def test_gdb_export_line(tmp_path):
    lines_gdf = gpd.GeoDataFrame(
        [{"id": 1, "geometry": LineString([(0, 0), (1, 1)])}],
        geometry="geometry",
        crs="EPSG:2263",
    )
    out = tmp_path / "out.zip"
    export_geodataset_from_postgres(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.gdb,
        pg_client=_make_pg_client(lines_gdf),
        geometry_type="lines",
        layer="mytable_lines",
    )
    assert out.exists()
    result = gpd.read_file(f"zip://{out}!out.gdb", layer="mytable_lines")
    assert len(result) == 1
    assert result.geom_type.iloc[0] in ("LineString", "MultiLineString")


def test_gdb_export_mixed_geometry_raises(tmp_path):
    """GDB layer with mixed geometry types raises a clear error."""
    out = tmp_path / "out.zip"
    with pytest.raises(ValueError, match="different families"):
        export_geodataset_from_postgres(
            table_name="mytable",
            file_path=out,
            format=ExportFormat.gdb,
            pg_client=_make_pg_client(_mixed_gdf.copy()),
            layer="mixed",
        )


def test_default_filename_shapefile(tmp_path):
    """export() should generate name.zip (not name.shp) for shapefile format."""
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets: []
exports:
  output_folder: {output_folder}
  datasets:
  - name: mytable
    format: shp
""".format(output_folder=str(tmp_path / "output"))
    )

    with patch(
        "dcpy.lifecycle.builds.export.export_dataset_from_postgres"
    ) as mock_export:
        mock_export.return_value = None
        export(recipe_path, pg_client=MagicMock())

    assert mock_export.called
    call_kwargs = mock_export.call_args
    file_path: Path = (
        call_kwargs[1]["file_path"] if call_kwargs[1] else call_kwargs[0][1]
    )
    assert file_path.suffix == ".zip", f"Expected .zip suffix, got {file_path.suffix}"
    assert "mytable" in file_path.name


def test_default_filename_gdb(tmp_path):
    """export() should generate name.zip (not name.gdb) for gdb format."""
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets: []
exports:
  output_folder: {output_folder}
  datasets:
  - name: mytable
    format: gdb
    custom:
      layer: mylayer
""".format(output_folder=str(tmp_path / "output"))
    )

    # GDB entries bypass export_dataset_from_postgres; patch _write_gdb_zip directly
    with patch("dcpy.lifecycle.builds.export._write_gdb_zip") as mock_write:
        with patch("dcpy.lifecycle.builds.export._read_filtered_gdf") as mock_read:
            mock_read.return_value = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
            export(recipe_path, pg_client=MagicMock())

    assert mock_write.called
    file_path: Path = mock_write.call_args[0][1]
    assert file_path.suffix == ".zip", f"Expected .zip suffix, got {file_path.suffix}"
    assert "mytable" in file_path.name


_MINIMAL_RECIPE = """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets: []
exports:
  output_folder: {output_folder}
  datasets: []
"""


def test_export_copies_target_dir_when_present(tmp_path):
    """export() zips the dbt target/ sibling directory into diagnostics/dbt.zip."""
    output_folder = tmp_path / "output"
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(_MINIMAL_RECIPE.format(output_folder=str(output_folder)))

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text('{"results": []}')
    compiled_dir = target_dir / "compiled"
    compiled_dir.mkdir()
    (compiled_dir / "x.sql").write_text("select 1")

    export(recipe_path, pg_client=MagicMock())

    # target/ now goes to diagnostics/dbt.zip
    zip_path = output_folder / "diagnostics" / "dbt.zip"
    assert zip_path.exists()
    assert zip_path.is_file()
    # Verify contents are in the zip
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        assert "run_results.json" in names
        assert "compiled/x.sql" in names


def test_export_warns_when_target_dir_missing(tmp_path):
    """export() logs a debug message and continues when target/ does not exist."""
    output_folder = tmp_path / "output"
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(_MINIMAL_RECIPE.format(output_folder=str(output_folder)))

    with patch("dcpy.lifecycle.builds.export.logger") as mock_logger:
        export(recipe_path, pg_client=MagicMock())

    # dbt.zip should not exist in diagnostics/ since target/ wasn't created
    assert not (output_folder / "diagnostics" / "dbt.zip").exists()
    # Check debug logs for missing artifact directory message
    debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
    assert any("target" in msg for msg in debug_calls)
