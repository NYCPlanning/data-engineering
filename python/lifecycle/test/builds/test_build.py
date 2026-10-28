import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from shapely import LineString, MultiPolygon, Point, Polygon

from dcpy.lifecycle.builds.export import (
    export,
    export_dataset_from_duckdb,
    export_geodataset_from_postgres,
    zip_groups,
)
from dcpy.lifecycle.builds.models import ExportFormat
from dcpy.utils import duckdb as duckdb_utils

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
    from dcpy.utils.datastores import write_gdb_zip

    points_gdf = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
    polygons_gdf = _mixed_gdf[_mixed_gdf.geom_type != "Point"].copy()

    out = tmp_path / "combined.zip"
    with tempfile.TemporaryDirectory() as tmp_str:
        write_gdb_zip(
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

    from dcpy.utils.datastores import write_gdb_zip

    points_gdf = _mixed_gdf[_mixed_gdf.geom_type == "Point"].copy()
    table_df = pd.DataFrame({"nodeid": [1, 2, 3], "stname": ["A ST", "B AVE", "C PL"]})

    out = tmp_path / "combined.zip"
    with tempfile.TemporaryDirectory() as tmp_str:
        write_gdb_zip(
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

    # GDB entries bypass export_dataset_from_postgres; patch write_gdb_zip directly
    with patch("dcpy.utils.datastores.write_gdb_zip") as mock_write:
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


def test_csv_export_from_duckdb(tmp_path):
    client = duckdb_utils.DuckDBClient(
        db_path=tmp_path / "test.duckdb", schema="test_schema"
    )
    client.conn.execute("CREATE TABLE test_schema.mytable AS SELECT 1 AS a, 'x' AS b")

    out = tmp_path / "mytable.csv"
    export_dataset_from_duckdb(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.csv,
        duckdb_client=client,
    )

    assert out.read_text() == "a,b\n1,x\n"
    client.close()


def test_parquet_export_from_duckdb(tmp_path):
    import pandas as pd

    client = duckdb_utils.DuckDBClient(
        db_path=tmp_path / "test.duckdb", schema="test_schema"
    )
    client.conn.execute("CREATE TABLE test_schema.mytable AS SELECT 1 AS a, 'x' AS b")

    out = tmp_path / "mytable.parquet"
    export_dataset_from_duckdb(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.parquet,
        duckdb_client=client,
    )

    result = pd.read_parquet(out)
    assert result.to_dict("records") == [{"a": 1, "b": "x"}]
    client.close()


def test_dat_export_from_duckdb_uses_crlf_no_header(tmp_path):
    client = duckdb_utils.DuckDBClient(
        db_path=tmp_path / "test.duckdb", schema="test_schema"
    )
    client.conn.execute("CREATE TABLE test_schema.mytable AS SELECT 1 AS a, 'x' AS b")

    out = tmp_path / "mytable.dat"
    export_dataset_from_duckdb(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.dat,
        duckdb_client=client,
    )

    assert out.read_bytes() == b"1,x\r\n"
    client.close()


@pytest.mark.parametrize(
    "format, expected",
    [
        (ExportFormat.csv, b"a,b\n1,z\n2,y\n3,x\n"),
        (ExportFormat.dat, b"1,z\r\n2,y\r\n3,x\r\n"),
    ],
)
def test_no_layout_export_from_duckdb_honors_order_by(tmp_path, format, expected):
    client = duckdb_utils.DuckDBClient(
        db_path=tmp_path / "test.duckdb", schema="test_schema"
    )
    client.conn.execute(
        "CREATE TABLE test_schema.mytable AS "
        "SELECT * FROM (VALUES (3, 'x'), (1, 'z'), (2, 'y')) t(a, b)"
    )

    out = tmp_path / "mytable.out"
    export_dataset_from_duckdb(
        table_name="mytable",
        file_path=out,
        format=format,
        duckdb_client=client,
        order_by="b DESC",
    )

    assert out.read_bytes() == expected
    client.close()


def test_parquet_export_from_duckdb_honors_order_by(tmp_path):
    import pandas as pd

    client = duckdb_utils.DuckDBClient(
        db_path=tmp_path / "test.duckdb", schema="test_schema"
    )
    client.conn.execute(
        "CREATE TABLE test_schema.mytable AS SELECT * FROM (VALUES (2), (3), (1)) t(a)"
    )

    out = tmp_path / "mytable.parquet"
    export_dataset_from_duckdb(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.parquet,
        duckdb_client=client,
        order_by="a",
    )

    assert pd.read_parquet(out).a.tolist() == [1, 2, 3]
    client.close()


def test_geo_export_from_duckdb_rejects_order_by(tmp_path):
    client = duckdb_utils.DuckDBClient(
        db_path=tmp_path / "test.duckdb", schema="test_schema"
    )
    client.conn.execute("CREATE TABLE test_schema.mytable AS SELECT 1 AS a")

    with pytest.raises(ValueError, match="order_by"):
        export_dataset_from_duckdb(
            table_name="mytable",
            file_path=tmp_path / "out.gpkg",
            format=ExportFormat.geopackage,
            duckdb_client=client,
            order_by="a",
        )
    client.close()


def test_unsupported_format_from_duckdb_raises(tmp_path):
    client = duckdb_utils.DuckDBClient(
        db_path=tmp_path / "test.duckdb", schema="test_schema"
    )
    client.conn.execute("CREATE TABLE test_schema.mytable AS SELECT 1 AS a")

    with pytest.raises(NotImplementedError, match="unsupported_format"):
        export_dataset_from_duckdb(
            table_name="mytable",
            file_path=tmp_path / "out.zip",
            format="unsupported_format",
            duckdb_client=client,
        )
    client.close()


def test_export_routes_to_duckdb_when_recipe_uses_duckdb(tmp_path):
    """export() reads from DuckDB, not postgres, when the recipe's inputs all target DuckDB."""
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets:
  - name: mytable
    destination: duckdb
exports:
  output_folder: {output_folder}
  datasets:
  - name: mytable
    format: csv
""".format(output_folder=str(tmp_path / "output"))
    )

    with patch(
        "dcpy.lifecycle.builds.export.export_dataset_from_duckdb"
    ) as mock_export:
        mock_export.return_value = None
        export(recipe_path, duckdb_client=MagicMock())

    assert mock_export.called


def test_postgres_export_defaults_to_standard_build_dir_not_recipe_lock_dir(
    tmp_path, monkeypatch
):
    """A postgres-backed build with no explicit output_folder and no BUILD_ENV_OUTPUT_DIR
    must default to the standard {product}/{version} build dir - not recipe_lock_path's own
    directory. Products (e.g. template) write build-stage artifacts like data dictionaries
    to that standard path via dcpy.lifecycle.config.get_build_dir() directly, independent of
    wherever recipe.lock.yml itself happens to live (e.g. inside the product's source dir in
    CI); export() has to agree with that same location or it silently won't find them."""
    monkeypatch.delenv("BUILD_ENV_OUTPUT_DIR", raising=False)
    lifecycle_dir = tmp_path / "lifecycle_data"
    monkeypatch.setenv("DCPY_LIFECYCLE_DATA_DIR", str(lifecycle_dir))

    # recipe.lock.yml lives somewhere other than the standard build dir, mirroring a real
    # product's checkout directory (e.g. products/template/recipe.lock.yml in CI).
    product_dir = tmp_path / "some_other_dir"
    product_dir.mkdir()
    recipe_path = product_dir / "recipe.lock.yml"
    recipe_path.write_text(
        """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets: []
exports:
  datasets: []
"""
    )

    export(recipe_path, pg_client=MagicMock())

    from dcpy.lifecycle.config import get_build_dir

    expected_output_folder = get_build_dir("test", "24Q1")
    assert expected_output_folder.exists()
    assert not (product_dir / "dataset_files").exists()


def test_duckdb_build_commands_and_export_agree_on_duckdb_path(tmp_path, monkeypatch):
    """Build commands and export run as separate processes from load. Without
    BUILD_ENV_OUTPUT_DIR, both must still resolve the duckdb file to the standard build dir,
    not recipe_lock_path's own directory."""
    import os

    from dcpy.lifecycle.builds import build, plan
    from dcpy.lifecycle.config import get_build_dir

    monkeypatch.delenv("BUILD_ENV_OUTPUT_DIR", raising=False)
    monkeypatch.setenv("DUCKDB_PATH", "placeholder")  # restored after _set_duckdb_path
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets:
  - name: mytable
    destination: duckdb
exports:
  datasets:
  - name: mytable
    format: csv
"""
    )
    # no stage-dirs features: the duckdb file sits in the build root, as before
    expected = get_build_dir("test", "24Q1") / "test_24Q1.duckdb"

    build._set_duckdb_path(plan.recipe_from_yaml(recipe_path))
    assert os.environ["DUCKDB_PATH"] == str(expected)

    with (
        patch("dcpy.lifecycle.builds.export.duckdb_utils.DuckDBClient") as mock_client,
        patch("dcpy.lifecycle.builds.export.export_dataset_from_duckdb"),
    ):
        export(recipe_path)
    assert mock_client.call_args.kwargs["db_path"] == expected


def test_export_mixed_destinations_falls_back_to_postgres(tmp_path):
    """A recipe with both postgres- and duckdb-destined inputs falls back to the postgres
    export path (gdb/shapefile export and mixed-backend export aren't supported for DuckDB)."""
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets:
  - name: pgtable
    destination: postgres
  - name: ducktable
    destination: duckdb
exports:
  output_folder: {output_folder}
  datasets:
  - name: pgtable
    format: csv
""".format(output_folder=str(tmp_path / "output"))
    )

    with patch(
        "dcpy.lifecycle.builds.export.export_dataset_from_postgres"
    ) as mock_export:
        mock_export.return_value = None
        export(recipe_path, pg_client=MagicMock())

    assert mock_export.called


def test_geoparquet_export(tmp_path):
    out = tmp_path / "out.parquet"
    export_geodataset_from_postgres(
        table_name="mytable",
        file_path=out,
        format=ExportFormat.geoparquet,
        pg_client=_make_pg_client(_mixed_gdf.copy()),
        geometry_type="polygons",
    )
    result = gpd.read_parquet(out)
    assert len(result) == 1
    assert result.geom_type.iloc[0] == "MultiPolygon"


def test_gpkg_entries_sharing_a_filename_become_layers_of_one_file(tmp_path):
    """export() groups gpkg entries by filename, like gdb, and writes an unzipped .gpkg."""
    import pyogrio

    output_folder = tmp_path / "output"
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
  - name: places
    filename: combined.gpkg
    format: gpkg
    custom: {{ geometry_type: points }}
  - name: boundaries
    filename: combined.gpkg
    format: gpkg
    custom: {{ geometry_type: polygons }}
""".format(output_folder=str(output_folder))
    )

    pg_client = MagicMock()
    pg_client.read_table_gdf.return_value = _mixed_gdf.copy()
    export(recipe_path, pg_client=pg_client)

    out = output_folder / "dataset_files" / "combined.gpkg"
    assert [name for name, _ in pyogrio.list_layers(out)] == ["places", "boundaries"]


@pytest.mark.parametrize(
    "format, expected_name",
    [("gpkg", "mytable.gpkg"), ("geoparquet", "mytable.parquet")],
)
def test_default_filename_geo_formats(tmp_path, format, expected_name):
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
    format: {format}
""".format(output_folder=str(tmp_path / "output"), format=format)
    )

    pg_client = MagicMock()
    pg_client.read_table_gdf.return_value = _mixed_gdf[
        _mixed_gdf.geom_type == "Point"
    ].copy()
    export(recipe_path, pg_client=pg_client)

    assert (tmp_path / "output" / "dataset_files" / expected_name).exists()


_LAYERED_RECIPE = """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets: []
exports:
  output_folder: {output_folder}
  datasets:
  - name: mytable
    filename: out.gpkg
    format: gpkg
    custom: {custom}
"""


def test_layered_export_keeps_geometry_by_default(tmp_path):
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        _LAYERED_RECIPE.format(output_folder=tmp_path / "output", custom="{}")
    )
    pg_client = MagicMock()
    pg_client.read_table_gdf.return_value = _mixed_gdf[
        _mixed_gdf.geom_type == "Point"
    ].copy()

    export(recipe_path, pg_client=pg_client)

    result = gpd.read_file(tmp_path / "output" / "dataset_files" / "out.gpkg")
    assert result.geom_type.iloc[0] == "Point"
    pg_client.read_table_df.assert_not_called()


def test_layered_export_spatial_false_writes_plain_table(tmp_path):
    import pandas as pd

    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        _LAYERED_RECIPE.format(
            output_folder=tmp_path / "output", custom="{ spatial: false }"
        )
    )
    pg_client = MagicMock()
    pg_client.read_table_df.return_value = pd.DataFrame({"id": [1, 2]})

    export(recipe_path, pg_client=pg_client)

    result = gpd.read_file(tmp_path / "output" / "dataset_files" / "out.gpkg")
    assert list(result.columns) == ["id"]
    pg_client.read_table_gdf.assert_not_called()


def test_layered_export_spatial_false_with_geometry_type_raises(tmp_path):
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        _LAYERED_RECIPE.format(
            output_folder=tmp_path / "output",
            custom="{ spatial: false, geometry_type: points }",
        )
    )
    with pytest.raises(ValueError, match="spatial: false and geometry_type"):
        export(recipe_path, pg_client=MagicMock())


def test_parquet_first_export_then_package(tmp_path, monkeypatch):
    """package_from_parquet: gdb entries export as parquet layers + a manifest, and
    `package` folds them into the FGDB the entries name."""
    from dcpy.lifecycle.builds.export import package

    client = duckdb_utils.DuckDBClient(tmp_path / "t.duckdb", "s")
    client.conn.execute(
        "CREATE TABLE s.boundaries AS SELECT 1 AS id, "
        "ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))') AS geom"
    )
    client.conn.execute("CREATE TABLE s.names AS SELECT 'x' AS name")
    lock = tmp_path / "recipe.lock.yml"
    lock.write_text(
        f"""
name: t
product: t
version: v1
inputs:
  datasets:
  - name: src
    version: v1
    file_type: csv
    destination: duckdb
    source: edm.recipes.datasets
exports:
  output_folder: {tmp_path / "output"}
  package_from_parquet: true
  crs: EPSG:2263
  datasets:
  - name: boundaries
    filename: things.gdb.zip
    format: gdb
  - name: names
    filename: things.gdb.zip
    format: gdb
    custom: {{ spatial: false }}
"""
    )
    monkeypatch.setenv("BUILD_ENV_ROOT_DIR", str(tmp_path))
    monkeypatch.delenv(
        "BUILD_ENV_OUTPUT_DIR", raising=False
    )  # set by other tests' loads
    out = export(lock, duckdb_client=client)
    assert out is not None
    assert (out / "raw_exports" / "manifest.json").exists()
    assert (out / "raw_exports" / "things" / "boundaries.parquet").exists()
    assert not (out / "dataset_files").exists()  # deliverables are package's job

    packaged = package(lock)
    assert packaged == tmp_path / "package"  # <root>/package
    gdb = f"zip://{packaged / 'dataset_files' / 'things.gdb.zip'}!things.gdb"
    boundaries = gpd.read_file(gdb, layer="boundaries")
    assert len(boundaries) == 1
    assert boundaries.crs.to_epsg() == 2263
    assert len(gpd.read_file(gdb, layer="names")) == 1
    client.close()


def test_package_stamps_layer_metadata(tmp_path, monkeypatch):
    """custom.product_metadata: `package` writes the layer with Esri metadata built from
    its product-metadata dataset (OpenFileGDB DOCUMENTATION); other layers get none."""
    import shutil
    from unittest.mock import MagicMock

    import pyogrio

    from dcpy.geospatial.shapefile_metadata import Metadata
    from dcpy.lifecycle.builds import export as export_module
    from dcpy.lifecycle.builds.export import package
    from dcpy.product_metadata.models.metadata.product import (
        Dataset,
        DatasetAttributes,
        DatasetColumn,
    )

    client = duckdb_utils.DuckDBClient(tmp_path / "t.duckdb", "s")
    client.conn.execute(
        "CREATE TABLE s.boundaries AS SELECT 1 AS id, "
        "ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))') AS geom"
    )
    client.conn.execute("CREATE TABLE s.names AS SELECT 'x' AS name")
    lock = tmp_path / "recipe.lock.yml"
    lock.write_text(
        f"""
name: t
product: t
version: v1
inputs:
  datasets:
  - name: src
    version: v1
    file_type: csv
    destination: duckdb
    source: edm.recipes.datasets
exports:
  output_folder: {tmp_path / "output"}
  package_from_parquet: true
  crs: EPSG:2263
  datasets:
  - name: boundaries
    filename: things.gdb.zip
    format: gdb
    custom:
      product_metadata: {{ product: p, dataset: d, file_id: things_gdb }}
  - name: names
    filename: things.gdb.zip
    format: gdb
    custom: {{ spatial: false }}
"""
    )
    monkeypatch.setenv("BUILD_ENV_ROOT_DIR", str(tmp_path))
    monkeypatch.delenv("BUILD_ENV_OUTPUT_DIR", raising=False)
    export(lock, duckdb_client=client)
    client.close()

    org_md = MagicMock()
    dataset_md = org_md.product.return_value.dataset.return_value
    dataset_md.calculate_layer_dataset_metadata.return_value = Dataset(
        attributes=DatasetAttributes(
            display_name="Boundaries", description="d", each_row_is_a="Boundary"
        ),
        columns=[
            DatasetColumn(
                id="id",
                name="id",
                data_type="integer",
                description="Id.",
                custom={"fgdb_data_type": "Integer"},
            )
        ],
    )
    monkeypatch.setattr(export_module.product_metadata, "load", lambda: org_md)

    packaged = package(lock)

    org_md.product.assert_called_once_with("p")
    org_md.product.return_value.dataset.assert_called_once_with("d")
    dataset_md.calculate_layer_dataset_metadata.assert_called_once_with(
        file_id="things_gdb", layer="boundaries"
    )
    shutil.unpack_archive(packaged / "dataset_files" / "things.gdb.zip", tmp_path)

    def layer_xml(layer: str) -> str:
        # via pyogrio, not dcpy.geospatial.gdb.fgdb: importing osgeo here breaks later
        # tests' parquet reads (the gdal/pyarrow clash)
        frame = pyogrio.read_dataframe(
            tmp_path / "things.gdb", sql=f"GetLayerMetadata {layer}"
        )
        return frame.iloc[0, 0] or ""

    md = Metadata.from_xml(layer_xml("boundaries"))
    assert md.data_id_info.id_citation.res_title == "Boundaries"
    assert [a.attrlabl.value for a in md.eainfo.detailed.attr] == ["id"]
    assert layer_xml("names") == ""


def test_package_writes_layer_shapefiles(tmp_path, monkeypatch):
    """custom.product_metadata.shapefile: `package` also writes the layer as that
    shapefile file: its filename, the layer's field names cut as ArcGIS cuts them
    (shape fields last), omitted columns dropped, and its metadata as .shp.xml."""
    import zipfile
    from unittest.mock import MagicMock

    import pyogrio

    from dcpy.geospatial.shapefile_metadata import Metadata
    from dcpy.lifecycle.builds import export as export_module
    from dcpy.lifecycle.builds.export import package
    from dcpy.product_metadata.models.metadata.product import (
        Dataset,
        DatasetAttributes,
        DatasetColumn,
    )

    client = duckdb_utils.DuckDBClient(tmp_path / "t.duckdb", "s")
    client.conn.execute(
        "CREATE TABLE s.boundaries AS SELECT 4.0 AS SHAPE_Length, "
        "2::SMALLINT AS SchoolDist, 'x' AS Internal, 'Bronx' AS BOROUGHNAME_LONG, "
        "ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))') AS geom"
    )
    lock = tmp_path / "recipe.lock.yml"
    lock.write_text(
        f"""
name: t
product: t
version: 26C
inputs:
  datasets:
  - name: src
    version: v1
    file_type: csv
    destination: duckdb
    source: edm.recipes.datasets
exports:
  output_folder: {tmp_path / "output"}
  package_from_parquet: true
  crs: EPSG:2263
  datasets:
  - name: boundaries
    filename: things.gdb.zip
    format: gdb
    custom:
      layer: nysd
      product_metadata: {{ product: p, dataset: d, file_id: things_gdb, shapefile: shp }}
"""
    )
    monkeypatch.setenv("BUILD_ENV_ROOT_DIR", str(tmp_path))
    monkeypatch.delenv("BUILD_ENV_OUTPUT_DIR", raising=False)
    export(lock, duckdb_client=client)
    client.close()

    attributes = DatasetAttributes(
        display_name="School Districts", description="d", each_row_is_a="District"
    )
    gdb_columns = [
        DatasetColumn(id="shape_length", name="SHAPE_Length"),
        DatasetColumn(id="schooldist", name="SchoolDist"),
        DatasetColumn(id="internal", name="Internal"),
        DatasetColumn(id="boro", name="BOROUGHNAME_LONG"),
    ]
    shp_columns = [
        # names as Socrata shows them, not the field names
        DatasetColumn(id="schooldist", name="schooldist", description="District."),
        DatasetColumn(id="boro", name="boroname"),
        DatasetColumn(id="shape_length", name="Shape_Length"),
    ]
    org_md = MagicMock()
    dataset_md = org_md.product.return_value.dataset.return_value
    dataset_md.calculate_layer_dataset_metadata.return_value = Dataset(
        attributes=attributes, columns=gdb_columns
    )
    dataset_md.calculate_file_dataset_metadata.return_value = Dataset(
        attributes=attributes, columns=shp_columns
    )
    dataset_md.get_file_and_overrides.return_value.file.filename = "nysd.zip"
    monkeypatch.setattr(export_module.product_metadata, "load", lambda: org_md)

    packaged = package(lock)

    dataset_md.calculate_file_dataset_metadata.assert_called_with(file_id="shp")
    shp_zip = packaged / "dataset_files" / "shapefiles" / "nysd.zip"
    shp = f"/vsizip/{shp_zip}/nysd_26c/nysd.shp"
    info = pyogrio.read_info(shp)
    assert list(info["fields"]) == ["SchoolDist", "BOROUGHNAM", "Shape_Leng"]
    assert info["features"] == 1
    assert pyogrio.read_dataframe(shp).crs.to_epsg() == 2263
    with zipfile.ZipFile(shp_zip) as zf:
        md = Metadata.from_xml(zf.read("nysd_26c/nysd.shp.xml").decode())
    assert md.data_id_info.id_citation.res_title == "School Districts"
    assert [a.attrlabl.value for a in md.eainfo.detailed.attr] == [
        "SchoolDist",
        "BOROUGHNAM",
        "Shape_Leng",
    ]


def test_qa_gdb_metadata_drift(tmp_path, monkeypatch):
    """The QA check compares each stamped layer's metadata attrs with its fields."""
    from unittest.mock import MagicMock

    from dcpy.lifecycle.builds import export as export_module
    from dcpy.lifecycle.builds import plan
    from dcpy.lifecycle.builds.export import package
    from dcpy.lifecycle.builds.qa import check_gdb_metadata, gdb_metadata_drift
    from dcpy.product_metadata.models.metadata.product import (
        Dataset,
        DatasetAttributes,
        DatasetColumn,
    )

    client = duckdb_utils.DuckDBClient(tmp_path / "t.duckdb", "s")
    client.conn.execute(
        "CREATE TABLE s.boundaries AS SELECT 1 AS id, 'a' AS code, "
        "ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))') AS geom"
    )
    lock = tmp_path / "recipe.lock.yml"
    lock.write_text(
        f"""
name: t
product: t
version: v1
inputs:
  datasets:
  - name: src
    version: v1
    file_type: csv
    destination: duckdb
    source: edm.recipes.datasets
exports:
  output_folder: {tmp_path / "output"}
  package_from_parquet: true
  crs: EPSG:2263
  datasets:
  - name: boundaries
    filename: things.zip
    format: gdb
    custom:
      product_metadata: {{ product: p, dataset: d, file_id: things_gdb }}
"""
    )
    monkeypatch.setenv("BUILD_ENV_ROOT_DIR", str(tmp_path))
    monkeypatch.delenv("BUILD_ENV_OUTPUT_DIR", raising=False)
    export(lock, duckdb_client=client)
    client.close()
    recipe = plan.recipe_from_yaml(lock)

    assert gdb_metadata_drift(recipe) == [
        f"things.zip:boundaries: packaged file "
        f"{tmp_path / 'package' / 'dataset_files' / 'things.zip'} not found"
    ]

    def package_with_columns(*names: str) -> None:
        org_md = MagicMock()
        org_md.product.return_value.dataset.return_value.calculate_layer_dataset_metadata.return_value = Dataset(
            attributes=DatasetAttributes(
                display_name="Boundaries", description="d", each_row_is_a="Boundary"
            ),
            columns=[DatasetColumn(id=n.lower(), name=n) for n in names],
        )
        monkeypatch.setattr(export_module.product_metadata, "load", lambda: org_md)
        package(lock)

    package_with_columns("id", "code")
    assert gdb_metadata_drift(recipe) == []
    check_gdb_metadata(recipe)

    package_with_columns("id", "gone")
    assert gdb_metadata_drift(recipe) == [
        "things.zip:boundaries: fields without metadata: ['code']",
        "things.zip:boundaries: metadata attrs that aren't fields: ['gone']",
    ]
    with pytest.raises(ValueError, match="fields without metadata"):
        check_gdb_metadata(recipe)


def test_parquet_first_gdb_layer_recurved_from_arcs(tmp_path, monkeypatch):
    """custom.arcs: the layer's arcs export alongside it, and `package` writes the FGDB
    layer with true curves where its vertices match an arc."""
    from pyogrio.raw import read_arrow

    from dcpy.geospatial.curves import linearize_arc
    from dcpy.lifecycle.builds.export import package

    arc = [(10.0, 0.0), (7.0710678118654755, 7.0710678118654755), (0.0, 10.0)]
    ring = linearize_arc(*arc) + [(0.0, 0.0), arc[0]]
    wkt = "MULTIPOLYGON(((" + ", ".join(f"{x!r} {y!r}" for x, y in ring) + ")))"
    client = duckdb_utils.DuckDBClient(tmp_path / "t.duckdb", "s")
    client.conn.execute(
        f"CREATE TABLE s.quarter AS SELECT 1 AS id, ST_GeomFromText('{wkt}') AS geom"
    )
    client.conn.execute(
        "CREATE TABLE s.quarter_arcs AS SELECT "
        + ", ".join(
            f"{v!r} AS {c}"
            for c, v in zip(
                ["x0", "y0", "x1", "y1", "x2", "y2"], sum(map(list, arc), [])
            )
        )
    )
    lock = tmp_path / "recipe.lock.yml"
    lock.write_text(
        f"""
name: t
product: t
version: v1
inputs:
  datasets:
  - name: src
    version: v1
    file_type: csv
    destination: duckdb
    source: edm.recipes.datasets
exports:
  output_folder: {tmp_path / "output"}
  package_from_parquet: true
  crs: EPSG:2263
  datasets:
  - name: quarter
    filename: things.gdb.zip
    format: gdb
    custom: {{ arcs: [quarter_arcs] }}
"""
    )
    monkeypatch.setenv("BUILD_ENV_ROOT_DIR", str(tmp_path))
    monkeypatch.delenv("BUILD_ENV_OUTPUT_DIR", raising=False)
    out = export(lock, duckdb_client=client)
    assert out is not None
    assert (out / "raw_exports" / "things" / "quarter.arcs.parquet").exists()
    packaged = package(lock)
    gdb = f"/vsizip/{packaged / 'dataset_files' / 'things.gdb.zip'}/things.gdb"
    _, table = read_arrow(gdb, layer="quarter")
    wkb = table.column(table.num_columns - 1)[0].as_py()
    assert int.from_bytes(wkb[1:5], "little") == 12  # MultiSurface
    client.close()


def test_parquet_first_fixed_width(tmp_path, monkeypatch):
    """A dat entry with custom.layout exports its unformatted rows to parquet, and
    `package` formats them into the fixed-width file."""
    from dcpy.lifecycle.builds.export import package

    client = duckdb_utils.DuckDBClient(tmp_path / "t.duckdb", "s")
    client.conn.execute(
        "CREATE TABLE s.rows AS SELECT * FROM (VALUES (1, 'ab'), (2, NULL), (3, 'c')) t (id, code)"
    )
    (tmp_path / "layout.csv").write_text(
        "field_name,field_length,justify_and_fill,blank_if_none\n"
        "id,3,RJZF,FALSE\ncode,4,LJSF,TRUE\n"
    )
    lock = tmp_path / "recipe.lock.yml"
    lock.write_text(
        f"""
name: t
product: t
version: v1
inputs:
  datasets:
  - name: src
    version: v1
    file_type: csv
    destination: duckdb
    source: edm.recipes.datasets
exports:
  output_folder: {tmp_path / "output"}
  package_from_parquet: true
  datasets:
  - name: rows
    group: flat_files
    filename: rows.dat
    format: dat
    custom: {{ layout: layout.csv, where: "id < 3", order_by: id }}
  - name: rows
    group: flat_files
    filename: rows_eof.dat
    format: dat
    custom: {{ layout: layout.csv, where: "id < 3", order_by: id, eof_marker: true }}
"""
    )
    monkeypatch.setenv("BUILD_ENV_ROOT_DIR", str(tmp_path))
    monkeypatch.delenv(
        "BUILD_ENV_OUTPUT_DIR", raising=False
    )  # set by other tests' loads
    out = export(lock, duckdb_client=client)
    assert out is not None
    assert (out / "raw_exports" / "flat_files" / "rows.parquet").exists()
    packaged = package(lock, product_path=tmp_path)
    assert (packaged / "dataset_files" / "flat_files" / "rows.dat").read_bytes() == (
        b"001ab  \r\n002    \r\n"
    )
    # eof_marker: a DOS end-of-file byte after the last line
    assert (
        packaged / "dataset_files" / "flat_files" / "rows_eof.dat"
    ).read_bytes() == b"001ab  \r\n002    \r\n\x1a"
    client.close()


_NO_EXPORTS_RECIPE = """\
name: Test Product
product: test
version: 24Q1
build_name: 24Q1
inputs:
  datasets: []
"""
_NO_EXPORTS_STAGED_RECIPE = (
    _NO_EXPORTS_RECIPE
    + """\
stage_config:
  builds.qa:
    commands:
    - name: noop
      run: "true"
"""
)


@pytest.mark.parametrize(
    "recipe_text, subdir",
    [(_NO_EXPORTS_RECIPE, ""), (_NO_EXPORTS_STAGED_RECIPE, "build")],
    ids=["not_staged", "staged"],
)
def test_export_recipe_data_without_exports_uses_recipe_build_dir(
    tmp_path, monkeypatch, recipe_text, subdir
):
    """With no exports, the metadata is written to the build dir: the root for a recipe
    that doesn't use stage dirs, <root>/build for one that does."""
    from dcpy.lifecycle.builds import build

    for var in ("BUILD_ENV_OUTPUT_DIR", "BUILD_ENV_ROOT_DIR"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DCPY_LIFECYCLE_DATA_DIR", str(tmp_path / "lifecycle_data"))
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(recipe_text)

    from dcpy.lifecycle.config import get_build_dir

    expected = get_build_dir("test", "24Q1") / subdir
    with patch("dcpy.lifecycle.builds.metadata.write_build_metadata") as write_meta:
        build.run_single_command(
            product_path=tmp_path / "test",
            recipe_lock_path=recipe_path,
            command_name="export_recipe_data",
        )
    assert expected.is_dir()
    assert write_meta.call_args.kwargs["output_folder"] == expected


def test_zip_groups_zips_only_marked_groups(tmp_path):
    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(
        """\
name: Test Product
product: test
version: 24Q1
inputs:
  datasets:
  - name: mytable
    destination: duckdb
exports:
  groups:
    flat_files:
      zip: true
    other:
      zip: false
  datasets:
  - name: mytable
    group: flat_files
    format: csv
"""
    )
    stage = tmp_path / "stage"
    for group in ("flat_files", "other"):
        (stage / "dataset_files" / group).mkdir(parents=True)
        (stage / "dataset_files" / group / "a.txt").write_text("a")

    zipped = zip_groups(recipe_path, stage)

    assert zipped == [stage / "dataset_files" / "flat_files.zip"]
    assert not (stage / "dataset_files" / "flat_files").exists()
    assert (stage / "dataset_files" / "other" / "a.txt").exists()
    with zipfile.ZipFile(zipped[0]) as zf:
        assert zf.namelist() == ["a.txt"]
