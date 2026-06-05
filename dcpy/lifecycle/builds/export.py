import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal

import typer
from shapely import MultiPoint, MultiPolygon

from dcpy.lifecycle import config
from dcpy.lifecycle.builds import config as build_config
from dcpy.lifecycle.builds import metadata, plan
from dcpy.lifecycle.builds.plan.models import ExportDataset, ExportFormat
from dcpy.utils import postgres
from dcpy.utils.logging import logger

STAGE = "builds.build"

# Both single and multi variants are treated as the same geometry family so that
# PostGIS columns with mixed Point/MultiPoint (or Polygon/MultiPolygon) aren't
# silently split or dropped when filtering by geometry_type.
_POINT_TYPES = ["Point", "MultiPoint"]
_POLYGON_TYPES = ["Polygon", "MultiPolygon"]


def export_dataset_from_postgres(
    table_name: str,
    file_path: Path,
    format: ExportFormat,
    pg_client: postgres.PostgresClient,
    *,
    header: bool = True,
    line_endings: Literal["lf", "crlf"] = "lf",
    **kwargs,  # this is a little sloppy - but need to ignore other custom things like 'formatting'
) -> None:
    """Export a table from postgres in the specified format."""
    logger.info(f"Exporting table {table_name} to {file_path} in format {format}")
    if file_path.exists():
        file_path.unlink()
    match format:
        case ExportFormat.csv:
            pg_client.export_to_csv(
                table_name=table_name,
                output_path=file_path,
                include_header=header,
            )
        case ExportFormat.dat:
            pg_client.export_to_csv(
                table_name=table_name,
                output_path=file_path,
                include_header=False,
            )

            line_endings = "crlf"
        case ExportFormat.parquet:
            df = pg_client.read_table_df(table_name)
            df.to_parquet(file_path, index=False)
        case ExportFormat.shapefile | ExportFormat.gdb:
            export_geodataset_from_postgres(
                table_name=table_name,
                file_path=file_path,
                format=format,
                pg_client=pg_client,
                **kwargs,
            )
        case _:
            raise NotImplementedError(
                f"Export of dataset format {format} not implemented yet"
            )

    if line_endings == "crlf":
        with open(file_path, "rb") as f_in:
            content = f_in.read().replace(b"\n", b"\r\n")
        with open(file_path, "wb") as f:
            f.write(content)


def export_geodataset_from_postgres(
    table_name: str,
    file_path: Path,
    format: ExportFormat,
    pg_client: postgres.PostgresClient,
    *,
    geom_column: str = "geom",
    geometry_type: str | None = None,  # "points" | "polygons" | None (no filter)
    layer: str | None = None,
) -> None:
    """Export a geospatial table from postgres as a zipped shapefile or FGDB."""
    logger.info(
        f"Exporting geospatial table {table_name} to {file_path} in format {format}"
    )
    gdf = _read_filtered_gdf(table_name, pg_client, geom_column, geometry_type)

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp_dir = Path(tmp_str)
        if format == ExportFormat.shapefile:
            _write_shapefile_zip(gdf, table_name, file_path, tmp_dir)
        elif format == ExportFormat.gdb:
            _write_gdb_zip([(layer or table_name, gdf)], file_path, tmp_dir)


def _read_filtered_gdf(
    table_name: str,
    pg_client: postgres.PostgresClient,
    geom_column: str = "geom",
    geometry_type: str | None = None,
):
    gdf = pg_client.read_table_gdf(table_name, geom_column=geom_column)
    if geometry_type == "points":
        gdf = gdf[gdf.geom_type.isin(_POINT_TYPES)]
    elif geometry_type == "polygons":
        gdf = gdf[gdf.geom_type.isin(_POLYGON_TYPES)]
    return gdf


def _normalize_to_single_geom_type(gdf, label: str):
    """Normalize a GDF to a single geometry type within a family.

    PostGIS columns often contain a mix of Point/MultiPoint or Polygon/MultiPolygon.
    Both shapefiles and GDB layers require a single geometry type, so we promote to
    the multi-variant when both are present. Raises if types span different families
    (e.g. Point + Polygon), which requires an explicit geometry_type filter.
    """
    types_set = set(gdf.geom_type.dropna().unique())
    if types_set <= set(_POINT_TYPES):
        # Exact equality avoids a no-op copy when already single-type.
        if types_set == {"Point", "MultiPoint"}:
            gdf = gdf.copy()
            gdf.geometry = gdf.geometry.apply(
                lambda g: MultiPoint([g]) if g.geom_type == "Point" else g
            )
    elif types_set <= set(_POLYGON_TYPES):
        if types_set == {"Polygon", "MultiPolygon"}:
            gdf = gdf.copy()
            gdf.geometry = gdf.geometry.apply(
                lambda g: MultiPolygon([g]) if g.geom_type == "Polygon" else g
            )
    else:
        raise ValueError(
            f"'{label}' contains geometry types from different families: "
            f"{sorted(types_set)}. Specify geometry_type='points' or "
            "'polygons' in custom."
        )
    return gdf


def _write_shapefile_zip(gdf, table_name: str, file_path: Path, tmp_dir: Path) -> None:
    if gdf.empty:
        raise ValueError(
            f"No features to export for '{table_name}' shapefile "
            "(geometry_type filter returned zero rows)"
        )
    gdf = _normalize_to_single_geom_type(gdf, table_name)
    gdf.to_file(tmp_dir / table_name)
    shutil.make_archive(str(file_path.with_suffix("")), "zip", tmp_dir / table_name)


def _write_gdb_zip(
    layers: list[tuple[str, Any]], file_path: Path, tmp_dir: Path
) -> None:
    """Write one or more named layers to a zipped FGDB.

    Each entry in `layers` is (layer_name, gdf). OpenFileGDB requires a single
    geometry type per layer; use geometry_type='points' or 'polygons' in the
    recipe custom fields to filter before reaching here.
    """
    gdb_name = file_path.stem
    gdb_path = tmp_dir / f"{gdb_name}.gdb"
    # OpenFileGDB driver requires mode="w" to create the file, then mode="a" to
    # append subsequent layers — you can't write multiple layers in a single call.
    first = True
    for layer_name, gdf in layers:
        if gdf.empty:
            raise ValueError(
                f"No features for GDB layer '{layer_name}' "
                "(geometry_type filter returned zero rows)"
            )
        gdf = _normalize_to_single_geom_type(gdf, layer_name)
        gdf.to_file(
            str(gdb_path),
            driver="OpenFileGDB",
            layer=layer_name,
            mode="w" if first else "a",
        )
        first = False
    shutil.make_archive(
        str(file_path.with_suffix("")), "zip", tmp_dir, f"{gdb_name}.gdb"
    )


def _output_filename(output: ExportDataset) -> str:
    if output.format in (ExportFormat.shapefile, ExportFormat.gdb):
        default_ext = "zip"
    else:
        default_ext = output.format.value
    return output.filename or f"{output.name}.{default_ext}"


def export(
    recipe_lock_path: Path, pg_client: postgres.PostgresClient | None = None
) -> Path | None:
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))

    if not recipe.exports:
        logger.info("No exports defined in recipe, skipping export step")
        return None

    pg_client = pg_client or postgres.PostgresClient(schema=metadata.build_name())
    logger.info(
        f"Exporting build outputs for {recipe.name} from schema {pg_client.schema}"
    )

    output_folder = (
        recipe.exports.output_folder
        or config.local_data_path_for_stage(STAGE) / recipe.product / pg_client.schema
    )

    if output_folder.exists():
        shutil.rmtree(output_folder)
    output_folder.mkdir(parents=True)

    for filename in plan.ARTIFACTS:
        source_path = Path(recipe_lock_path).parent / filename
        if not source_path.exists():
            logger.warning(f"Expected build artifact {source_path} does not exist")
            continue
        shutil.copy(source_path, output_folder / filename)

    for dirname in build_config.BUILD_ARTIFACT_DIRS:
        source_dir = Path(recipe_lock_path).parent / dirname
        if source_dir.is_dir():
            shutil.copytree(source_dir, output_folder / dirname)
        else:
            logger.warning(
                f"Expected build artifact directory {source_dir} does not exist"
            )

    # GDB entries are grouped by output filename so multiple tables can share one file.
    # All other formats are written one entry at a time.
    gdb_groups: defaultdict[str, list[ExportDataset]] = defaultdict(list)

    for output in recipe.exports.datasets:
        filename = _output_filename(output)
        if output.format == ExportFormat.gdb:
            gdb_groups[filename].append(output)
        else:
            export_dataset_from_postgres(
                table_name=output.name,
                file_path=output_folder / filename,
                pg_client=pg_client,
                format=output.format,
                **output.custom or {},
            )

    for filename, gdb_entries in gdb_groups.items():
        layers = []
        for output in gdb_entries:
            custom = output.custom or {}
            layer_name = custom.get("layer", output.name)
            logger.info(
                f"Reading table '{output.name}' as layer '{layer_name}' for {filename}"
            )
            gdf = _read_filtered_gdf(
                output.name,
                pg_client,
                geom_column=custom.get("geom_column", "geom"),
                geometry_type=custom.get("geometry_type"),
            )
            layers.append((layer_name, gdf))
        with tempfile.TemporaryDirectory() as tmp_str:
            _write_gdb_zip(layers, output_folder / filename, Path(tmp_str))

    if recipe.exports.zip_name:
        zip_path = output_folder / f"{recipe.exports.zip_name}.zip"
        subprocess.call(["zip", "-r", str(zip_path), str(output_folder)])
        logger.info(f"Zipped export folder to {zip_path}")

    return output_folder


app = typer.Typer(add_completion=False)


@app.command("export")
def _export(
    recipe_lock_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Path of recipe lock file to use",
    ),
):
    recipe_lock_path = recipe_lock_path or (
        Path(plan.DEFAULT_RECIPE).parent / "recipe.lock.yml"
    )
    export(recipe_lock_path)
