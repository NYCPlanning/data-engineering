import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal

import geopandas as gpd
import pyogrio
import typer
from shapely import MultiLineString, MultiPoint, MultiPolygon

from dcpy.lifecycle import config
from dcpy.lifecycle.builds import config as build_config
from dcpy.lifecycle.builds import metadata, plan
from dcpy.lifecycle.builds.config import BUILD_STAGE_KEY
from dcpy.lifecycle.builds.models import ExportDataset, ExportFormat
from dcpy.utils import postgres
from dcpy.utils.logging import logger

# Both single and multi variants are treated as the same geometry family so that
# PostGIS columns with mixed Point/MultiPoint (or Polygon/MultiPolygon) aren't
# silently split or dropped when filtering by geometry_type.
_POINT_TYPES = ["Point", "MultiPoint"]
_POLYGON_TYPES = ["Polygon", "MultiPolygon"]
_LINE_TYPES = ["LineString", "MultiLineString"]


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
            # Convert geometry columns (WKBElement) to WKT strings for parquet compatibility
            from geoalchemy2.elements import WKBElement

            for col in df.columns:
                if len(df) > 0 and isinstance(df[col].iloc[0], WKBElement):
                    # Convert WKBElement to WKT string
                    df[col] = df[col].apply(lambda x: x.desc if x is not None else None)
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
    elif geometry_type == "lines":
        gdf = gdf[gdf.geom_type.isin(_LINE_TYPES)]
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
    elif types_set <= set(_LINE_TYPES):
        if types_set == {"LineString", "MultiLineString"}:
            gdf = gdf.copy()
            gdf["geometry"] = gdf["geometry"].apply(
                lambda g: MultiLineString([g]) if g.geom_type == "LineString" else g
            )
    else:
        raise ValueError(
            f"'{label}' contains geometry types from different families: "
            f"{sorted(types_set)}. Specify geometry_type='points', 'polygons', "
            "or 'lines' in custom."
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
    # OpenFileGDB requires creating the file on the first layer, then appending the
    # rest — you can't write multiple layers in one call. write_dataframe handles both
    # GeoDataFrames (spatial, one geometry type per layer) and plain DataFrames
    # (non-spatial tables like node_stname / altnames, which have no geometry column).
    for i, (layer_name, gdf) in enumerate(layers):
        if gdf.empty:
            raise ValueError(f"No rows to export for GDB layer '{layer_name}'")
        if isinstance(gdf, gpd.GeoDataFrame):
            gdf = _normalize_to_single_geom_type(gdf, layer_name)
        pyogrio.write_dataframe(
            gdf,
            str(gdb_path),
            driver="OpenFileGDB",
            layer=layer_name,
            append=i > 0,
        )
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
    recipe_lock_path: Path,
    pg_client: postgres.PostgresClient | None = None,
) -> Path | None:
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))

    if not recipe.exports:
        logger.info("No exports defined in recipe, skipping export step")
        return None

    pg_client = pg_client or postgres.PostgresClient(schema=metadata.build_name())
    logger.info(
        f"Exporting build outputs for {recipe.name} from schema {pg_client.schema}"
    )

    # Use version for output path, not schema/branch name
    if recipe.exports and recipe.exports.output_folder:
        output_folder = recipe.exports.output_folder
    else:
        if not recipe.version:
            raise ValueError("Recipe version must be set for export")
        output_folder = (
            config.local_data_path_for_stage(BUILD_STAGE_KEY)
            / recipe.product
            / recipe.version
        )

    # Create output folder if it doesn't exist (preserves existing artifacts like attachments)
    output_folder.mkdir(parents=True, exist_ok=True)

    # Only clean the dataset_files folder (where exports will go)
    # This preserves attachments and other build artifacts created before export
    dataset_files_folder = output_folder / "dataset_files"
    if dataset_files_folder.exists():
        shutil.rmtree(dataset_files_folder)
    dataset_files_folder.mkdir(parents=True)

    # Ensure attachments folder exists for source_data_versions.csv
    attachments_folder = output_folder / "attachments"
    attachments_folder.mkdir(parents=True, exist_ok=True)

    for filename in plan.ARTIFACTS:
        source_path = Path(recipe_lock_path).parent / filename
        if not source_path.exists():
            logger.warning(f"Expected build artifact {source_path} does not exist")
            continue
        # Put source_data_versions.csv in attachments folder, others in root
        if filename == "source_data_versions.csv":
            shutil.copy(source_path, attachments_folder / filename)
        else:
            shutil.copy(source_path, output_folder / filename)

    # Copy artifact directories from recipe_lock_path parent (build output directory)
    # Skip dataset_files since it's being generated by export
    for dirname in build_config.BUILD_ARTIFACT_DIRS:
        if dirname == "dataset_files":
            continue  # Skip dataset_files - it's generated by export, not copied
        source_dir = Path(recipe_lock_path).parent / dirname
        if source_dir.is_dir():
            # Special handling for target directory - move to diagnostics/dbt
            if dirname == "target":
                diagnostics_dbt_dir = output_folder / "diagnostics" / "dbt"
                shutil.copytree(source_dir, diagnostics_dbt_dir)
                logger.info("Copied dbt target directory to diagnostics/dbt")
            else:
                # Remove existing directory if it exists before copying
                dest_dir = output_folder / dirname
                if dest_dir.exists():
                    shutil.rmtree(dest_dir)
                shutil.copytree(source_dir, dest_dir)
                logger.info(f"Copied artifact directory {dirname} from build output")
        else:
            logger.debug(f"Artifact directory {dirname} does not exist in build output")

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
                file_path=dataset_files_folder / filename,
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
            if "geometry_type" in custom:
                gdf = _read_filtered_gdf(
                    output.name,
                    pg_client,
                    geom_column=custom.get("geom_column", "geom"),
                    geometry_type=custom["geometry_type"],
                )
            else:
                # Non-spatial GDB layer (no geometry_type) — read as a plain table.
                gdf = pg_client.read_table_df(output.name)
            layers.append((layer_name, gdf))
        with tempfile.TemporaryDirectory() as tmp_str:
            _write_gdb_zip(layers, dataset_files_folder / filename, Path(tmp_str))

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
    output_path = export(recipe_lock_path)
    if output_path:
        typer.echo(f"Export completed: {output_path}")
