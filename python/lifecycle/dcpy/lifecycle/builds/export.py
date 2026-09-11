import os
import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Literal

import typer
from dcpy.lifecycle import config
from dcpy.lifecycle.builds import config as build_config
from dcpy.lifecycle.builds import metadata, plan
from dcpy.lifecycle.builds.models import (
    ExportDataset,
    ExportFormat,
    InputDatasetDestination,
)
from dcpy.utils import datastores, postgres
from dcpy.utils import duckdb as duckdb_utils
from dcpy.utils.logging import logger


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


def export_dataset_from_duckdb(
    table_name: str,
    file_path: Path,
    format: ExportFormat,
    duckdb_client: duckdb_utils.DuckDBClient,
    *,
    header: bool = True,
    line_endings: Literal["lf", "crlf"] = "lf",
    **kwargs,  # ignore other custom things like 'formatting', matching the postgres path
) -> None:
    """Export a table from DuckDB in the specified format."""
    logger.info(
        f"Exporting table {table_name} from DuckDB to {file_path} in format {format}"
    )
    if file_path.exists():
        file_path.unlink()
    match format:
        case ExportFormat.csv:
            duckdb_client.export_to_csv(
                table_name=table_name, output_path=file_path, include_header=header
            )
        case ExportFormat.dat:
            duckdb_client.export_to_csv(
                table_name=table_name, output_path=file_path, include_header=False
            )
            line_endings = "crlf"
        case ExportFormat.parquet:
            duckdb_client.export_to_parquet(
                table_name=table_name, output_path=file_path
            )
        case ExportFormat.shapefile | ExportFormat.gdb:
            duckdb_utils.export_geodataset_from_duckdb(
                table_name=table_name,
                file_path=file_path,
                format=format,
                duckdb_client=duckdb_client,
                **kwargs,
            )
        case _:
            raise NotImplementedError(
                f"Export of dataset format {format} from DuckDB not implemented yet"
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
            datastores.write_shapefile_zip(gdf, table_name, file_path, tmp_dir)
        elif format == ExportFormat.gdb:
            datastores.write_gdb_zip([(layer or table_name, gdf)], file_path, tmp_dir)


def _read_filtered_gdf(
    table_name: str,
    client: postgres.PostgresClient | duckdb_utils.DuckDBClient,
    geom_column: str = "geom",
    geometry_type: str | None = None,
):
    gdf = client.read_table_gdf(table_name, geom_column=geom_column)
    if geometry_type == "points":
        gdf = gdf[gdf.geom_type.isin(datastores.POINT_TYPES)]
    elif geometry_type == "polygons":
        gdf = gdf[gdf.geom_type.isin(datastores.POLYGON_TYPES)]
    elif geometry_type == "lines":
        gdf = gdf[gdf.geom_type.isin(datastores.LINE_TYPES)]
    return gdf


def _output_filename(output: ExportDataset) -> str:
    if output.format in (ExportFormat.shapefile, ExportFormat.gdb):
        default_ext = "zip"
    else:
        default_ext = output.format.value
    return output.filename or f"{output.name}.{default_ext}"


def _default_build_output_dir(recipe, recipe_lock_path: Path | None) -> Path:
    """Resolve the build output directory a recipe's artifacts (duckdb file, exports) live in.

    Same priority order as load_source_data_from_resolved_recipe: BUILD_ENV_OUTPUT_DIR (set
    by the build environment, or by a local dev workflow pointing at a custom directory) >
    recipe_lock_path's own directory > the standard {product}/{version} build dir. Export has
    to agree with wherever load actually put the duckdb file, or it can't find it.
    """
    if "BUILD_ENV_OUTPUT_DIR" in os.environ:
        return Path(os.environ["BUILD_ENV_OUTPUT_DIR"])
    if recipe_lock_path is not None:
        return recipe_lock_path.parent
    if not recipe.version:
        raise ValueError("Recipe version must be set for export")
    return config.get_build_dir(recipe.product, recipe.version)


def _default_duckdb_client(
    recipe, recipe_lock_path: Path | None = None
) -> duckdb_utils.DuckDBClient:
    if not recipe.version:
        raise ValueError("Recipe version must be set for export")
    duckdb_path = (
        _default_build_output_dir(recipe, recipe_lock_path)
        / f"{recipe.product}_{recipe.version}.duckdb"
    )
    return duckdb_utils.DuckDBClient(db_path=duckdb_path, schema=metadata.build_name())


def export(
    recipe_lock_path: Path,
    pg_client: postgres.PostgresClient | None = None,
    duckdb_client: duckdb_utils.DuckDBClient | None = None,
) -> Path | None:
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))

    if not recipe.exports:
        logger.info("No exports defined in recipe, skipping export step")
        return None

    # A recipe's export backend follows where its input datasets actually landed. Mixed
    # postgres+duckdb recipes fall back to postgres (the long-standing default) since
    # mixed-backend exports aren't supported.
    destinations = {ds.destination for ds in recipe.inputs.datasets}
    uses_duckdb = InputDatasetDestination.duckdb in destinations
    uses_postgres = InputDatasetDestination.postgres in destinations

    if uses_duckdb and not uses_postgres:
        duckdb_client = duckdb_client or _default_duckdb_client(
            recipe, Path(recipe_lock_path)
        )
        logger.info(
            f"Exporting build outputs for {recipe.name} from DuckDB schema {duckdb_client.schema}"
        )
    else:
        pg_client = pg_client or postgres.PostgresClient(schema=metadata.build_name())
        logger.info(
            f"Exporting build outputs for {recipe.name} from schema {pg_client.schema}"
        )

    # Use version for output path, not schema/branch name. For duckdb-backed builds,
    # defaults to the same directory the duckdb file actually lives in (see
    # _default_build_output_dir) so dataset_files lands next to it rather than off in the
    # standard {product}/{version} path while BUILD_ENV_OUTPUT_DIR points somewhere else.
    # Postgres-backed builds have no such file to co-locate with, so they skip straight to
    # BUILD_ENV_OUTPUT_DIR-or-standard-path - matching where other build-stage steps (e.g.
    # products/template's own data-dictionary generation) already put their artifacts.
    if recipe.exports and recipe.exports.output_folder:
        output_folder = recipe.exports.output_folder
    elif duckdb_client is not None:
        output_folder = _default_build_output_dir(recipe, Path(recipe_lock_path))
    elif "BUILD_ENV_OUTPUT_DIR" in os.environ:
        output_folder = Path(os.environ["BUILD_ENV_OUTPUT_DIR"])
    else:
        if not recipe.version:
            raise ValueError("Recipe version must be set for export")
        output_folder = config.get_build_dir(recipe.product, recipe.version)

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
        dest_path = (
            attachments_folder / filename
            if filename == "source_data_versions.csv"
            else output_folder / filename
        )
        # output_folder defaults to recipe_lock_path's own directory (see
        # _default_build_output_dir) -- when they're literally the same directory, the
        # artifact's already there
        if source_path.resolve() != dest_path.resolve():
            shutil.copy(source_path, dest_path)

    # Copy artifact directories from recipe_lock_path parent (build output directory)
    # Skip dataset_files since it's being generated by export
    for dirname in build_config.BUILD_ARTIFACT_DIRS:
        if dirname == "dataset_files":
            continue  # Skip dataset_files - it's generated by export, not copied
        source_dir = Path(recipe_lock_path).parent / dirname
        if source_dir.is_dir():
            # Special handling for target directory - zip to diagnostics/dbt.zip
            if dirname == "target":
                diagnostics_dir = output_folder / "diagnostics"
                diagnostics_dir.mkdir(parents=True, exist_ok=True)
                zip_path = diagnostics_dir / "dbt"
                shutil.make_archive(str(zip_path), "zip", source_dir)
                logger.info("Zipped dbt target directory to diagnostics/dbt.zip")
            else:
                dest_dir = output_folder / dirname
                # output_folder defaults to recipe_lock_path's own directory (see
                # _default_build_output_dir) -- when they're the same directory, the
                # artifact's already there
                if source_dir.resolve() != dest_dir.resolve():
                    if dest_dir.exists():
                        shutil.rmtree(dest_dir)
                    shutil.copytree(source_dir, dest_dir)
                    logger.info(
                        f"Copied artifact directory {dirname} from build output"
                    )
        else:
            logger.debug(f"Artifact directory {dirname} does not exist in build output")

    # GDB entries are grouped by output filename so multiple tables can share one file.
    # All other formats are written one entry at a time.
    gdb_groups: defaultdict[str, list[ExportDataset]] = defaultdict(list)

    for output in recipe.exports.datasets:
        filename = _output_filename(output)
        if output.format == ExportFormat.gdb:
            # Grouped below so multiple tables can share one .gdb file, regardless of backend.
            gdb_groups[filename].append(output)
        elif duckdb_client is not None:
            export_dataset_from_duckdb(
                table_name=output.name,
                file_path=dataset_files_folder / filename,
                duckdb_client=duckdb_client,
                format=output.format,
                **output.custom or {},
            )
        else:
            assert pg_client is not None
            export_dataset_from_postgres(
                table_name=output.name,
                file_path=dataset_files_folder / filename,
                pg_client=pg_client,
                format=output.format,
                **output.custom or {},
            )

    geo_client = duckdb_client if duckdb_client is not None else pg_client
    for filename, gdb_entries in gdb_groups.items():
        assert geo_client is not None
        layers = []
        allow_empty: set[str] = set()
        for output in gdb_entries:
            custom = output.custom or {}
            layer_name = custom.get("layer", output.name)
            logger.info(
                f"Reading table '{output.name}' as layer '{layer_name}' for {filename}"
            )
            if "geometry_type" in custom:
                gdf = _read_filtered_gdf(
                    output.name,
                    geo_client,
                    geom_column=custom.get("geom_column", "geom"),
                    geometry_type=custom["geometry_type"],
                )
            else:
                # Non-spatial GDB layer (no geometry_type) — read as a plain table.
                gdf = geo_client.read_table_df(output.name)
            layers.append((layer_name, gdf))
            if custom.get("allow_empty"):
                allow_empty.add(layer_name)
        with tempfile.TemporaryDirectory() as tmp_str:
            datastores.write_gdb_zip(
                layers, dataset_files_folder / filename, Path(tmp_str), allow_empty
            )

    if recipe.exports.zip_name:
        zip_path = output_folder / f"{recipe.exports.zip_name}.zip"
        # Zip from within output_folder so entries are relative (e.g. "dataset_files/..."),
        # not an absolute path chain -- and exclude the duckdb file itself, which can live
        # alongside these artifacts (see _default_build_output_dir) but isn't a deliverable.
        subprocess.call(
            ["zip", "-r", str(zip_path), ".", "-x", "*.duckdb"],
            cwd=output_folder,
        )
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
