import json
import os
import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Literal

import typer

from dcpy.geospatial.gdb.write import ParquetLayer, write_gdb_from_parquet
from dcpy.lifecycle.builds import config as build_config
from dcpy.lifecycle.builds import metadata, plan
from dcpy.lifecycle.builds.models import (
    LAYERED_EXPORT_FORMATS,
    ExportDataset,
    ExportFormat,
    InputDatasetDestination,
)
from dcpy.utils import datastores, fixed_width, postgres
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
        case (
            ExportFormat.shapefile
            | ExportFormat.gdb
            | ExportFormat.geopackage
            | ExportFormat.geoparquet
        ):
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
        case (
            ExportFormat.shapefile
            | ExportFormat.gdb
            | ExportFormat.geopackage
            | ExportFormat.geoparquet
        ):
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
    """Export a geospatial table from postgres as a zipped shapefile or FGDB, a
    GeoPackage, or GeoParquet."""
    logger.info(
        f"Exporting geospatial table {table_name} to {file_path} in format {format}"
    )
    gdf = _read_filtered_gdf(table_name, pg_client, geom_column, geometry_type)

    if format == ExportFormat.geopackage:
        datastores.write_gpkg([(layer or table_name, gdf)], file_path)
        return
    if format == ExportFormat.geoparquet:
        datastores.write_geoparquet(gdf, table_name, file_path)
        return
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
    return datastores.filter_geometry_type(gdf, geometry_type, table_name)


# raw exported data (e.g. parquet layers); dataset_files holds only what's delivered
LAYERS_DIR = "raw_exports"
LAYER_MANIFEST = "manifest.json"


def _write_parquet_layers(
    filename: str,
    layers: list[tuple[str, "object"]],
    allow_empty: set[str],
    layer_options: dict[str, dict[str, str]],
    output_folder: Path,
    crs: str | None,
) -> dict:
    """Write each layer of one layered export to <output>/raw_exports/<filename>/<layer>.parquet
    and return its manifest entry. Same rules as datastores._write_layers: one geometry
    type per layer, and an empty layer is an error unless allowed."""
    import geopandas as gpd

    # e.g. raw_exports/district_gdb/v26C_Districts/ for district_gdb/v26C_Districts.gdb.zip
    target = Path(filename)
    folder = (
        output_folder
        / LAYERS_DIR
        / target.parent
        / target.name.removesuffix(".zip").removesuffix(".gdb")
    )
    folder.mkdir(parents=True, exist_ok=True)
    entries = []
    for layer_name, frame in layers:
        geometry_type = None
        if frame.empty and layer_name not in allow_empty:  # type: ignore[attr-defined]
            raise ValueError(f"No rows to export for layer '{layer_name}'")
        if isinstance(frame, gpd.GeoDataFrame):
            if frame.empty:
                geometry_type = "MultiPolygon"  # nothing to infer from
            else:
                frame = datastores._normalize_to_single_geom_type(frame, layer_name)
            if frame.crs is None and crs:
                frame = frame.set_crs(crs)
        path = folder / f"{layer_name}.parquet"
        frame.to_parquet(path)  # type: ignore[attr-defined]
        entries.append(
            {
                "name": layer_name,
                "path": str(path.relative_to(output_folder)),
                "geometry_type": geometry_type,
                "layer_options": layer_options.get(layer_name, {}),
            }
        )
    return {"filename": filename, "format": "gdb", "layers": entries}


def _export_dat_source(
    output: ExportDataset,
    filename: str,
    duckdb_client: duckdb_utils.DuckDBClient,
    output_folder: Path,
) -> dict:
    """Export a fixed-width entry's unformatted rows to raw_exports/<group>/<file>.parquet
    and return its manifest entry.

    custom: layout (the column layout CSV, relative to the product directory),
    layout_source (the unformatted model; default: the entry's name), where / order_by
    (optional SQL for the rows and their order in the file).
    """
    custom = output.custom or {}
    source = custom.get("layout_source", output.name)
    query = f"SELECT * FROM {duckdb_client.schema}.{source}"
    if custom.get("where"):
        query += f" WHERE {custom['where']}"
    if custom.get("order_by"):
        query += f" ORDER BY {custom['order_by']}"
    target = Path(filename)
    parquet = output_folder / LAYERS_DIR / target.parent / f"{target.stem}.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    duckdb_client.export_to_parquet(table_name=source, output_path=parquet, query=query)
    return {
        "filename": filename,
        "format": "dat",
        "path": str(parquet.relative_to(output_folder)),
        "layout": custom["layout"],
    }


def _copy_entry(filename: str) -> dict:
    return {"filename": filename, "format": "copy", "path": f"{LAYERS_DIR}/{filename}"}


def _zip_outputs(output_folder: Path, zip_name: str) -> None:
    # Resolve to absolute first: the zip subprocess below runs with cwd=output_folder,
    # so a relative zip_path (output_folder is often just "output", per recipe.yml)
    # would get re-resolved against that cwd by the zip command itself, landing at
    # output_folder/output_folder/zip_name.zip instead of output_folder/zip_name.zip.
    zip_path = (output_folder / f"{zip_name}.zip").resolve()
    # Zip from within output_folder so entries are relative (e.g. "dataset_files/..."),
    # not an absolute path chain -- and exclude the duckdb file itself, which can live
    # alongside these artifacts (see build_config.get_duckdb_path) but isn't a
    # deliverable, and the intermediate parquet layers.
    subprocess.call(
        ["zip", "-r", str(zip_path), ".", "-x", "*.duckdb", f"{LAYERS_DIR}/*"],
        cwd=output_folder,
    )
    logger.info(f"Zipped export folder to {zip_path}")


def _default_duckdb_client(recipe) -> duckdb_utils.DuckDBClient:
    if not recipe.version:
        raise ValueError("Recipe version must be set for export")
    duckdb_path = build_config.get_duckdb_path(
        recipe.product, recipe.version, recipe.duckdb_filename
    )
    return duckdb_utils.DuckDBClient(db_path=duckdb_path, schema=metadata.build_name())


def _copy_build_artifacts(recipe_lock_path: Path, output_folder: Path) -> None:
    """Copy the build's artifacts (source_data_versions.csv into attachments/, build
    metadata, the dbt target as diagnostics/dbt.zip) next to the deliverables."""
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
        # When the lock file lives in the build output dir, the artifact's already there
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
                # When the lock file lives in the build output dir, it's already there
                if source_dir.resolve() != dest_dir.resolve():
                    if dest_dir.exists():
                        shutil.rmtree(dest_dir)
                    shutil.copytree(source_dir, dest_dir)
                    logger.info(
                        f"Copied artifact directory {dirname} from build output"
                    )
        else:
            logger.debug(f"Artifact directory {dirname} does not exist in build output")


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
        duckdb_client = duckdb_client or _default_duckdb_client(recipe)
        logger.info(
            f"Exporting build outputs for {recipe.name} from DuckDB schema {duckdb_client.schema}"
        )
    else:
        pg_client = pg_client or postgres.PostgresClient(schema=metadata.build_name())
        logger.info(
            f"Exporting build outputs for {recipe.name} from schema {pg_client.schema}"
        )

    if recipe.exports and recipe.exports.output_folder:
        output_folder = recipe.exports.output_folder
    else:
        if not recipe.version:
            raise ValueError("Recipe version must be set for export")
        output_folder = build_config.get_build_output_dir(
            recipe.product, recipe.version
        )

    output_folder.mkdir(parents=True, exist_ok=True)
    from_parquet = recipe.exports.package_from_parquet
    manifest: list[dict] = []
    if from_parquet:
        # Parquet-first: export only writes raw_exports/ in the build dir; `package`
        # assembles the deliverables (dataset_files, attachments, zip) in the package dir
        dataset_files_folder = output_folder / LAYERS_DIR
        if dataset_files_folder.exists():
            shutil.rmtree(dataset_files_folder)
        dataset_files_folder.mkdir(parents=True)
    else:
        # Only clean the dataset_files folder (where exports will go)
        # This preserves attachments and other build artifacts created before export
        dataset_files_folder = output_folder / "dataset_files"
        if dataset_files_folder.exists():
            shutil.rmtree(dataset_files_folder)
        dataset_files_folder.mkdir(parents=True)
        _copy_build_artifacts(Path(recipe_lock_path), output_folder)

    # GDB and GeoPackage entries are grouped by output filename so multiple tables can
    # share one file. All other formats are written one entry at a time.
    layered_groups: defaultdict[tuple[ExportFormat, str], list[ExportDataset]] = (
        defaultdict(list)
    )

    for output in recipe.exports.datasets:
        filename = str(output.output_path)
        (dataset_files_folder / filename).parent.mkdir(parents=True, exist_ok=True)
        custom = output.custom or {}
        if output.format in LAYERED_EXPORT_FORMATS:
            # Grouped below, regardless of backend.
            layered_groups[(output.format, filename)].append(output)
        elif (
            from_parquet
            and output.format == ExportFormat.dat
            and custom.get("layout")
            and duckdb_client is not None
        ):
            # parquet-first fixed-width: export the unformatted rows; `package` formats
            # them with the layout (dcpy.utils.fixed_width), not the database
            manifest.append(
                _export_dat_source(output, filename, duckdb_client, output_folder)
            )
        else:
            if duckdb_client is not None:
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
            if from_parquet:
                # exported as-is into raw_exports/; `package` copies it into dataset_files
                manifest.append(_copy_entry(filename))

    geo_client = duckdb_client if duckdb_client is not None else pg_client
    for (format, filename), entries in layered_groups.items():
        assert geo_client is not None
        layers = []
        allow_empty: set[str] = set()
        layer_options: dict[str, dict[str, str]] = {}
        for output in entries:
            custom = output.custom or {}
            layer_name = output.layer_name
            logger.info(
                f"Reading table '{output.name}' as layer '{layer_name}' for {filename}"
            )
            if custom.get("spatial", True):
                gdf = _read_filtered_gdf(
                    output.name,
                    geo_client,
                    geom_column=custom.get("geom_column", "geom"),
                    geometry_type=custom.get("geometry_type"),
                )
            elif "geometry_type" in custom:
                raise ValueError(
                    f"'{output.name}' sets both spatial: false and geometry_type"
                )
            else:
                gdf = geo_client.read_table_df(output.name)
            layers.append((layer_name, gdf))
            if custom.get("allow_empty"):
                allow_empty.add(layer_name)
            if custom.get("layer_options"):
                layer_options[layer_name] = custom["layer_options"]
        if from_parquet and format == ExportFormat.gdb:
            manifest.append(
                _write_parquet_layers(
                    filename,
                    layers,
                    allow_empty,
                    layer_options,
                    output_folder,
                    recipe.exports.crs,
                )
            )
        elif format == ExportFormat.geopackage:
            datastores.write_gpkg(layers, dataset_files_folder / filename, allow_empty)
            if from_parquet:
                manifest.append(_copy_entry(filename))
        else:
            with tempfile.TemporaryDirectory() as tmp_str:
                datastores.write_gdb_zip(
                    layers, dataset_files_folder / filename, Path(tmp_str), allow_empty
                )

    if from_parquet:
        manifest_dir = output_folder / LAYERS_DIR
        manifest_dir.mkdir(parents=True, exist_ok=True)
        (manifest_dir / LAYER_MANIFEST).write_text(json.dumps(manifest, indent=2))
        logger.info(
            f"Wrote {len(manifest)} parquet exports to package; "
            "`builds export package` folds them into their FGDBs (and zips the outputs)"
        )
    elif recipe.exports.zip_name:
        _zip_outputs(output_folder, recipe.exports.zip_name)

    return output_folder


def package(recipe_lock_path: Path, product_path: Path | None = None) -> Path:
    """Assemble a `package_from_parquet` export's deliverables in the package dir
    ({DCPY_LIFECYCLE_DATA_DIR}/package/<product>/<version>): copy the build artifacts
    into attachments/, then from the build dir's raw_exports/ (per its manifest) copy
    files, format fixed-width files and fold parquet layers into FGDBs, all into
    dataset_files/. Then run the recipe's packaging commands (stage_config
    builds.package; they see BUILD_DIR / PACKAGE_DIR / QA_DIR) and zip the outputs if
    the recipe names a zip."""
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))
    assert recipe.exports, "No exports defined in recipe"
    assert recipe.version, "Recipe version must be set for packaging"
    build_folder = recipe.exports.output_folder or build_config.get_build_output_dir(
        recipe.product, recipe.version
    )
    package_folder = build_config.get_package_output_dir(recipe.product, recipe.version)
    package_folder.mkdir(parents=True, exist_ok=True)
    dataset_files = package_folder / "dataset_files"
    if dataset_files.exists():
        shutil.rmtree(dataset_files)
    dataset_files.mkdir(parents=True)
    _copy_build_artifacts(Path(recipe_lock_path), package_folder)

    manifest_path = build_folder / LAYERS_DIR / LAYER_MANIFEST
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else []
    for entry in manifest:
        target = dataset_files / entry["filename"]
        target.parent.mkdir(parents=True, exist_ok=True)
        if entry["format"] == "copy":
            shutil.copy(build_folder / entry["path"], target)
        elif entry["format"] == "dat":
            layout = fixed_width.load_layout(
                (product_path or Path.cwd()) / entry["layout"]
            )
            n = fixed_width.parquet_to_dat(build_folder / entry["path"], layout, target)
            logger.info(f"Packaged {n} fixed-width lines into {entry['filename']}")
        else:
            layers = [
                ParquetLayer(
                    name=layer["name"],
                    path=build_folder / layer["path"],
                    geometry_type=layer["geometry_type"],
                    layer_options=layer["layer_options"],
                )
                for layer in entry["layers"]
            ]
            write_gdb_from_parquet(layers, target)
            logger.info(f"Packaged {len(layers)} layers into {entry['filename']}")

    if build_config.PACKAGE_STAGE_KEY in recipe.stage_config:
        from dcpy.lifecycle.builds.build import run_build_commands

        os.environ.update(build_config.stage_dirs_env(recipe.product, recipe.version))
        run_build_commands(
            product_path=product_path or Path.cwd(),
            recipe_lock_path=Path(recipe_lock_path),
            stage_key=build_config.PACKAGE_STAGE_KEY,
        )
    if recipe.exports.zip_name:
        _zip_outputs(package_folder, recipe.exports.zip_name)
    return package_folder


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


@app.command("package")
def _package(
    recipe_lock_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Path of recipe lock file to use",
    ),
):
    """Fold a parquet-first export's layers into its FGDBs."""
    recipe_lock_path = recipe_lock_path or (
        Path(plan.DEFAULT_RECIPE).parent / "recipe.lock.yml"
    )
    typer.echo(f"Packaged: {package(recipe_lock_path)}")
