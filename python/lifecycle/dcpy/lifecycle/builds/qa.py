"""The build's QA stage: first a built-in check that the packaged FGDB layers' stamped
Esri metadata matches their fields (gdb_metadata_drift), then the recipe's stage_config
builds.qa commands (e.g. comparing packaged outputs against production). Commands see
BUILD_DIR, PACKAGE_DIR and QA_DIR, and write their results under QA_DIR
({DCPY_LIFECYCLE_DATA_DIR}/builds/qa/<product>/<version>)."""

import os
from pathlib import Path

import typer

from dcpy.lifecycle.builds import config as build_config
from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.builds.models import ExportFormat, Recipe
from dcpy.utils.logging import logger


def gdb_metadata_drift(recipe: Recipe) -> list[str]:
    """For every gdb export entry that names its product-metadata dataset
    (custom.product_metadata), compare the attrs in the packaged layer's Esri metadata
    with the layer's fields, by name. Returns one line per problem: a missing FGDB or
    metadata, fields without an attr, attrs without a field. Esri-managed fields
    (OBJECTID, the geometry) aren't layer fields here, so they're not expected."""
    assert recipe.exports and recipe.version
    stamped = [
        ds
        for ds in recipe.exports.datasets
        if ds.format == ExportFormat.gdb and (ds.custom or {}).get("product_metadata")
    ]
    if not stamped:
        return []
    # local imports: dcpy-geospatial (and gdal) only ship with the `geo` extra. Metadata
    # is read via pyogrio, not fgdb.read_metadata: importing osgeo breaks later parquet
    # reads in the same process (the gdal/pyarrow clash)
    import pyogrio

    from dcpy.geospatial.gdb import fgdb
    from dcpy.geospatial.shapefile_metadata import Metadata

    dataset_files = (
        build_config.get_package_output_dir(recipe.product, recipe.version)
        / "dataset_files"
    )
    problems = []
    for ds in stamped:
        where = f"{ds.output_path}:{ds.layer_name}"
        path = dataset_files / str(ds.output_path)
        if not path.exists():
            problems.append(f"{where}: packaged file {path} not found")
            continue
        gdb = fgdb.resolve_gdb_path(path)
        fields = list(pyogrio.read_info(gdb, layer=ds.layer_name)["fields"])
        xml = pyogrio.read_dataframe(gdb, sql=f"GetLayerMetadata {ds.layer_name}")
        if not xml.iloc[0, 0]:
            problems.append(f"{where}: no layer metadata")
            continue
        metadata = Metadata.from_xml(xml.iloc[0, 0])
        attrs = [attr.attrlabl.value for attr in metadata.eainfo.detailed.attr]
        if missing := [f for f in fields if f not in attrs]:
            problems.append(f"{where}: fields without metadata: {missing}")
        if extra := [a for a in attrs if a not in fields]:
            problems.append(f"{where}: metadata attrs that aren't fields: {extra}")
    return problems


def check_gdb_metadata(recipe: Recipe) -> None:
    """Raise if any stamped FGDB layer's metadata has drifted from its fields."""
    problems = gdb_metadata_drift(recipe)
    if problems:
        raise ValueError(
            "FGDB layer metadata doesn't match the layers' fields (update the "
            "product-metadata datasets the recipe names):\n" + "\n".join(problems)
        )
    logger.info("FGDB layer metadata matches the packaged layers' fields")


def run_qa(recipe_lock_path: Path, product_path: Path | None = None) -> Path:
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))
    assert recipe.version, "Recipe version must be set for QA"
    if build_config.QA_STAGE_KEY not in recipe.stage_config:
        raise ValueError(
            f"Recipe has no stage_config.{build_config.QA_STAGE_KEY}.commands to run"
        )
    dirs = build_config.stage_dirs_env(recipe.product, recipe.version)
    Path(dirs["QA_DIR"]).mkdir(parents=True, exist_ok=True)
    os.environ.update(dirs)
    logger.info(f"Running QA for {recipe.product} {recipe.version}: {dirs}")
    if recipe.exports:
        check_gdb_metadata(recipe)

    from dcpy.lifecycle.builds.build import run_build_commands

    run_build_commands(
        product_path=product_path or Path.cwd(),
        recipe_lock_path=Path(recipe_lock_path),
        stage_key=build_config.QA_STAGE_KEY,
    )
    return Path(dirs["QA_DIR"])


app = typer.Typer(add_completion=False)


@app.command("run")
def _run(
    recipe_lock_path: Path = typer.Option(
        ..., "--recipe-path", "-r", help="Path of recipe lock file to use"
    ),
    product_path: Path = typer.Option(
        None, "--product-path", "-p", help="Product directory (default: cwd)"
    ),
):
    """Run the recipe's QA commands (stage_config builds.qa)."""
    typer.echo(f"QA outputs: {run_qa(recipe_lock_path, product_path)}")


@app.command("gdb-metadata")
def _gdb_metadata(
    recipe_lock_path: Path = typer.Option(
        ..., "--recipe-path", "-r", help="Path of recipe lock file to use"
    ),
):
    """Check that each packaged FGDB layer's stamped metadata matches its fields."""
    check_gdb_metadata(plan.recipe_from_yaml(Path(recipe_lock_path)))
