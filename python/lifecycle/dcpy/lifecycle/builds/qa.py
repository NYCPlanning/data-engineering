"""The build's QA stage: the recipe's stage_config builds.qa commands (e.g. comparing
packaged outputs against production). Commands see BUILD_DIR, PACKAGE_DIR and QA_DIR,
and write their results under QA_DIR ({DCPY_LIFECYCLE_DATA_DIR}/builds/qa/<product>/<version>)."""

import os
from pathlib import Path

import typer

from dcpy.lifecycle.builds import config as build_config
from dcpy.lifecycle.builds import plan
from dcpy.utils.logging import logger


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
