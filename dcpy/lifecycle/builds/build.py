from pathlib import Path
import typer

from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.connector_registry import connectors


app = typer.Typer(add_completion=False)

STAGE = "builds.build"


@app.command("upload")
def _upload_build(
    build_path: Path,
    recipe_lock_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Path of recipe lock file to use",
    ),
):
    """Upload a build to the destination configured in the recipe."""
    recipe = plan.recipe_from_yaml(
        recipe_lock_path or (Path(build_path).parent / "recipe.lock.yml")
    )

    stage_config = recipe.stage_config[STAGE]
    # TODO: eventually we should add stage_config defaults in lifecycle.config
    assert stage_config.destination, "A destination must be defined"

    connector_key = stage_config.destination_key or recipe.product

    result = connectors[stage_config.destination].push(
        build_path=build_path,
        key=connector_key,
        connector_args=stage_config.get_connector_args_dict(),
        # TODO: eventually also pass the metadata from the build stage output, which would allow us to skip passing the build path
    )
    typer.echo(result)
