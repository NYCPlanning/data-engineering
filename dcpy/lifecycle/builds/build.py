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
    recipe_lock_path = recipe_lock_path or (Path(build_path).parent / "recipe.lock.yml")
    recipe = plan.recipe_from_yaml(recipe_lock_path)
    stage_config = recipe.stage_config[STAGE]
    stage_config_dict = {a.name: a.value for a in stage_config.connector_args or []}

    assert stage_config.destination, "A destination must be defined"
    result = connectors[stage_config.destination].push(
        key=stage_config.destination_key or "",
        recipe=recipe,
        stage_config=stage_config_dict,
        build_path=build_path,
        # TODO: also pass the stage output
    )
    typer.echo(result)
