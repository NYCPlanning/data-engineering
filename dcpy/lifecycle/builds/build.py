from pathlib import Path
import typer

from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.connector_registry import connectors


app = typer.Typer(add_completion=False)

STAGE = "builds.build"


@app.command("upload")
def _upload_build(
    recipe_lock_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Path of recipe lock file to use",
    ),
    build_path: Path = typer.Option(
        None,
        "--build-path",
        "-b",
        help="Path to the build",
    ),
):
    """Upload a build to the destination configured in the recipe."""
    recipe_lock_path = recipe_lock_path or (
        Path(plan.DEFAULT_RECIPE).parent / "recipe.lock.yml"
    )
    recipe = plan.recipe_from_yaml(recipe_lock_path)
    stage_config = recipe.stage_config[STAGE]
    conn_args = {a.name: a.value for a in stage_config.connector_args or []}

    assert stage_config.destination, "A destination must be defined"
    result = connectors[stage_config.destination].push(
        key=stage_config.destination_key or "",
        version=recipe.version or "",
        **conn_args,
    )
    typer.echo(result)
