from pathlib import Path
import shutil
import subprocess
import tempfile
import typer

from dcpy.utils import postgres
from dcpy.utils.logging import logger
from dcpy.models.lifecycle.builds import ExportFormat
from dcpy.lifecycle import config
from dcpy.lifecycle.builds import metadata, plan
from dcpy.lifecycle.connector_registry import connectors


app = typer.Typer(add_completion=False)

STAGE = "builds.build"


def export_dataset_from_postgres(
    table_name: str,
    file_path: Path,
    format: ExportFormat,
    pg_client: postgres.PostgresClient,
) -> None:
    """Export a table from postgres in the specified format."""
    logger.info(f"Exporting table {table_name} to {file_path} in format {format}")
    if file_path.exists():
        file_path.unlink()
    match format:
        case ExportFormat.csv:
            pg_client.export_to_csv(table_name=table_name, output_path=file_path)
        case ExportFormat.dat:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".tmp") as temp_file:
                temp_path = Path(temp_file.name)

                pg_client.export_to_csv(
                    table_name=table_name,
                    output_path=temp_path,
                    include_header=False,
                )

                # Convert line endings from LF to CRLF
                with open(temp_path, "r") as f_in:
                    with open(file_path, "wb") as f_out:
                        f_out.write(f_in.read().encode().replace(b"\n", b"\r\n"))
        case _:
            raise NotImplementedError(
                f"Export of dataset format {format} not implemented yet"
            )


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

    for output in recipe.exports.datasets:
        # for now, assumed that postgres is source
        filename = output.filename or f"{output.name}.{output.format.value}"
        export_dataset_from_postgres(
            table_name=output.name,
            file_path=output_folder / filename,
            pg_client=pg_client,
            format=output.format,
        )

    if recipe.exports.zip_name:
        zip_path = output_folder / f"{recipe.exports.zip_name}.zip"
        subprocess.call(["zip", "-r", str(zip_path), str(output_folder)])
        logger.info(f"Zipped export folder to {zip_path}")

    return output_folder


@app.command("export")
def _cli_wrapper_(
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


if __name__ == "__main__":
    app()


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
