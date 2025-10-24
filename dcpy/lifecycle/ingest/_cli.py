from pathlib import Path
import typer

from .run import ingest
from dcpy.configuration import INGEST_DEF_DIR
from . import validate

app = typer.Typer(add_completion=False)


@app.command("ingest")
def _cli_wrapper_run(
    dataset_id: str = typer.Argument(),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Version of dataset being archived",
    ),
    mode: str = typer.Option(None, "-m", "--mode", help="Preprocessing mode"),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Push to latest folder in s3"
    ),
    push: bool = typer.Option(False, "--push", "-p"),
    csv: bool = typer.Option(
        False, "-c", "--csv", help="Output csv locally as well as parquet"
    ),
    local_file_path: Path = typer.Option(
        None,
        "--local-file-path",
        "-f",
        help="Use local file path as source, overriding source in definition",
    ),
    definition_dir: Path = typer.Option(
        INGEST_DEF_DIR,
        "--definition-dir",
        "-d",
        help="Local path to folder with definitions. Overrides `TEMPLATE_DIR` env variable.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="If existing version found, overwrite. This should be phased out, but is needed to support ZTL workflow",
    ),
):
    ingest(
        dataset_id,
        version,
        mode=mode,
        latest=latest,
        push=push,
        output_csv=csv,
        local_file_path=local_file_path,
        definition_dir=definition_dir,
        overwrite_okay=overwrite,
    )


@app.command("validate_definitions")
def _cli_wrapper_validate(
    path: Path = typer.Argument(
        help="Path to definition file or folder containing definition files to validate",
    ),
):
    """Validate definition file(s)."""
    if path.is_file():
        try:
            validate.validate_definition_file(path)
            typer.echo("✓ Template file validation passed")
        except Exception as e:
            typer.echo(f"Validation failed: {e}", err=True)
            raise typer.Exit(1)
    else:
        errors = validate.validate_definition_folder(path)
        if errors:
            for error in errors:
                typer.echo(error, err=True)
            raise typer.Exit(1)
        typer.echo("✓ All definitions validated successfully")
