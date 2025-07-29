from pathlib import Path
import typer

from .run import ingest
from dcpy.configuration import TEMPLATE_DIR

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
        help="Use local file path as source, overriding source in template",
    ),
    template_dir: Path = typer.Option(
        TEMPLATE_DIR,
        "--template-dir",
        "-t",
        help="Local path to folder with templates. Overrides `TEMPLATE_DIR` env variable.",
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
        template_dir=template_dir,
        overwrite_okay=overwrite,
    )


from .validate import validate_template_folder
from .validate import validate_template_file


@app.command("validate")
def _cli_wrapper_validate(
    path: Path = typer.Argument(
        help="Path to template file or folder containing template files to validate",
    ),
    print_report: bool = typer.Option(
        True,
        "--report/--no-report",
        help="Print validation report",
    ),
    raise_on_error: bool = typer.Option(
        False,
        "--strict",
        "-s",
        help="Raise exception if any validation errors found",
    ),
):
    """Validate template file(s)."""
    if path.is_file():
        # Handle single file validation
        try:
            validate_template_file(path)
            typer.echo("✓ Template file validation passed")
        except Exception as e:
            typer.echo(f"Validation failed: {e}", err=True)
            if raise_on_error:
                raise typer.Exit(1)
    elif path.is_dir():
        # Handle folder validation
        errors = validate_template_folder(
            folder_path=path,
            print_report=print_report,
            raise_on_error=raise_on_error,
        )
        if not errors and not print_report:
            typer.echo("✓ All templates validated successfully")
    else:
        typer.echo(f"Error: Path '{path}' is neither a file nor a directory", err=True)
        raise typer.Exit(1)
