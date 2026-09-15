import shutil
from pathlib import Path

import typer

from dcpy.configuration import INGEST_DEF_DIR
from dcpy.lifecycle.ingest import plan, run, validate
from dcpy.utils.logging import logger

app = typer.Typer(add_completion=False)


@app.command("resolve")
def _cli_resolve(
    dataset_id: str = typer.Argument(help="Dataset id"),
    version: str | None = typer.Option(
        None,
        "-v",
        "--version",
        help="Version of dataset being archived",
    ),
    staging_dir: Path = typer.Option(run.INGEST_STAGING_DIR, "--staging-dir", "-s"),
):
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
        staging_dir.mkdir(parents=True)

    resolved_config = plan.resolve_definition(
        dataset_id,
        version=version,
        definition_dir=INGEST_DEF_DIR,
        local_file_path=None,
    )

    resolved_config.dump_json(staging_dir / "definition.lock.json")


@app.command("extract")
def _cli_extract(
    staging_dir: Path = typer.Option(run.INGEST_STAGING_DIR, "--staging-dir", "-s"),
    push: bool = typer.Option(False, "--push", "-p"),
):
    return run.extract_and_archive_raw_dataset(
        staging_dir=staging_dir,
        push=push,
    )


@app.command("transform")
def _cli_process_datasets(
    staging_dir: Path = typer.Option(run.INGEST_STAGING_DIR, "--staging-dir", "-s"),
    mode: str | None = typer.Option(None, "-m", "--mode", help="Preprocessing mode"),
    output_csv: bool = typer.Option(
        False, "-c", "--csv", help="Output csv locally as well as parquet"
    ),
    datasets_filter: list[str] = typer.Option(
        None,
        "--datasets",
        "-D",
        help="Specific datasets to ingest within a source-centric definition",
    ),
):
    return run.process_datasets(
        staging_dir=staging_dir,
        mode=mode,
        output_csv=output_csv,
        datasets_filter=datasets_filter,
    )


@app.command("archive_datasets")
def _cli_archive_datasets(
    staging_dir: Path = typer.Option(run.INGEST_STAGING_DIR, "--staging-dir", "-s"),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Push to latest folder in s3"
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="If existing version found, overwrite. This should be phased out, but is needed to support ZTL workflow",
    ),
):
    return run.archive_transformed_datasets(
        staging_dir=staging_dir,
        latest=latest,
        overwrite_okay=overwrite,
    )


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
    datasets_filter: list[str] = typer.Option(
        None,
        "--datasets",
        "-D",
        help="Specific datasets to ingest within a source-centric definition",
    ),
):
    run.ingest(
        dataset_id,
        version,
        mode=mode,
        datasets_filter=datasets_filter,
        latest=latest,
        push=push,
        output_csv=csv,
        local_file_path=local_file_path,
        definition_dir=definition_dir,
        overwrite_okay=overwrite,
    )


@app.command("process_archived_datasource")
def _cli_process_archived_datasource(
    dataset_id: str = typer.Argument(),
    timestamp: str = typer.Option(
        None,
        "-t",
        "--timestamp",
        help="Timestamp version of the archived datasource. Defaults to latest",
    ),
    mode: str | None = typer.Option(None, "-m", "--mode", help="Preprocessing mode"),
    csv: bool = typer.Option(
        False, "-c", "--csv", help="Output csv locally as well as parquet"
    ),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Push to latest folder in s3"
    ),
    push: bool = typer.Option(False, "--push", "-p"),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="If existing version found, overwrite. This should be phased out, but is needed to support ZTL workflow",
    ),
):
    run.process_archived_datasource(
        dataset_id,
        timestamp_str=timestamp,
        mode=mode,
        latest=latest,
        push=push,
        output_csv=csv,
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
        errors: dict = validate.find_definition_file_validation_errors(
            path.stem, path.parent
        )
    else:
        errors = validate.find_definition_folder_validation_errors(path)
    if errors:
        for file in errors:
            logger.error(f"Error(s) in definition {file}:\n{errors[file]}")
        raise typer.Exit(1)
    typer.echo("✓ Definition(s) validated, no errors found")
