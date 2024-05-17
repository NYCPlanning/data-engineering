from pathlib import Path
import typer

from dcpy.connectors.edm import recipes
from . import configure, extract, transform

TMP_DIR = Path("tmp")


def run(
    dataset: str,
    version: str | None = None,
    staging_dir: Path | None = None,
    skip_archival: bool = False,
):
    config = configure.get_config(dataset, version)
    transform.validate_processing_steps(config.name, config.processing_steps)

    if not staging_dir:
        staging_dir = TMP_DIR / config.archival_timestamp.isoformat()
        staging_dir.mkdir(parents=True)

    # download dataset
    extract.download_file_from_source(
        config.source, config.raw_filename, config.version, staging_dir
    )
    file_path = staging_dir / config.raw_filename

    if not skip_archival:
        # archive to edm-recipes/raw_datasets
        recipes.archive_raw_dataset(config, staging_dir / config.raw_filename)

    init_parquet = "init.parquet"
    transform.to_parquet(
        config.file_format, file_path, dir=staging_dir, output_filename=init_parquet
    )

    transform.run_processing_steps(
        config.name,
        config.processing_steps,
        staging_dir / init_parquet,
        staging_dir / config.filename,
    )

    if not skip_archival:
        recipes.archive_dataset(config, staging_dir / config.filename)


app = typer.Typer(add_completion=False)


@app.command()
def _cli_wrapper_run(
    dataset: str = typer.Argument(),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Version of dataset being archived",
    ),
    skip_archival: bool = typer.Option(False, "--skip-archival", "-s"),
):
    run(dataset, version, skip_archival=skip_archival)
