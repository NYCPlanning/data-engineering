import pandas as pd
from pathlib import Path
import typer

from dcpy.models.lifecycle.ingest import Config
from dcpy.connectors.edm import recipes
from . import configure, extract, transform

TMP_DIR = Path("tmp")


def update_freshness(
    config: Config,
    staging_dir: Path,
    *,
    latest: bool,
) -> Config:
    """
    This function is called after a dataset has been preprocessed, just before archival
    It's called in the case that the version of the dataset in the config (either provided or calculated)
      already exists

    The last archived dataset with the same version is pulled in by pandas and compared to what was just processed
    If they are identical, the last archived dataset has its config updated to reflect that it was checked but not re-archived
    If they differ, the version is "patched" and a new patched version is archived
    """
    new = pd.read_parquet(staging_dir / config.filename)
    comparison = recipes.read_df(config.dataset)
    if new.equals(comparison):
        original_archival_timestamp = recipes.update_freshness(
            config.dataset_key, config.archival_timestamp
        )
        config.archival_timestamp = original_archival_timestamp
        if latest:
            recipes.set_latest(config.dataset_key, config.acl)
        return config
    else:
        raise FileExistsError(
            f"Archived dataset '{config.dataset_key}' already exists and has different data."
        )


def run(
    dataset_id: str,
    version: str | None = None,
    *,
    staging_dir: Path | None = None,
    mode: str | None = None,
    latest: bool = False,
    skip_archival: bool = False,
    output_csv: bool = False,
) -> Config:
    config = configure.get_config(dataset_id, version=version, mode=mode)
    transform.validate_processing_steps(config.id, config.processing_steps)

    if not staging_dir:
        staging_dir = TMP_DIR / dataset_id / config.archival_timestamp.isoformat()
        staging_dir.mkdir(parents=True)
    else:
        staging_dir.mkdir(parents=True, exist_ok=True)

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

    transform.preprocess(
        config.id,
        config.processing_steps,
        staging_dir / init_parquet,
        staging_dir / config.filename,
        output_csv=output_csv,
    )

    if not skip_archival:
        if recipes.exists(config.dataset):
            config = update_freshness(config, staging_dir, latest=latest)
        else:
            recipes.archive_dataset(
                config, staging_dir / config.filename, latest=latest
            )

    return config


app = typer.Typer(add_completion=False)


@app.command()
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
    skip_archival: bool = typer.Option(False, "--skip-archival", "-s"),
    csv: bool = typer.Option(
        False, "-c", "--csv", help="Output csv locally as well as parquet"
    ),
):
    run(
        dataset_id,
        version,
        mode=mode,
        latest=latest,
        skip_archival=skip_archival,
        output_csv=csv,
    )
