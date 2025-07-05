import json
from pathlib import Path
import shutil

from dcpy.utils.logging import logger
from dcpy.models.lifecycle.ingest import Config, RawConfig
from dcpy.configuration import TEMPLATE_DIR
from dcpy.lifecycle import config
from dcpy.lifecycle.ingest.connectors import raw_datastore, processed_datastore

from . import configure, extract, transform, validate

LIFECYCLE_STAGE = "ingest"
INGEST_DIR = config.local_data_path_for_stage(LIFECYCLE_STAGE)

INGEST_STAGING_DIR = INGEST_DIR / "staging"
INGEST_OUTPUT_DIR = INGEST_DIR / "datasets"
CONFIG_FILENAME = "config.json"


def ingest_raw_dataset(
    dataset_id: str,
    version: str | None = None,
    *,
    dataset_staging_dir: Path,
    push: bool = False,
    template_dir: Path | None = TEMPLATE_DIR,
    local_file_path: Path | None = None,
) -> RawConfig:
    if template_dir is None:
        raise KeyError("Missing required env variable: 'TEMPLATE_DIR'")
    config = configure.get_raw_config(
        dataset_id,
        version=version,
        template_dir=template_dir,
        local_file_path=local_file_path,
    )
    # should still do this
    # transform.validate_processing_steps(config.id, config.ingestion.processing_steps)

    with open(dataset_staging_dir / CONFIG_FILENAME, "w") as f:
        json.dump(config.model_dump(mode="json"), f, indent=4)

    # download dataset
    filepath = extract.download_file_from_source(
        config.source,
        config.version,
        dataset_staging_dir,
    )
    config.archival.raw_filename = filepath.name

    if push:
        raw_datastore.push(
            dataset_id,
            version=config.archival.archival_timestamp.isoformat(),
            filepath=filepath,
            config=config,
        )

    return config


def ingest_processed_dataset(
    config: Config,
    *,
    dataset_staging_dir: Path,
    ingest_output_dir: Path,
    latest: bool = False,
    push: bool = False,
    output_csv: bool = False,
    overwrite_okay: bool = False,
) -> Config:
    transform.validate_processing_steps(config.id, config.ingestion.processing_steps)

    init_filename = "init.parquet"
    transform.to_parquet(
        config.ingestion.file_format,
        filepath,
        dir=dataset_staging_dir,
        output_filename=init_filename,
    )

    config.ingestion.processing_steps_summaries = transform.process(
        config.id,
        config.ingestion.processing_steps,
        config.columns,
        dataset_staging_dir / init_filename,
        dataset_staging_dir / config.filename,
        output_csv=output_csv,
    )

    with open(dataset_staging_dir / CONFIG_FILENAME, "w") as f:
        json.dump(config.model_dump(mode="json"), f, indent=4)

    dataset_output_dir = ingest_output_dir / dataset_id / config.version
    dataset_output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Copying {config.filename} to {dataset_output_dir}")
    shutil.copy(dataset_staging_dir / CONFIG_FILENAME, dataset_output_dir)
    shutil.copy(dataset_staging_dir / config.filename, dataset_output_dir)

    version_exists = processed_datastore.version_exists(dataset_id, config.version)
    if version_exists and not overwrite_okay:
        validate.validate_against_existing_version(
            dataset_id,
            config.version,
            dataset_staging_dir / config.filename,
        )

    if push and (overwrite_okay or not version_exists):
        assert config.archival.acl
        processed_datastore.push(
            dataset_id,
            version=config.version,
            filepath=dataset_staging_dir / config.filename,
            config=config,
            overwrite=overwrite_okay,
            latest=latest,
        )
    else:
        logger.info("Skipping archival")
    return config


def ingest_previously_processed_dataset(
    dataset_id: str,
    archival_timestamp: str,
    *,
    mode: str | None = None,
    dataset_staging_dir: Path,
    ingest_output_dir: Path = INGEST_OUTPUT_DIR,
    latest: bool = False,
    push: bool = False,
    output_csv: bool = False,
    overwrite_okay: bool = False,
):
    archival_timestamp = archival_timestamp or raw_datastore.get_latest_version(
        dataset_id
    )
    raw_config = raw_datastore.get_config(dataset_id, archival_timestamp)
    configs = configure.get_configs(raw_config, mode=mode)
    for config in configs:
        ingest_processed_dataset(
            config,
            dataset_staging_dir=dataset_staging_dir,
            ingest_output_dir=ingest_output_dir,
            latest=latest,
            push=push,
            output_csv=output_csv,
            overwrite_okay=overwrite_okay,
        )


def ingest(
    dataset_id: str,
    *,
    version: str | None = None,
    mode: str | None = None,
    dataset_staging_dir: Path,
    ingest_output_dir: Path = INGEST_OUTPUT_DIR,
    latest: bool = False,
    push: bool = False,
    output_csv: bool = False,
    overwrite_okay: bool = False,
    local_file_path: Path | None = None,
):
    if not dataset_staging_dir:
        dataset_staging_dir = (
            INGEST_STAGING_DIR / dataset_id / datetime.now().isoformat()
        )
        dataset_staging_dir.mkdir(parents=True)
    else:
        dataset_staging_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Using {dataset_staging_dir} to stage data")

    raw_config = ingest_raw_dataset(
        dataset_id,
        version=version,
        dataset_staging_dir=dataset_staging_dir,
        push=push,
        local_file_path=local_file_path,
    )
    configs = configure.get_configs(raw_config, mode=mode)
    for config in configs:
        ingest_processed_dataset(
            config,
            dataset_staging_dir=dataset_staging_dir,
            ingest_output_dir=ingest_output_dir,
            latest=latest,
            push=push,
            output_csv=output_csv,
            overwrite_okay=overwrite_okay,
        )
