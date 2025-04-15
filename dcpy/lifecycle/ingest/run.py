import json
from pathlib import Path
import shutil

from dcpy.utils.logging import logger
from dcpy.models.lifecycle.ingest import Config
from dcpy.configuration import TEMPLATE_DIR
from dcpy.lifecycle import config
from dcpy.lifecycle.ingest.connectors import raw_datastore, processed_datastore

from . import configure, extract, transform, validate

LIFECYCLE_STAGE = "ingest"
INGEST_DIR = config.local_data_path_for_stage(LIFECYCLE_STAGE)

INGEST_STAGING_DIR = INGEST_DIR / "staging"
INGEST_OUTPUT_DIR = INGEST_DIR / "datasets"
CONFIG_FILENAME = "config.json"


def ingest(
    dataset_id: str,
    version: str | None = None,
    *,
    dataset_staging_dir: Path | None = None,
    ingest_output_dir: Path = INGEST_OUTPUT_DIR,
    mode: str | None = None,
    latest: bool = False,
    push: bool = False,
    output_csv: bool = False,
    template_dir: Path | None = TEMPLATE_DIR,
    local_file_path: Path | None = None,
) -> Config:
    if template_dir is None:
        raise KeyError("Missing required env variable: 'TEMPLATE_DIR'")
    config = configure.get_config(
        dataset_id,
        version=version,
        mode=mode,
        template_dir=template_dir,
        local_file_path=local_file_path,
    )
    transform.validate_processing_steps(config.id, config.ingestion.processing_steps)

    if not dataset_staging_dir:
        dataset_staging_dir = (
            INGEST_STAGING_DIR
            / dataset_id
            / config.archival.archival_timestamp.isoformat()
        )
        dataset_staging_dir.mkdir(parents=True)
    else:
        dataset_staging_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Using {dataset_staging_dir} to stage data")

    with open(dataset_staging_dir / CONFIG_FILENAME, "w") as f:
        json.dump(config.model_dump(mode="json"), f, indent=4)

    # download dataset
    filepath = extract.download_file_from_source(
        config.ingestion.source,
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
            latest=latest,
        )

    init_parquet = "init.parquet"
    transform.to_parquet(
        config.ingestion.file_format,
        filepath,
        dir=dataset_staging_dir,
        output_filename=init_parquet,
    )

    config.ingestion.processing_steps_summaries = transform.process(
        config.id,
        config.ingestion.processing_steps,
        config.columns,
        dataset_staging_dir / init_parquet,
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

    is_new = validate.validate_against_existing_versions(
        config.dataset, dataset_staging_dir / config.filename
    )
    if push and is_new:
        assert config.archival.acl
        processed_datastore.push(
            dataset_id,
            version=config.version,
            filepath=dataset_staging_dir / config.filename,
            config=config,
            overwrite=False,  ## TODO - allow this via flag?
            latest=latest,
        )
    else:
        logger.info("Skipping archival")
    return config
