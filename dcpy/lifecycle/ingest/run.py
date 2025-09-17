from pathlib import Path
import shutil

from dcpy.utils.logging import logger
from dcpy.utils import metadata
from dcpy.models.lifecycle.ingest.configuration import (
    ResolvedDownstreamDataset,
    ResolvedDataSource,
    ArchivedDataSource,
    IngestedDataset,
    Transformation,
)
from dcpy.configuration import TEMPLATE_DIR
from dcpy.lifecycle import config
from dcpy.lifecycle.ingest.connectors import raw_datastore, processed_datastore

from . import configure, extract, transform, validate

LIFECYCLE_STAGE = "ingest"
INGEST_DIR = config.local_data_path_for_stage(LIFECYCLE_STAGE)

INGEST_STAGING_DIR = INGEST_DIR / "staging"
INGEST_OUTPUT_DIR = INGEST_DIR / "datasets"
RESOLVED_CONFIG_FILENAME = "config.json"
STAGING_DATASOURCE_CONFIG_FILENAME = "datasource_config.json"
DATASET_CONFIG_FILENAME = "dataset_config.json"


def extract_and_archive_raw_dataset(
    config: ResolvedDataSource,
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
    push: bool = False,
    run_details: metadata.RunDetails | None = None,
) -> ArchivedDataSource:
    run_details = run_details or metadata.get_run_details()

    # download dataset
    datasource_config = extract.extract_source(
        config,
        run_details,
        staging_dir,
    )

    datasource_config.dump_json(staging_dir / STAGING_DATASOURCE_CONFIG_FILENAME)

    if push:
        raw_datastore.push(
            config.id,
            version=datasource_config.timestamp.isoformat(),
            filepath=staging_dir / datasource_config.archival.raw_filename,
            acl=datasource_config.acl,
            config=datasource_config,
            latest=True,
        )

    return datasource_config


def transform_dataset(
    transformation: ResolvedDownstreamDataset,
    raw_filename: str,
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
    mode: str | None = None,
    run_details: metadata.RunDetails | None = None,
    output_csv: bool = False,
) -> Transformation:
    run_details = run_details or metadata.get_run_details()
    ds_id = transformation.id or ""  # todo
    dataset_staging_dir = staging_dir / ds_id

    init_parquet = "_init.parquet"
    transform.to_parquet(
        transformation.file_format,
        staging_dir / raw_filename,
        dir=dataset_staging_dir,
        output_filename=init_parquet,
    )

    processing_steps = transform.determine_processing_steps(
        transformation.processing_steps, target_crs=transformation.target_crs, mode=mode
    )

    processing_steps_summaries = transform.process(
        ds_id,
        processing_steps,
        transformation.columns,
        dataset_staging_dir / init_parquet,
        dataset_staging_dir / f"{ds_id}.parquet",  # TODO
        output_csv=output_csv,
    )

    return Transformation(
        target_crs=transformation.target_crs,
        file_format=transformation.file_format,
        processing_steps=processing_steps,
        processing_mode=mode,
        processing_steps_summaries=processing_steps_summaries,
        run_details=run_details,
    )


def transform_datasets(
    datasource_config: ArchivedDataSource,
    version: str,
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
    mode: str | None = None,
    output_csv: bool = False,
) -> list[IngestedDataset]:
    configs = []

    for dataset in datasource_config.datasets:
        logger.info(f"Transforming dataset {dataset.id}")
        assert dataset.id  # todo
        dataset_staging_dir = staging_dir / dataset.id
        if dataset_staging_dir.exists():
            logger.warning(
                f"Dataset staging directory {dataset_staging_dir} already exists, removing"
            )
            shutil.rmtree(dataset_staging_dir)
        dataset_staging_dir.mkdir()

        transformation_result = transform_dataset(
            dataset,
            raw_filename=datasource_config.archival.raw_filename,
            staging_dir=staging_dir,
            mode=mode,
            run_details=datasource_config.archival.run_details,
            output_csv=output_csv,
        )

        dataset_config = IngestedDataset(
            id=dataset.id,
            version=version,
            crs=dataset.target_crs,
            attributes=dataset.attributes,
            archival=datasource_config.archival,
            transformation=transformation_result,
            columns=dataset.columns,
        )

        dataset_config.dump_json(dataset_staging_dir / DATASET_CONFIG_FILENAME)

        configs.append(dataset_config)

    return configs


def ingest(
    dataset_id: str,
    version: str | None = None,
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
    ingest_output_dir: Path = INGEST_OUTPUT_DIR,
    mode: str | None = None,
    latest: bool = False,
    push: bool = False,
    output_csv: bool = False,
    definition_dir: Path | None = TEMPLATE_DIR,
    local_file_path: Path | None = None,
    overwrite_okay: bool = False,
) -> list[IngestedDataset]:
    if definition_dir is None:
        raise KeyError("Missing required env variable: 'TEMPLATE_DIR'")

    run_details = metadata.get_run_details()

    if staging_dir.exists():
        logger.warning(f"Staging directory {staging_dir} already exists, removing")
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)
    logger.info(f"Using {staging_dir} to stage data")

    resolved_config = configure.resolve_config(
        dataset_id,
        version=version,
        definition_dir=definition_dir,
        local_file_path=local_file_path,
    )

    resolved_config.dump_json(staging_dir / "definition.lock.json")

    datasource_config = extract_and_archive_raw_dataset(
        resolved_config,
        staging_dir=staging_dir,
        push=push,
        run_details=run_details,
    )

    version = (
        datasource_config.version
        or extract.determine_version_from_extracted_file(
            staging_dir / datasource_config.archival.raw_filename, datasource_config
        )
    )

    configs = transform_datasets(
        datasource_config,
        version=version,
        staging_dir=staging_dir,
        mode=mode,
        output_csv=output_csv,
    )

    for dataset_config in configs:
        dataset_staging_dir = staging_dir / dataset_config.id
        dataset_output_dir = ingest_output_dir / dataset_id / dataset_config.version
        dataset_output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Copying {dataset_config.filename} to {dataset_output_dir}")
        shutil.copy(dataset_staging_dir / DATASET_CONFIG_FILENAME, dataset_output_dir)
        shutil.copy(dataset_staging_dir / dataset_config.filename, dataset_output_dir)

        version_exists = processed_datastore.version_exists(
            dataset_id, dataset_config.version
        )
        if version_exists and not overwrite_okay:
            validate.validate_against_existing_version(
                dataset_id,
                dataset_config.version,
                dataset_staging_dir / dataset_config.filename,
            )

        if push and (overwrite_okay or not version_exists):
            assert dataset_config.archival.acl
            processed_datastore.push(
                dataset_id,
                version=dataset_config.version,
                filepath=dataset_staging_dir / dataset_config.filename,
                config=config,
                overwrite=overwrite_okay,
                latest=latest,
            )
        else:
            logger.info("Skipping archival")
        configs.append(dataset_config)

    return configs
