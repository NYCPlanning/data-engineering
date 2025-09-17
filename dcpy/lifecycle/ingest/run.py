from pathlib import Path
import shutil

from dcpy.utils.logging import logger
from dcpy.utils import metadata
from dcpy.models.lifecycle.ingest import (
    ResolvedDownstreamDataset,
    ResolvedDataSource,
    ArchivedDataSource,
    IngestedDataset,
    Transformation,
)
from dcpy.configuration import INGEST_DEF_DIR
from dcpy.lifecycle import config
from dcpy.lifecycle.ingest.connectors import (
    get_raw_datastore_connector,
    get_processed_datastore_connector,
)

from . import extract, plan, transform, validate

LIFECYCLE_STAGE = "ingest"
INGEST_DIR = config.local_data_path_for_stage(LIFECYCLE_STAGE)

INGEST_STAGING_DIR = INGEST_DIR / "staging"
INGEST_OUTPUT_DIR = INGEST_DIR / "datasets"
RESOLVED_CONFIG_FILENAME = "definition.lock.json"
STAGING_DATASOURCE_CONFIG_FILENAME = "datasource.json"
DATASET_CONFIG_FILENAME = "config.json"


def setup_staging_dir(dir: Path):
    if dir.exists():
        logger.warning(f"Staging directory {dir} already exists, removing")
        shutil.rmtree(dir)
    dir.mkdir(parents=True)
    logger.info(f"Using {dir} to stage data")


def extract_and_archive_raw_dataset(
    config: ResolvedDataSource,
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
    push: bool = False,
    run_details: metadata.RunDetails | None = None,
    _setup_staging_dir: bool = True,
) -> ArchivedDataSource:
    """
    Given a resolved data source configuration, extract the raw dataset from its source
    and optionally archive it.
    """
    if _setup_staging_dir:
        setup_staging_dir(staging_dir)

    run_details = run_details or metadata.get_run_details()

    # download dataset
    datasource_config = extract.extract_source(
        config,
        run_details,
        staging_dir,
    )

    datasource_config.dump_json(staging_dir / STAGING_DATASOURCE_CONFIG_FILENAME)

    if push:
        get_raw_datastore_connector().push(
            config.id,
            version=datasource_config.timestamp.isoformat(),
            filepath=staging_dir / datasource_config.raw_filename,
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
    """
    Given a resolved downstream dataset configuration, transform the raw dataset
    by converting to parquet and running all defined processing steps
    """
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


def process_datasource(
    datasource_config: ArchivedDataSource,
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
    mode: str | None = None,
    output_csv: bool = False,
    run_details: metadata.RunDetails | None = None,
) -> list[IngestedDataset]:
    """Given an archived data source configuration, transform all defined downstream datasets"""

    run_details = run_details or datasource_config.run_details

    version = (
        datasource_config.version
        or extract.determine_version_from_extracted_file(
            staging_dir / datasource_config.raw_filename, datasource_config
        )
        or run_details.timestamp.strftime("%Y%m%d")
    )

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
            raw_filename=datasource_config.raw_filename,
            staging_dir=staging_dir,
            mode=mode,
            run_details=run_details,
            output_csv=output_csv,
        )

        dataset_config = IngestedDataset(
            id=dataset.id,
            version=version,
            crs=dataset.target_crs,
            attributes=dataset.attributes,
            source=datasource_config.details,
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
    definition_dir: Path = INGEST_DEF_DIR,
    local_file_path: Path | None = None,
    overwrite_okay: bool = False,
) -> list[IngestedDataset]:
    """
    Main function to run the full ingest lifecycle for a given datasource.
    - resolve definition of dataset/datasource
    - extract raw data from source
    - optionally archive raw data
    - transform raw data into downstream dataset(s)
    - optionally archive downstream dataset(s)
    """
    validate.validate_definition(dataset_id, definition_dir)

    if definition_dir is None:
        raise KeyError("Missing required env variable: 'TEMPLATE_DIR'")

    run_details = metadata.get_run_details()

    setup_staging_dir(staging_dir)

    resolved_config = plan.resolve_config(
        dataset_id,
        version=version,
        definition_dir=definition_dir,
        local_file_path=local_file_path,
    )

    resolved_config.dump_json(staging_dir / RESOLVED_CONFIG_FILENAME)

    datasource_config = extract_and_archive_raw_dataset(
        resolved_config,
        staging_dir=staging_dir,
        push=push,
        run_details=run_details,
        _setup_staging_dir=False,
    )

    configs = process_datasource(
        datasource_config,
        staging_dir=staging_dir,
        mode=mode,
        output_csv=output_csv,
    )

    for dataset_config in configs:
        dataset_id = dataset_config.id
        dataset_staging_dir = staging_dir / dataset_config.id
        dataset_output_dir = ingest_output_dir / dataset_id / dataset_config.version
        dataset_output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Copying {dataset_config.filename} to {dataset_output_dir}")
        shutil.copy(dataset_staging_dir / DATASET_CONFIG_FILENAME, dataset_output_dir)
        shutil.copy(dataset_staging_dir / dataset_config.filename, dataset_output_dir)

        version_exists = get_processed_datastore_connector().version_exists(
            dataset_id, dataset_config.version
        )
        if version_exists and not overwrite_okay:
            validate.validate_data_against_existing_version(
                dataset_id,
                dataset_config.version,
                dataset_staging_dir / dataset_config.filename,
            )

        if push and (overwrite_okay or not version_exists):
            assert dataset_config.source.acl
            get_processed_datastore_connector().push(
                dataset_id,
                version=dataset_config.version,
                filepath=dataset_staging_dir / dataset_config.filename,
                config=dataset_config,
                overwrite=overwrite_okay,
                latest=latest,
            )
        else:
            logger.info("Skipping archival")

    return configs
