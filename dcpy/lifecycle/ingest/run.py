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
RESOLVED_DEFINITION_FILENAME = "definition.lock.json"
DATASOURCE_DETAILS_FILENAME = "datasource.json"
INGESTED_DATASET_FILENAME = "config.json"


def _setup_staging_dir(dir: Path):
    if dir.exists():
        logger.warning(f"Staging directory {dir} already exists, removing")
        shutil.rmtree(dir)
    dir.mkdir(parents=True)
    logger.info(f"Using {dir} to stage data")


def extract_and_archive_raw_dataset(
    *,
    resolved_definition: ResolvedDataSource | None = None,
    staging_dir: Path = INGEST_STAGING_DIR,
    push: bool = False,
    run_details: metadata.RunDetails | None = None,
    setup_staging_dir: bool = False,
) -> ArchivedDataSource:
    """
    Given a resolved data source configuration, extract the raw dataset from its source
    and optionally archive it.
    """
    if setup_staging_dir:
        _setup_staging_dir(staging_dir)

    resolved_definition = resolved_definition or ResolvedDataSource.from_path(
        staging_dir / RESOLVED_DEFINITION_FILENAME
    )

    run_details = run_details or metadata.get_run_details()

    datasource_details = extract.extract_source(
        resolved_definition,
        run_details,
        staging_dir,
    )

    datasource_details.dump_json(staging_dir / DATASOURCE_DETAILS_FILENAME)

    if push:
        datastore = get_raw_datastore_connector()
        logger.info(f"Archiving {datasource_details.raw_filename} to {datastore}")
        datastore.push(
            datasource_details.id,
            version=datasource_details.timestamp.isoformat(),
            filepath=staging_dir / datasource_details.raw_filename,
            acl=datasource_details.acl,
            config=datasource_details,
            latest=True,
        )

    return datasource_details


def _transform_dataset(
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
        dataset_staging_dir / f"{ds_id}.parquet",  # TODO this is a tad inelegant
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


def process_datasets(
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
    datasource: ArchivedDataSource | None = None,
    mode: str | None = None,
    output_csv: bool = False,
    run_details: metadata.RunDetails | None = None,
    datasets_filter: list[str] | str | None = None,
) -> list[IngestedDataset]:
    """Given an archived data source configuration, transform all defined downstream datasets"""
    datasource = datasource or ArchivedDataSource.from_path(
        staging_dir / DATASOURCE_DETAILS_FILENAME
    )

    run_details = run_details or metadata.get_run_details()

    version = (
        datasource.version
        or extract.determine_version_from_extracted_file(
            staging_dir / datasource.raw_filename, datasource
        )
        or run_details.timestamp.strftime("%Y%m%d")
    )

    ingested_datasets = []

    datasets = datasource.datasets
    if isinstance(datasets_filter, str):
        datasets_filter = [datasets_filter]
    if datasets_filter:
        dataset_ids = {ds.id for ds in datasets}
        if not set(datasets_filter).issubset(dataset_ids):
            raise ValueError(
                f"Declared dataset(s) {set(datasets_filter).difference(dataset_ids)} not found in datasource details. Found dataset(s): {dataset_ids}"
            )
        datasets = [ds for ds in datasets if ds.id in datasets_filter]

    for dataset in datasets:
        logger.info(f"Transforming dataset {dataset.id}")
        dataset_staging_dir = staging_dir / dataset.id
        if dataset_staging_dir.exists():
            logger.warning(
                f"Dataset staging directory {dataset_staging_dir} already exists, removing"
            )
            shutil.rmtree(dataset_staging_dir)
        dataset_staging_dir.mkdir()

        transformation_result = _transform_dataset(
            dataset,
            raw_filename=datasource.raw_filename,
            staging_dir=staging_dir,
            mode=mode,
            run_details=run_details,
            output_csv=output_csv,
        )

        ingested_dataset = IngestedDataset(
            id=dataset.id,
            version=version,
            crs=dataset.target_crs,
            attributes=dataset.attributes,
            source=datasource.details,
            transformation=transformation_result,
            columns=dataset.columns,
        )

        ingested_dataset.dump_json(dataset_staging_dir / INGESTED_DATASET_FILENAME)
        ingested_datasets.append(ingested_dataset)

    return ingested_datasets


def archive_transformed_datasets(
    *,
    datasets: list[IngestedDataset] | None = None,
    staging_dir: Path,
    latest: bool,
    overwrite_okay: bool = False,
) -> None:
    if not datasets:
        dataset_folders = [p for p in staging_dir.rglob("*/") if p.is_dir()]
        if not dataset_folders:
            raise FileNotFoundError(
                f"No dataset-specific folders found in staging dir '{staging_dir}'. Please make sure prior ingest steps have been run."
            )
        datasets = [
            IngestedDataset.from_path(dataset_folder / INGESTED_DATASET_FILENAME)
            for dataset_folder in dataset_folders
        ]
    datastore = get_processed_datastore_connector()
    for dataset in datasets:
        dataset_folder = staging_dir / dataset.id
        version_exists = datastore.version_exists(dataset.id, dataset.version)
        if version_exists and not overwrite_okay:
            validate.validate_data_against_existing_version(
                dataset.id,
                dataset.version,
                dataset_folder / dataset.filename,
            )

        if overwrite_okay or not version_exists:
            assert dataset.source.acl
            logger.info(f"Archiving {dataset.filename} to {datastore}")
            datastore.push(
                dataset.id,
                version=dataset.version,
                filepath=dataset_folder / dataset.filename,
                config=dataset,
                overwrite=overwrite_okay,
                latest=latest,
            )
        else:
            logger.info("Skipping archival")


def ingest(
    dataset_id: str,
    version: str | None = None,
    *,
    staging_dir: Path = INGEST_STAGING_DIR,
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

    _setup_staging_dir(staging_dir)

    resolved_definition = plan.resolve_definition(
        dataset_id,
        version=version,
        definition_dir=definition_dir,
        local_file_path=local_file_path,
    )

    resolved_definition.dump_json(staging_dir / RESOLVED_DEFINITION_FILENAME)

    extract_and_archive_raw_dataset(
        resolved_definition=resolved_definition,
        staging_dir=staging_dir,
        push=push,
        run_details=run_details,
        setup_staging_dir=False,
    )

    datasets = process_datasets(
        staging_dir=staging_dir,
        mode=mode,
        output_csv=output_csv,
        run_details=run_details,
    )

    if push:  ## TODO should probably just always push, but easily have default dev setting to be to "local" datastore
        archive_transformed_datasets(
            staging_dir=staging_dir,
            latest=latest,
            overwrite_okay=overwrite_okay,
        )

    return datasets


def process_archived_datasource(
    ds_id: str,
    *,
    timestamp_str: str | None = None,
    staging_dir: Path = INGEST_STAGING_DIR,
    mode: str | None = None,
    output_csv: bool = False,
    latest: bool = False,
    datasets_filter: list[str] | str | None = None,
    push: bool = False,
    overwrite_okay: bool = False,
) -> list[IngestedDataset]:
    _setup_staging_dir(staging_dir)
    raw_datastore = get_raw_datastore_connector()

    timestamp_str = timestamp_str or raw_datastore.get_latest_version(ds_id)
    datasource = ArchivedDataSource(
        **raw_datastore._get_config_obj(ds_id, timestamp_str)
    )

    datasource.dump_json(staging_dir / DATASOURCE_DETAILS_FILENAME)

    raw_datastore.pull(
        ds_id, staging_dir, version=timestamp_str, filename=datasource.raw_filename
    )

    datasets = process_datasets(
        datasource=datasource,
        staging_dir=staging_dir,
        mode=mode,
        output_csv=output_csv,
        run_details=metadata.get_run_details(),
        datasets_filter=datasets_filter,
    )

    if push:
        archive_transformed_datasets(
            datasets=datasets,
            staging_dir=staging_dir,
            latest=latest,
            overwrite_okay=overwrite_okay,
        )

    return datasets
