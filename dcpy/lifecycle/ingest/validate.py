import geopandas as gpd
import pandas as pd
from pathlib import Path
from pydantic import ValidationError
from tempfile import TemporaryDirectory

from dcpy.models.lifecycle.ingest import (
    Source,
    ProcessingStep,
    DatasetDefinition,
    DataSourceDefinition,
)
from dcpy.utils import introspect
from dcpy.utils.logging import logger
from dcpy.lifecycle.ingest import connectors, plan, transform


def find_source_validation_errors(source: Source) -> dict:
    violations: dict = {}
    if source.type not in connectors.source_connectors.list_registered():
        violations["invalid source type"] = (
            f"Connector with id '{source.type}' not registered"
        )
    connector = connectors.source_connectors[source.type]
    func = connector._pull if "_pull" in dir(connector) else connector.pull  # type: ignore
    kwarg_violations = introspect.validate_kwargs(
        func,
        source.model_dump(),
        ignore_args=["type", "destination_path", "version"],
        strict_enums=False,
    )
    if kwarg_violations:
        violations["invalid arguments"] = kwarg_violations
    return violations


def _validate_pd_series_func(
    *, function_name: str, column_name: str = "", geo=False, **kwargs
) -> str | dict[str, str]:
    parts = function_name.split(".")
    if geo:
        func = gpd.GeoSeries()
        func_str = "gpd.GeoSeries"
    else:
        func = pd.Series()
        func_str = "pd.Series"
    for part in parts:
        if part not in func.__dir__():
            return f"'{func_str}' has no attribute '{part}'"
        func = func.__getattribute__(part)
        func_str += f".{part}"
    return introspect.validate_kwargs(func, kwargs)  # type: ignore


def find_processing_step_validation_errors(
    dataset_id: str, processing_steps: list[ProcessingStep]
) -> dict:
    """
    Given config of ingest dataset, validates that defined processing steps
    exist and that appropriate arguments are supplied. Returns a dictionary of violations
    """
    violations: dict[str, str | dict[str, str]] = {}
    processor = transform.ProcessingFunctions(dataset_id)
    for step in processing_steps:
        if step.name not in processor.__dir__():
            violations[step.name] = "Function not found"
        else:
            func = getattr(processor, step.name)

            # assume that function takes args "self, df"
            kw_error = introspect.validate_kwargs(
                func, step.args, raise_error=False, ignore_args=["self", "df"]
            )
            if kw_error:
                violations[step.name] = kw_error

            # extra validation needed
            elif step.name == "pd_series_func":
                series_error = _validate_pd_series_func(**step.args)
                if series_error:
                    violations[step.name] = series_error

    return violations


def find_definition_validation_errors(
    definition: DatasetDefinition | DataSourceDefinition,
) -> dict:
    """Validate a single template object."""
    violations = {}

    source_violations = find_source_validation_errors(definition.source)
    if source_violations:
        violations["source"] = source_violations

    match definition:
        case DatasetDefinition():
            invalid_processing_steps = find_processing_step_validation_errors(
                definition.id, definition.ingestion.processing_steps
            )
        case DataSourceDefinition():
            invalid_processing_steps = {}
            for d in definition.datasets:
                errors = find_processing_step_validation_errors(
                    d.id,
                    d.processing_steps,
                )
                if errors:
                    invalid_processing_steps[d.id] = errors

    if invalid_processing_steps:
        violations["processing steps"] = invalid_processing_steps

    return violations


def find_definition_file_validation_errors(filepath: Path) -> dict[str, str]:
    """Validate a single definition file."""
    logger.debug(f"Validating template at '{filepath}'")
    try:
        definition = plan.read_definition_file(filepath, version="version")
        return find_definition_validation_errors(definition)
    except (ValidationError, ValueError) as e:
        return {"malformatted yml": str(e)}


def validate_definition(ds_id: str, definition_dir: Path) -> None:
    file = definition_dir / f"{ds_id}.yml"
    errors = find_definition_file_validation_errors(file)
    if errors:
        raise ValidationError(errors)


def find_definition_folder_validation_errors(
    folder_path: Path,
) -> dict[str, dict[str, str]]:
    """Validate all definition files in a folder and return a list of error messages."""
    if not folder_path.exists():
        raise FileNotFoundError(f"Definition directory '{folder_path}' doesn't exist.")

    errors = {}
    for file_path in folder_path.glob("*"):
        file_errors = find_definition_file_validation_errors(file_path)
        if file_errors:
            errors[file_path.name] = file_errors

    return errors


def validate_data_against_existing_version(
    ds: str, version: str, filepath: Path
) -> None:
    """
    This function is called after a dataset has been processed, just before archival
    It's called in the case that the version of the dataset in the config (either provided or calculated)
    already exists

    The last archived dataset with the same version is pulled in by pandas and compared to what was just processed
    If they differ, an error is raised
    """
    processed_datastore = connectors.get_processed_datastore_connector()
    if not processed_datastore._is_library(ds, version):
        with TemporaryDirectory() as tmp:
            existing_file = processed_datastore.pull_versioned(ds, version, Path(tmp))[
                "path"
            ]
            new = pd.read_parquet(filepath)
            existing = pd.read_parquet(existing_file)
            if new.equals(existing):
                logger.info(
                    f"Dataset id='{ds}' version='{version}' already exists and matches newly processed data"
                )
            else:
                raise FileExistsError(
                    f"Archived dataset id='{ds}' version='{version}' already exists and has different data."
                )

    # if previous was archived with library, we both expect some potential slight changes and will not compare
    else:
        logger.warning(
            f"Config of existing dataset id='{ds}' version='{version}' cannot be parsed."
        )
