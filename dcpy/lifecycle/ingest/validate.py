import geopandas as gpd
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
import yaml

from dcpy.utils.logging import logger
from dcpy.utils import introspect
from dcpy.lifecycle.ingest.connectors import processed_datastore
from dcpy.models.lifecycle.ingest import Template, ProcessingStep
from .transform import ProcessingFunctions


def validate_against_existing_version(ds: str, version: str, filepath: Path) -> None:
    """
    This function is called after a dataset has been processed, just before archival
    It's called in the case that the version of the dataset in the config (either provided or calculated)
    already exists

    The last archived dataset with the same version is pulled in by pandas and compared to what was just processed
    If they differ, an error is raised
    """
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


def validate_processing_steps(
    dataset_id: str, processing_steps: list[ProcessingStep]
) -> dict:
    """
    Given config of ingest dataset, validates that defined processing steps
    exist and that appropriate arguments are supplied. Returns a dictionary of violations
    """
    violations: dict[str, str | dict[str, str]] = {}
    processor = ProcessingFunctions(dataset_id)
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


def validate_template_file(filepath: Path) -> None:
    """Validate a single template file."""
    with open(filepath, "r") as f:
        s = yaml.safe_load(f)
    template = Template(**s)
    invalid_processing_steps = validate_processing_steps(
        template.id, template.ingestion.processing_steps
    )
    if invalid_processing_steps:
        raise Exception(f"Invalid processing steps:\n{invalid_processing_steps}")


def validate_template_folder(folder_path: Path) -> list[str]:
    """Validate all template files in a folder and return a list of error messages."""
    if not folder_path.exists():
        return [f"Template directory '{folder_path}' doesn't exist."]

    errors = []
    for file_path in folder_path.glob("*"):
        if file_path.is_file():
            try:
                validate_template_file(file_path)
            except (TypeError, ValueError) as e:
                errors.append(f"{file_path.name}: {str(e)}")

    return errors
