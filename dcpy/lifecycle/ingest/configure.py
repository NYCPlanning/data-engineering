import jinja2
from jinja2 import meta
from pathlib import Path
import yaml

from dcpy.models.lifecycle.ingest.definitions import (
    Source,
    ProcessingStep,
    IngestDefinitionSimple,
    IngestDefinitionOneToMany,
)
from dcpy.models.lifecycle.ingest.configuration import (
    ResolvedDownstreamDataset,
    ResolvedDataSource,
)
from dcpy.utils import introspect
from dcpy.utils.logging import logger
from dcpy.utils.collections import deep_merge_dict
from dcpy.lifecycle.ingest.connectors import source_connectors
from .transform import ProcessingFunctions


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja definition string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_definition(
    filepath: Path, version: str | None = None
) -> IngestDefinitionSimple | IngestDefinitionOneToMany:
    """
    Given _id id, read yml definition in definition_dir of given dataset
    and insert version as jinja var if provided.
    """
    with open(filepath, "r") as f:
        definition_string = f.read()
    vars = get_jinja_vars(definition_string)
    if vars == {"version"}:
        definition_string = jinja2.Template(definition_string).render(version=version)
    elif vars:
        raise Exception(
            f"'version' is only suppored jinja var. Vars in definition: {vars}"
        )
    definition_yml = yaml.safe_load(definition_string)
    if "datasets" in definition_yml:
        return IngestDefinitionOneToMany(**definition_yml)
    else:
        return IngestDefinitionSimple(**definition_yml)


def get_version(source: Source) -> str | None:
    connector = source_connectors[source.type]
    if "get_latest_version" in connector.__dir__():
        return connector.get_latest_version(**source.model_dump())
    else:
        return None


def find_source_validation_errors(source: Source) -> dict:
    violations: dict = {}
    if source.type not in source_connectors.list_registered():
        violations["invalid source type"] = (
            f"Connector with id '{source.type}' not registered"
        )
    connector = source_connectors[source.type]
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


def find_definition_validation_errors(
    definition: IngestDefinitionSimple | IngestDefinitionOneToMany,
) -> dict:
    """Validate a single template object."""
    violations = {}

    source_violations = find_source_validation_errors(definition.source)
    if source_violations:
        violations["source"] = source_violations

    match definition:
        case IngestDefinitionSimple():
            invalid_processing_steps = find_processing_step_validation_errors(
                definition.id, definition.ingestion.processing_steps
            )
        case IngestDefinitionOneToMany():
            invalid_processing_steps = {}
            for d in definition.datasets:
                errors = find_processing_step_validation_errors(
                    d.id or "",  # todo
                    d.processing_steps,
                )
                if errors:
                    invalid_processing_steps[d.id or ""] = errors

    if invalid_processing_steps:
        violations["processing_steps"] = invalid_processing_steps

    return violations


def resolve_config(
    dataset_id: str,
    version: str | None = None,
    *,
    definition_dir: Path,
    local_file_path: Path | None = None,
) -> ResolvedDataSource:
    """Generate config object for dataset and optional version"""
    definition_filepath = definition_dir / f"{dataset_id}.yml"

    logger.info(f"Reading definition from {definition_filepath}")
    definition = read_definition(definition_filepath, version=version)
    version = version or get_version(definition.source)
    definition = read_definition(definition_filepath, version=version)

    violations = find_definition_validation_errors(definition)
    if violations:
        raise ValueError(f"definition violations found: {violations}")

    source = (
        Source(type="local_file", key=str(local_file_path))
        if local_file_path
        else definition.source
    )

    match definition:
        case IngestDefinitionSimple():
            args = definition.ingestion.model_dump()
            args.pop("source")
            downstream_datasets = [
                ResolvedDownstreamDataset(
                    id=definition.id,
                    acl=definition.acl,
                    attributes=definition.attributes,
                    **args,
                )
            ]
        case IngestDefinitionOneToMany():
            downstream_datasets = [
                ResolvedDownstreamDataset(
                    **deep_merge_dict(
                        definition.dataset_defaults.model_dump(),
                        d.model_dump(exclude_defaults=True, exclude_none=True),
                    )
                )
                for d in definition.datasets
            ]

        case _:
            raise Exception("Unknown definition format")

    return ResolvedDataSource(
        id=definition.id,
        acl=definition.acl,
        version=version,
        attributes=definition.attributes,
        source=source,
        datasets=downstream_datasets,
    )
