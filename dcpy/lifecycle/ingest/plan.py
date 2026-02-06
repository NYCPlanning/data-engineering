import jinja2
from jinja2 import meta
from pathlib import Path
import yaml

from dcpy.models.lifecycle.ingest import (
    Source,
    IngestDefinition,
    DatasetDefinition,
    DataSourceDefinition,
    ResolvedDownstreamDataset,
    ResolvedDataSource,
)
from dcpy.utils.logging import logger
from dcpy.utils.collections import deep_merge_dict
from dcpy.lifecycle.ingest.connectors import source_connectors


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja definition string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_definition_file(
    filepath: Path, version: str | None = None
) -> DatasetDefinition | DataSourceDefinition:
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
        raise ValueError(
            f"'version' is only suppored jinja var. Vars in definition: {vars}"
        )
    definition_yml = yaml.safe_load(definition_string)
    return IngestDefinition.validate_python(definition_yml)


def get_version(source: Source) -> str | None:
    connector = source_connectors[source.type]
    return (
        connector.get_latest_version(**source.model_dump())
        if "get_latest_version" in connector.__dir__()
        else None
    )


def resolve_definition(
    dataset_id: str,
    version: str | None = None,
    *,
    definition_dir: Path,
    local_file_path: Path | None = None,
) -> ResolvedDataSource:
    """Generate ResolvedDataSource object based on definition file"""
    definition_filepath = definition_dir / f"{dataset_id}.yml"

    logger.info(f"Reading definition from {definition_filepath}")
    definition = read_definition_file(definition_filepath, version=version)

    source = (
        Source(type="local_file", key=str(local_file_path))
        if local_file_path
        else definition.source
    )
    version = version or get_version(source)
    definition = read_definition_file(definition_filepath, version=version)

    match definition:
        case DatasetDefinition():
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
        case DataSourceDefinition():
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
