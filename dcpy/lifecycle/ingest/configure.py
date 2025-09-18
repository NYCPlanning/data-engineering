import jinja2
from jinja2 import meta
from pathlib import Path
import yaml

from dcpy.models.lifecycle.ingest.definitions import (
    Source,
    DatasetDefinitionSimple,
    DatasetDefinitionOneToMany,
)
from dcpy.models.lifecycle.ingest.configuration import (
    DatasetTransformation,
    ResolvedConfig,
)
from dcpy.utils.logging import logger
from dcpy.utils.collections import deep_merge_dict
from dcpy.lifecycle.ingest.connectors import source_connectors
# from dcpy.lifecycle.ingest.validate import find_definition_validation_errors


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja definition string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_definition(
    filepath: Path, version: str | None = None
) -> DatasetDefinitionSimple | DatasetDefinitionOneToMany:
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
        return DatasetDefinitionOneToMany(**definition_yml)
    else:
        return DatasetDefinitionSimple(**definition_yml)


def get_version(source: Source) -> str | None:
    connector = source_connectors[source.type]
    if "get_latest_version" in connector.__dir__():
        return connector.get_latest_version(**source.model_dump())
    else:
        return None


def resolve_config(
    dataset_id: str,
    version: str | None = None,
    *,
    definition_dir: Path,
    local_file_path: Path | None = None,
) -> ResolvedConfig:
    """Generate config object for dataset and optional version"""
    definition_filepath = definition_dir / f"{dataset_id}.yml"

    logger.info(f"Reading definition from {definition_filepath}")
    definition = read_definition(definition_filepath, version=version)
    version = version or get_version(definition.source)
    definition = read_definition(definition_filepath, version=version)

    # violations = find_definition_validation_errors(definition)
    # if violations:
    #    raise ValueError(f"definition violations found: {violations}")

    source = (
        Source(type="local_file", key=str(local_file_path))
        if local_file_path
        else definition.source
    )

    match definition:
        case DatasetDefinitionSimple():
            args = definition.ingestion.model_dump()
            args.pop("source")
            transformations = [
                DatasetTransformation(
                    id=definition.id,
                    acl=definition.acl,
                    attributes=definition.attributes,
                    **args,
                )
            ]

        case DatasetDefinitionOneToMany():
            transformations = [
                DatasetTransformation(
                    **deep_merge_dict(
                        definition.dataset_defaults.model_dump(), d.model_dump()
                    )
                )
                for d in definition.datasets
            ]

        case _:
            raise Exception("Unknown definition format")

    return ResolvedConfig(
        id=definition.id,
        acl=definition.acl,
        version=version,
        attributes=definition.attributes,
        source=source,
        transformation=transformations,
    )
