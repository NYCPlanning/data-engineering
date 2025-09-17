import jinja2
from jinja2 import meta
from pathlib import Path
import yaml

from dcpy.models.lifecycle.ingest.definitions import (
    Source,
    DatasetDefinitionSimple,
    DatasetDefinitionOneToMany,
    DatasetTransformation,
)
from dcpy.models.lifecycle.ingest.configuration import ResolvedConfig
from dcpy.utils.logging import logger
from dcpy.lifecycle.ingest.connectors import source_connectors
# from dcpy.lifecycle.ingest.validate import find_template_validation_errors


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja template string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_definition(
    filepath: Path, version: str | None = None
) -> DatasetDefinitionSimple | DatasetDefinitionOneToMany:
    """
    Given _id id, read yml template in template_dir of given dataset
    and insert version as jinja var if provided.
    """
    with open(filepath, "r") as f:
        template_string = f.read()
    vars = get_jinja_vars(template_string)
    if vars == {"version"}:
        template_string = jinja2.Template(template_string).render(version=version)
    elif vars:
        raise Exception(
            f"'version' is only suppored jinja var. Vars in template: {vars}"
        )
    template_yml = yaml.safe_load(template_string)
    if "datasets" in template_yml:
        return DatasetDefinitionOneToMany(**template_yml)
    else:
        return DatasetDefinitionSimple(**template_yml)


def get_version(source: Source) -> str | None:
    connector = source_connectors[source.type]
    if "get_latest_version" in connector.__dir__():
        return connector.get_latest_version(**source.model_dump())
    else:
        return None


def get_resolved_config(
    dataset_id: str,
    version: str | None = None,
    *,
    template_dir: Path,
    local_file_path: Path | None = None,
) -> ResolvedConfig:
    """Generate config object for dataset and optional version"""
    definition_filepath = template_dir / f"{dataset_id}.yml"

    logger.info(f"Reading template from {definition_filepath}")
    template = read_definition(definition_filepath, version=version)
    version = version or get_version(template.source)
    template = read_definition(definition_filepath, version=version)

    # violations = find_template_validation_errors(template)
    # if violations:
    #    raise ValueError(f"Template violations found: {violations}")

    source = (
        Source(type="local_file", key=str(local_file_path))
        if local_file_path
        else template.source
    )

    match template:
        case DatasetDefinitionSimple():
            args = template.ingestion.model_dump()
            args.pop("source")
            transformations = [
                DatasetTransformation(
                    id=template.id,
                    acl=template.acl,
                    attributes=template.attributes,
                    **args,
                )
            ]

        case DatasetDefinitionOneToMany():
            transformations = template.datasets

        case _:
            raise Exception("Unknown definition format")

    return ResolvedConfig(
        id=template.id,
        acl=template.acl,
        attributes=template.attributes,
        source=source,
        transformation=transformations,
    )
