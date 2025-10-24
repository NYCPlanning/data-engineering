from datetime import datetime
import jinja2
from jinja2 import meta
from pathlib import Path
import yaml

from dcpy.models.lifecycle.ingest import (
    ArchivalMetadata,
    Ingestion,
    Source,
    ProcessingStep,
    Definition,
    Config,
)
from dcpy.utils import metadata
from dcpy.utils.logging import logger
from dcpy.lifecycle.ingest.connectors import source_connectors
from dcpy.lifecycle.ingest.validate import find_definition_validation_errors


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja definition string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_definition(
    dataset_id: str, definition_dir: Path, version: str | None = None
) -> Definition:
    """
    Given _id id, read yml definition in definition_dir of given dataset
    and insert version as jinja var if provided.
    """
    file = definition_dir / f"{dataset_id}.yml"
    with open(file, "r") as f:
        definition_string = f.read()
    vars = get_jinja_vars(definition_string)
    if vars == {"version"}:
        definition_string = jinja2.Template(definition_string).render(version=version)
    elif vars:
        raise Exception(
            f"'version' is only suppored jinja var. Vars in definition: {vars}"
        )
    definition_yml = yaml.safe_load(definition_string)
    return Definition(**definition_yml)


def get_version(source: Source, timestamp: datetime):
    connector = source_connectors[source.type]
    version = connector.get_latest_version(**source.model_dump())
    return version or timestamp.strftime("%Y%m%d")


def determine_processing_steps(
    steps: list[ProcessingStep],
    *,
    target_crs: str | None,
    mode: str | None,
) -> list[ProcessingStep]:
    # TODO default steps like this should probably be configuration
    step_names = {p.name for p in steps}

    if target_crs and "reproject" not in step_names:
        reprojection = ProcessingStep(name="reproject", args={"target_crs": target_crs})
        steps = [reprojection] + steps

    if mode:
        modes = {s.mode for s in steps}
        if mode not in modes:
            raise ValueError(f"mode '{mode}' is not present in definition")

    steps = [s for s in steps if s.mode is None or s.mode == mode]

    return steps


def get_config(
    dataset_id: str,
    version: str | None = None,
    *,
    mode: str | None = None,
    definition_dir: Path,
    local_file_path: Path | None = None,
) -> Config:
    """Generate config object for dataset and optional version"""
    run_details = metadata.get_run_details()

    logger.info(f"Reading definition from {definition_dir / dataset_id}.yml")
    definition = read_definition(
        dataset_id, version=version, definition_dir=definition_dir
    )
    version = version or get_version(definition.ingestion.source, run_details.timestamp)
    definition = read_definition(
        dataset_id, version=version, definition_dir=definition_dir
    )

    violations = find_definition_validation_errors(definition)
    if violations:
        raise ValueError(f"Template violations found: {violations}")

    if local_file_path:
        definition.ingestion.source = Source(
            type="local_file", key=str(local_file_path)
        )

    processing_steps = determine_processing_steps(
        definition.ingestion.processing_steps,
        target_crs=definition.ingestion.target_crs,
        mode=mode,
    )

    ingestion = Ingestion(
        target_crs=definition.ingestion.target_crs,
        source=definition.ingestion.source,
        file_format=definition.ingestion.file_format,
        processing_mode=mode,
        processing_steps=processing_steps,
    )

    archival = ArchivalMetadata(
        archival_timestamp=run_details.timestamp,
        raw_filename="",  # TODO - this is now set after pulling file
        acl=definition.acl,
    )

    return Config(
        id=definition.id,
        version=version,
        crs=ingestion.target_crs,
        attributes=definition.attributes,
        archival=archival,
        ingestion=ingestion,
        columns=definition.columns,
        run_details=run_details,
    )
