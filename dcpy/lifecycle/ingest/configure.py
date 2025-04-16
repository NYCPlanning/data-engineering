from datetime import datetime
import jinja2
from jinja2 import meta
from pathlib import Path
import yaml

from dcpy.models.lifecycle.ingest import (
    ArchivalMetadata,
    Ingestion,
    LocalFileSource,
    Source,
    ProcessingStep,
    Template,
    Config,
)
from dcpy.utils import metadata
from dcpy.utils.logging import logger
from dcpy.lifecycle.ingest.connectors import source_connectors


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja template string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_template(
    dataset_id: str, template_dir: Path, version: str | None = None
) -> Template:
    """
    Given _id id, read yml template in template_dir of given dataset
    and insert version as jinja var if provided.
    """
    file = template_dir / f"{dataset_id}.yml"
    with open(file, "r") as f:
        template_string = f.read()
    vars = get_jinja_vars(template_string)
    if vars == {"version"}:
        template_string = jinja2.Template(template_string).render(version=version)
    elif vars:
        raise Exception(
            f"'version' is only suppored jinja var. Vars in template: {vars}"
        )
    template_yml = yaml.safe_load(template_string)
    return Template(**template_yml)


def get_version(source: Source, timestamp: datetime):
    connector = source_connectors[source.type]
    version = connector.get_latest_version(source.get_key(), **source.model_dump())
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
            raise ValueError(f"mode '{mode}' is not present in template")

    steps = [s for s in steps if s.mode is None or s.mode == mode]

    return steps


def get_config(
    dataset_id: str,
    version: str | None = None,
    *,
    mode: str | None = None,
    template_dir: Path,
    local_file_path: Path | None = None,
) -> Config:
    """Generate config object for dataset and optional version"""
    run_details = metadata.get_run_details()

    logger.info(f"Reading template from {template_dir / dataset_id}.yml")
    template = read_template(dataset_id, version=version, template_dir=template_dir)
    version = version or get_version(template.ingestion.source, run_details.timestamp)
    template = read_template(dataset_id, version=version, template_dir=template_dir)

    if local_file_path:
        template.ingestion.source = LocalFileSource(
            type="local_file", path=local_file_path
        )

    processing_steps = determine_processing_steps(
        template.ingestion.processing_steps,
        target_crs=template.ingestion.target_crs,
        mode=mode,
    )

    ingestion = Ingestion(
        target_crs=template.ingestion.target_crs,
        source=template.ingestion.source,
        file_format=template.ingestion.file_format,
        processing_mode=mode,
        processing_steps=processing_steps,
    )

    archival = ArchivalMetadata(
        archival_timestamp=run_details.timestamp,
        raw_filename="",  # TODO - this is now set after pulling file
        acl=template.acl,
    )

    return Config(
        id=template.id,
        version=version,
        crs=ingestion.target_crs,
        attributes=template.attributes,
        archival=archival,
        ingestion=ingestion,
        columns=template.columns,
        run_details=run_details,
    )
