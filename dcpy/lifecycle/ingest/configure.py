from datetime import datetime
import jinja2
from jinja2 import meta
import os
from pathlib import Path
from urllib.parse import urlparse
import yaml

from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    S3Source,
    ScriptSource,
    Source,
    PreprocessingStep,
    Template,
    Config,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.utils import metadata
from dcpy.utils.logging import logger
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors.edm import publishing

TEMPLATE_DIR = Path(__file__).parent / "templates"


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja template string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_template(
    dataset_id: str, version: str | None = None, template_dir: Path = TEMPLATE_DIR
) -> Template:
    """
    Given _id id, read yml template in template_dir of given dataset
    and insert version as jinja var if provided.
    """
    file = template_dir / f"{dataset_id}.yml"
    logger.info(f"Reading template from {file}")
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


def get_version(source: Source, timestamp: datetime | None = None) -> str:
    """
    Given parsed dataset template, determine version.
    If version's source has no custom logic, returns formatted date
    from provided datetime
    """
    match source:
        case socrata.Source():
            return extract_socrata.get_version(source)
        case GisDataset():
            return publishing.get_latest_gis_dataset_version(source.name)
        case _:
            if timestamp is None:
                raise TypeError(
                    f"Version cannot be dynamically determined for source of type {source.type}"
                )
            return timestamp.strftime("%Y%m%d")


def get_filename(source: Source, ds_id: str) -> str:
    """From parsed config template, determine filename"""
    match source:
        case LocalFileSource():
            return source.path.name
        case GisDataset():
            return f"{source.name}.zip"
        case ScriptSource():
            return f"{ds_id}.parquet"
        case web_models.FileDownloadSource():
            return os.path.basename(urlparse(source.url).path)
        case web_models.GenericApiSource():
            return f"{ds_id}.{source.format}"
        case socrata.Source():
            return f"{ds_id}.{source.extension}"
        case S3Source():
            return Path(source.key).name
        case _:
            raise NotImplementedError(
                f"Source type {source} not supported for get_filename"
            )


def get_config(
    dataset_id: str, version: str | None = None, mode: str | None = None
) -> Config:
    """Generate config object for dataset and optional version"""
    run_details = metadata.get_run_details()
    template = read_template(dataset_id, version=version)
    filename = get_filename(template.source, template.id)
    version = version or get_version(template.source, run_details.timestamp)
    template = read_template(dataset_id, version=version)
    processing_steps = template.processing_steps

    if template.target_crs:
        reprojection = PreprocessingStep(
            name="reproject", args={"target_crs": template.target_crs}
        )
        processing_steps = [reprojection] + processing_steps

    # TODO default steps like this should probably be configuration
    processing_step_names = {p.name for p in processing_steps}
    if "clean_column_names" not in processing_step_names:
        clean_column_names = PreprocessingStep(
            name="clean_column_names", args={"replace": {" ": "_"}, "lower": True}
        )
        processing_steps.append(clean_column_names)

    if mode:
        modes = {s.mode for s in processing_steps}
        if mode not in modes:
            raise ValueError(f"mode '{mode}' is not present in template '{dataset_id}'")

    processing_steps = [s for s in processing_steps if s.mode is None or s.mode == mode]

    # create config object
    return Config(
        id=template.id,
        version=version,
        attributes=template.attributes,
        archival_timestamp=run_details.timestamp,
        raw_filename=filename,
        acl=template.acl,
        target_crs=template.target_crs,
        source=template.source,
        file_format=template.file_format,
        processing_mode=mode,
        processing_steps=processing_steps,
        run_details=run_details,
    )
