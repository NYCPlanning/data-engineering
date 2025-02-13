from datetime import datetime
import jinja2
from jinja2 import meta
import os
from pathlib import Path
from urllib.parse import urlparse
import yaml

from dcpy.models.lifecycle.ingest import (
    ArchivalMetadata,
    Ingestion,
    LocalFileSource,
    S3Source,
    ScriptSource,
    DEPublished,
    ESRIFeatureServer,
    Source,
    ProcessingStep,
    Template,
    Config,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.utils import metadata
from dcpy.utils.logging import logger
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors.esri import arcgis_feature_service
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
        case DEPublished():
            version = publishing.get_latest_version(source.product)
            if not version:
                raise FileNotFoundError(
                    "Unable to determine latest version. If archiving known version, please provide it."
                )
            return version
        case ESRIFeatureServer():
            return arcgis_feature_service.get_data_last_updated(
                source.feature_server_layer
            ).strftime("%Y%m%d")
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
        case DEPublished():
            return source.filename
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
        case ESRIFeatureServer():
            return f"{ds_id}.json"
        case _:
            raise NotImplementedError(
                f"Source type {source} not supported for get_filename"
            )


def determine_processing_steps(
    steps: list[ProcessingStep],
    *,
    target_crs: str | None,
    has_geom: bool,
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
    template_dir: Path = TEMPLATE_DIR,
    local_file_path: Path | None = None,
) -> Config:
    """Generate config object for dataset and optional version"""
    run_details = metadata.get_run_details()
    template = read_template(dataset_id, version=version, template_dir=template_dir)

    filename = get_filename(template.ingestion.source, template.id)
    version = version or get_version(template.ingestion.source, run_details.timestamp)
    template = read_template(dataset_id, version=version, template_dir=template_dir)

    if local_file_path:
        template.ingestion.source = LocalFileSource(
            type="local_file", path=local_file_path
        )

    processing_steps = determine_processing_steps(
        template.ingestion.processing_steps,
        target_crs=template.ingestion.target_crs,
        has_geom=template.has_geom,
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
        raw_filename=filename,
        acl=template.acl,
    )

    # create config object
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
