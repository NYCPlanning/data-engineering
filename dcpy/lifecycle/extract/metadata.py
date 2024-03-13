from __future__ import annotations
import jinja2
from jinja2 import meta
from datetime import datetime
from pathlib import Path
import yaml

from dcpy.models.lifecycle.extract import Template, Config

from dcpy.utils.logging import logger
from dcpy.models.connectors import socrata
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors.edm import publishing
from . import TEMPLATE_DIR


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja template string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_template(
    dataset: str, version: str | None = None, template_dir: Path = TEMPLATE_DIR
) -> Template:
    """Given dataset name, read yml template in template_dir of given dataset
    and insert version as jinja var if provided."""
    file = template_dir / f"{dataset}.yml"
    logger.info(f"Reading template from {file}")
    with open(file, "r") as f:
        template_string = f.read()
    vars = get_jinja_vars(template_string)
    if not version and len(vars) > 0:
        if vars == {"version"}:
            raise Exception(
                "Version must be supplied explicitly to be rendered in template"
            )
        else:
            raise Exception(f"Unsupported jinja vars found in template: {vars}")
    else:
        if len(vars) > 0 and vars != {"version"}:
            vars.discard("version")
            raise Exception(
                f"'version' is only suppored jinja var. Unsupported vars in template: {vars}"
            )
        template_string = jinja2.Template(template_string).render(version=version)
    template_yml = yaml.safe_load(template_string)
    return Template(**template_yml)


def get_version(template: Template, timestamp: datetime) -> str:
    """Given parsed dataset template, determine version.
    If version's source has no custom logic, returns formatted date
    from provided datetime"""
    match template.source:
        case socrata.Source() as socrata_source:
            return extract_socrata.get_version(socrata_source)
        case publishing.GisDataset() as gis_dataset:
            return publishing.get_latest_gis_dataset_version(gis_dataset.name)
        case _:
            return timestamp.strftime("%Y%m%d")


def get_config(
    template: Template, version: str, timestamp: datetime, file_name: str
) -> Config:
    """Simple wrapper to produce a recipes ExtractConfig from a parsed template
    and other computed values"""
    return Config(
        name=template.name,
        version=version,
        archival_timestamp=timestamp,
        acl=template.acl,
        raw_filename=file_name,
        source=template.source,
        transform_to_parquet_metadata=template.transform_to_parquet_metadata,
    )
