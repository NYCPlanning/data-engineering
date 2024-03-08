from __future__ import annotations
import jinja2
from datetime import datetime
from pydantic import BaseModel
import yaml

from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing
from dcpy.library.validator import LibraryConfig
from . import TEMPLATE_DIR, utils


class Template(
    BaseModel, use_enum_values=True, extra="forbid", arbitrary_types_allowed=True
):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    name: str
    acl: recipes.ValidAclValues

    ## these two fields might merge to "source" or something equivalent at some point
    ## for now, they are distinct so that they can be worked on separately
    ## when implemented, "None" should not be valid type
    source: recipes.ExtractConfig.Source.Options
    transform_to_parquet_metadata: None

    ## this is the original library template, included just for reference while we build out our new templates
    library_dataset: LibraryConfig | None = None


def read_template(dataset: str, **kwargs) -> Template:
    file = TEMPLATE_DIR / f"{dataset}.yml"
    logger.info(f"Reading template from {file}")
    with open(file, "r") as f:
        template_string = f.read()
    template_string = jinja2.Template(template_string).render(**kwargs)
    template_yml = yaml.safe_load(template_string)
    return Template(**template_yml)


def get_version(template: Template, timestamp: datetime) -> str:
    match template.source:
        case recipes.ExtractConfig.Source.Socrata() as socrata:
            return utils.Socrata.get_version(socrata)
        case recipes.ExtractConfig.Source.EdmPublishingGisDataset() as gis_dataset:
            return publishing.get_latest_gis_dataset_version(gis_dataset.name)
        case _:
            return timestamp.strftime("%Y%m%d")


def get_config(
    template: Template, version: str, timestamp: datetime, file_name: str
) -> recipes.ExtractConfig:
    return recipes.ExtractConfig(
        name=template.name,
        version=version,
        archival_timestamp=timestamp,
        acl=template.acl,
        raw_filename=file_name,
        source=template.source,
    )
