from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel
import yaml

from dcpy.connectors.edm import recipes
from dcpy.library.validator import DatasetDefinition
from . import TEMPLATE_DIR


class Template(
    BaseModel, use_enum_values=True, extra="forbid", arbitrary_types_allowed=True
):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    name: str
    acl: recipes.ValidAclValues

    ## these two fields might merge to "source" or something equivalent at some point
    ## for now, they are distinct so that they can be worked on separately
    ## when implemented, "None" should not be valid type
    archive_raw_metadata: ArchiveRawMetadata.WebEndpoint | ArchiveRawMetadata.Socrata | None
    transform_to_parquet_metadata: None

    ## this is the original library template, included just for reference while we build out our new templates
    library_dataset: DatasetDefinition

    class ArchiveRawMetadata:
        # might need to differentiate between file download endpoint and something that returns just json response
        class WebEndpoint(BaseModel):
            url: str

        class Socrata(BaseModel):
            uid: str
            file_format: str


class Import(BaseModel, use_enum_values=True, extra="forbid"):
    template: Template
    version: str
    timestamp: datetime


def read_template(dataset: str) -> Template:
    file = TEMPLATE_DIR / f"{dataset}.yml"
    with open(file, "r") as f:
        s = yaml.safe_load(f)
    return Template(**s)


def get_import_config(dataset: str, version: str | None = None) -> Import:
    raise NotImplemented
