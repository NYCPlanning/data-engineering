from datetime import datetime
import importlib
import os

from pathlib import Path
from urllib.parse import urlparse
from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    ScriptSource,
    Template,
    Config,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors import web
from . import TMP_DIR, configure


def download_file_from_source(template: Template, version: str, dir=TMP_DIR) -> Path:
    """From parsed config template and version, download raw data from source
    and return path of saved local file."""
    match template.source:
        ## Non reqeust-based methods
        case LocalFileSource():
            return template.source.path
        case GisDataset():
            return publishing.download_gis_dataset(template.source.name, version, dir)
        case ScriptSource():
            module = importlib.import_module(
                f"dcpy.connectors.{template.source.connector}.{template.source.function}"
            )
            logger.info(f"Running custom ingestion script {template.name}.py")
            return module.runner(dir)

        ## request-based methods
        case web_models.FileDownloadSource():
            return web.download_file(
                template.source.url,
                os.path.basename(urlparse(template.source.url).path),
                dir,
            )
        case web_models.GenericApiSource():
            return web.download_file(
                template.source.endpoint,
                f"{template.name}.{template.source.format}",
                dir,
            )
        case socrata.Source():
            return web.download_file(
                extract_socrata.get_download_url(template.source),
                f"{template.name}.{template.source.extension}",
                dir,
            )


def extract_and_archive_raw_dataset(dataset: str, version: str | None) -> Config:
    """From dataset name and optional version,
    1. parse template
    2. determine version from source or set it as today's date if missing
    3. download raw file from source
    4. generate recipes config object
    5. archive raw dataset with config
    Returns config object."""
    # gather metadata
    timestamp = datetime.now()
    template = configure.read_template(dataset, version=version)
    version = version or configure.get_version(template, timestamp)

    # get file
    file = download_file_from_source(template, version)

    # create "final" config file
    config = configure.get_config(template, version, timestamp, file.name)
    # archive to edm-recipes/raw_datasets
    recipes.archive_raw_dataset(config, file)

    return config
