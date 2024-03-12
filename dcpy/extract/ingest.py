from datetime import datetime
import importlib
import os
from pathlib import Path
import requests
from urllib.parse import urlparse

from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing
from . import TMP_DIR, PARQUET_PATH, metadata, utils


def _download_file_web(url: str, file_name: str, dir: Path) -> Path:
    """Simple wrapper to download a file using requests.get.
    Returns filepath."""
    file = dir / file_name
    logger.info(f"downloading {url} to {file}")
    response = requests.get(url)
    response.raise_for_status()
    with open(file, "wb") as f:
        f.write(response.content)
    return file


def download_file_from_source(
    template: metadata.Template, version: str, dir=TMP_DIR
) -> Path:
    """From parsed config template and version, download raw data from source
    and return path of saved local file."""
    match template.source:
        ## Non reqeust-based methods
        case recipes.ExtractConfig.Source.LocalFile() as local_file:
            return local_file.path
        case recipes.ExtractConfig.Source.EdmPublishingGisDataset() as dataset:
            return publishing.download_gis_dataset(dataset.name, version, dir)
        case recipes.ExtractConfig.Source.Script() as s:
            module = importlib.import_module(
                f"{s.script_module or 'dcpy.extract.ingest_scripts'}.{s.script_name or template.name}"
            )
            logger.info(f"Running custom ingestion script {template.name}.py")
            return module.runner(dir)

        ## request-based methods
        case recipes.ExtractConfig.Source.FileDownload() as file_download:
            return _download_file_web(
                file_download.url,
                os.path.basename(urlparse(file_download.url).path),
                dir,
            )
        case recipes.ExtractConfig.Source.Api() as api:
            return _download_file_web(
                api.endpoint, f"{template.name}.{api.format}", dir
            )
        case recipes.ExtractConfig.Source.Socrata() as socrata:
            return _download_file_web(
                utils.Socrata.get_url(socrata),
                f"{template.name}.{socrata.extension}",
                dir,
            )


def extract_and_archive_raw_dataset(dataset: str, version: str | None):
    """From dataset name and optional version,
    1. parse template
    2. determine version from source or set it as today's date if missing
    3. download raw file from source
    4. generate recipes config object
    5. archive raw dataset with config
    Returns config object."""
    # gather metadata
    timestamp = datetime.now()
    template = metadata.read_template(dataset, version=version)
    version = version or metadata.get_version(template, timestamp)

    # get file
    file = download_file_from_source(template, version)

    # create "final" config file
    config = metadata.get_config(template, version, timestamp, file.name)
    # archive to edm-recipes/raw_datasets
    recipes.archive_raw_dataset(config, file)

    return config


def transform_to_parquet(config: recipes.ExtractConfig):
    """Given config of archived raw dataset, transform it to parquet"""
    ### idea is that this will dump to PARQUET_PATH - other functions will assume parquet file is there as well
    raise NotImplemented


def validate_dataset(config: recipes.ExtractConfig):
    """Given config of imported dataset, validate data expectations/contract"""
    raise NotImplemented
