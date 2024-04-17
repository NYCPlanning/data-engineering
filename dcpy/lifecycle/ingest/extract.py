from datetime import datetime
import importlib
from pandas import DataFrame

from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    ScriptSource,
    Source,
    Config,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors import web
from . import TMP_DIR, configure


def download_file_from_source(
    source: Source, filename: str, version: str, dir=TMP_DIR
) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    match source:
        ## Non reqeust-based methods
        case LocalFileSource():
            pass
        case GisDataset():
            publishing.download_gis_dataset(source.name, version, dir)
        case ScriptSource():
            module = importlib.import_module(
                f"dcpy.connectors.{source.connector}.{source.function}"
            )
            logger.info(f"Running custom ingestion script {source.function}.py")
            df = module.extract()
            assert isinstance(df, DataFrame)
            df.to_parquet(path)

        ## request-based methods
        case web_models.FileDownloadSource():
            web.download_file(source.url, path)
        case web_models.GenericApiSource():
            web.download_file(source.endpoint, path)
        case socrata.Source():
            extract_socrata.download(source, path)
        case _:
            raise NotImplementedError(
                f"Source type {source.type} not supported for download_file_from_source"
            )


def extract_and_archive_raw_dataset(dataset: str, version: str | None) -> Config:
    """
    From dataset name and optional version,
    1. parse template
    2. fetch or generate necessary metadata for configuration
    3. generate config object
    4. download dataset from source
    5. archive raw dataset with config
    Returns config object.
    """
    # generate/fetch metadata for configuration
    timestamp = datetime.now()
    template = configure.read_template(dataset, version=version)
    filename = configure.get_filename(template.source, template.name)
    version = version or configure.get_version(template.source, timestamp)

    # create config object
    config = configure.get_config(template, version, timestamp, filename)

    # download dataset
    download_file_from_source(template.source, filename, version)

    # archive to edm-recipes/raw_datasets
    recipes.archive_raw_dataset(config, TMP_DIR / filename)

    return config
