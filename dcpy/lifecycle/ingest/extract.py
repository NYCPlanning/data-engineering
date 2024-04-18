import importlib
from pandas import DataFrame
import shutil

from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    ScriptSource,
    Source,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.utils.logging import logger
from dcpy.connectors.edm import publishing
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors import web
from . import TMP_DIR


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
            shutil.copy(source.path, dir)
        case GisDataset():
            publishing.download_gis_dataset(source.name, version, dir)
        case ScriptSource():
            module = importlib.import_module(
                f"dcpy.connectors.{source.connector}.{source.function}"
            )
            logger.info(f"Running custom ingestion script {source.function}.py")
            df: DataFrame = module.extract()
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
